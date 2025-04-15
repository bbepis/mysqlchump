using MySqlConnector;
using Sylvan.Data.Csv;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Pipelines;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace mysqlchump.Import;

internal class CsvImporter : BaseImporter
{
	protected StreamReader StreamReader { get; set; }
	protected CsvDataReader CsvReader { get; set; }

	protected Type[] ColumnTypes { get; set; }

	public CsvImporter(ImportOptions options, Stream dataStream) : base(options, dataStream) { }

	protected override async Task<(bool foundAnotherTable, string createTableSql, ulong? approxRows)> ReadToNextTable()
	{
		StreamReader = new StreamReader(DataStream, Utility.NoBomUtf8);
		CsvReader = await CsvDataReader.CreateAsync(ImportOptions.CsvFixInvalid ? new MysqlInvalidCsvReader(StreamReader) : StreamReader,
			new CsvDataReaderOptions { BufferSize = 128000, HasHeaders = ImportOptions.CsvUseHeaderRow, ResultSetMode = ResultSetMode.MultiResult });

		return (true, null, null);
	}

	protected override async Task<(bool shouldInsert, string tableName, ColumnInfo[] columns)> ProcessTableCreation(Func<MySqlConnection> createConnection, string createTableSql)
	{
		string[] columns;

		if (ImportOptions.CsvUseHeaderRow)
		{
			columns = new string[CsvReader.FieldCount];

			for (int i = 0; i < columns.Length; i++)
				columns[i] = CsvReader.GetName(i);
		}
		else
		{
			columns = ImportOptions.CsvManualColumns;
		}

		ColumnTypes = new Type[columns.Length];
		var sqlColumnTypes = new string[columns.Length];

		await using (var connection = createConnection())
		{
			await using var readCommand = connection.CreateCommand();
			readCommand.CommandText = $"SELECT * FROM `{ImportOptions.TargetTable}` LIMIT 0";
			await using var sqlReader = await readCommand.ExecuteReaderAsync();

			var schema = await sqlReader.GetColumnSchemaAsync();

			for (var i = 0; i < columns.Length; i++)
			{
				var columnName = columns[i];

				var dbColumn = schema.FirstOrDefault(x => x.ColumnName.Equals(columnName, StringComparison.OrdinalIgnoreCase));

				if (dbColumn == null)
					throw new Exception($"Seemingly invalid column list; cannot find column '{columnName}'.{(ImportOptions.CsvUseHeaderRow ? "" : " Maybe try specifying column list manually?")}");

				ColumnTypes[i] = dbColumn.DataType;
				sqlColumnTypes[i] = dbColumn.DataTypeName;
			}
		}

		return (true, ImportOptions.TargetTable, columns.Select((x, i) => new ColumnInfo(x, sqlColumnTypes[i])).ToArray());
	}

	private StringBuilder queryBuilder = new StringBuilder(1_000_000);

	protected override (int rows, bool canContinue, string sqlCommand) ReadDataSql(string tableName, ColumnInfo[] columns)
	{
		const int insertLimit = 4000;
		int count = 0;

		queryBuilder.Clear();
		queryBuilder.Append($"INSERT{(ImportOptions.InsertIgnore ? " IGNORE" : "")} INTO `{tableName}` ({string.Join(',', columns.Select(x => x.name))}) VALUES ");

		bool needsComma = false;
		bool canReadMore = true;

		for (; count < insertLimit; count++)
		{
			if (!CsvReader.Read())
			{
				canReadMore = false;
				break;
			}

			queryBuilder.Append(needsComma ? ",(" : "(");
			needsComma = true;

			for (int i = 0; i < ColumnTypes.Length; i++)
			{
				if (i > 0)
					queryBuilder.Append(",");

				var rawValue = CsvReader.GetString(i);

				if (MemoryExtensions.Equals(rawValue, "\\N", StringComparison.Ordinal))
				{
					queryBuilder.Append("NULL");
				}
				else if (ColumnTypes[i] == typeof(string) || ColumnTypes[i] == typeof(DateTime))
				{
					queryBuilder.Append('\'');
					queryBuilder.Append(rawValue.Replace("'", "''"));
					queryBuilder.Append('\'');
				}
				else
				{
					queryBuilder.Append(rawValue);
				}
			}

			queryBuilder.Append(")");
		}

		if (count == 0)
			return (0, false, null);

		queryBuilder.Append(";");

		return (count, canReadMore, queryBuilder.ToString());
	}
	
	protected override (int rows, bool canContinue) ReadDataCsv(PipeWriter pipeWriter, string tableName, ColumnInfo[] columns) => throw new NotImplementedException();

	public override void Dispose()
	{
		CsvReader?.DisposeAsync().AsTask().Wait();
		StreamReader?.Dispose();
		DataStream.Dispose();
	}
}

internal class MysqlInvalidCsvReader : TextReader
{
	private TextReader baseReader;

	private char[] underlyingBuffer = new char[4096];
	private int bufferRemaining;

	public MysqlInvalidCsvReader(TextReader baseReader)
	{
		this.baseReader = baseReader;
	}

	public override int Read()
	{
		throw new NotImplementedException();
	}

	public override int Read(char[] buffer, int index, int count) => Read(new Span<char>(buffer, index, count));

	public override int Read(Span<char> buffer)
	{
		if (buffer.Length == 0)
			return 0;

		while (bufferRemaining < buffer.Length && bufferRemaining < underlyingBuffer.Length)
		{
			var underlyingRead = baseReader.Read(underlyingBuffer.AsSpan(bufferRemaining));

			if (underlyingRead == 0)
				break;

			bufferRemaining += underlyingRead;
		}

		if (bufferRemaining == 0)
			return 0;

		// calculate safe cutoff

		int usableLength = Math.Min(bufferRemaining, buffer.Length);

		while (underlyingBuffer[usableLength - 1] == '\\')
			usableLength -= 1;
		
		// do scan and transformations
		
		foreach (var index in Locate(underlyingBuffer.AsSpan(0, usableLength)))
		{
			int backslashCount = 1;
			
			for (int innerIndex = index - 1; innerIndex >= 0; innerIndex--)
			{
				if (underlyingBuffer[innerIndex] != '\\')
					break;

				backslashCount++;
			}

			if (backslashCount % 2 == 1)
				underlyingBuffer[index] = '"';
		}

		// copy fixed buffer over safely

		underlyingBuffer.AsSpan(0, usableLength).CopyTo(buffer);

		// move over any leftover data

		if (bufferRemaining - usableLength == 0)
		{
			bufferRemaining = 0;
		}
		else
		{
			underlyingBuffer.AsSpan(usableLength, bufferRemaining - usableLength).CopyTo(underlyingBuffer);
			bufferRemaining -= usableLength;
		}

		return usableLength;
	}

	// extremely optimized version from https://stackoverflow.com/a/283648

	private static readonly List<int> emptyIndexes = new();

	private static List<int> Locate(ReadOnlySpan<char> self)
	{
		List<int> list = null;

		int actualLength = self.Length - 1;

		for (int i = 0; i < actualLength; i++)
		{
			if (self[i] != '\\')
				continue;

			if (self[++i] != '"')
				continue;

			list ??= new List<int>();
			list.Add(i - 1);
		}

		return list ?? emptyIndexes;
	}
}