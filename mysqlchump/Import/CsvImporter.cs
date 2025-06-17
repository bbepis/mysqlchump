using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Pipelines;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MySqlConnector;
using Sylvan.Data.Csv;

namespace mysqlchump.Import;

public class CsvImporter : BaseImporter
{
	public bool IsMysqlFormat { get; private set; }

	protected StreamReader StreamReader { get; set; }
	protected CsvDataReader CsvReader { get; set; }

	protected Type[] ColumnTypes { get; set; }

	public CsvImporter(ImportOptions options, Stream dataStream, bool isMysqlFormat) : base(options, dataStream)
	{
		IsMysqlFormat = isMysqlFormat;
	}

	protected bool HasReadFile = false;
	protected override async Task<(bool foundAnotherTable, string createTableSql, ulong? approxRows)> ReadToNextTable()
	{
		if (HasReadFile)
			return (false, null, null);

		StreamReader = new StreamReader(DataStream, Utility.NoBomUtf8);
		CsvReader = await CsvDataReader.CreateAsync(IsMysqlFormat ? new MysqlInvalidCsvReader(StreamReader) : StreamReader,
			new CsvDataReaderOptions { BufferSize = 128000, HasHeaders = ImportOptions.CsvUseHeaderRow, ResultSetMode = ResultSetMode.SingleResult, Delimiter = ',' });

		HasReadFile = true;

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

			if (columns.Length != CsvReader.FieldCount)
				throw new Exception($"Incorrect amount of columns specified; CSV file has {CsvReader.FieldCount} columns but {columns.Length} was specified");
		}

		ColumnTypes = new Type[columns.Length];
		var columnInfo = new ColumnInfo[columns.Length];

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

				var type = ColumnDataType.Default;

				if (dbColumn.DataTypeName.Contains("BINARY", StringComparison.OrdinalIgnoreCase)
				    || dbColumn.DataTypeName.Contains("BLOB", StringComparison.OrdinalIgnoreCase))
				{
					type = ColumnDataType.Binary;
				}
				else if (dbColumn.DataTypeName.Contains("DATE", StringComparison.OrdinalIgnoreCase))
				{
					type = ColumnDataType.Date;
				}

				columnInfo[i] = new ColumnInfo(columnName, type, dbColumn.DataTypeName);
			}
		}

		return (true, ImportOptions.TargetTable, columnInfo);
	}

	private StringBuilder queryBuilder = new StringBuilder(1_000_000);

	private byte[] b64Buffer = new byte[64];
	protected override (int rows, bool canContinue, string sqlCommand) ReadDataSql(string tableName, ColumnInfo[] columns)
	{
		int count = 0;

		queryBuilder.Clear();
		queryBuilder.Append($"INSERT{(ImportOptions.InsertIgnore ? " IGNORE" : "")} INTO `{tableName}` ({string.Join(',', columns.Select(x => x.name))}) VALUES ");

		bool needsComma = false;
		bool canReadMore = true;

		for (; count < ImportOptions.InsertBatchSize; count++)
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

				var cellValue = CsvReader.GetFieldSpan(i);

				if (cellValue.Equals("\\N", StringComparison.Ordinal))
				{
					queryBuilder.Append("NULL");
				}
				else if (columns[i].type == ColumnDataType.Binary)
				{
					if (cellValue.Length == 0)
					{
						queryBuilder.Append("''");
					}
					else
					{
						if (b64Buffer.Length < cellValue.Length)
							b64Buffer = new byte[cellValue.Length];

						if (!Convert.TryFromBase64Chars(cellValue, b64Buffer, out var decodeLength))
							throw new Exception($"Failed to decode base64 string: '{cellValue}'");

						queryBuilder.Append("_binary 0x");
						queryBuilder.Append(Utility.ByteArrayToString(b64Buffer.AsSpan(0, decodeLength)));
					}
				}
				else if (ColumnTypes[i] == typeof(string) || ColumnTypes[i] == typeof(DateTime))
				{
					queryBuilder.Append('\'');
					if (!cellValue.ContainsAny("\'\\"))
						queryBuilder.Append(cellValue);
					else
						queryBuilder.Append(cellValue.ToString().Replace("\\", "\\\\").Replace("'", "''"));

					queryBuilder.Append('\'');
				}
				else
				{
					queryBuilder.Append(cellValue);
				}
			}

			queryBuilder.Append(")");
		}

		if (count == 0)
			return (0, false, null);

		queryBuilder.Append(";");

		return (count, canReadMore, queryBuilder.ToString());
	}

	protected override (int rows, bool canContinue) ReadDataCsv(PipeWriter pipeWriter, string tableName, ColumnInfo[] columns)
	{
		bool canContinue = true;

		const int minimumSpanSize = 4 * 1024 * 1024;

		using var writer = new PipeTextWriter(pipeWriter, minimumSpanSize);
		int rows = 0;


		while (true)
		{
			if (rows >= 2000)
				break;

			if (!CsvReader.Read())
			{
				canContinue = false;
				break;
			}
			
			writer.Write("\n");

			for (int columnNum = 0; columnNum < columns.Length; columnNum++)
			{
				if (columnNum > 0)
					writer.Write(",");

				var cellValue = CsvReader.GetFieldSpan(columnNum);

				if (cellValue.Equals("\\N", StringComparison.Ordinal))
				{
					writer.Write("\\N");
				}
				else
				{
					writer.WriteCsvString(cellValue, true);
				}
			}

			rows++;
		}

		writer.Flush();

		return (rows, canContinue);
	}

	public override void Dispose()
	{
		CsvReader?.DisposeAsync().AsTask().Wait();
		StreamReader?.Dispose();
		DataStream.Dispose();
	}
}

public class MysqlInvalidCsvReader : TextReader
{
	private TextReader baseReader;

	private char[] underlyingBuffer = new char[4096];
	private int bufferRemaining;

	public MysqlInvalidCsvReader(TextReader baseReader)
	{
		this.baseReader = baseReader;
	}

	public override int Read() => throw new NotImplementedException();

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

		int bufferCopied = 0;
		int lastIndex = 0;

		// calculate safe cutoff

		int usableLength = Math.Min(bufferRemaining, buffer.Length);

		while (underlyingBuffer[usableLength - 1] == '\\')
			usableLength -= 1;
		
		// do scan and transformations
		
		foreach (var index in Locate(underlyingBuffer.AsSpan(0, usableLength)))
		{
			var length = index - lastIndex;
			underlyingBuffer.AsSpan(lastIndex, length).CopyTo(buffer.Slice(bufferCopied));

			bufferCopied += length;
			lastIndex = index + 1;

			if (underlyingBuffer[index + 1] == '\"')
			{
				buffer[bufferCopied++] = '\"';
			}
		}

		// copy remaining data wholesale
		if (lastIndex < usableLength)
		{
			var remainingLength = usableLength - lastIndex;
			underlyingBuffer.AsSpan(lastIndex, usableLength - lastIndex).CopyTo(buffer.Slice(bufferCopied));
			bufferCopied += remainingLength;
		}

		bufferRemaining -= usableLength;

		// move any leftover data back to the start of the buffer
		if (bufferRemaining > 0)
		{
			underlyingBuffer.AsSpan(usableLength, bufferRemaining).CopyTo(underlyingBuffer);
		}

		return bufferCopied;
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

			if (self[i + 1] == 'N')
				continue;

			list ??= new List<int>();
			list.Add(i);

			if (self[i + 1] == '\\')
				i++;
		}

		return list ?? emptyIndexes;
	}
}