using MySqlConnector;
using Sylvan.Data.Csv;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace mysqlchump.Import;

internal class CsvImporter : BaseImporter
{
	public async Task ImportAsync(Stream dataStream, string table, string[] columns, bool fixInvalidCsv, int parallelCount, bool useHeaders, bool insertIgnore, Func<MySqlConnection> createConnection)
	{
		using var reader = new StreamReader(dataStream, Encoding.UTF8, leaveOpen: true);

		var csvReader = await CsvDataReader.CreateAsync(fixInvalidCsv ? new MysqlInvalidCsvReader(reader) : reader, 
			new CsvDataReaderOptions { BufferSize = 128000, HasHeaders = useHeaders, ResultSetMode = ResultSetMode.MultiResult });

		if (useHeaders)
		{
			columns = new string[csvReader.FieldCount];

			for (int i = 0; i < columns.Length; i++)
				columns[i] = csvReader.GetName(i);
		}

		Type[] columnTypes = new Type[columns.Length];

		await using (var connection = createConnection())
		{
			await using var readCommand = connection.CreateCommand();
			readCommand.CommandText = $"SELECT * FROM `{table}` LIMIT 0";
			await using var sqlReader = await readCommand.ExecuteReaderAsync();

			var schema = await sqlReader.GetColumnSchemaAsync();

			for (var i = 0; i < columns.Length; i++)
			{
				var columnName = columns[i];

				var dbColumn = schema.FirstOrDefault(x => x.ColumnName.Equals(columnName, StringComparison.OrdinalIgnoreCase));

				if (dbColumn == null)
					throw new Exception($"Seemingly invalid column list; cannot find column '{columnName}'.{(useHeaders ? "" : " Maybe try specifying column list manually?")}");

				columnTypes[i] = dbColumn.DataType;
			}
		}

		await DoParallelInserts(parallelCount, null, table, createConnection, async (channel, reportRowCount) =>
		{
			try
			{
				while (true)
				{
					var (canContinue, insertQuery, rowCount) = GetNextInsertBatch(csvReader, table, columns, columnTypes, insertIgnore);

					if (insertQuery == null)
						break;

					reportRowCount(rowCount);

					await channel.Writer.WriteAsync(insertQuery);

					if (!canContinue)
						break;
				}
			}
			catch (Exception ex)
			{
				Console.Error.WriteLine($"Error when reading data source @ R{csvReader.RowNumber} ({csvReader.GetString(0)})");
				Console.Error.WriteLine(ex.ToString());
			}

			channel.Writer.Complete();
		});
	}

	private StringBuilder queryBuilder = new StringBuilder(1_000_000);

	private (bool canContinue, string insertQuery, int rowCount) GetNextInsertBatch(CsvDataReader csvReader, string table, string[] columnNames, Type[] columnTypes, bool insertIgnore)
	{
		const int insertLimit = 4000;
		int count = 0;

		queryBuilder.Clear();
		queryBuilder.Append($"INSERT{(insertIgnore ? " IGNORE" : "")} INTO `{table}` ({string.Join(',', columnNames)}) VALUES ");

		bool needsComma = false;

		for (; count < insertLimit; count++)
		{
			if (!csvReader.Read())
				break;
			
			queryBuilder.Append(needsComma ? ",(" : "(");
			needsComma = true;

			for (int i = 0; i < columnTypes.Length; i++)
			{
				if (i > 0)
					queryBuilder.Append(",");
				
				var rawValue = csvReader.GetString(i);

				if (MemoryExtensions.Equals(rawValue, "\\N", StringComparison.Ordinal))
				{
					queryBuilder.Append("NULL");
				}
				else if (columnTypes[i] == typeof(string) || columnTypes[i] == typeof(DateTime))
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
			return (false, null, 0);

		queryBuilder.Append(";");

		return (!(count < insertLimit), queryBuilder.ToString(), count);
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