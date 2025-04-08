using System;
using System.Buffers.Text;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.SqlTypes;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MySqlConnector;

namespace mysqlchump.Export;

public class JsonDumper : BaseDumper
{
	public JsonDumper(MySqlConnection connection) : base(connection) { }

	public override bool CompatibleWithMultiTableStdout => true;

	private StreamWriter JsonWriter { get; set; } = null;

	public override async Task WriteStartTableAsync(string table, Stream outputStream, bool writeSchema, bool truncate)
	{
		if (JsonWriter == null)
		{
			// start writing new file
			JsonWriter = new StreamWriter(outputStream, new UTF8Encoding(false), 20 * 1024 * 1024);

			JsonWriter.Write("{\"version\":2,\"tables\":[");
		}
		else
		{
			JsonWriter.Write(",");
		}

		JsonWriter.Write("{\"name\":");
		WriteJsonString(table);
		JsonWriter.Write(",\"create_statement\":");
		WriteJsonString(await GetCreateSql(table));
	}

	public override async Task WriteInsertQueries(string table, string query, Stream outputStream, MySqlTransaction transaction = null)
	{
		await using var selectCommand = new MySqlCommand(query, Connection, transaction);

		selectCommand.CommandTimeout = 3600;

		ulong? totalRowCount;

		await using (var rowCountCommand = new MySqlCommand(query, Connection, transaction))
		{
			rowCountCommand.CommandText =
				$"SELECT TABLE_ROWS FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table}' AND TABLE_SCHEMA = '{Connection.Database}';";
			totalRowCount = (ulong?)await rowCountCommand.ExecuteScalarAsync();
		}

		await using var reader = await selectCommand.ExecuteReaderAsync();

		JsonWriter.Write(",\"columns\":{");

		Columns = (await reader.GetColumnSchemaAsync()).AsEnumerable().ToArray();

		bool isFirst = true;

		foreach (var column in Columns)
		{
			if (!isFirst)
				JsonWriter.Write(",");
			isFirst = false;

			WriteJsonString(column.ColumnName);
			JsonWriter.Write(":");
			WriteJsonString(column.DataTypeName);
		}

		JsonWriter.Write("},\"approx_count\":");
		JsonWriter.Write(totalRowCount.HasValue ? totalRowCount.Value.ToString() : "null");
		JsonWriter.Write(",\"rows\":[");

		long rowCounter = 0;
			
		var dumpTimer = new Stopwatch();
		dumpTimer.Start();
			
		void WriteProgress(long currentRow)
		{
			if (Console.IsErrorRedirected)
				return;

			if (OperatingSystem.IsWindows())
				Console.CursorLeft = 0;
			else
				Console.Error.Write("\u001b[1000D"); // move cursor to the left

			double percentage = totalRowCount.HasValue ? 100 * currentRow / (double)totalRowCount.Value : 0;
			if (percentage > 100 || double.IsNaN(percentage))
			{
				// mysql stats are untrustworthy
				percentage = 100;
			}

			Console.Error.Write($"{table} - {currentRow:N0} / {(totalRowCount.HasValue ? $"~{totalRowCount:N0}" : "?")} ({(totalRowCount.HasValue ? percentage.ToString("N2") : "?")} %)");
		}
		
		

		try
		{
			bool firstRow = true;

			while (await reader.ReadAsync())
			{
				if (dumpTimer.ElapsedMilliseconds >= 1000)
				{
					WriteProgress(rowCounter);
					dumpTimer.Restart();
				}

				if (!firstRow)
					JsonWriter.Write(",");

				firstRow = false;
				JsonWriter.Write("[");

				for (int i = 0; i < Columns.Length; i++)
				{
					if (i > 0)
						JsonWriter.Write(",");

					var column = Columns[i];

					object value;

					if (column.DataType == typeof(decimal) && !reader.IsDBNull(i))
						value = reader.GetMySqlDecimal(i);
					else
						value = reader[i];

					WriteJsonMySqlValue(column, value);
				}

				JsonWriter.Write("]");
				rowCounter++;
			}
		}
		catch (Exception ex)
		{
			List<string> columnValues = new List<string>();

			for (int i = 0; i < reader.FieldCount; i++)
			{
				try
				{
					if (Columns[i].DataType == typeof(decimal) && reader[i] != DBNull.Value)
						columnValues.Add(reader.GetMySqlDecimal(i).ToString() ?? "<NULL>");
					else
						columnValues.Add(reader[i]?.ToString() ?? "<NULL>");
				}
				catch
				{
					columnValues.Add($"<ERROR> ({Columns[i].DataType})");
				}
			}

			Console.Error.WriteLine();
			Console.Error.WriteLine(string.Join(", ", Columns.Select(x => x.ColumnName)));
			Console.Error.WriteLine(string.Join(", ", columnValues));

			Console.Error.WriteLine(ex);

			throw;
		}
			
		WriteProgress(rowCounter);

		if (!Console.IsErrorRedirected)
			Console.Error.WriteLine();

		JsonWriter.Write("],\"actual_count\":");
		JsonWriter.Write(rowCounter.ToString());
		JsonWriter.Write("}");
	}

	public override async Task WriteEndTableAsync(string table, Stream outputStream, bool tablesRemainingForStream)
	{
		if (!tablesRemainingForStream)
		{
			JsonWriter.Write("]}");
			JsonWriter.Close();
			JsonWriter = null;
		}
	}

	private void WriteJsonMySqlValue(DbColumn column, object value)
	{
		if (value == null || value == DBNull.Value)
		{
			JsonWriter.Write("null");
			return;
		}

		var columnType = column.DataType;

		if (columnType == typeof(byte)
			|| columnType == typeof(sbyte)
			|| columnType == typeof(ushort)
			|| columnType == typeof(short)
			|| columnType == typeof(uint)
			|| columnType == typeof(int)
			|| columnType == typeof(ulong)
			|| columnType == typeof(long)
			|| columnType == typeof(float)
			|| columnType == typeof(double)
			|| columnType == typeof(decimal))
		{
			JsonWriter.Write(value.ToString());
			return;
		}

		if (columnType == typeof(MySqlDecimal))
		{
			JsonWriter.Write(((MySqlDecimal)value).ToString());
			return;
		}

		if (columnType == typeof(DateTime))
		{
			var dtValue = (DateTime)value;
			JsonWriter.Write($"\"{dtValue:yyyy-MM-ddTH:mm:ss.fffZ}\"");
			return;
		}

		if (columnType == typeof(bool))
		{
			JsonWriter.Write((bool)value ? "true" : "false");
			return;
		}

		if (columnType == typeof(string))
		{
			WriteJsonString((string)value);
			return;
		}

		if (columnType == typeof(byte[]))
		{
			var data = (byte[])value;

			if (data.Length > 512 * 1024)
				throw new Exception("Cannot currently handle binary blobs bigger than 512kb");

			Span<char> chars = stackalloc char[Base64.GetMaxEncodedToUtf8Length(data.Length)];

			if (!Convert.TryToBase64Chars(data, chars, out int charsWritten))
				throw new Exception("Could not convert data to base 64");

			JsonWriter.Write("\"");
			JsonWriter.Write(chars);
			JsonWriter.Write("\"");
			return;
		}

		throw new SqlTypeException($"Could not represent type: {column.DataTypeName}");
	}

	public void WriteJsonString(ReadOnlySpan<char> input)
	{
		JsonWriter.Write('"');

		int runStart = 0;

		for (int i = 0; i < input.Length; i++)
		{
			char c = input[i];
			string escapeSequence = null;

			switch (c)
			{
				case '\"':
					escapeSequence = "\\\"";
					break;
				case '\\':
					escapeSequence = "\\\\";
					break;
				case '\b':
					escapeSequence = "\\b";
					break;
				case '\f':
					escapeSequence = "\\f";
					break;
				case '\n':
					escapeSequence = "\\n";
					break;
				case '\r':
					escapeSequence = "\\r";
					break;
				case '\t':
					escapeSequence = "\\t";
					break;
				default:
					if (c < ' ')
					{
						escapeSequence = "\\u" + ((int)c).ToString("x4");
					}
					break;
			}

			if (escapeSequence != null)
			{
				if (i > runStart)
				{
					JsonWriter.Write(input.Slice(runStart, i - runStart));
				}
				JsonWriter.Write(escapeSequence);
				runStart = i + 1;
			}
		}

		if (runStart < input.Length)
		{
			JsonWriter.Write(input.Slice(runStart, input.Length - runStart));
		}

		JsonWriter.Write('\"');
	}
}