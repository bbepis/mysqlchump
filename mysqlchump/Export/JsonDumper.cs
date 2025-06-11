using System;
using System.Buffers.Text;
using System.Data.Common;
using System.Data.SqlTypes;
using System.IO.Pipelines;
using System.Threading.Tasks;
using MySqlConnector;

namespace mysqlchump.Export;

public class JsonDumper : BaseDumper
{
	public JsonDumper(MySqlConnection connection, DumpOptions dumpOptions) : base(connection, dumpOptions) { }

	public override bool CanMultiplexTables => true;

	private PipeTextWriter JsonWriter { get; set; } = null;
	
	protected override async Task PerformDump(string table, MySqlDataReader reader, PipeWriter writer, DbColumn[] schema, string createSql, ulong? estimatedRows)
	{
		if (JsonWriter == null)
		{
			// start writing new file
			JsonWriter = new PipeTextWriter(writer);

			JsonWriter.Write("{\"version\":2,\"tables\":[");
		}
		else
		{
			JsonWriter.Write(",");
		}

		JsonWriter.Write("{\"name\":");
		WriteJsonString(table);
		JsonWriter.Write(",\"create_statement\":");
		WriteJsonString(createSql);

		JsonWriter.Write(",\"columns\":{");

		bool isFirst = true;

		foreach (var column in schema)
		{
			if (!isFirst)
				JsonWriter.Write(",");
			isFirst = false;

			WriteJsonString(column.ColumnName);
			JsonWriter.Write(":");
			WriteJsonString(column.DataTypeName);
		}

		JsonWriter.Write("},\"approx_count\":");
		JsonWriter.Write(estimatedRows.HasValue ? estimatedRows.Value.ToString() : "null");
		JsonWriter.Write(",\"rows\":[");

		bool firstRow = true;

		while (await reader.ReadAsync())
		{
			if (!firstRow)
				JsonWriter.Write(",");

			firstRow = false;
			JsonWriter.Write("[");

			for (int i = 0; i < schema.Length; i++)
			{
				if (i > 0)
					JsonWriter.Write(",");

				var column = schema[i];

				object value;

				if (column.DataType == typeof(decimal) && !reader.IsDBNull(i))
					value = reader.GetMySqlDecimal(i);
				else
					value = reader[i];

				WriteJsonMySqlValue(column, value);
			}

			JsonWriter.Write("]");
			ExportedRows++;
		}

		JsonWriter.Write("],\"actual_count\":");
		JsonWriter.Write(ExportedRows.ToString());
		JsonWriter.Write("}");
	}

	public override async Task FinishDump(PipeWriter writer)
	{
		JsonWriter.Write("]}");
		JsonWriter.Flush();
		JsonWriter = null;
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

			WriteJsonString(chars.Slice(0, charsWritten));
			return;
		}

		throw new SqlTypeException($"Could not represent type: {column.DataTypeName}");
	}

	public void WriteJsonString(ReadOnlySpan<char> input)
	{
		JsonWriter.Write("\"");

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

		JsonWriter.Write("\"");
	}
}