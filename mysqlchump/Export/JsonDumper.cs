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
		JsonWriter.WriteJsonString(table);
		JsonWriter.Write(",\"create_statement\":");
		JsonWriter.WriteJsonString(createSql);

		JsonWriter.Write(",\"columns\":{");

		bool isFirst = true;

		foreach (var column in schema)
		{
			if (!isFirst)
				JsonWriter.Write(",");
			isFirst = false;

			JsonWriter.WriteJsonString(column.ColumnName);
			JsonWriter.Write(":");
			JsonWriter.WriteJsonString(column.DataTypeName);
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

				if (reader.IsDBNull(i))
				{
					JsonWriter.Write("null");
				}
				else if (column.DataType == typeof(string))
				{
					JsonWriter.WriteJsonString((string)reader.GetValue(i));
				}
				else
				{
					value = column.DataType == typeof(decimal)
						? reader.GetMySqlDecimal(i)
						: reader[i];

					WriteJsonMySqlValue(column, value);
				}
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
		var columnType = column.DataType;

		if (value is byte @byte)       { JsonWriter.Write(@byte); return; }
		if (value is sbyte @sbyte)     { JsonWriter.Write(@sbyte); return; }
		if (value is ushort @ushort)   { JsonWriter.Write(@ushort); return; }
		if (value is short @short)     { JsonWriter.Write(@short); return; }
		if (value is uint @uint)       { JsonWriter.Write(@uint); return; }
		if (value is int @int)         { JsonWriter.Write(@int); return; }
		if (value is ulong @ulong)     { JsonWriter.Write(@ulong); return; }
		if (value is long @long)       { JsonWriter.Write(@long); return; }
		if (value is float @float)     { JsonWriter.Write(@float); return; }
		if (value is double @double)   { JsonWriter.Write(@double); return; }
		if (value is decimal @decimal) { JsonWriter.Write(@decimal); return; }

		if (value is MySqlDecimal mySqlDecimal)
		{
			JsonWriter.Write(mySqlDecimal.ToString());
			return;
		}

		if (columnType == typeof(DateTime))
		{
			var dtValue = (DateTime)value;
			JsonWriter.Write("\"");
			JsonWriter.Write(dtValue, "yyyy-MM-ddTH:mm:ss.fffZ");
			JsonWriter.Write("\"");
			return;
		}

		if (columnType == typeof(bool))
		{
			JsonWriter.Write((bool)value ? "true" : "false");
			return;
		}

		if (value is byte[] data)
		{
			JsonWriter.Write("\"");
			JsonWriter.WriteBase64(data);
			JsonWriter.Write("\"");
			return;
		}

		throw new SqlTypeException($"Could not represent type: {column.DataTypeName}");
	}
}