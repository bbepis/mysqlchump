using System;
using System.Buffers.Text;
using System.Data.Common;
using System.Data.SqlTypes;
using System.IO.Pipelines;
using System.Threading.Tasks;
using MySqlConnector;

namespace mysqlchump.Export
{
	public class CsvDumper : BaseDumper
	{
		public override bool CanMultiplexTables => false;
		public bool IsMysqlFormat { get; private set; }

		public CsvDumper(MySqlConnection connection, DumpOptions dumpOptions, bool isMysqlFormat) : base(connection, dumpOptions)
		{
			IsMysqlFormat = isMysqlFormat;
		}

		protected override async Task PerformDump(string table, MySqlDataReader reader, PipeWriter writer, DbColumn[] schema, string createSql, ulong? estimatedRows)
		{
			var textWriter = new PipeTextWriter(writer);

			bool needsNewline = false;

			if (DumpOptions.CsvWriteHeader)
			{
				bool rowStart = true;

				foreach (var column in schema)
				{
					if (!rowStart)
						textWriter.Write(",");

					textWriter.WriteCsvString(column.ColumnName, IsMysqlFormat);

					rowStart = false;
				}

				needsNewline = true;
			}

			while (await reader.ReadAsync())
			{
				if (needsNewline)
					textWriter.Write("\n");
				needsNewline = true;

				bool rowStart = true;
				for (int i = 0; i < schema.Length; i++)
				{
					DbColumn column = schema[i];
					if (!rowStart)
						textWriter.Write(",");

					if (reader.IsDBNull(i))
					{
						textWriter.Write("\\N");
					}
					else
					{
						object value = column.DataType == typeof(decimal)
							? reader.GetMySqlDecimal(i)
							: reader[i];

						WriteCsvMySqlValue(textWriter, value);
					}

					rowStart = false;
				}

				ExportedRows++;
			}

			textWriter.Flush();
		}

		private void WriteCsvMySqlValue(PipeTextWriter writer, object value)
		{
			if (value is string @string)
			{
				writer.WriteCsvString(@string, IsMysqlFormat);
				return;
			}

			if (value is byte @byte)       { writer.Write(@byte); return; }
			if (value is sbyte @sbyte)     { writer.Write(@sbyte); return; }
			if (value is ushort @ushort)   { writer.Write(@ushort); return; }
			if (value is short @short)     { writer.Write(@short); return; }
			if (value is uint @uint)       { writer.Write(@uint); return; }
			if (value is int @int)         { writer.Write(@int); return; }
			if (value is ulong @ulong)     { writer.Write(@ulong); return; }
			if (value is long @long)       { writer.Write(@long); return; }
			if (value is float @float)     { writer.Write(@float); return; }
			if (value is double @double)   { writer.Write(@double); return; }
			if (value is decimal @decimal) { writer.Write(@decimal); return; }

			if (value is MySqlDecimal mySqlDecimal)
			{
				writer.Write(mySqlDecimal.ToString());
				return;
			}

			if (value is DateTime dtValue)
			{
				writer.Write("\"");
				writer.Write(dtValue, "yyyy-MM-dd HH:mm:ss");
				writer.Write("\"");
				return;
			}

			if (value is bool @bool)
			{
				writer.Write(@bool ? "1" : "0");
				return;
			}

			if (value is byte[] data)
			{
				writer.WriteBase64(data);
				return;
			}

			throw new SqlTypeException($"Could not represent type: {value.GetType()}");
		}
	}
}