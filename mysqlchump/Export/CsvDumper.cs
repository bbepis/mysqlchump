using System;
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

		public CsvDumper(MySqlConnection connection, DumpOptions dumpOptions) : base(connection, dumpOptions) { }

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

					StringToCsvCell(column.ColumnName, textWriter);

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

					object value = column.DataType == typeof(decimal) && !reader.IsDBNull(i)
						? reader.GetMySqlDecimal(i)
						: reader[i];

					StringToCsvCell(GetCsvMySqlStringRepresentation(column, value), textWriter);

					rowStart = false;
				}

				ExportedRows++;
			}

			textWriter.Flush();
		}

		private static void StringToCsvCell(ReadOnlySpan<char> str, PipeTextWriter textWriter)
		{
			if (str.IndexOfAny(",\"\r\n\\") == -1)
			{
				textWriter.Write(str);
				return;
			}

			textWriter.Write("\"");
			int segmentStart = 0;
			for (int i = 0; i < str.Length; i++)
			{
				string replacement = null;

				switch (str[i])
				{
					case '"':
						replacement = "\"\"";
						break;
					case '\\':
						replacement = "\\\\";
						break;
					default:
						continue;
				}

				if (i > segmentStart)
					textWriter.Write(str.Slice(segmentStart, i - segmentStart));

				textWriter.Write(replacement);
				segmentStart = i + 1;
			}

			if (segmentStart < str.Length)
				textWriter.Write(str.Slice(segmentStart));
			textWriter.Write("\"");
		}

		private static string GetCsvMySqlStringRepresentation(DbColumn column, object value)
		{
			if (value == null || value == DBNull.Value)
				return "\\N";

			var columnType = column.DataType;

			if (columnType == typeof(string))
			{
				return (string)value;
			}

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
				return value.ToString();
			}

			if (columnType == typeof(bool))
				return (bool)value ? "1" : "0";

			if (columnType == typeof(DateTime))
			{
				var dtValue = (DateTime)value;
				return dtValue.ToString("yyyy-MM-dd HH:mm:ss");
			}

			if (columnType == typeof(byte[]))
			{
				return Convert.ToBase64String((byte[])value);
			}

			throw new SqlTypeException($"Could not represent type: {column.DataTypeName} ({columnType?.FullName ?? "<NULL>"})");
		}
	}
}