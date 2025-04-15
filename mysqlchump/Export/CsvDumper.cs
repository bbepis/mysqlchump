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
				foreach (var column in schema)
				{
					if (!rowStart)
						textWriter.Write(",");

					object value = (column.DataType == typeof(decimal) || column.DataType == typeof(MySqlDecimal)) && reader is MySqlDataReader mysqlReader
						? mysqlReader.GetMySqlDecimal(column.ColumnName)
						: reader[column.ColumnName];

					StringToCsvCell(GetCsvMySqlStringRepresentation(column, value), textWriter);

					rowStart = false;
				}
			}

			textWriter.Flush();
		}

		// https://stackoverflow.com/a/6377656
		private static void StringToCsvCell(ReadOnlySpan<char> str, PipeTextWriter textWriter)
		{
			if (str.IndexOfAny(",\"\r\n") == -1)
			{
				textWriter.Write(str);
				return;
			}

			textWriter.Write("\"");
			int segmentStart = 0;
			for (int i = 0; i < str.Length; i++)
			{
				if (str[i] == '"')
				{
					if (i > segmentStart)
						textWriter.Write(str.Slice(segmentStart, i - segmentStart));

					textWriter.Write("\"\"");
					segmentStart = i + 1;
				}
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
				return value.ToString().Replace("\\", "\\\\");
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

			throw new SqlTypeException($"Could not represent type: {column.DataTypeName} ({columnType?.FullName ?? "<NULL>"})");
		}
	}
}