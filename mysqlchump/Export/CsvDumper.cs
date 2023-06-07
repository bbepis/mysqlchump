using System;
using System.Data.Common;
using System.Data.SqlTypes;
using System.Text;
using MySqlConnector;

namespace mysqlchump.Export
{
	public class CsvDumper : BaseDumper
	{
		public CsvDumper(MySqlConnection connection) : base(connection) { }

        private bool HasWrittenSchema = false;

		protected override void CreateInsertLine(DbDataReader reader, StringBuilder builder)
		{
            if (!HasWrittenSchema)
            {
				WriteSchemaLine(builder);
				HasWrittenSchema = true;
            }

			bool rowStart = true;
			foreach (var column in Columns)
			{
				if (!rowStart)
					builder.Append(",");

				object value = (column.DataType == typeof(decimal) || column.DataType == typeof(MySqlDecimal)) && reader is MySqlDataReader mysqlReader
					? mysqlReader.GetMySqlDecimal(column.ColumnName)
					: reader[column.ColumnName];

				StringToCsvCell(GetCsvMySqlStringRepresentation(column, value), builder);

				rowStart = false;
			}

			builder.AppendLine();
		}
		
        private void WriteSchemaLine(StringBuilder builder)
        {
            bool rowStart = true;

            foreach (var column in Columns)
            {
                if (!rowStart)
                    builder.Append(",");

                StringToCsvCell(column.ColumnName, builder);

				rowStart = false;
			}

			builder.AppendLine();
		}

		// https://stackoverflow.com/a/6377656
		private static void StringToCsvCell(string str, StringBuilder builder)
		{
			if (str.Contains(",") || str.Contains("\"") || str.Contains("\r") || str.Contains("\n"))
			{
				builder.Append("\"");
				foreach (char nextChar in str)
				{
					builder.Append(nextChar);
					if (nextChar == '"')
						builder.Append("\"");
				}
				builder.Append("\"");
			}
			else
			{
				builder.Append(str);
			}
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

			throw new SqlTypeException($"Could not represent type: {column.DataTypeName}");
		}
	}
}