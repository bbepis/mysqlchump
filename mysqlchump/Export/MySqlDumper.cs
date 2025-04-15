using System;
using System.Data.Common;
using System.Data.SqlTypes;
using System.IO.Pipelines;
using System.Linq;
using System.Threading.Tasks;
using MySqlConnector;

namespace mysqlchump.Export
{
	public class MySqlDumper : BaseDumper
	{
		public MySqlDumper(MySqlConnection connection, DumpOptions dumpOptions) : base(connection, dumpOptions) { }

		public override bool CanMultiplexTables => true;

		private bool IsFirstTableForFile = true;

		protected override async Task PerformDump(string table, MySqlDataReader reader, PipeWriter writer, DbColumn[] schema, string createSql, ulong? estimatedRows)
		{
			var textWriter = new PipeTextWriter(writer);

			if (!IsFirstTableForFile)
				textWriter.Write("\n\n\n");

			if (DumpOptions.MysqlWriteCreateTable)
			{
				textWriter.Write(createSql);
				textWriter.Write(";\n\n\n");
			}

			//var autoIncrement = await GetAutoIncrementValue(table);

			//if (autoIncrement.HasValue)
			//	textWriter.Write($"ALTER TABLE `{table}` AUTO_INCREMENT={autoIncrement};\n\n");

			textWriter.Write("SET SESSION time_zone = \"+00:00\";\n");
			textWriter.Write("SET SESSION FOREIGN_KEY_CHECKS = 0;\n");
			textWriter.Write("SET SESSION UNIQUE_CHECKS = 0;\n");

			//await writer.WriteAsync("ALTER INSTANCE DISABLE INNODB REDO_LOG;\n\n");

			if (DumpOptions.MysqlWriteTruncate)
				textWriter.Write($"TRUNCATE `{table}`;\n");

			textWriter.Write("SET autocommit=0;\n");
			textWriter.Write("START TRANSACTION;\n");

			textWriter.Write("\n");

			while (true)
			{
				int currentRowCount = 0;
				int maxInsertLength = 2048;
				bool hasEnded = false;

				textWriter.Write($"INSERT INTO `{table}` ({string.Join(", ", schema.Select(column => $"`{column.ColumnName}`"))}) VALUES\n");

				while (currentRowCount <= maxInsertLength)
				{
					if (!await reader.ReadAsync())
					{
						hasEnded = true;
						break;
					}

					if (currentRowCount > 0)
						textWriter.Write(",");

					textWriter.Write("(");

					for (int i = 0; i < schema.Length; i++)
					{
						DbColumn column = schema[i];

						if (i > 0)
							textWriter.Write(", ");

						object value = (column.DataType == typeof(decimal) || column.DataType == typeof(MySqlDecimal))
									   && reader is MySqlDataReader mysqlReader
									   && !reader.IsDBNull(column.ColumnOrdinal!.Value)
							? mysqlReader.GetMySqlDecimal(column.ColumnName)
							: reader[column.ColumnName];

						textWriter.Write(GetMySqlStringRepresentation(column, value));
					}

					textWriter.Write(")");

					currentRowCount++;
					ExportedRows++;
				}

				textWriter.Write(";\n\n");

				if (hasEnded)
					break;
			}

			textWriter.Write("COMMIT;\n");
			IsFirstTableForFile = false;
		}

		public override async Task FinishDump(PipeWriter writer)
		{
			IsFirstTableForFile = true;
		}

		protected static string GetMySqlStringRepresentation(DbColumn column, object value)
		{
			if (value == null || value == DBNull.Value)
				return "NULL";

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
				return value.ToString();
			}

			if (columnType == typeof(MySqlDecimal))
			{
				return ((MySqlDecimal)value).ToString();
			}

			if (columnType == typeof(DateTime))
			{
				var dtValue = (DateTime)value;
				return $"'{dtValue:yyyy-MM-dd HH:mm:ss}'";
			}

			if (columnType == typeof(bool))
				return (bool)value ? "1" : "0";

			if (columnType == typeof(string))
			{
				var innerString = MySqlHelper.EscapeString(value.ToString())
					.Replace("\r", "\\r")
					.Replace("\n", "\\n")
					.Replace("\0", "\\0");

				return $"'{innerString}'";
			}

			if (columnType == typeof(byte[]))
				return "_binary 0x" + Utility.ByteArrayToString((byte[])value);

			throw new SqlTypeException($"Could not represent type: {column.DataTypeName}");
		}
	}
}