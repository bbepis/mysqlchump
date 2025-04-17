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
			if (IsFirstTableForFile)
			{
				textWriter.Write("SET SESSION time_zone = \'+00:00\';\n");
				textWriter.Write("SET SESSION FOREIGN_KEY_CHECKS = 0;\n");
				textWriter.Write("SET SESSION UNIQUE_CHECKS = 0;\n");
				textWriter.Write("SET autocommit=0;\n");
			}

			//await writer.WriteAsync("ALTER INSTANCE DISABLE INNODB REDO_LOG;\n\n");

			if (DumpOptions.MysqlWriteTruncate)
				textWriter.Write($"TRUNCATE `{table}`;\n");

			textWriter.Write("START TRANSACTION;\n");

			textWriter.Write("\n");

			var insertString = $"INSERT INTO `{table}` ({string.Join(", ", schema.Select(column => $"`{column.ColumnName}`"))}) VALUES\n";

			while (true)
			{
				int currentRowCount = 0;
				int maxInsertLength = 8192;
				bool hasEnded = false;

				textWriter.Write(insertString);

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

						object value = column.DataType == typeof(decimal) && !reader.IsDBNull(i)
							? reader.GetMySqlDecimal(i)
							: reader[i];

						WriteMySqlStringRepresentation(column, value, textWriter);
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
			textWriter.Flush();
		}

		public override async Task FinishDump(PipeWriter writer)
		{
			IsFirstTableForFile = true;
		}

		protected static void WriteMySqlStringRepresentation(DbColumn column, object value, PipeTextWriter textWriter)
		{
			if (value == null || value == DBNull.Value)
			{
				textWriter.Write("NULL");
				return;
			}

			switch (value)
			{
				case byte:
				case sbyte:
				case ushort:
				case short:
				case uint:
				case int:
				case ulong:
				case long:
				case float:
				case double:
					textWriter.Write(value.ToString());
					return;
				case bool:
					textWriter.Write((bool)value ? "1" : "0");
					return;
				case string:
					WriteMysqlString((string)value, textWriter);
					return;
				case byte[]:
					textWriter.Write("_binary 0x");
					textWriter.WriteHex((byte[])value);
					return;
				case MySqlDecimal:
					textWriter.Write(((MySqlDecimal)value).ToString());
					return;
				case DateTime:
					var dtValue = (DateTime)value;

					Span<char> charOutput = stackalloc char[24];

					if (!dtValue.TryFormat(charOutput, out int written, "yyyy-MM-dd HH:mm:ss"))
						throw new Exception("Failed to format date string");

					textWriter.Write(charOutput.Slice(0, written));
					return;
			}

			throw new SqlTypeException($"Could not represent type: {column.DataTypeName}");
		}

		private static void WriteMysqlString(ReadOnlySpan<char> input, PipeTextWriter writer)
		{
			writer.Write("\'");

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
					case '\'':
						escapeSequence = "\\\'";
						break;
					case '\\':
						escapeSequence = "\\\\";
						break;
					case '\b':
						escapeSequence = "\\b";
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
					case '\0':
						escapeSequence = "\\0";
						break;
				}

				if (escapeSequence != null)
				{
					if (i > runStart)
					{
						writer.Write(input.Slice(runStart, i - runStart));
					}
					writer.Write(escapeSequence);
					runStart = i + 1;
				}
			}

			if (runStart < input.Length)
			{
				writer.Write(input.Slice(runStart, input.Length - runStart));
			}

			writer.Write("\'");
		}
	}
}