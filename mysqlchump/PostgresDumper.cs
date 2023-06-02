using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MySqlConnector;

namespace mysqlchump
{
	public class PostgresDumper : BaseDumper
	{
		public PostgresDumper(MySqlConnection connection) : base(connection) { }

		private Dictionary<string, DataRow> SourceTableSchema { get; set; } = new Dictionary<string, DataRow>();
		private Dictionary<string, string> DestinationTableTypes{ get; set; } = new Dictionary<string, string>();

		private async Task GetSchema(string table)
		{
			string databaseName = Connection.Database;

			const string commandText =
				"SELECT * " +
					"FROM INFORMATION_SCHEMA.COLUMNS " +
					"WHERE TABLE_SCHEMA = @databasename AND TABLE_NAME = @tablename " +
					"ORDER BY ORDINAL_POSITION ASC";

			await using var createTableCommand = new MySqlCommand(commandText, Connection)
				.SetParam("@databasename", databaseName)
				.SetParam("@tablename", table);


			
			await using var reader = await createTableCommand.ExecuteReaderAsync();

			var dbTable = new DataTable();
			dbTable.Load(reader);

			//if (!await reader.ReadAsync())
			//	throw new ArgumentException($"Could not find table `{table}`");

			//var dataTable = await reader.GetSchemaTableAsync();

			//do
			//{
			//	var row = dataTable.NewRow();
			//	reader.GetValues(row.ItemArray);

			//	SourceTableSchema[reader.GetString("COLUMN_NAME")] = row;
			//}
			//while (await reader.ReadAsync());


			if (dbTable.Rows.Count == 0)
				throw new ArgumentException($"Could not find table `{table}`");

			foreach (DataRow row in dbTable.Rows)
			{
				SourceTableSchema[(string)row["COLUMN_NAME"]] = row;
			}
		}

		private async Task<ulong> GetAutoIncrementValue(string table)
		{
			string databaseName = Connection.Database;

			const string commandText =
				"SELECT AUTO_INCREMENT " +
					"FROM INFORMATION_SCHEMA.TABLES " +
					"WHERE TABLE_SCHEMA = @databasename AND TABLE_NAME = @tablename";

			await using var createTableCommand = new MySqlCommand(commandText, Connection)
				.SetParam("@databasename", databaseName)
				.SetParam("@tablename", table);

			return Convert.ToUInt64(await createTableCommand.ExecuteScalarAsync());
		}

		public override async Task WriteStartTableAsync(string table, Stream outputStream, bool writeSchema, bool truncate)
		{
			await GetSchema(table);

			if (!writeSchema)
				return;

			await using var writer = new StreamWriter(outputStream, Utility.NoBomUtf8, 4096, true);

			await writer.WriteLineAsync($"CREATE SCHEMA IF NOT EXISTS \"import\";");
			await writer.WriteLineAsync();

			await writer.WriteLineAsync($"CREATE TABLE IF NOT EXISTS \"import\".\"{table}\" (");

			bool firstRun = true;

			bool pkEnabled = SourceTableSchema.Count(kv => (string)kv.Value["COLUMN_KEY"] == "PRI") == 1;

			foreach (var (columnName, schemaRow) in SourceTableSchema)
			{
				if (!firstRun)
					await writer.WriteLineAsync(",");

				firstRun = false;

				// There will be a lot of assumptions / limitations here.

				// Indexes (other than a single-column PRIMARY KEY) are not supported.
				// Constraints (other than UNIQUE) are not supported.
				// Generated columns will not be handled correctly.
				// Default values/expressions are not supported.

				await writer.WriteAsync("\t\"");
				await writer.WriteAsync(columnName);
				await writer.WriteAsync("\"\t");

				string typeName;
				bool autoIncrement =
					((string)schemaRow["EXTRA"]).Contains("auto_increment", StringComparison.OrdinalIgnoreCase);

				var dataType = (string)schemaRow["DATA_TYPE"];
				switch (dataType.ToLower())
				{
					// Integer types are hard to represent exactly as mysql supports unsigned types.

					case "tinyint":
					{
						// There's no 1-byte datatype in postgres, so the unsigned limitation does not apply here
						//  since we have to use the 2 byte type.
						typeName = autoIncrement ? "smallserial" : "smallint";
						break;
					}

					case "int":
					{
						// Use the next size up integer if unsigned.
						if (((string)schemaRow["COLUMN_TYPE"]).Contains("unsigned", StringComparison.OrdinalIgnoreCase))
							typeName = autoIncrement ? "bigserial" : "bigint";
						else
							typeName = autoIncrement ? "serial" : "integer";
						break;
					}

					case "bigint":
					{
						// There's no next size up, so if the column type is unsigned then there may be clipping issues.
						typeName = autoIncrement ? "bigserial" : "bigint";
						break;
					}

					// String types are easier but there's no different sizes for text blobs.

					case "enum": // Note that I've included enums here. I don't have a way to create enum types yet (and mysql treats them the same as strings anyway)
					case "varchar":
					case "char":
					{
						object maxLength = schemaRow["CHARACTER_MAXIMUM_LENGTH"];

						if (maxLength == DBNull.Value || maxLength == null)
							throw new Exception("Expected a character length but got nothing");

						typeName = dataType != "char"
							? $"varchar({maxLength})"
							: $"char({maxLength})";

						break;
					}

					case "tinytext":
					case "text":
					case "mediumtext":
					case "longtext":
					{
						// There's no distinction between the four in postgres.
						typeName = "text";
						break;
					}

					// As far as I'm aware there's no types in mysql that have timezone information.

					case "datetime":
					case "timestamp":
					{
						typeName = "timestamp without time zone";
						break;
					}

					// There's no fixed binary type in postgres, which is shithouse.

					case "binary":
					case "varbinary":
					{
						typeName = "bytea";
						break;
					}

					// This makes the assumption that people are using 'bit(1)' for boolean types.

					case "bit":
					{
						if (Convert.ToInt32(schemaRow["NUMERIC_PRECISION"]) != 1)
							goto default;

						typeName = "boolean";
						break;
					}

					default:
						throw new Exception($"Unknown mysql datatype: {dataType}");
				}

				DestinationTableTypes[columnName] = typeName;

				await writer.WriteAsync(typeName);

				if (pkEnabled && (string)schemaRow["COLUMN_KEY"] == "PRI")
					await writer.WriteAsync(" PRIMARY KEY");
				else if ((string)schemaRow["COLUMN_KEY"] == "UNI")
					await writer.WriteAsync(" UNIQUE");

				if ((string)schemaRow["IS_NULLABLE"] == "YES")
					await writer.WriteAsync(" NULL");
				else
					await writer.WriteAsync(" NOT NULL");
			}

			await writer.WriteLineAsync("");
			await writer.WriteLineAsync(");");

			string autoIncrementColumn = SourceTableSchema.FirstOrDefault(kv =>
				((string)kv.Value["EXTRA"]).Contains("auto_increment", StringComparison.OrdinalIgnoreCase)).Key;

			if (autoIncrementColumn != null)
			{
				var autoIncrementValue = await GetAutoIncrementValue(table);

				await writer.WriteLineAsync("");
				await writer.WriteLineAsync("");
				await writer.WriteLineAsync(
					$"SELECT setval('\"import\".\"{table}_{autoIncrementColumn}_seq\"', {autoIncrementValue});");
			}

			await writer.WriteLineAsync("");
			await writer.WriteLineAsync("");
		}

		protected override void StartInsertBatch(string table, DbDataReader reader, StringBuilder builder)
		{
			builder.AppendLine($"INSERT INTO \"import\".\"{table}\" ({string.Join(", ", Columns.Select(column => $"\"{column.ColumnName}\""))}) VALUES");
		}

		protected override void CreateInsertLine(DbDataReader reader, StringBuilder builder)
		{
			builder.Append("(");

			bool rowStart = true;
			foreach (var column in Columns)
			{
				if (!rowStart)
					builder.Append(", ");

				if (DestinationTableTypes[column.ColumnName] == "boolean")
				{
					builder.Append(GetPostgresStringRepresentation(column, (ulong)reader[column.ColumnName] != 0));
				}
				else
					builder.Append(GetPostgresStringRepresentation(column, reader[column.ColumnName]));

				rowStart = false;
			}

			builder.Append(")");
		}

		protected override void WriteInsertContinuation(StringBuilder builder)
		{
			builder.AppendLine(",");
			
		}

		protected override void WriteInsertEnd(StringBuilder builder)
		{
			builder.AppendLine(";");
		}

		protected override void EndInsertBatch(DbDataReader reader, StringBuilder builder)
		{
			builder.AppendLine(";\n\n");
		}
	}
}