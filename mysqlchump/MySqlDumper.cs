using System;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MySqlConnector;

namespace mysqlchump
{
	public class MySqlDumper : BaseDumper
	{
		public MySqlDumper(MySqlConnection connection) : base(connection) { }

		public override async Task WriteTableSchemaAsync(string table, Stream outputStream)
		{
			using (var writer = new StreamWriter(outputStream, Utility.NoBomUtf8, 4096, true))
			using (var createTableCommand = new MySqlCommand($"SHOW CREATE TABLE `{table}`", Connection))
			using (var reader = await createTableCommand.ExecuteReaderAsync())
			{
				if (!await reader.ReadAsync())
					throw new ArgumentException($"Could not find table `{table}`");

				string createSql = (string)reader["Create Table"];

				createSql = createSql.Replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS");

				await writer.WriteAsync(createSql);
				await writer.WriteAsync(";\n\n\n");
			}
		}

		protected override void StartInsertBatch(string table, DbDataReader reader, StringBuilder builder)
		{
        private async Task<ulong?> GetAutoIncrementValue(string table, MySqlTransaction transaction = null)
        {
            string databaseName = Connection.Database;

            const string commandText =
                "SELECT AUTO_INCREMENT " +
                "FROM INFORMATION_SCHEMA.TABLES " +
                "WHERE TABLE_SCHEMA = @databasename AND TABLE_NAME = @tablename";

            await using var createTableCommand = new MySqlCommand(commandText, Connection, transaction)
                .SetParam("@databasename", databaseName)
                .SetParam("@tablename", table);


            var result = await createTableCommand.ExecuteScalarAsync();

            if (result == DBNull.Value)
                return null;

			return Convert.ToUInt64(result);
        }

		public override async Task WriteAutoIncrementAsync(string table, Stream outputStream, MySqlTransaction transaction = null)
        {
            await using var writer = new StreamWriter(outputStream, Utility.NoBomUtf8, 4096, true);

            var autoIncrement = await GetAutoIncrementValue(table, transaction);
			
			if (autoIncrement.HasValue)
			    await writer.WriteAsync($"ALTER TABLE `{table}` AUTO_INCREMENT={autoIncrement};\n\n");
        }

			builder.AppendLine($"INSERT INTO `{table}` ({string.Join(", ", Columns.Select(column => $"`{column.ColumnName}`"))}) VALUES");
		}

		protected override void CreateInsertLine(DbDataReader reader, StringBuilder builder)
		{
			builder.Append("(");

			bool rowStart = true;
			foreach (var column in Columns)
			{
				if (!rowStart)
					builder.Append(", ");

				builder.Append(GetMySqlStringRepresentation(column, reader[column.ColumnName]));

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