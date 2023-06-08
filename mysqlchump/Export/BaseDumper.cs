using System;
using System.Data.Common;
using System.IO;
using System.Threading.Tasks;
using MySqlConnector;

namespace mysqlchump.Export
{
	public abstract class BaseDumper
	{
		public MySqlConnection Connection { get; }

		protected BaseDumper(MySqlConnection connection)
		{
			Connection = connection;
		}

		protected DbColumn[] Columns { get; set; }

		public abstract bool CompatibleWithMultiTableStdout { get; }

		public virtual Task WriteStartTableAsync(string table, Stream outputStream, bool writeSchema, bool truncate) => Task.CompletedTask;
		public virtual Task WriteEndTableAsync(string table, Stream outputStream, bool tablesRemainingForStream) => Task.CompletedTask;

		public static async Task InitializeConnection(MySqlConnection connection)
		{
			// Set the session timezone to UTC so that we get consistent UTC timestamps

			using (var command = new MySqlCommand(@"SET SESSION time_zone = ""+00:00"";", connection))
			{
				await command.ExecuteNonQueryAsync();
			}
		}

		public abstract Task WriteInsertQueries(string table, string query, Stream outputStream, MySqlTransaction transaction = null);
		
		protected async Task<string> GetCreateSql(string table)
		{
			await using var createTableCommand = new MySqlCommand($"SHOW CREATE TABLE `{table}`", Connection);
			await using var reader = await createTableCommand.ExecuteReaderAsync();


			if (!await reader.ReadAsync())
				throw new ArgumentException($"Could not find table `{table}`");

			string createSql = (string)reader["Create Table"];

			createSql = createSql.Replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS");
			return createSql;
		}
	}
}