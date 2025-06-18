using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Diagnostics;
using System.IO.Pipelines;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MySqlConnector;

namespace mysqlchump.Export;

public class DumpOptions
{
	public string SelectQuery { get; set; }

	public bool MysqlWriteCreateTable { get; set; }
	public bool MysqlWriteTruncate { get; set; }

	public bool CsvWriteHeader { get; set; }
	public bool Silent { get; set; }
}

public abstract class BaseDumper
{
	public abstract bool CanMultiplexTables { get; }

	protected DumpOptions DumpOptions { get; }
	protected MySqlConnection Connection { get; }

	protected long ExportedRows;

	protected BaseDumper(MySqlConnection connection, DumpOptions dumpOptions)
	{
		Connection = connection;
		DumpOptions = dumpOptions;
	}

	public async Task ExportAsync(PipeWriter writer, string table)
	{
		string formattedQuery = DumpOptions.SelectQuery.Replace("{table}", table);

		var dumpTimer = new Stopwatch();
		dumpTimer.Start();

		var createSql = await GetCreateSql(table);
		ulong? estimatedRowCount = await GetEstimatedRowCount(table);

		using var selectCommand = Connection.CreateCommand();
		selectCommand.CommandText = formattedQuery;

		selectCommand.CommandTimeout = 3600;
		await selectCommand.PrepareAsync();

		using var reader = await selectCommand.ExecuteReaderAsync();
		var columns = reader.GetColumnSchema().AsEnumerable().ToArray();

		ExportedRows = 0;

		var progressDisabled = Console.IsErrorRedirected || DumpOptions.Silent;

		var cts = new CancellationTokenSource();
		var progressTask = Task.Run(async () =>
		{
			if (progressDisabled)
				return;

			while (true)
			{
				Console.Error.Write("\u001b[1000D"); // move cursor to the left

				double percentage = 100 * ExportedRows / (double)estimatedRowCount.Value;
				if (percentage > 100)
				{
					// mysql stats are untrustworthy
					percentage = 100;
				}

				Console.Error.Write($"{table} - {ExportedRows:N0} / {(estimatedRowCount.HasValue ? $"~{estimatedRowCount:N0}" : "?")} ({(estimatedRowCount.HasValue ? percentage.ToString("N2") : "?")} %)");

				if (cts.IsCancellationRequested)
					return;

				try
				{
					await Task.Delay(1000, cts.Token);
				}
				catch (TaskCanceledException) { }
			}
		});

		try
		{
			await PerformDump(table, reader, writer, columns, createSql, estimatedRowCount);
		}
		catch (Exception ex)
		{
			List<string> columnValues = new List<string>();

			for (int i = 0; i < reader.FieldCount; i++)
			{
				try
				{
					columnValues.Add(reader[i]?.ToString() ?? "<NULL>");
				}
				catch
				{
					columnValues.Add("<ERROR>");
				}
			}

			Console.Error.WriteLine();
			Console.Error.WriteLine(string.Join(", ", columns.Select(x => x.ColumnName)));
			Console.Error.WriteLine(string.Join(", ", columnValues));

			Console.WriteLine(ex.ToStringDemystified());

			throw;
		}

		cts.Cancel();
		progressTask.Wait();

		if (!progressDisabled)
			Console.Error.WriteLine();
	}

	protected abstract Task PerformDump(string table, MySqlDataReader reader, PipeWriter writer, DbColumn[] schema, string createSql, ulong? estimatedRows);

	public virtual Task FinishDump(PipeWriter writer) => Task.CompletedTask;

	public static async Task InitializeConnection(MySqlConnection connection)
	{
		// Set the session timezone to UTC so that we get consistent UTC timestamps

		await using var command = connection.CreateCommand();
		command.CommandText = @"SET SESSION time_zone = ""+00:00"";";

		await command.ExecuteNonQueryAsync();
	}

	protected async Task<string> GetCreateSql(string table)
	{
		await using var createTableCommand = Connection.CreateCommand();
		createTableCommand.CommandText = $"SHOW CREATE TABLE `{table}`";

		await using var reader = await createTableCommand.ExecuteReaderAsync();

		if (!await reader.ReadAsync())
			throw new ArgumentException($"Could not find table `{table}`");

		string createSql = (string)reader["Create Table"];

		createSql = createSql.Replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS");
		return createSql;
	}

	protected async Task<ulong?> GetEstimatedRowCount(string table)
	{
		var query = $"SELECT TABLE_ROWS FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table}' AND TABLE_SCHEMA = '{Connection.Database}';";

		await using var command = Connection.CreateCommand();
		command.CommandText = query;

		return (ulong)await command.ExecuteScalarAsync();
	}

	protected async Task<ulong?> GetAutoIncrementValue(string table)
	{
		string databaseName = Connection.Database;

		const string commandText =
			"SELECT AUTO_INCREMENT " +
			"FROM INFORMATION_SCHEMA.TABLES " +
			"WHERE TABLE_SCHEMA = @databasename AND TABLE_NAME = @tablename";

		await using var command = Connection.CreateCommand()
			.SetParam("@databasename", databaseName)
			.SetParam("@tablename", table);

		command.CommandText = commandText;

		var result = await command.ExecuteScalarAsync();

		if (result == DBNull.Value)
			return null;

		return Convert.ToUInt64(result);
	}
}