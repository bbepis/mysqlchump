using Nito.AsyncEx;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using MySqlConnector;
using System.IO.Pipelines;
using System.Collections.Generic;
using System.IO;
using mysqlchump.SqlParsing;
using Index = mysqlchump.SqlParsing.Index;

namespace mysqlchump.Import;

public enum ImportMechanism
{
	SqlStatements,
	LoadDataInfile
}

public class ImportOptions
{
	public ImportMechanism ImportMechanism { get; set; }

	public bool AggressiveUnsafe { get; set; }
	public string[] SourceTables { get; set; }
	public int ParallelThreads { get; set; }
	public bool InsertIgnore { get; set; }
	public bool SetInnoDB { get; set; }
	public bool SetCompressed { get; set; }
	public bool NoCreate { get; set; }
	public bool DeferIndexes { get; set; }
	public bool StripIndexes { get; set; }

	public string TargetTable { get; set; }
	public bool CsvFixInvalid { get; set; }
	public bool CsvUseHeaderRow { get; set; }
	public string[] CsvManualColumns { get; set; }
}

public abstract class BaseImporter : IDisposable
{
	protected ImportOptions ImportOptions { get; }
	protected Stream DataStream { get; }

	public BaseImporter(ImportOptions options, Stream dataStream)
	{
		ImportOptions = options;
		DataStream = dataStream;
	}
	protected record QueuedIndex(string text, string tableName, string indexName, bool isFk, string sql);
	protected record ColumnInfo(string name, string type);

	protected Channel<QueuedIndex> ReindexQueue { get; } = Channel.CreateUnbounded<QueuedIndex>();
	protected List<QueuedIndex> PendingIndexes { get; } = new();

	public async Task ImportAsync(Func<MySqlConnection> createConnection)
	{
		Task reindexTask = null;

		if (ImportOptions.DeferIndexes)
		{
			reindexTask = Task.Run(async () =>
			{
				await using (var connection = createConnection())
				{
					using var command = connection.CreateCommand();
					command.CommandTimeout = 9999999;

					command.CommandText = "SET FOREIGN_KEY_CHECKS = 0;";
					await command.ExecuteNonQueryAsync();

					await foreach (var (text, tableName, indexName, isFk, sql) in ReindexQueue.Reader.ReadAllAsync())
					{
						command.CommandTimeout = 9999999;

						// check if index exists first
						if (isFk)
						{
							command.CommandText = $@"
SELECT COUNT(*)
FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS
WHERE CONSTRAINT_SCHEMA = '{connection.Database}'
	AND TABLE_NAME = '{tableName}'
	AND CONSTRAINT_NAME = '{indexName}';";
						}
						else
						{
							command.CommandText = $@"
SELECT COUNT(*)
FROM INFORMATION_SCHEMA.STATISTICS
WHERE table_schema = '{connection.Database}'
	AND table_name = '{tableName}'
	AND INDEX_NAME = '{indexName}';";
						}

						var exists = (long)await command.ExecuteScalarAsync();

						if (exists <= 0)
						{
							Console.Error.WriteLine(text);
							command.CommandText = sql;

							await command.ExecuteNonQueryAsync();
						}
					}

					command.CommandText = "SET FOREIGN_KEY_CHECKS = 1;";
					await command.ExecuteNonQueryAsync();
				}
			});
		}

		while (true)
		{
			var (foundAnotherTable, createTableSql, approxRows) = await ReadToNextTable();

			if (!foundAnotherTable)
				break;

			var (shouldInsert, tableName, columns) = await ProcessTableCreation(createConnection, createTableSql);

			if (!shouldInsert)
			{
				Console.Error.WriteLine($"Skipping table '{tableName}' ({(approxRows.HasValue ? $"~{approxRows}" : "?")} rows)");
				continue;
			}

			await PerformParallelImport(ImportOptions, approxRows, tableName, columns, createConnection);
		}

		ReindexQueue.Writer.Complete();

		if (reindexTask != null)
			await reindexTask;
	}

	protected abstract Task<(bool foundAnotherTable, string createTableSql, ulong? approxRows)> ReadToNextTable();

	protected virtual async Task<(bool shouldInsert, string tableName, ColumnInfo[] columns)> ProcessTableCreation(Func<MySqlConnection> createConnection, string createTableSql)
	{
		var parsedTable = CreateTableParser.Parse(createTableSql);

		var columnInfo = parsedTable.Columns.Select(x => new ColumnInfo(x.Name, x.DataType)).ToArray();

		if (ImportOptions.SourceTables != null
			&& ImportOptions.SourceTables.Length > 0
			&& !ImportOptions.SourceTables.Any(x => x == "*")
			&& !ImportOptions.SourceTables.Any(x => parsedTable.Name.Equals(x, StringComparison.OrdinalIgnoreCase)))
		{
			return (false, parsedTable.Name, columnInfo);
		}

		if (ImportOptions.SetInnoDB)
		{
			parsedTable.Options["ENGINE"] = "InnoDB";

			parsedTable.Options.Remove("COMPRESSION"); // tokudb
			parsedTable.Options["ROW_FORMAT"] = "DYNAMIC";

			createTableSql = parsedTable.ToCreateTableSql();
		}

		if (ImportOptions.SetCompressed)
		{
			parsedTable.Options["ROW_FORMAT"] = "COMPRESSED";

			createTableSql = parsedTable.ToCreateTableSql();
		}

		var removedIndexes = new List<Index>();
		var removedFks = new List<ForeignKey>();

		if (ImportOptions.DeferIndexes || ImportOptions.StripIndexes)
		{
			removedIndexes.AddRange(parsedTable.Indexes.Where(x => x.Type != IndexType.Primary));
			parsedTable.Indexes.RemoveAll(x => x.Type != IndexType.Primary);

			removedFks.AddRange(parsedTable.ForeignKeys);
			parsedTable.ForeignKeys.Clear();

			createTableSql = parsedTable.ToCreateTableSql();
		}

		await using (var connection = createConnection())
		{
			using var command = connection.CreateCommand();

			command.CommandTimeout = 9999999;

			// check if table exists first
			command.CommandText = $@"SELECT COUNT(*)
							FROM information_schema.TABLES
							WHERE TABLE_SCHEMA = '{connection.Database}' AND TABLE_NAME = '{parsedTable.Name}'";

			var exists = (long)await command.ExecuteScalarAsync();
			
			if (exists != 0)
			{
				Console.Error.WriteLine($"Table '{parsedTable.Name}' already exists, so it will not be created.");
			}
			else if (ImportOptions.NoCreate)
			{
				Console.Error.WriteLine($"Table '{parsedTable.Name}' will be skipped as it does not exist.");
				return (false, parsedTable.Name, columnInfo);
			}
			else
			{
				command.CommandText = createTableSql;
				await command.ExecuteNonQueryAsync();

				foreach (var index in removedIndexes)
				{
					PendingIndexes.Add(new(
						$"Creating index {parsedTable.Name}.{index.Name}...",
						parsedTable.Name,
						index.Name,
						false,
						$"ALTER TABLE `{parsedTable.Name}` ADD {index.ToSql()};"));
				}

				foreach (var fk in removedFks)
				{
					PendingIndexes.Add(new(
						$"Creating foreign key {parsedTable.Name}.{fk.Name}...",
						parsedTable.Name,
						fk.Name,
						true,
						$"ALTER TABLE `{parsedTable.Name}` ADD {fk.ToSql()};"));
				}
			}
		}

		return (true, parsedTable.Name, columnInfo);
	}

	protected async Task PerformParallelImport(ImportOptions importOptions, ulong? approxCount, string tableName,
		ColumnInfo[] columns, Func<MySqlConnection> createConnection)
	{
		var transactionSemaphore = new AsyncSemaphore(1);

		long processedRowCount = 0;

		void writeProgress()
		{
			Console.Error.Write("\u001b[1000D"); // move cursor to the left

			double percentage = 100 * (processedRowCount / (double)approxCount.GetValueOrDefault());

			if (percentage > 100 || double.IsNaN(percentage))
				percentage = 100;

			Console.Error.Write($"{tableName} - {processedRowCount:N0} / ~{approxCount?.ToString("N0") ?? "?"} - ({percentage:N2}%)");
		}

		var cts = new CancellationTokenSource();

		bool printProgress = !Console.IsErrorRedirected;

		if (printProgress)
			_ = Task.Run(async () =>
			{
				while (!cts.IsCancellationRequested)
				{
					writeProgress();
					await Task.Delay(1000);
				}
			});

		var workerTasks = new List<Task>();

		var sqlCommandChannel = Channel.CreateBounded<string>(2);
		var pipeWriters = new PipeWriter[importOptions.ParallelThreads];

		var blobColumnDictionary = new Dictionary<string, string>();

		foreach (var column in columns)
		{
			if (column.type == "BLOB" || column.type.Contains("BIT"))
			{
				blobColumnDictionary[column.name] = $"@var{blobColumnDictionary.Count + 1}";
			}
		}

		workerTasks.AddRange(Enumerable.Range(0, importOptions.ParallelThreads).Select(i => Task.Run(async () =>
		{
			await using var connection = createConnection();

			Task sendCommand(string commandText)
			{
				using var command = connection.CreateCommand();

				command.CommandTimeout = 9999999;
				command.CommandText = commandText;

				return command.ExecuteNonQueryAsync();
			}

			string commandText = null;

			try
			{
				using (var @lock = await transactionSemaphore.LockAsync())
					await sendCommand("SET SESSION time_zone = \"+00:00\"; SET SESSION autocommit=0; SET SESSION UNIQUE_CHECKS=0; SET SESSION FOREIGN_KEY_CHECKS=0; START TRANSACTION;");

				if (importOptions.ImportMechanism == ImportMechanism.SqlStatements)
				{
					await foreach (var text in sqlCommandChannel.Reader.ReadAllAsync())
					{
						commandText = text;

						await sendCommand(commandText);
					}
				}
				else if (importOptions.ImportMechanism == ImportMechanism.LoadDataInfile)
				{
					var pipeline = new Pipe(new PipeOptions(pauseWriterThreshold: 1 * 1024 * 1024, resumeWriterThreshold: 512 * 1024));

					pipeWriters[i] = pipeline.Writer;

					var loader = new MySqlBulkLoader(connection)
					{
						CharacterSet = "utf8mb4",
						Local = true,
						ConflictOption = importOptions.InsertIgnore ? MySqlBulkLoaderConflictOption.Ignore : MySqlBulkLoaderConflictOption.None,
						FieldQuotationOptional = true,
						FieldQuotationCharacter = '\"',
						EscapeCharacter = '\\',
						LineTerminator = "\n",
						FieldTerminator = ",",
						TableName = $"`{tableName}`",
						NumberOfLinesToSkip = 1,
						SourceStream = pipeline.Reader.AsStream(true)
					};

					loader.Columns.AddRange(columns.Select(x => blobColumnDictionary.GetValueOrDefault(x.name, $"`{x.name}`")));
					loader.Expressions.AddRange(blobColumnDictionary.Select(x =>
					{
						var type = columns.First(y => y.name == x.Key).type;

						if (type == "BLOB")
							return $"`{x.Key}`=FROM_BASE64({x.Value})";
						if (type.Contains("BIT"))
							return $"`{x.Key}`=CAST({x.Value} as signed)";

						throw new Exception("Unexpected CSV type conversion");
					}));

					await loader.LoadAsync();
					await pipeline.Reader.CompleteAsync();
				}

				commandText = null;

				using (var @lock = await transactionSemaphore.LockAsync())
					await sendCommand("COMMIT;");
			}
			catch (Exception ex)
			{
				if (commandText != null)
					Console.Error.WriteLine(commandText);

				Console.Error.WriteLine(ex);
			}
		})).ToArray());


		if (importOptions.ImportMechanism == ImportMechanism.SqlStatements)
		{
			try
			{
				while (true)
				{
					var (rowCount, canContinue, sqlQuery) = ReadDataSql(tableName, columns);

					processedRowCount += rowCount;

					if (sqlQuery == null)
						break;

					await sqlCommandChannel.Writer.WriteAsync(sqlQuery);

					if (!canContinue)
						break;
				}
			}
			catch (Exception ex)
			{
				Console.Error.WriteLine($"Error when reading data source");
				Console.Error.WriteLine(ex.ToString());
			}

			sqlCommandChannel.Writer.Complete();
		}
		else if (importOptions.ImportMechanism == ImportMechanism.LoadDataInfile)
		{
			workerTasks.Add(Task.Run(async () =>
			{
				var flushTasks = new Task[pipeWriters.Length];

				try
				{
					while (true)
					{
						int writerIndex = 0;
						PipeWriter writer;

						while (true)
						{
							bool found = false;
							for (int i = 0; i < pipeWriters.Length; i++)
							{
								if (pipeWriters[i] == null)
									continue;

								if (flushTasks[i] != null && flushTasks[i].IsFaulted)
									Console.WriteLine(flushTasks[i].Exception);

								if (flushTasks[i] == null || flushTasks[i].IsCompleted)
								{
									writerIndex = i;
									found = true;
									break;
								}
							}

							if (found)
							{
								writer = pipeWriters[writerIndex];
								break;
							}

							// all writers are currently busy.
							await Task.Delay(2);
						}

						var (rowCount, canContinue) = ReadDataCsv(writer, tableName, columns);

						if (rowCount == 0)
							break;

						//await writer.FlushAsync();
						flushTasks[writerIndex] = Task.Run(async () => await writer.FlushAsync());

						processedRowCount += rowCount;

						if (!canContinue)
							break;
					}

					foreach (var writer in pipeWriters)
						if (writer != null)
							await writer.CompleteAsync();
				}
				catch (Exception ex)
				{
					Console.Error.WriteLine($"Error when reading data source");
					Console.Error.WriteLine(ex.ToString());

					foreach (var writer in pipeWriters)
						if (writer != null)
							await writer.CompleteAsync(ex);
				}
			}));
		}

		await Task.WhenAll(workerTasks);

		if (printProgress)
		{
			cts.Cancel();
			writeProgress();
			Console.Error.WriteLine();
		}
	}

	protected abstract (int rows, bool canContinue) ReadDataCsv(PipeWriter pipeWriter, string tableName, ColumnInfo[] columns);
	protected abstract (int rows, bool canContinue, string sqlCommand) ReadDataSql(string tableName, ColumnInfo[] columns);
	public abstract void Dispose();
}