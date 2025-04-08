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

namespace mysqlchump.Import;

internal enum ImportMechanism
{
	SqlStatements,
	LoadDataInfile
}

internal class ImportOptions
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
}

internal class BaseImporter
{
	protected async Task DoParallelSqlInserts(int maxConcurrency, ulong? approxCount, string tableName,
		Func<MySqlConnection> createConnection, Func<Channel<string>, Action<long>, Task> producerTask)
	{
		var channel = Channel.CreateBounded<string>(2);
		var transactionSemaphore = new AsyncSemaphore(1);

		long processedRowCount = 0;

		void writeProgress()
		{
			if (OperatingSystem.IsWindows())
				Console.CursorLeft = 0;
			else
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

		var enqueueTask = Task.Run(() => producerTask(channel, i => processedRowCount += i));

		var sendTasks = Enumerable.Range(0, maxConcurrency).Select(async _ =>
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
					await sendCommand("SET SESSION time_zone = \"+00:00\"; SET autocommit=0; SET UNIQUE_CHECKS=0; START TRANSACTION;");

				await foreach (var text in channel.Reader.ReadAllAsync())
				{
					commandText = text;

					await sendCommand(commandText);
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
		}).ToArray();

		await Task.WhenAll(sendTasks.Append(enqueueTask));

		if (printProgress)
		{
			cts.Cancel();
			writeProgress();
			Console.Error.WriteLine();
		}
	}

	protected async Task DoParallelCsvImports(ImportOptions options, ulong? approxCount, string tableName,
        List<(string columnName, string type)> columns,
        Func<MySqlConnection> createConnection, Action<PipeWriter[], Action<long>> producerTask)
	{
		var transactionSemaphore = new AsyncSemaphore(1);

		long processedRowCount = 0;

		void writeProgress()
		{
			lock (Console.Error)
			{
				if (OperatingSystem.IsWindows())
					Console.CursorLeft = 0;
				else
					Console.Error.Write("\u001b[1000D"); // move cursor to the left

				double percentage = 100 * (processedRowCount / (double)approxCount.GetValueOrDefault());

				if (percentage > 100 || double.IsNaN(percentage))
					percentage = 100;

				Console.Error.Write($"{tableName} - {processedRowCount:N0} / ~{approxCount?.ToString("N0") ?? "?"} - ({percentage:N2}%)");
			}
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

		var blobColumnDictionary = new Dictionary<string, string>();

		foreach (var column in columns)
		{
			if (column.type == "BLOB" || column.type.Contains("BIT"))
			{
				blobColumnDictionary[column.columnName] = $"@var{blobColumnDictionary.Count + 1}";
			}
		}

		var pipelineWriters = new PipeWriter[options.ParallelThreads];

		var sendTasks = Enumerable.Range(0, options.ParallelThreads).Select(async i =>
        {
            var pipeline = new Pipe(new PipeOptions(pauseWriterThreshold: 1 * 1024 * 1024, resumeWriterThreshold: 512 * 1024));

			pipelineWriters[i] = pipeline.Writer;

            await using var connection = createConnection();
            var loader = new MySqlBulkLoader(connection)
            {
                CharacterSet = "utf8mb4",
                Local = true,
                ConflictOption = options.InsertIgnore ? MySqlBulkLoaderConflictOption.Ignore : MySqlBulkLoaderConflictOption.None,
                FieldQuotationOptional = true,
                FieldQuotationCharacter = '\"',
                EscapeCharacter = '\\',
                LineTerminator = "\n",
                FieldTerminator = ",",
                TableName = $"`{tableName}`",
				NumberOfLinesToSkip = 1,
                SourceStream = pipeline.Reader.AsStream(true)
            };

            loader.Columns.AddRange(columns.Select(x => blobColumnDictionary.GetValueOrDefault(x.columnName, $"`{x.columnName}`")));
            loader.Expressions.AddRange(blobColumnDictionary.Select(x =>
			{
				var type = columns.First(y => y.columnName == x.Key).type;

				if (type == "BLOB")
					return $"`{x.Key}`=FROM_BASE64({x.Value})";
				if (type.Contains("BIT"))
					return $"`{x.Key}`=CAST({x.Value} as signed)";

				throw new Exception("Unexpected CSV type conversion");
			}));

            Task sendCommand(string commandText)
            {
                using var command = connection.CreateCommand();

                command.CommandTimeout = 9999999;
                command.CommandText = commandText;

                return command.ExecuteNonQueryAsync();
            }

            try
            {
                await sendCommand("SET SESSION time_zone = \"+00:00\"; SET autocommit=0; SET UNIQUE_CHECKS=0;");

				MySqlTransaction transaction;

                using (var @lock = await transactionSemaphore.LockAsync())
					transaction = await connection.BeginTransactionAsync(System.Data.IsolationLevel.Snapshot);

				await loader.LoadAsync();
                await pipeline.Reader.CompleteAsync();

				using (var @lock = await transactionSemaphore.LockAsync())
                    await transaction.CommitAsync();
            }
			catch (Exception ex)
            {
                Console.WriteLine(ex);
                await pipeline.Reader.CompleteAsync(ex);
            }
        }).ToArray();

		var enqueueTask = Task.Run(() => producerTask(pipelineWriters, i => processedRowCount += i));

        await Task.WhenAll(sendTasks.Append(enqueueTask));

        if (printProgress)
		{
			cts.Cancel();
			writeProgress();
            Logging.WriteLine();
		}
	}
}