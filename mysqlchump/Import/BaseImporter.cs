using Nito.AsyncEx;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using MySqlConnector;

namespace mysqlchump.Import;

internal class BaseImporter
{
	protected async Task DoParallelInserts(int maxConcurrency, ulong? approxCount, string tableName,
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
			connection.Open();

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
}