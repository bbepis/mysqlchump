using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MySqlConnector;
using Nito.AsyncEx;

namespace mysqlchump.Import;

internal class MysqlBatchImporter
{
    public async Task ImportAsync(Stream dataStream, Func<MySqlConnection> createConnection)
    {
        using var reader = new StreamReader(dataStream);

        const int concurrentLimit = 16;

        var queue = new AsyncProducerConsumerQueue<string>(concurrentLimit);
        var commitSemaphore = new AsyncSemaphore(1);

        var enqueueTask = Task.Run(async () =>
        {
            while (true)
            {
                var result = GetNextInsertBatch(reader);

                if (result == null)
                    break;

                await queue.EnqueueAsync(result);
            }

            queue.CompleteAdding();
        });

        var sendTasks = Enumerable.Range(0, concurrentLimit).Select(async _ =>
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

            await sendCommand("SET SESSION time_zone = \"+00:00\"; SET autocommit=0; SET UNIQUE_CHECKS=0; SET sql_log_bin=OFF; START TRANSACTION;");

            while (await queue.OutputAvailableAsync())
            {
                var commandText = await queue.DequeueAsync();

                await sendCommand(commandText);
            }

            using (var @lock = await commitSemaphore.LockAsync())
                await sendCommand("COMMIT; SET UNIQUE_CHECKS=1;");
        }).ToArray();

        await Task.WhenAll(sendTasks.Append(enqueueTask));
    }

    private string GetNextInsertBatch(StreamReader reader)
    {
        var builder = new StringBuilder();

        bool initialInsert = false;

        while (true)
        {
            if (builder.Length >= 1600000)
                break;

            var line = reader.ReadLine();

            if (line == null)
                break;

            if (!line.StartsWith("INSERT INTO"))
                continue;

            if (!initialInsert)
            {
                builder.Append(line.TrimEnd(';'));
                initialInsert = true;
            }
            else
            {
                int lengthOffset = line.EndsWith(';') ? -1 : 0;

                const string searchString = "VALUES ";
                int posOffset = line.IndexOf(searchString) + searchString.Length;

                int length = (line.Length - posOffset) + lengthOffset;

                builder.Append(',');
                builder.Append(line, posOffset, length);
            }
        }

        if (builder.Length == 0)
            return null;

        builder.Append(';');

        return builder.ToString();
    }
}