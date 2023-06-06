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

        var queue = new AsyncProducerConsumerQueue<string>(4);
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

    private StringBuilder queryBuilder = new StringBuilder(2_000_000);

    private string GetNextInsertBatch(StreamReader reader)
    {
        bool initialInsert = false;

        queryBuilder.Clear();

        while (true)
        {
            if (queryBuilder.Length >= 1600000)
                break;

            var line = reader.ReadLine();

            if (line == null)
                break;

            if (!line.StartsWith("INSERT INTO"))
                continue;

            if (!initialInsert)
            {
                queryBuilder.Append(line.TrimEnd(';'));
                initialInsert = true;
            }
            else
            {
                int lengthOffset = line.EndsWith(';') ? -1 : 0;

                const string searchString = "VALUES ";
                int posOffset = line.IndexOf(searchString) + searchString.Length;

                int length = (line.Length - posOffset) + lengthOffset;

                queryBuilder.Append(',');
                queryBuilder.Append(line, posOffset, length);
            }
        }

        if (queryBuilder.Length == 0)
            return null;

        queryBuilder.Append(';');

        return queryBuilder.ToString();
    }
}