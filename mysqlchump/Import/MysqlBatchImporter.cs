using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using MySqlConnector;

namespace mysqlchump.Import;

internal class MysqlBatchImporter
{
    public async Task ImportAsync(Stream dataStream, MySqlConnection connection)
    {
        using var reader = new StreamReader(dataStream);

        Task<string> getNext() => Task.Run(() => GetNextInsertBatch(reader));

        Task sendCommand(string commandText)
        {
            using var command = connection.CreateCommand();

            command.CommandText = commandText;

            return command.ExecuteNonQueryAsync();
        }

        await sendCommand("SET autocommit=0; SET UNIQUE_CHECKS=0; SET sql_log_bin=OFF; ALTER INSTANCE DISABLE INNODB REDO_LOG; START TRANSACTION;");

        var currentBatch = await getNext();
        

        while (true)
        {
            var nextBatchTask = getNext();

            if (currentBatch != null)
                await sendCommand(currentBatch);

            var nextBatch = await nextBatchTask;

            if (nextBatch == null)
                break;

            currentBatch = nextBatch;
        }

        await sendCommand("COMMIT; SET UNIQUE_CHECKS=1; ALTER INSTANCE ENABLE INNODB REDO_LOG;");
    }

    private string GetNextInsertBatch(StreamReader reader)
    {
        var builder = new StringBuilder();

        bool initialInsert = false;

        while (true)
        {
            if (builder.Length >= 16000)
                break;

            var line = reader.ReadLine();

            //Console.WriteLine($"[{line}]");

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

            //Console.WriteLine(builder.Length);
        }

        if (builder.Length == 0)
            return null;

        builder.Append(';');

        //Console.WriteLine(builder.Length);

        return builder.ToString();
    }
}