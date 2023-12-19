using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using MySqlConnector;

namespace mysqlchump.Import;

internal class MysqlRawImporter
{
	public async Task ImportAsync(Stream dataStream, Func<MySqlConnection> createConnection)
	{
		using var reader = new StreamReader(dataStream);

		await using var connection = createConnection();
		connection.Open();

		Task sendCommand(string commandText)
		{
			using var command = connection.CreateCommand();

			command.CommandTimeout = 9999999;
			command.CommandText = commandText;

			return command.ExecuteNonQueryAsync();
		}
		
		var commandText = reader.ReadToEnd();

		await sendCommand(commandText);
	}
}