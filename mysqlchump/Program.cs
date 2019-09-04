using System;
using System.Data;
using System.IO;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;

namespace mysqlchump
{
	class Program
	{
		static async Task Main(string[] args)
		{
			var arguments = Arguments.Parse(args);

			if (args.Length == 0 || arguments.Switches.ContainsKey("help"))
			{
				Console.WriteLine("Usage: mysqlchump --table <table> --connectionString <connection string> [--select <select sql statement>] [--append] <output file>");
				return;
			}

			string table = arguments["table"];
			string connectionString = arguments["connectionString"];
			string selectStatement = arguments["select"] ?? $"SELECT * FROM `{table}`";
			string outputFile = arguments.Values[0];

			if (table == null)
				throw new ArgumentException("Expecting argument: --table");

			if (connectionString == null)
				throw new ArgumentException("Expecting argument: --connectionString");

			if (outputFile == null)
				throw new ArgumentException("Expecting argument: output file");

			bool append = arguments.Switches.ContainsKey("append");


			using (var fileStream = new FileStream(outputFile, append ? FileMode.Append : FileMode.Create))
			using (var connection = new MySqlConnection(connectionString))
			{
				await connection.OpenAsync();

				var dumper = new Dumper(connection);
				await dumper.WriteTableSchemaAsync(table, fileStream);

				using (var transaction = await connection.BeginTransactionAsync(IsolationLevel.RepeatableRead))
				{
					await dumper.WriteInsertQueries(table, selectStatement, fileStream, transaction);
				}
			}
		}
	}
}