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
				Console.WriteLine("Usage:");
				Console.WriteLine("mysqlchump --table <table> --connectionString <connection string> [--select <select sql statement>] [--append] (--stdout | <output file>)");
				Console.WriteLine("mysqlchump --tables <comma separated table names> --connectionString <connection string> [--select <select sql statement>] [<output folder>]");
				return;
			}

			bool append = arguments.Switches.ContainsKey("append");
			bool standardOut = arguments.Switches.ContainsKey("stdout");

			string table = arguments["table"];
			string connectionString = arguments["connectionString"];
			string selectStatement = arguments["select"] ?? "SELECT * FROM `{table}`";
			string outputFile = null;

			string[] tables = arguments["tables"]?.Split(',', StringSplitOptions.RemoveEmptyEntries);
			
			if (!standardOut)
				outputFile = arguments.Values[0];

			if (table == null && tables == null)
				throw new ArgumentException("Expecting argument: --table or --tables");

			if (connectionString == null)
				throw new ArgumentException("Expecting argument: --connectionString");

			bool singleTableMode = table != null;

			if (singleTableMode)
			{
				if (!standardOut && outputFile == null)
					throw new ArgumentException("Expecting argument: output file");

				await DumpSingleTable(table, selectStatement, connectionString, outputFile, standardOut, append);
			}
			else
			{
				string outputFolderPath = arguments.Values.Count > 0
					? arguments.Values[0]
					: Environment.CurrentDirectory;

				await DumpMultipleTables(tables, selectStatement, connectionString, outputFolderPath);
			}
		}

		static async Task DumpTableToStream(string table, string selectStatement, MySqlConnection connection, Stream stream)
		{
			var dumper = new Dumper(connection);
			await dumper.WriteTableSchemaAsync(table, stream);

			using (var transaction = await connection.BeginTransactionAsync(IsolationLevel.RepeatableRead))
			{
				await dumper.WriteInsertQueries(table, selectStatement, stream, transaction);
			}
		}

		static async Task DumpSingleTable(string table, string selectStatement, string connectionString, string outputFile, bool standardOut, bool append)
		{
			var stream = standardOut
				? Console.OpenStandardOutput()
				: new FileStream(outputFile, append ? FileMode.Append : FileMode.Create);

			string formattedQuery = selectStatement.Replace("{table}", table);

			using (var connection = new MySqlConnection(connectionString))
			{
				await connection.OpenAsync();

				await DumpTableToStream(table, formattedQuery, connection, stream);
			}

			await stream.FlushAsync();

			if (!standardOut)
				stream.Dispose();
		}

		static async Task DumpMultipleTables(string[] tables, string selectStatement, string connectionString, string outputFolder)
		{
			var dateTime = DateTime.Now;

			using (var connection = new MySqlConnection(connectionString))
			{
				await connection.OpenAsync();

				foreach (var table in tables)
				{
					string dumpFileName = $"dump_{dateTime:yyyy-MM-dd_hh-mm-ss}_{table}.sql";

					using (var stream = new FileStream(Path.Combine(outputFolder, dumpFileName), FileMode.CreateNew))
					{
						string formattedQuery = selectStatement.Replace("{table}", table);

						await DumpTableToStream(table, formattedQuery, connection, stream);
					}
				}
			}
		}
	}
}