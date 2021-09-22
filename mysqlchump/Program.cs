using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Threading.Tasks;
using MySqlConnector;

namespace mysqlchump
{
	class Program
	{
		static async Task Main(string[] args)
		{
			var arguments = Arguments.Parse<MySqlChumpArguments>(args);

			if (args.Length == 0 || arguments.Help)
			{
				Console.WriteLine("Usage:");
				Console.WriteLine("mysqlchump --table <table> --connectionString <connection string> [--select <select sql statement>] [--format mysql|csv] [--append] (--stdout | <output file>)");
				Console.WriteLine("mysqlchump --tables <comma separated table names> --connectionString <connection string> [--select <select sql statement>] [--format mysql|csv] [<output folder>]");
				return;
			}
			
			string selectStatement = arguments.SelectQuery ?? "SELECT * FROM `{table}`";

			string[] tables = arguments.Tables?.Split(',', StringSplitOptions.RemoveEmptyEntries);

			if (!arguments.StdOut && arguments.Values.Count < 1)
				throw new ArgumentException("Expecting argument: output file");

			if (arguments.Table == null && tables == null)
				throw new ArgumentException("Expecting argument: --table or --tables");

			if (arguments.ConnectionString == null)
				throw new ArgumentException("Expecting argument: --connectionString");

			if (arguments.Format.ToLower() != "csv" && arguments.Format.ToLower() != "mysql")
				throw new ArgumentException($"Unknown format: {arguments.Format}\r\nOnly 'csv' and 'mysql' are accepted");

			bool isCsv = arguments.Format.ToLower() == "csv";

			bool singleTableMode = arguments.Table != null;

			if (singleTableMode)
			{
				await DumpSingleTable(arguments.Table, isCsv, selectStatement, arguments.ConnectionString, arguments.Values[0], arguments.StdOut, arguments.Append);
			}
			else
			{
				string outputFolderPath = arguments.Values.Count > 0
					? arguments.Values[0]
					: Environment.CurrentDirectory;

				await DumpMultipleTables(tables, isCsv, selectStatement, arguments.ConnectionString, outputFolderPath);
			}
		}

		static async Task DumpTableToStream(string table, bool isCsv, string selectStatement, MySqlConnection connection, Stream stream)
		{
			BaseDumper dumper = isCsv
				? (BaseDumper)new CsvDumper(connection)
				: (BaseDumper)new MySqlDumper(connection);
			
			await dumper.WriteTableSchemaAsync(table, stream);

			await using (var transaction = await connection.BeginTransactionAsync(IsolationLevel.RepeatableRead))
			{
				await dumper.WriteInsertQueries(table, selectStatement, stream, transaction);
			}
		}

		static async Task DumpSingleTable(string table, bool isCsv, string selectStatement, string connectionString, string outputFile, bool standardOut, bool append)
		{
			var stream = standardOut
				? Console.OpenStandardOutput()
				: new FileStream(outputFile, append ? FileMode.Append : FileMode.Create);

			string formattedQuery = selectStatement.Replace("{table}", table);

			await using (var connection = new MySqlConnection(connectionString))
			{
				await connection.OpenAsync();

				await DumpTableToStream(table, isCsv, formattedQuery, connection, stream);
			}

			await stream.FlushAsync();

			if (!standardOut)
				await stream.DisposeAsync();
		}

		static async Task DumpMultipleTables(string[] tables, bool isCsv, string selectStatement, string connectionString, string outputFolder)
		{
			var dateTime = DateTime.Now;

			await using var connection = new MySqlConnection(connectionString);

			await connection.OpenAsync();

			foreach (var table in tables)
			{
				string dumpFileName = $"dump_{dateTime:yyyy-MM-dd_hh-mm-ss}_{table}.sql";

				await using (var stream = new FileStream(Path.Combine(outputFolder, dumpFileName), FileMode.CreateNew))
				{
					string formattedQuery = selectStatement.Replace("{table}", table);

					await DumpTableToStream(table, isCsv, formattedQuery, connection, stream);
				}
			}
		}


		private class MySqlChumpArguments : IArgumentCollection
		{
			public IList<string> Values { get; set; }
			
			[CommandDefinition("Table", "t", "table")]
			public string Table { get; set; }
			
			[CommandDefinition("Tables", null, "tables")]
			public string Tables { get; set; }

			[CommandDefinition("Connection String", "c", "connectionString")]
			public string ConnectionString { get; set; }

			[CommandDefinition("Output format", null, "format")]
			public string Format { get; set; } = "mysql";

			[CommandDefinition("Select query", "s", "select")]
			public string SelectQuery { get; set; }

			[CommandDefinition("Append", null, "append")]
			public bool Append { get; set; }

			[CommandDefinition("Standard out", null, "stdout")]
			public bool StdOut { get; set; }

			[CommandDefinition("Help", "h", "help")]
			public bool Help { get; set; }
		}
	}
}