using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using MySqlConnector;
using NArgs;

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

			if (!arguments.StdOut && arguments.Values.Count < 1)
				throw new ArgumentException("Expecting argument: output file");

			if (arguments.Table == null && arguments.Tables == null)
				throw new ArgumentException("Expecting argument: --table or --tables");

			if (string.IsNullOrWhiteSpace(arguments.ConnectionString))
				throw new ArgumentException("Expecting argument: --connectionString");

			bool singleTableMode = arguments.Table != null;

			try
			{
				if (singleTableMode)
				{
					await DumpSingleTable(arguments.Table, arguments.Format, selectStatement, arguments.ConnectionString, arguments.Values[0], arguments.StdOut, arguments.Append);
				}
				else
				{
					string outputFolderPath = arguments.Values.Count > 0
						? arguments.Values[0]
						: Environment.CurrentDirectory;

					await DumpMultipleTables(arguments.Tables, arguments.Format, selectStatement, arguments.ConnectionString, outputFolderPath);
				}
			}
			catch (Exception ex)
			{
				var errorBuilder = new StringBuilder();
				errorBuilder.AppendLine("Unrecoverable error");
				errorBuilder.AppendLine(ex.ToStringDemystified());

				if (arguments.StdOut)
				{
					var stderr = new StreamWriter(Console.OpenStandardError());
					stderr.Write(errorBuilder.ToString());
				}
				else
				{
					Console.Write(errorBuilder.ToString());
				}
			}
		}

		static async Task DumpTableToStream(string table, OutputFormatEnum outputFormat, string selectStatement, MySqlConnection connection, Stream stream)
		{
			BaseDumper dumper = outputFormat switch
			{
				OutputFormatEnum.mysql => new MySqlDumper(connection),
				OutputFormatEnum.postgres => new PostgresDumper(connection),
				OutputFormatEnum.csv => new CsvDumper(connection),
				_ => throw new ArgumentOutOfRangeException(nameof(outputFormat), outputFormat, null)
			};
			
			await dumper.WriteTableSchemaAsync(table, stream);

			await using (var transaction = await connection.BeginTransactionAsync(IsolationLevel.RepeatableRead))
			{
				await dumper.WriteInsertQueries(table, selectStatement, stream, transaction);
			}
		}

		static async Task DumpSingleTable(string table, OutputFormatEnum outputFormat, string selectStatement, string connectionString, string outputFile, bool standardOut, bool append)
		{
			var stream = standardOut
				? Console.OpenStandardOutput()
				: new FileStream(outputFile, append ? FileMode.Append : FileMode.Create);

			string formattedQuery = selectStatement.Replace("{table}", table);

			await using (var connection = new MySqlConnection(connectionString))
			{
				await connection.OpenAsync();

				await DumpTableToStream(table, outputFormat, formattedQuery, connection, stream);
			}

			await stream.FlushAsync();

			if (!standardOut)
				await stream.DisposeAsync();
		}

		private static async Task<string[]> GetAllTablesFromDatabase(MySqlConnection connection)
		{
			string databaseName = connection.Database;

			const string commandText =
				"SELECT TABLE_NAME " +
				"FROM INFORMATION_SCHEMA.TABLES " +
				"WHERE TABLE_SCHEMA = @databasename";

			await using var createTableCommand = new MySqlCommand(commandText, connection)
				.SetParam("@databasename", databaseName);

			await using var reader = await createTableCommand.ExecuteReaderAsync();

			List<string> tableList = new List<string>();

			while (await reader.ReadAsync())
				tableList.Add(reader.GetString(0));

			return tableList.ToArray();
		}

		static async Task DumpMultipleTables(string multiTableArgument, OutputFormatEnum outputFormat, string selectStatement, string connectionString, string outputFolder)
		{
			var dateTime = DateTime.Now;

			await using var connection = new MySqlConnection(connectionString);

			await connection.OpenAsync();

			string[] tables;

			if (multiTableArgument == "*")
			{
				tables = await GetAllTablesFromDatabase(connection);
			}
			else
			{
				tables = multiTableArgument.Split(',', StringSplitOptions.RemoveEmptyEntries);
			}

			foreach (var table in tables)
			{
				string dumpFileName = $"dump_{dateTime:yyyy-MM-dd_hh-mm-ss}_{table}.sql";

				await using (var stream = new FileStream(Path.Combine(outputFolder, dumpFileName), FileMode.CreateNew))
				{
					string formattedQuery = selectStatement.Replace("{table}", table);

					await DumpTableToStream(table, outputFormat, formattedQuery, connection, stream);
				}
			}
		}

		internal enum OutputFormatEnum
		{
			mysql,
			postgres,
			csv
		}

		private class MySqlChumpArguments : IArgumentCollection
		{
			public IList<string> Values { get; set; }
			
			[CommandDefinition("t", "table", Description = "Specify the table to be dumped.")]
			public string Table { get; set; }
			
			[CommandDefinition(null, "tables", Description = "Specify the tables to be dumped, in a comma-delimited string.")]
			public string Tables { get; set; }

			[CommandDefinition("c", "connectionString", Description = "The connection string used to connect to the database.")]
			public string ConnectionString { get; set; }

			[CommandDefinition("f", "format", Description = "The format to output when generating the dump.")]
			public OutputFormatEnum Format { get; set; } = OutputFormatEnum.mysql;

			[CommandDefinition("s", "select", Description = "The select query to use when filtering rows/columns. If not specified, will dump the entire table.\nCurrent table is specified with \"{table}\"")]
			public string SelectQuery { get; set; }

			[CommandDefinition(null, "append", Description = "If specified, will append to the specified file instead of overwriting")]
			public bool Append { get; set; }

			[CommandDefinition(null, "stdout", Description = "Pipe to stdout instead of writing to a file")]
			public bool StdOut { get; set; }

			[CommandDefinition("h", "help", Description = "Display this help message")]
			public bool Help { get; set; }
		}
	}
}