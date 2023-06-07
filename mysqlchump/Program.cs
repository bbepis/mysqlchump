using System;
using System.Collections.Generic;
using System.CommandLine;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using mysqlchump.Export;
using mysqlchump.Import;
using MySqlConnector;

namespace mysqlchump;

class Program
{
	static async Task<int> Main(string[] args)
	{
		return await CreateRootCommand().InvokeAsync(args);
	}

	private static RootCommand CreateRootCommand()
	{
		var rootCommand = new RootCommand("MySQLChump data transfer tool v1.2");

		var tableOption = new Option<string[]>(new[] { "--table", "-t" }, "The table to be dumped. Can be specified multiple times, or passed '*' to dump all tables.");
		var tablesOption = new Option<string>("--tables", "A comma-separated list of tables to dump.");
		var connectionStringOption = new Option<string>("--connection-string", "A connection string to use to connect to the database. Not required if -s -d -u -p have been specified");
		var serverOption = new Option<string>(new[] { "--server", "-s" }, () => "localhost", "The server to connect to.");
		var portOption = new Option<ushort>(new[] { "--port", "-o" }, () => 3306, "The port of the server to connect to.");
		var databaseOption = new Option<string>(new[] { "--database", "-d" }, "The database to use when dumping.");
		var usernameOption = new Option<string>(new[] { "--username", "-u" }, "The username to connect with.");
		var passwordOption = new Option<string>(new[] { "--password", "-p" }, "The password to connect with.");
		var outputFormatOption = new Option<OutputFormatEnum>(new[] { "--output-format", "-f" }, () => OutputFormatEnum.mysql, "The output format to create when dumping.");
		var selectOption = new Option<string>(new[] { "--select", "-q" }, () => "SELECT * FROM `{table}`", "The select query to use when filtering rows/columns. If not specified, will dump the entire table.\nTable being examined is specified with \"{table}\".");
		var noCreationOption = new Option<bool>(new[] { "--no-creation" }, "Don't output table creation statements.");
		var truncateOption = new Option<bool>(new[] { "--truncate" }, "Prepend data insertions with a TRUNCATE command.");
		var appendOption = new Option<bool>(new[] { "--append" }, "If specified, will append to the specified file instead of overwriting.");
		var outputFileArgument = new Argument<string>("output location", "Specify either a file or a folder to output to. '-' for stdout, otherwise defaults to creating files in the current directory") { Arity = ArgumentArity.ZeroOrOne };

        var exportCommand = new Command("export", "Exports data from a database")
		{
			tableOption, tablesOption, connectionStringOption, serverOption, portOption, databaseOption, usernameOption, passwordOption, outputFormatOption, selectOption, noCreationOption, truncateOption, appendOption, outputFileArgument
		};

        exportCommand.SetHandler(async context =>
		{
			var result = context.ParseResult;

			var tables = (result.GetValueForOption(tablesOption)?.Split(',') ?? Array.Empty<string>()).Select(x => x.Trim())
                .Union(result.GetValueForOption(tableOption) ?? Array.Empty<string>()).ToArray();

			if (tables.Length == 0)
			{
				Console.WriteLine("No tables specified, exiting");
				context.ExitCode = 1;
				return;
			}

			string connectionString = result.GetValueForOption(connectionStringOption);

			if (string.IsNullOrWhiteSpace(connectionString))
			{
				var csBuilder = new MySqlConnectionStringBuilder();
				csBuilder.Server = result.GetValueForOption(serverOption);
				csBuilder.Port = result.GetValueForOption(portOption);
				csBuilder.Database = result.GetValueForOption(databaseOption);
				csBuilder.UserID = result.GetValueForOption(usernameOption);
				csBuilder.Password = result.GetValueForOption(passwordOption);

				connectionString = csBuilder.ToString();
			}

            context.ExitCode = await ExportMainAsync(tables, connectionString, result.GetValueForOption(outputFormatOption),
				result.GetValueForOption(selectOption), result.GetValueForOption(noCreationOption), result.GetValueForOption(truncateOption),
				result.GetValueForOption(appendOption), result.GetValueForArgument(outputFileArgument));
		});

		rootCommand.Add(exportCommand);
		
        var inputFormatOption = new Option<InputFormatEnum>(new[] { "--input-format", "-f" }, () => InputFormatEnum.mysqlForceBatch, "The input format to use when importing.");
        var importTableOption = new Option<string>(new[] { "--table", "-t" }, "The table to import to. Only relevant for CSV data");
        var csvUseHeadersOption = new Option<bool>(new[] { "--csv-use-headers" }, "Use the first row in the CSV as header data to determine column names.");
        var csvImportColumnsOption = new Option<string>(new[] { "--csv-columns" }, "A comma-separated list of columns that the CSV corresponds to. Ignored if --csv-use-headers is specified");
        var csvFixInvalidMysqlOption = new Option<bool>(new[] { "--csv-fix-mysql" }, "Enables a pre-processor to fix invalid CSV files generated by MySQL. Note that enabling this will break functional CSV files");
        var inputFileArgument = new Argument<string>("input file", "Specify a file to read from. Otherwise - for stdin") { Arity = ArgumentArity.ExactlyOne };
        
        var importCommand = new Command("import", "Imports data to a database")
        {
            connectionStringOption, serverOption, portOption, databaseOption, usernameOption, passwordOption, inputFormatOption, importTableOption,
            csvImportColumnsOption, csvUseHeadersOption, csvFixInvalidMysqlOption, inputFileArgument
        };

        importCommand.SetHandler(async context =>
        {
            var result = context.ParseResult;

            string connectionString = result.GetValueForOption(connectionStringOption);

            if (string.IsNullOrWhiteSpace(connectionString))
            {
                var csBuilder = new MySqlConnectionStringBuilder();
                csBuilder.Server = result.GetValueForOption(serverOption);
                csBuilder.Port = result.GetValueForOption(portOption);
                csBuilder.Database = result.GetValueForOption(databaseOption);
                csBuilder.UserID = result.GetValueForOption(usernameOption);
                csBuilder.Password = result.GetValueForOption(passwordOption);

                connectionString = csBuilder.ToString();
            }

            var inputFormat = result.GetValueForOption(inputFormatOption);
            var importTable = result.GetValueForOption(importTableOption);
				
            var csvUseHeaders = result.GetValueForOption(csvUseHeadersOption);
            var csvColumns = result.GetValueForOption(csvImportColumnsOption);

            if (inputFormat == InputFormatEnum.csv && string.IsNullOrWhiteSpace(csvColumns) && !csvUseHeaders)
            {
				Console.WriteLine("Either columns (--csv-columns) or use headers (--csv-use-headers) required for CSV import");
                context.ExitCode = 1;
                return;
            }
            if (inputFormat == InputFormatEnum.csv && string.IsNullOrWhiteSpace(importTable))
            {
				Console.WriteLine("Table (-t) required for CSV import");
                context.ExitCode = 1;
                return;
            }

            context.ExitCode = await ImportMainAsync(connectionString, inputFormat, result.GetValueForArgument(inputFileArgument), importTable,
                (csvUseHeaders || string.IsNullOrWhiteSpace(csvColumns)) ? null : csvColumns.Split(',').Select(x => x.Trim()).ToArray(),
                result.GetValueForOption(csvFixInvalidMysqlOption), csvUseHeaders);
        });

		rootCommand.AddCommand(importCommand);

		return rootCommand;
	}

	static async Task<int> ExportMainAsync(string[] tables, string connectionString, OutputFormatEnum outputFormat, string selectQuery, bool noCreation, bool truncate, bool append, string outputLocation)
	{
		bool stdout = outputLocation == "-";
		string folder = Directory.Exists(outputLocation)
			? outputLocation
			: string.IsNullOrWhiteSpace(outputLocation) ? Directory.GetCurrentDirectory() : null;
		bool folderMode = folder != null;

		Stream currentStream = null;

		try
		{
			var dateTime = DateTime.Now;

			await using var connection = new MySqlConnection(connectionString);

			await connection.OpenAsync();

			await BaseDumper.InitializeConnection(connection);

			if (tables.Any(x => x == "*"))
				tables = await GetAllTablesFromDatabase(connection);

			if (stdout && folder == null)
			{
				currentStream = stdout
					? Console.OpenStandardOutput()
					: new FileStream(outputLocation, append ? FileMode.Append : FileMode.Create);
			}

            foreach (var table in tables)
            {
				if (folderMode)
				{
					currentStream = new FileStream(Path.Combine(folder, $"dump_{dateTime:yyyy-MM-dd_hh-mm-ss}_{table}.sql"), FileMode.CreateNew);
                }

                string formattedQuery = selectQuery.Replace("{table}", table);

                await DumpTableToStream(table, noCreation, truncate, outputFormat, formattedQuery, connection, currentStream);

                if (folderMode)
                {
					await currentStream.DisposeAsync();
                }
            }

			return 0;
		}
		catch (Exception ex)
		{
			var errorBuilder = new StringBuilder();
			errorBuilder.AppendLine("Unrecoverable error");
			errorBuilder.AppendLine(ex.ToStringDemystified());

			if (stdout)
			{
				var stderr = new StreamWriter(Console.OpenStandardError());
				stderr.Write(errorBuilder.ToString());
			}
			else
			{
				Console.Write(errorBuilder.ToString());
			}

			return 1;
		}
		finally
		{
			if (!stdout && currentStream != null)
				await currentStream.DisposeAsync();
		}
	}

	static async Task<int> ImportMainAsync(string connectionString, InputFormatEnum inputFormat, string inputLocation, string table, string[] csvColumns, bool csvFixInvalidMysql, bool csvUseHeaders)
	{
		bool stdin = inputLocation == "-";

		Stream currentStream = stdin
            ? Console.OpenStandardInput()
            : new FileStream(inputLocation, FileMode.Open, FileAccess.Read, FileShare.Read);

		try
		{
            var createConnection = () => new MySqlConnection(connectionString);

			await using (var connection = createConnection())
			{
                // test connection string
			    await connection.OpenAsync();
            }

            if (inputFormat == InputFormatEnum.mysqlForceBatch)
            {
                var importer = new MysqlBatchImporter();
                await importer.ImportAsync(currentStream, createConnection);
            }
			else if (inputFormat == InputFormatEnum.csv)
            {
                var importer = new CsvImporter();
				await importer.ImportAsync(currentStream, table, csvColumns, csvFixInvalidMysql, csvUseHeaders, createConnection);
            }

			return 0;
		}
		catch (Exception ex)
		{
			var errorBuilder = new StringBuilder();
			errorBuilder.AppendLine("Unrecoverable error");
			errorBuilder.AppendLine(ex.ToStringDemystified());
			
			Console.Error.Write(errorBuilder.ToString());

			return 1;
		}
		finally
		{
			if (!stdin)
				await currentStream.DisposeAsync();
		}
	}

	static async Task DumpTableToStream(string table, bool skipSchema, bool truncate, OutputFormatEnum outputFormat, string selectStatement, MySqlConnection connection, Stream stream)
	{
		BaseDumper dumper = outputFormat switch
		{
			OutputFormatEnum.mysql => new MySqlDumper(connection),
			OutputFormatEnum.postgres => new PostgresDumper(connection),
			OutputFormatEnum.csv => new CsvDumper(connection),
			_ => throw new ArgumentOutOfRangeException(nameof(outputFormat), outputFormat, null)
		};

		await dumper.WriteStartTableAsync(table, stream, !skipSchema, truncate);

		await using (var transaction = await connection.BeginTransactionAsync(IsolationLevel.RepeatableRead))
		{
			await dumper.WriteInsertQueries(table, selectStatement, stream, transaction);
		}

		await dumper.WriteEndTableAsync(table, stream);
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

	internal enum OutputFormatEnum
	{
		mysql,
		postgres,
		csv
	}

    internal enum InputFormatEnum
    {
        mysqlForceBatch,
		csv
    }
}