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
		var rootCommand = new RootCommand("MySQLChump data transfer tool v1.7");

		var tableOption = new Option<string[]>(new[] { "--table", "-t" }, "The table to be dumped. Can be specified multiple times, or passed '*' to dump all tables. Supports globbing with '*' and '?' characters");
		var tablesOption = new Option<string>("--tables", "A comma-separated list of tables to dump.");
		var connectionStringOption = new Option<string>("--connection-string", "A connection string to use to connect to the database. Not required if -s -d -u -p have been specified");
		var serverOption = new Option<string>(new[] { "--server", "-s" }, () => "localhost", "The server to connect to.");
		var portOption = new Option<ushort>(new[] { "--port", "-o" }, () => 3306, "The port of the server to connect to.");
		var databaseOption = new Option<string>(new[] { "--database", "-d" }, "The database to use when dumping.") { Arity = ArgumentArity.ExactlyOne };
		var usernameOption = new Option<string>(new[] { "--username", "-u" }, () => "root", "The username to connect with.");
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
				if (string.IsNullOrEmpty(result.GetValueForOption(databaseOption)))
					throw new Exception("Database option is required when not using a connection string");

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
		var importMechanismOption = new Option<ImportMechanism>(new[] { "--import-mechanism", "-m" }, () => ImportMechanism.SqlStatements, "The import mechanism to use when importing.");
		var importTableOption = new Option<string>(new[] { "--table", "-t" }, "The table to import to. Only relevant for CSV data");
		var sourceTablesOption = new Option<string[]>(new[] { "--source-table", "-T" }, "(JSON only) List of tables to import from, from a dump that contains multiple tables");
		var parallelOption = new Option<byte>(new[] { "--parallel", "-j" }, () => 12, "The amount of parallel insert threads to use.");
		var insertIgnoreOption = new Option<bool>(new[] { "--insert-ignore" }, "Changes INSERT to INSERT IGNORE. Useful for loading into existing datasets, but can be slower");
		var importNoCreationOption = new Option<bool>(new[] { "--no-creation" }, "(JSON only) Don't run CREATE TABLE statement.");
		var csvUseHeadersOption = new Option<bool>(new[] { "--csv-use-headers" }, "Use the first row in the CSV as header data to determine column names.");
		var csvImportColumnsOption = new Option<string>(new[] { "--csv-columns" }, "A comma-separated list of columns that the CSV corresponds to. Ignored if --csv-use-headers is specified");
		var csvFixInvalidMysqlOption = new Option<bool>(new[] { "--csv-fix-mysql" }, "Enables a pre-processor to fix invalid CSV files generated by MySQL. Note that enabling this will break functional CSV files");
		var aggressiveUnsafeOption = new Option<bool>(new[] { "--aggressive-unsafe" }, "Enables aggressive binary log options to drastically increase import speed. Note that this requires root, or an account with the SUPER privilege. If the database crashes during import, ALL databases in the database could become corrupt.");
		var setInnoDbOption = new Option<bool>(new[] { "--set-innodb" }, "(JSON only) Forces created tables to use InnoDB as the storage engine, with ROW_FORMAT=DYNAMIC. Removes any COMPRESSION option (e.g. from TokuDB)");
		var setCompressedOption = new Option<bool>(new[] { "--set-compressed" }, "(JSON only) Forces created tables to use ROW_FORMAT=COMPRESSED (overrides --set-innodb)");
		var deferIndexesOption = new Option<bool>(new[] { "--defer-indexes" }, "(JSON only) Does not create indexes upfront, but instead after data has been inserted for better performance. Does nothing with --no-create");
		var stripIndexesOption = new Option<bool>(new[] { "--strip-indexes" }, "(JSON only) Does not create indexes at all. Does nothing with --no-creation or --defer-indexes");
		
		var inputFileArgument = new Argument<string>("input file", "Specify a file to read from. Otherwise - for stdin") { Arity = ArgumentArity.ExactlyOne };
		
		var importCommand = new Command("import", "Imports data to a database")
		{
			connectionStringOption, serverOption, portOption, databaseOption, usernameOption, passwordOption, inputFormatOption, importMechanismOption, importTableOption, parallelOption,
			insertIgnoreOption, csvImportColumnsOption, csvUseHeadersOption, csvFixInvalidMysqlOption, inputFileArgument, importNoCreationOption, aggressiveUnsafeOption,
			setInnoDbOption, setCompressedOption, sourceTablesOption, deferIndexesOption, stripIndexesOption
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
				csBuilder.AllowLoadLocalInfile = true;

				connectionString = csBuilder.ToString();
			}

			var inputFormat = result.GetValueForOption(inputFormatOption);
			var importMechanism = result.GetValueForOption(importMechanismOption);
			var importTable = result.GetValueForOption(importTableOption);
			var sourceTables = result.GetValueForOption(sourceTablesOption);
			
			var parallel = result.GetValueForOption(parallelOption);
				
			var insertIgnore = result.GetValueForOption(insertIgnoreOption);
			var noCreate = result.GetValueForOption(importNoCreationOption);
			var aggressiveUnsafe = result.GetValueForOption(aggressiveUnsafeOption);

			var setInnoDb = result.GetValueForOption(setInnoDbOption);
			var setCompressed = result.GetValueForOption(setCompressedOption);
			var deferIndexes = result.GetValueForOption(deferIndexesOption);
			var stripIndexes = result.GetValueForOption(stripIndexesOption);

			var csvUseHeaders = result.GetValueForOption(csvUseHeadersOption);
			var csvColumns = result.GetValueForOption(csvImportColumnsOption);
			var csvFixInvalidMysql = result.GetValueForOption(csvFixInvalidMysqlOption);

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

			var inputFile = result.GetValueForArgument(inputFileArgument);

			Task<int> RunImport(string filename) =>
				ImportMainAsync(connectionString, inputFormat, filename, importTable, sourceTables,
					(csvUseHeaders || string.IsNullOrWhiteSpace(csvColumns)) ? null : csvColumns.Split(',').Select(x => x.Trim()).ToArray(),
					csvFixInvalidMysql, csvUseHeaders, new ImportOptions
					{
						ImportMechanism = importMechanism,
						ParallelThreads = parallel,
						AggressiveUnsafe = aggressiveUnsafe,
						InsertIgnore = insertIgnore,
						NoCreate = noCreate,
						SourceTables = sourceTables,
						SetInnoDB = setInnoDb,
						SetCompressed = setCompressed,
						DeferIndexes = deferIndexes,
						StripIndexes = stripIndexes,
					});

			if (Directory.Exists(inputFile))
			{
				var fileQueue = new Queue<string>(Directory.GetFiles(inputFile));

				while (fileQueue.Count > 0)
				{
					var nextFile = fileQueue.Dequeue();

					Console.WriteLine($"Reading '{nextFile}' ({fileQueue.Count} remaining)");

					var exitCode = await RunImport(nextFile);

					if (exitCode != 0)
					{
						fileQueue.Enqueue(nextFile);

						Console.WriteLine($"Import failed: '{nextFile}'");
					}
				}

				return;
			}

			context.ExitCode = await RunImport(inputFile);
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

		string extension = outputFormat switch
		{
			OutputFormatEnum.mysql => "sql",
			OutputFormatEnum.postgres => "sql",
			OutputFormatEnum.csv => "csv",
			OutputFormatEnum.json => "json",
			_ => throw new ArgumentOutOfRangeException(nameof(outputFormat), outputFormat, null)
		};

		try
		{
			var dateTime = DateTime.Now;

			await using var connection = new MySqlConnection(connectionString);

			BaseDumper createDumper() => outputFormat switch
			{
				OutputFormatEnum.mysql => new MySqlDumper(connection),
				OutputFormatEnum.postgres => new PostgresDumper(connection),
				OutputFormatEnum.csv => new CsvDumper(connection),
				OutputFormatEnum.json => new JsonDumper(connection),
				_ => throw new ArgumentOutOfRangeException(nameof(outputFormat), outputFormat, null)
			};

			BaseDumper dumper = createDumper();

			if (tables.Length > 1 && !folderMode && !dumper.CompatibleWithMultiTableStdout)
				throw new Exception("Selected dump format does not support multiple tables per file/stream");

			await connection.OpenAsync();

			await BaseDumper.InitializeConnection(connection);

			if (tables.Any(x => x.Contains('*') || x.Contains('?')))
			{
				var newTables = new HashSet<string>();
				var availableTables = await GetAllTablesFromDatabase(connection);

				foreach (var tableString in tables)
				{
					if (tableString.Contains('*') || tableString.Contains('?'))
					{
						foreach (var table in availableTables)
							if (Utility.Glob(table, tableString))
								newTables.Add(table);
					}
					else
					{
						newTables.Add(tableString);
					}
				}

				tables = newTables.ToArray();
			}

			tables = await SortTableNames(connection, tables);

			if (stdout || folder == null)
			{
				currentStream = stdout
					? Console.OpenStandardOutput()
					: new FileStream(outputLocation, append ? FileMode.Append : FileMode.Create, FileAccess.ReadWrite, FileShare.Read, 1024 * 1024);
			}

			for (var i = 0; i < tables.Length; i++)
			{
				var table = tables[i];
				
				if (folderMode)
				{
					currentStream =
						new FileStream(Path.Combine(folder, $"dump_{dateTime:yyyy-MM-dd_hh-mm-ss}_{table}.{extension}"),
							FileMode.CreateNew);
				}

				string formattedQuery = selectQuery.Replace("{table}", table);

				await dumper.WriteStartTableAsync(table, currentStream, !noCreation, truncate);

				await using (var transaction = await connection.BeginTransactionAsync(IsolationLevel.RepeatableRead))
				{
					await dumper.WriteInsertQueries(table, formattedQuery, currentStream, transaction);
				}

				await dumper.WriteEndTableAsync(table, currentStream, !folderMode && (i + 1) < tables.Length);

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

	static async Task<int> ImportMainAsync(string connectionString, InputFormatEnum inputFormat, string inputLocation,
		string table, string[] sourceTables, string[] csvColumns, bool csvFixInvalidMysql,
		bool csvUseHeaders, ImportOptions options)
	{
		bool stdin = inputLocation == "-";

		Stream currentStream = stdin
			? Console.OpenStandardInput()
			: new FileStream(inputLocation, FileMode.Open, FileAccess.Read, FileShare.Read);

		bool connectionSuccessful = false;
		bool redoLogEnabled = false;

		try
		{
			var createConnection = () =>
			{
				var connection = new MySqlConnection(connectionString);

				connection.Open();

				if (options.AggressiveUnsafe)
				{
					using var command = connection.CreateCommand();
					command.CommandTimeout = 99999;
					command.CommandText = "SET sql_log_bin = 0;";

					command.ExecuteScalar();
				}

				return connection;
			};

			await using (var connection = new MySqlConnection(connectionString))
			{
				// test connection string
				await connection.OpenAsync();

				connectionSuccessful = true;

				if (options.AggressiveUnsafe)
				{
					using var command = connection.CreateCommand();
					command.CommandTimeout = 99999;

					// check for SUPER grant
					command.CommandText = "SHOW GRANTS FOR CURRENT_USER;";

					bool hasSuper = false;

					var reader = command.ExecuteReader();
					while (reader.Read())
						hasSuper |= reader.GetString(0).Contains("SUPER", StringComparison.OrdinalIgnoreCase);
					reader.Close();

					if (!hasSuper)
					{
						Console.Error.WriteLine("Import FAILED: The user you have provided does not have SUPER privilege, which is required when using --aggressive-unsafe");
						Environment.Exit(1);
					}

					// continue as usual
					command.CommandText = "PURGE BINARY LOGS BEFORE NOW();";

					await command.ExecuteNonQueryAsync();

					command.CommandText = "SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE variable_name = 'Innodb_redo_log_enabled';";
					redoLogEnabled = (string)(await command.ExecuteScalarAsync()) == "ON";

					if (redoLogEnabled)
					{
						command.CommandText = "ALTER INSTANCE DISABLE INNODB REDO_LOG;";
						await command.ExecuteNonQueryAsync();
					}
				}
			}

			if (inputFormat == InputFormatEnum.mysqlForceBatch)
			{
				var importer = new MysqlBatchImporter();
				await importer.ImportAsync(currentStream, createConnection);
			}
			else if (inputFormat == InputFormatEnum.mysqlRaw)
			{
				var importer = new MysqlRawImporter();
				await importer.ImportAsync(currentStream, createConnection);
			}
			else if (inputFormat == InputFormatEnum.csv)
			{
				var importer = new CsvImporter();
				await importer.ImportAsync(currentStream, table, csvColumns, csvFixInvalidMysql, options.ParallelThreads, csvUseHeaders, options.InsertIgnore, createConnection);
			}
			else if (inputFormat == InputFormatEnum.json)
			{
				var importer = new JsonImporter();
				await importer.ImportAsync(currentStream, createConnection, options);
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

			if (connectionSuccessful && options.AggressiveUnsafe && redoLogEnabled)
			{
				await using var connection = new MySqlConnection(connectionString);

				await connection.OpenAsync();
				
				using var command = connection.CreateCommand();
				command.CommandTimeout = 99999;
				command.CommandText = "ALTER INSTANCE ENABLE INNODB REDO_LOG;";
				await command.ExecuteNonQueryAsync();
			}
		}
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

	private static async Task<string[]> SortTableNames(MySqlConnection connection, string[] tableNames)
	{
		string databaseName = connection.Database;

		const string commandText = @"SELECT 
    DISTINCT TABLE_NAME, REFERENCED_TABLE_NAME
FROM 
    information_schema.KEY_COLUMN_USAGE
WHERE 
    TABLE_SCHEMA = @databasename
    AND REFERENCED_TABLE_NAME IS NOT NULL
	AND TABLE_NAME != REFERENCED_TABLE_NAME";

		await using var createTableCommand = new MySqlCommand(commandText, connection)
			.SetParam("@databasename", databaseName);

		await using var reader = await createTableCommand.ExecuteReaderAsync();

		var fkList = new List<(string table, string referencedTable)>();

		while (await reader.ReadAsync())
			fkList.Add((reader.GetString(0), reader.GetString(1)));

		var sorted = TopologicalSort.SortItems(tableNames
			.Select(x => new TopologicalItem { Name = x, Dependencies = fkList.Where(fk => fk.table == x && tableNames.Contains(fk.referencedTable)).Select(y => y.referencedTable).ToArray() })
			.ToArray());

		return sorted;
	}

	internal enum OutputFormatEnum
	{
		mysql,
		postgres,
		csv,
		json
	}

	internal enum InputFormatEnum
	{
		mysqlForceBatch,
		mysqlRaw,
		csv,
		json
	}
}