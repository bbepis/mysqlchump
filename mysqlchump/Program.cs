using System;
using System.Collections.Generic;
using System.CommandLine;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.IO.Pipelines;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
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
		var rootCommand = new RootCommand("MySQLChump data transfer tool v2.0");

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
		
		var outputFileArgument = new Argument<string>("output location", "Specify either a file or a folder to output to. '-' for stdout, otherwise defaults to creating files in the current directory") { Arity = ArgumentArity.ZeroOrOne };

		var exportCommand = new Command("export", "Exports data from a database")
		{
			tableOption, tablesOption, connectionStringOption, serverOption, portOption, databaseOption, usernameOption, passwordOption, outputFormatOption, selectOption, noCreationOption, truncateOption, outputFileArgument
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
				csBuilder.IgnoreCommandTransaction = true;

				connectionString = csBuilder.ToString();
			}

			var options = new DumpOptions
			{
				SelectQuery = result.GetValueForOption(selectOption),
				MysqlWriteCreateTable = !result.GetValueForOption(noCreationOption),
				MysqlWriteTruncate = result.GetValueForOption(truncateOption)
			};

			context.ExitCode = await ExportMainAsync(tables, connectionString, result.GetValueForOption(outputFormatOption),
				result.GetValueForArgument(outputFileArgument), options);
		});

		rootCommand.Add(exportCommand);
		
		var inputFormatOption = new Option<InputFormat>(new[] { "--input-format", "-f" }, () => InputFormat.mysql, "The input format to use when importing.");
		var importMechanismOption = new Option<ImportMechanism>(new[] { "--import-mechanism", "-m" }, () => ImportMechanism.SqlStatements, "The import mechanism to use when importing.");
		var sourceTablesOption = new Option<string[]>(new[] { "--source-table", "-T" }, "(JSON only) List of tables to import from, from a dump that contains multiple tables");
		var parallelOption = new Option<byte>(new[] { "--parallel", "-j" }, () => 12, "The amount of parallel insert threads to use.");
		var insertIgnoreOption = new Option<bool>(new[] { "--insert-ignore" }, "Changes INSERT to INSERT IGNORE. Useful for loading into existing datasets, but can be slower");
		var importNoCreationOption = new Option<bool>(new[] { "--no-creation" }, "(JSON only) Don't run CREATE TABLE statement.");
		var csvUseHeadersOption = new Option<bool>(new[] { "--csv-use-headers" }, "Use the first row in the CSV as header data to determine column names.");
		var importTableOption = new Option<string>(new[] { "--table", "-t" }, "The destination table name to import to. Required for CSV data, optional for others");
		var csvImportColumnsOption = new Option<string>(new[] { "--csv-columns" }, "A comma-separated list of columns that the CSV corresponds to. Ignored if --csv-use-headers is specified");
		var aggressiveUnsafeOption = new Option<bool>(new[] { "--aggressive-unsafe" }, "Enables aggressive binary log options to drastically increase import speed. Note that this requires root, or an account with the SUPER privilege. If the database crashes during import, ALL databases in the database could become corrupt.");
		var setInnoDbOption = new Option<bool>(new[] { "--set-innodb" }, "(JSON only) Forces created tables to use InnoDB as the storage engine, with ROW_FORMAT=DYNAMIC. Removes any COMPRESSION option (e.g. from TokuDB)");
		var setCompressedOption = new Option<bool>(new[] { "--set-compressed" }, "(JSON only) Forces created tables to use ROW_FORMAT=COMPRESSED (overrides --set-innodb)");
		var deferIndexesOption = new Option<bool>(new[] { "--defer-indexes" }, "(JSON only) Does not create indexes upfront, but instead after data has been inserted for better performance. Does nothing with --no-create");
		var stripIndexesOption = new Option<bool>(new[] { "--strip-indexes" }, "(JSON only) Does not create indexes at all. Does nothing with --no-creation or --defer-indexes");
		
		var inputFileArgument = new Argument<string>("input file", "Specify a file to read from. Otherwise - for stdin") { Arity = ArgumentArity.ExactlyOne };
		
		var importCommand = new Command("import", "Imports data to a database")
		{
			connectionStringOption, serverOption, portOption, databaseOption, usernameOption, passwordOption, inputFormatOption, importMechanismOption, importTableOption, parallelOption,
			insertIgnoreOption, csvImportColumnsOption, csvUseHeadersOption, inputFileArgument, importNoCreationOption, aggressiveUnsafeOption,
			setInnoDbOption, setCompressedOption, sourceTablesOption, deferIndexesOption, stripIndexesOption
		};

		importCommand.SetHandler(async context =>
		{
			var result = context.ParseResult;

			string connectionString = result.GetValueForOption(connectionStringOption);

			var importMechanism = result.GetValueForOption(importMechanismOption);

			if (string.IsNullOrWhiteSpace(connectionString))
			{
				var csBuilder = new MySqlConnectionStringBuilder();
				csBuilder.Server = result.GetValueForOption(serverOption);
				csBuilder.Port = result.GetValueForOption(portOption);
				csBuilder.Database = result.GetValueForOption(databaseOption);
				csBuilder.UserID = result.GetValueForOption(usernameOption);
				csBuilder.Password = result.GetValueForOption(passwordOption);

				csBuilder.AllowLoadLocalInfile = importMechanism == ImportMechanism.LoadDataInfile;

				connectionString = csBuilder.ToString();
			}

			var inputFormat = result.GetValueForOption(inputFormatOption);
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

			if (inputFormat == InputFormat.csv && string.IsNullOrWhiteSpace(csvColumns) && !csvUseHeaders)
			{
				Console.WriteLine("Either columns (--csv-columns) or use headers (--csv-use-headers) required for CSV import");
				context.ExitCode = 1;
				return;
			}
			if (inputFormat == InputFormat.csv && string.IsNullOrWhiteSpace(importTable))
			{
				Console.WriteLine("Table (-t) required for CSV import");
				context.ExitCode = 1;
				return;
			}

			var inputFile = result.GetValueForArgument(inputFileArgument);

			Task<int> RunImport(string filename) =>
				ImportMainAsync(connectionString, inputFormat, filename,
					new ImportOptions
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
						TargetTable = importTable,
						CsvUseHeaderRow = csvUseHeaders,
						CsvManualColumns = csvColumns?.Split(',')
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

	static async Task<int> ExportMainAsync(string[] tables, string connectionString, OutputFormatEnum outputFormat, string outputLocation, DumpOptions options)
	{
		bool stdout = outputLocation == "-";
		string folder = Directory.Exists(outputLocation)
			? outputLocation
			: string.IsNullOrWhiteSpace(outputLocation) ? Directory.GetCurrentDirectory() : null;
		bool folderMode = folder != null;

		Stream currentStream = null;
		Pipe pipeline = null;
		Task pipelineReadTask = null;

		string extension = outputFormat switch
		{
			OutputFormatEnum.mysql => "sql",
			OutputFormatEnum.csv => "csv",
			OutputFormatEnum.mysqlCsv => "csv",
			OutputFormatEnum.json => "json",
			_ => throw new ArgumentOutOfRangeException(nameof(outputFormat), outputFormat, null)
		};

		try
		{
			var dateTime = DateTime.Now;

			await using var connection = new MySqlConnection(connectionString);

			BaseDumper dumper = outputFormat switch
			{
				OutputFormatEnum.mysql => new MysqlDumper(connection, options),
				//OutputFormatEnum.postgres => new PostgresDumper(connection),
				OutputFormatEnum.csv => new CsvDumper(connection, options, false),
				OutputFormatEnum.mysqlCsv => new CsvDumper(connection, options, true),
				OutputFormatEnum.json => new JsonDumper(connection, options),
				_ => throw new ArgumentOutOfRangeException(nameof(outputFormat), outputFormat, null)
			};

			if (tables.Length > 1 && !folderMode && !dumper.CanMultiplexTables)
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
					: new FileStream(outputLocation, FileMode.Create, FileAccess.ReadWrite, FileShare.Read, 4096);

				pipeline = new Pipe(new PipeOptions(pauseWriterThreshold: 1 * 1024 * 1024, resumeWriterThreshold: 512 * 1024));
                pipelineReadTask = Task.Run(() => pipeline.Reader.CopyToAsync(currentStream));
			}

			// TODO: query all tables upfront to ensure universal locking
			await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.Snapshot);

			for (var i = 0; i < tables.Length; i++)
			{
				var table = tables[i];
				
				if (folderMode)
				{
					currentStream =
						new FileStream(Path.Combine(folder, $"dump_{dateTime:yyyy-MM-dd_hh-mm-ss}_{table}.{extension}"),
							FileMode.CreateNew);

					pipeline = new Pipe(new PipeOptions(pauseWriterThreshold: 1 * 1024 * 1024, resumeWriterThreshold: 512 * 1024));
					pipelineReadTask = Task.Run(() => pipeline.Reader.CopyToAsync(currentStream));
				}

				await dumper.ExportAsync(pipeline.Writer, table);

				if (folderMode)
				{
					await dumper.FinishDump(pipeline.Writer);
					await pipeline.Writer.CompleteAsync();
					await pipelineReadTask;
					await currentStream.DisposeAsync();
				}
			}

			if (!folderMode)
			{
				await dumper.FinishDump(pipeline.Writer);
				await pipeline.Writer.CompleteAsync();
				await pipelineReadTask;
				await currentStream.DisposeAsync();
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

	static async Task<int> ImportMainAsync(string connectionString, InputFormat inputFormat, string inputLocation, ImportOptions options)
	{
		ThreadPool.SetMaxThreads(options.ParallelThreads * 2, options.ParallelThreads * 2);

		bool stdin = inputLocation == "-";

		Stream currentStream = stdin
			? Console.OpenStandardInput()
			: new FileStream(inputLocation, FileMode.Open, FileAccess.Read, FileShare.Read);

		bool connectionSuccessful = false;
		bool redoLogEnabled = false;
		string doubleWriteStatus = "ON";

		bool setInfileToFalse = false;
		bool resetAggressiveValues = false;

		async Task resetVariables(bool log)
		{
			if (!setInfileToFalse && !resetAggressiveValues)
				return;

			await using var connection = new MySqlConnection(connectionString);

			await connection.OpenAsync();

			using var command = connection.CreateCommand();
			command.CommandTimeout = 99999;

			if (resetAggressiveValues)
			{
				Console.Error.WriteLine("Resetting unsafe database variables...");

				command.CommandText = $"SET GLOBAL innodb_doublewrite = '{doubleWriteStatus}'";
				await command.ExecuteNonQueryAsync();

				if (redoLogEnabled)
				{
					command.CommandText = "ALTER INSTANCE ENABLE INNODB REDO_LOG;";
					await command.ExecuteNonQueryAsync();
				}
			}
			
			if (setInfileToFalse)
			{
				Console.Error.WriteLine("Resetting local_infile value...");

				command.CommandText = "SET GLOBAL local_infile = 0;";
				await command.ExecuteNonQueryAsync();
			}
		}

		try
		{
			var createConnection = () =>
			{
				var connection = new MySqlConnection(connectionString);

				connection.Open();

				using var command = connection.CreateCommand();
				command.CommandTimeout = 99999;

				if (options.AggressiveUnsafe)
				{
					command.CommandText = "SET sql_log_bin = 'OFF';";
					command.ExecuteScalar();
				}

				return connection;
			};

			Console.CancelKeyPress += (s, e) =>
			{
				Console.Error.WriteLine();

				if (!connectionSuccessful)
					return;

				Console.Error.WriteLine("Attempting to shut down gracefully...");

				Task.Run(async () => await resetVariables(true)).Wait();

				Console.Error.WriteLine("Completed.");
			};

			await using (var connection = new MySqlConnection(connectionString))
			{
				// test connection string
				await connection.OpenAsync();

				connectionSuccessful = true;

				using var command = connection.CreateCommand();
				command.CommandTimeout = 99999;

				// check for SUPER grant
				command.CommandText = "SHOW GRANTS FOR CURRENT_USER;";

				bool hasSuper = false;

				var dbPerms = new List<string>();

				var reader = command.ExecuteReader();
				while (reader.Read())
				{
					var perms = reader.GetString(0);

					var match = Regex.Match(perms, @"GRANT\s+(.+)\s+ON ([a-zA-Z0-9`*.]+) TO ");

					if (!match.Success)
						continue;

					var targetDb = match.Groups[2].Value.Trim('`');

					if (targetDb == "*.*" || targetDb == connection.Database)
					{
						var split = match.Groups[1].Value.Split(",");
						dbPerms.AddRange(split.Select(x => x.Trim().ToUpper()));
					}
				}
				reader.Close();

				hasSuper = dbPerms.Contains("SUPER")
					|| dbPerms.Contains("SYSTEM_VARIABLES_ADMIN");
				// you are FUCKED on mariadb. there's no way to know


				if (options.ImportMechanism == ImportMechanism.LoadDataInfile)
				{
					command.CommandText = "SELECT @@local_infile;";
					bool canLoadInfile = (long)await command.ExecuteScalarAsync() == 1;

					if (!canLoadInfile)
					{
						if (!hasSuper)
						{
							Console.Error.WriteLine("Import FAILED: The server is required to have local_infile=1 when using LoadDataInfile, and the user you have provided does not have privileges to configure it");
							Environment.Exit(1);
						}

						setInfileToFalse = true;
						command.CommandText = $"SET GLOBAL local_infile = 1;";
						await command.ExecuteScalarAsync();
					}
				}

				if (options.AggressiveUnsafe)
				{
					if (!hasSuper)
					{
						Console.Error.WriteLine("Import FAILED: The user you have provided does not have SUPER or SYSTEM_VARIABLES_ADMIN privilege, which is required when using --aggressive-unsafe");
						Environment.Exit(1);
					}

					// continue as usual
					command.CommandText = "PURGE BINARY LOGS BEFORE NOW();";

					await command.ExecuteNonQueryAsync();

					command.CommandText = "SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE variable_name = 'Innodb_redo_log_enabled';";
					redoLogEnabled = (string)(await command.ExecuteScalarAsync()) == "ON";

					command.CommandText = "SELECT @@innodb_doublewrite;";
					doubleWriteStatus = (string)await command.ExecuteScalarAsync();

					resetAggressiveValues = true;

					if (redoLogEnabled)
					{
						command.CommandText = "ALTER INSTANCE DISABLE INNODB REDO_LOG;";
						await command.ExecuteNonQueryAsync();
					}

					if (doubleWriteStatus == "ON")
					{
						command.CommandText = $"SET GLOBAL innodb_doublewrite = 'DETECT_ONLY'"; // we can't set OFF dynamically
						await command.ExecuteNonQueryAsync();
					}
				}
			}

			BaseImporter importer = inputFormat switch
			{
				InputFormat.csv => new CsvImporter(options, currentStream, false),
				InputFormat.mysqlCsv => new CsvImporter(options, currentStream, true),
				InputFormat.json => new JsonImporter(options, currentStream),
				InputFormat.mysql => new MysqlImporter(options, currentStream),
				_ => throw new ArgumentOutOfRangeException(nameof(inputFormat), $"Unknown import format: {inputFormat}"),
			};

			await importer.ImportAsync(createConnection);

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

			if (connectionSuccessful)
				await resetVariables(false);
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
}

internal enum OutputFormatEnum
{
	mysql,
	csv,
	mysqlCsv,
	json
}

public enum InputFormat
{
	mysql,
	csv,
	mysqlCsv,
	json
}