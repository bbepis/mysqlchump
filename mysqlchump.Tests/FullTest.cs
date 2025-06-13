using System.IO.Compression;
using System.IO.Pipelines;
using System.Text;
using mysqlchump.Export;
using mysqlchump.Import;
using MySqlConnector;
using Sylvan.Data.Csv;

namespace mysqlchump.Tests;

[TestFixture]
internal class FullTest
{
	internal class TestRowData
	{
		public int Id;
		public string? TextData;
		public byte[]? BinaryData;
		public DateTime? Date;
		public decimal? DecimalData;
	}

	[OneTimeSetUp]
	public void Setup()
	{
		Utility.IsTesting = true;
	}

	private MySqlConnection CreateConnection()
	{
		var connection = new MySqlConnection("Server=localhost;uid=root;database=test;AllowLoadLocalInfile=true");
		connection.Open();
		return connection;
	}

	private static readonly DateTime BaseDateTime = DateTime.Parse("2008-01-10 12:34:56");
	private TestRowData GenerateTestRowData(int id)
	{
		var random = new Random(id + 1000);

		string? textData = null;
		byte[]? binaryData = null;
		DateTime? date = null;
		decimal? decimalData = null;

		if (id % 509 != 0)
		{
			var stringLength = (int)Math.Floor(random.NextDouble() * 1025);

			Span<char> stringChars = stackalloc char[stringLength];

			const int asciiMin = 32;
			const int asciiMax = 126;
			const int asciiRange = (asciiMax - asciiMin) + 1;

			for (int i = 0; i < stringLength; i++)
				stringChars[i] = (char)(byte)Math.Floor(asciiMin + random.NextDouble() * asciiRange);

			textData = new string(stringChars);
		}

		if (id % 659 != 0)
		{
			var binaryLength = (int)Math.Floor(random.NextDouble() * 1025);

			binaryData = new byte[binaryLength];

			for (int i = 0; i < binaryLength; i++)
				binaryData[i] = (byte)Math.Floor(random.NextDouble() * 256);
		}

		if (id % 719 != 0)
		{
			const int maxRange = 60 * 60 * 24 * 365 * 10;
			date = BaseDateTime + TimeSpan.FromSeconds(random.NextDouble() * maxRange);
		}

		if (id % 109 != 0)
		{
			decimalData = (decimal)random.NextDouble();
		}

		return new()
		{
			Id = id,
			TextData = textData,
			BinaryData = binaryData,
			Date = date,
			DecimalData = decimalData
		};
	}

	private static async Task CreateDataTable(MySqlConnection connection, string tableName)
	{
		using var command = new MySqlCommand(@$"
DROP TABLE IF EXISTS `{tableName}`;
CREATE TABLE `{tableName}` (
	`id` INT NOT NULL,
	`textdata` VARCHAR(1024) NULL DEFAULT NULL COLLATE 'utf8mb4_0900_ai_ci',
	`binarydata` VARBINARY(1024) NULL DEFAULT NULL,
	`date` DATETIME NULL DEFAULT NULL,
	`decimaldata` DECIMAL(20,6) NULL DEFAULT NULL,
	PRIMARY KEY (`id`) USING BTREE
)
COLLATE='utf8mb4_0900_ai_ci'
ENGINE=InnoDB
;", connection);

		await command.ExecuteNonQueryAsync();
	}

	[Test, Explicit]
	public async Task GenerateCsvFile()
	{
		using var writer = new StreamWriter("generated.csv", false, Utility.NoBomUtf8);
		using var csvWriter = CsvDataWriter.Create(writer, new char[128000], new CsvDataWriterOptions()
		{
			BinaryEncoding = BinaryEncoding.Base64,
			WriteHeaders = true,
			DateTimeFormat = "yyyy-MM-dd HH:mm:ss",
			
		});

		using var connection = CreateConnection();
		using var command = connection.CreateCommand();
		command.CommandText = "SELECT * FROM data;";

		using var reader = await command.ExecuteReaderAsync();

		await csvWriter.WriteAsync(reader);
	}

	[Test]
	public async Task CreateTestTable()
	{
		await using var connection = CreateConnection();

		await CreateDataTable(connection, "data");

		using var command = connection.CreateCommand();

		try
		{
			int counter = 0;
			var stringbuilder = new StringBuilder();

			async Task submitText(string str)
			{
				if (counter++ == 0)
					stringbuilder.Append("INSERT INTO data VALUES ");
				else
					stringbuilder.Append(",");

				stringbuilder.Append(str);

				if (++counter == 200)
				{
					stringbuilder.Append(";");
					command.CommandText = stringbuilder.ToString();
					await command.ExecuteScalarAsync();
					counter = 0;
					stringbuilder.Clear();
				}
			}

			for (int i = 1; i < 10001; i++)
			{
				var row = GenerateTestRowData(i);

				await submitText($"({i},{
					(row.TextData != null ? $"'{MySqlHelper.EscapeString(row.TextData)}'" : "NULL")
				},{
					(row.BinaryData != null
						? row.BinaryData.Length > 0 ? "_binary 0x" + Convert.ToHexString(row.BinaryData) : "''"
						: "NULL")
				},{	
					(row.Date != null ? $"'{row.Date.Value:yyyy-MM-dd HH:mm:ss}'" : "NULL")
				},{
					(row.DecimalData != null ? row.DecimalData.Value : "NULL")
				})");
			}

			if (counter > 0)
			{
				stringbuilder.Append(";");
				command.CommandText = stringbuilder.ToString();
				await command.ExecuteScalarAsync();
			}
		}
		catch
		{
			Console.WriteLine(command.CommandText);
			throw;
		}
	}

	public async Task<MemoryStream> BaseExport(Func<MySqlConnection, Export.DumpOptions, BaseDumper> dumperFactory)
	{
		await using var connection = CreateConnection();

		var dumper = dumperFactory(connection, new Export.DumpOptions
		{
			SelectQuery = "SELECT * FROM `{table}`",
			MysqlWriteCreateTable = true,
			CsvWriteHeader = true
		});

		var memoryStream = new MemoryStream();
		var pipeline = new Pipe();

		var readTask = Task.Run(() => pipeline.Reader.CopyToAsync(memoryStream));

		await dumper.ExportAsync(pipeline.Writer, "data");
		await dumper.FinishDump(pipeline.Writer);
		await pipeline.Writer.CompleteAsync();
		await readTask;


		memoryStream.Position = 0;
		return memoryStream;
	}

	public async Task BaseImportTest(Stream inputStream, int threadCount, ImportMechanism importMechanism, bool invalidCsv,
		Func<ImportOptions, Stream, BaseImporter> importerFactory)
	{
		//using (var fileStream = new FileStream("C:\\temp\\e\\test_dump.csv", FileMode.Create))
		//using (var reader = new StreamReader(inputStream, Utility.NoBomUtf8))
		//using (var mysqlInvalidReader = new MysqlInvalidCsvReader(reader))
		//	//inputStream.CopyTo(fileStream);
		//	File.WriteAllText("C:\\temp\\e\\corrected_dump.csv", mysqlInvalidReader.ReadToEnd());
		//inputStream.Position = 0;

		await using var connection = CreateConnection();

		var command = new MySqlCommand(@"DROP TABLE IF EXISTS `data_secondary`;", connection);

		await command.ExecuteNonQueryAsync();

		var importer =
			importerFactory(
				new ImportOptions()
				{
					ImportMechanism = importMechanism,
					SourceTables = ["data"],
					TargetTable = "data_secondary",
					NoCreate = false,
					InsertBatchSize = 2000,
					CsvUseHeaderRow = true,
					CsvFixInvalid = invalidCsv,
					ParallelThreads = threadCount
				},
				inputStream);

		if (importer is CsvImporter)
		{
			await CreateDataTable(connection, "data_secondary");
		}

		await importer.ImportAsync(CreateConnection);


		command.CommandText = "SELECT COUNT(*) FROM data_secondary";
		var importCount = (long)await command.ExecuteScalarAsync();

		Assert.That(importCount, Is.EqualTo(10_000), "Not all rows were imported");


		command.CommandText = @"SELECT COUNT(*) 
FROM data d
LEFT JOIN data_secondary dt ON d.id = dt.id
WHERE 
	(d.textdata != dt.textdata OR (d.textdata IS NULL AND dt.textdata IS NOT NULL) OR (d.textdata IS NOT NULL AND dt.textdata IS NULL)) OR
	(d.binarydata != dt.binarydata OR (d.binarydata IS NULL AND dt.binarydata IS NOT NULL) OR (d.binarydata IS NOT NULL AND dt.binarydata IS NULL)) OR
	(d.date != dt.date OR (d.date IS NULL AND dt.date IS NOT NULL) OR (d.date IS NOT NULL AND dt.date IS NULL)) OR
	(d.decimaldata != dt.decimaldata OR (d.decimaldata IS NULL AND dt.decimaldata IS NOT NULL) OR (d.decimaldata IS NOT NULL AND dt.decimaldata IS NULL));";
		var mismatchCount = (long)await command.ExecuteScalarAsync();

		Assert.That(mismatchCount, Is.EqualTo(0), "Some rows had their data mangled");
	}

	public static IEnumerable<TestCaseData> TestCaseGenerator()
	{
		foreach (var threadCount in (int[])[1, 4])
		foreach (var importMechanism in (ImportMechanism[])[ImportMechanism.SqlStatements, ImportMechanism.LoadDataInfile])
		{
			TestCaseData createData(string formatName, string? fileGzImport, bool invalidCsv,
				Func<MySqlConnection, Export.DumpOptions, BaseDumper>? dumperFactory,
				Func<ImportOptions, Stream, BaseImporter> importerFactory)
			{
				var action = $"Import{(fileGzImport != null ? "" : "+Export")}";
				var mechanismName = importMechanism == ImportMechanism.SqlStatements ? "SQL" : "LoadInfile";
				var threading = threadCount == 1 ? "Singlethreaded" : $"Multithreaded ({threadCount})";

				return new TestCaseData(fileGzImport, threadCount, importMechanism, invalidCsv, dumperFactory, importerFactory)
				{
					TestName = $"{action} [{formatName}] - ({mechanismName}) ({threading})",
					//Properties =
					//{
					//	["action"] = new List<string>([action]),
					//}
				};
			}

			yield return createData("JSON", null, false,
				(connection, options) => new JsonDumper(connection, options),
				(options, stream) => new JsonImporter(options, stream));

			yield return createData("CSV", null, false,
				(connection, options) => new CsvDumper(connection, options),
				(options, stream) => new CsvImporter(options, stream));

			yield return createData("CSV (mysql export)", "data\\mysql_export.csv.gz", true, null,
				(options, stream) => new CsvImporter(options, stream));

			yield return createData("CSV (compliant)", "data\\csv_compliant.csv.gz", false, null,
				(options, stream) => new CsvImporter(options, stream));

			yield return createData("MySQL", null, false,
				(connection, options) => new MysqlDumper(connection, options),
				(options, stream) => new MysqlImporter(options, stream));

			yield return createData("mysqldump (opt)", "data\\test_dump.sql.gz", false, null,
				(options, stream) => new MysqlImporter(options, stream));

			yield return createData("mysqldump (skip-opt)", "data\\test_dump_skipopt.sql.gz", false, null,
				(options, stream) => new MysqlImporter(options, stream));
		}
	}

	[TestCaseSource(nameof(TestCaseGenerator))]
	public async Task DoExportImportTest(string? fileGzImport, int threadCount, ImportMechanism importMechanism, bool invalidCsv,
		Func<MySqlConnection, Export.DumpOptions, BaseDumper>? dumperFactory,
		Func<ImportOptions, Stream, BaseImporter> importerFactory)
	{
		if (fileGzImport != null)
		{
			await using var inputStream = new GZipStream(new FileStream(fileGzImport, FileMode.Open), CompressionMode.Decompress, false);
			await BaseImportTest(inputStream, threadCount, importMechanism, invalidCsv, importerFactory);
		}
		else
		{
			using var memoryStream = await BaseExport(dumperFactory);
			await BaseImportTest(memoryStream, threadCount, importMechanism, invalidCsv, importerFactory);
		}
	}
}