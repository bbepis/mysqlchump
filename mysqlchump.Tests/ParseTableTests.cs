using mysqlchump.SqlParsing;
using Newtonsoft.Json.Linq;
using NUnit.Framework.Legacy;
using static mysqlchump.SqlParsing.Index;
using Assert = NUnit.Framework.Legacy.ClassicAssert;
using Index = mysqlchump.SqlParsing.Index;

namespace mysqlchump.Tests;

public class TableTestcase
{
	public string Title { get; set; }
	public string CreateTable { get; set; }
	public Table ExpectedTable { get; set; }

	public TableTestcase(string title, string createTable, Table expectedTable)
	{
		Title = title;
		CreateTable = createTable;
		ExpectedTable = expectedTable;
	}
}

public class ParseTableTests
{
	public static readonly TableTestcase[] TestcaseData = [
		new TableTestcase("table1",
			@"CREATE TABLE IF NOT EXISTS `table1` (
				`doc_id` int unsigned NOT NULL AUTO_INCREMENT,
				`media_id` int unsigned NOT NULL DEFAULT '0',
				`poster_ip` decimal(39,0) unsigned NOT NULL DEFAULT '0',
				`num` int unsigned NOT NULL,
				`subnum` int unsigned NOT NULL,
				`thread_num` int unsigned NOT NULL DEFAULT '0',
				`op` tinyint(1) NOT NULL DEFAULT '0',
				`timestamp` int unsigned NOT NULL,
				`timestamp_expired` int unsigned NOT NULL,
				`preview_orig` varchar(20) DEFAULT NULL,
				`preview_w` smallint unsigned NOT NULL DEFAULT '0',
				`preview_h` smallint unsigned NOT NULL DEFAULT '0',
				`media_filename` text,
				`media_w` smallint unsigned NOT NULL DEFAULT '0',
				`media_h` smallint unsigned NOT NULL DEFAULT '0',
				`media_size` int unsigned NOT NULL DEFAULT '0',
				`media_hash` varchar(25) DEFAULT NULL,
				`media_orig` varchar(20) DEFAULT NULL,
				`spoiler` tinyint(1) NOT NULL DEFAULT '0',
				`deleted` tinyint(1) NOT NULL DEFAULT '0',
				`capcode` varchar(1) NOT NULL DEFAULT 'N',
				`email` varchar(100) DEFAULT NULL,
				`name` varchar(100) DEFAULT NULL,
				`trip` varchar(25) DEFAULT NULL,
				`title` varchar(100) DEFAULT NULL,
				`comment` text,
				`delpass` tinytext,
				`sticky` tinyint(1) NOT NULL DEFAULT '0',
				`locked` tinyint(1) NOT NULL DEFAULT '0',
				`poster_hash` varchar(8) DEFAULT NULL,
				`poster_country` varchar(2) DEFAULT NULL,
				`exif` text,
				PRIMARY KEY (`doc_id`),
				UNIQUE KEY `num_subnum_index` (`num`,`subnum`),
				KEY `thread_num_subnum_index` (`thread_num`,`num`,`subnum`),
				KEY `subnum_index` (`subnum`),
				KEY `op_index` (`op`),
				KEY `media_id_index` (`media_id`),
				KEY `media_hash_index` (`media_hash`),
				KEY `media_orig_index` (`media_orig`),
				KEY `name_trip_index` (`name`,`trip`),
				KEY `trip_index` (`trip`),
				KEY `email_index` (`email`),
				KEY `poster_ip_index` (`poster_ip`),
				KEY `timestamp_index` (`timestamp`)
			) ENGINE=TokuDB AUTO_INCREMENT=227642182 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			new Table
			{
				Name = "table1",
				Columns = {
					new Column
					{
						Name = "doc_id",
						DataType = "int",
						IsAutoIncrement = true,
						Unsigned = true
					},
					new Column
					{
						Name = "media_id",
						DataType = "int",
						DefaultValue = "\'0\'",
						Unsigned = true
					},
					new Column
					{
						Name = "poster_ip",
						DataType = "decimal(39,0)",
						DefaultValue = "\'0\'",
						Unsigned = true
					},
					new Column
					{
						Name = "num",
						DataType = "int",
						Unsigned = true
					},
					new Column
					{
						Name = "subnum",
						DataType = "int",
						Unsigned = true
					},
					new Column
					{
						Name = "thread_num",
						DataType = "int",
						DefaultValue = "\'0\'",
						Unsigned = true
					},
					new Column
					{
						Name = "op",
						DataType = "tinyint(1)",
						DefaultValue = "\'0\'"
					},
					new Column
					{
						Name = "timestamp",
						DataType = "int",
						Unsigned = true
					},
					new Column
					{
						Name = "timestamp_expired",
						DataType = "int",
						Unsigned = true
					},
					new Column
					{
						Name = "preview_orig",
						DataType = "varchar(20)",
						IsNullable = true,
						DefaultValue = "NULL"
					},
					new Column
					{
						Name = "preview_w",
						DataType = "smallint",
						DefaultValue = "\'0\'",
						Unsigned = true
					},
					new Column
					{
						Name = "preview_h",
						DataType = "smallint",
						DefaultValue = "\'0\'",
						Unsigned = true
					},
					new Column
					{
						Name = "media_filename",
						DataType = "text",
						IsNullable = true
					},
					new Column
					{
						Name = "media_w",
						DataType = "smallint",
						DefaultValue = "\'0\'",
						Unsigned = true
					},
					new Column
					{
						Name = "media_h",
						DataType = "smallint",
						DefaultValue = "\'0\'",
						Unsigned = true
					},
					new Column
					{
						Name = "media_size",
						DataType = "int",
						DefaultValue = "\'0\'",
						Unsigned = true
					},
					new Column
					{
						Name = "media_hash",
						DataType = "varchar(25)",
						IsNullable = true,
						DefaultValue = "NULL"
					},
					new Column
					{
						Name = "media_orig",
						DataType = "varchar(20)",
						IsNullable = true,
						DefaultValue = "NULL"
					},
					new Column
					{
						Name = "spoiler",
						DataType = "tinyint(1)",
						DefaultValue = "\'0\'"
					},
					new Column
					{
						Name = "deleted",
						DataType = "tinyint(1)",
						DefaultValue = "\'0\'"
					},
					new Column
					{
						Name = "capcode",
						DataType = "varchar(1)",
						DefaultValue = "\'N\'"
					},
					new Column
					{
						Name = "email",
						DataType = "varchar(100)",
						IsNullable = true,
						DefaultValue = "NULL"
					},
					new Column
					{
						Name = "name",
						DataType = "varchar(100)",
						IsNullable = true,
						DefaultValue = "NULL"
					},
					new Column
					{
						Name = "trip",
						DataType = "varchar(25)",
						IsNullable = true,
						DefaultValue = "NULL"
					},
					new Column
					{
						Name = "title",
						DataType = "varchar(100)",
						IsNullable = true,
						DefaultValue = "NULL"
					},
					new Column
					{
						Name = "comment",
						DataType = "text",
						IsNullable = true
					},
					new Column
					{
						Name = "delpass",
						DataType = "tinytext",
						IsNullable = true
					},
					new Column
					{
						Name = "sticky",
						DataType = "tinyint(1)",
						DefaultValue = "\'0\'"
					},
					new Column
					{
						Name = "locked",
						DataType = "tinyint(1)",
						DefaultValue = "\'0\'"
					},
					new Column
					{
						Name = "poster_hash",
						DataType = "varchar(8)",
						IsNullable = true,
						DefaultValue = "NULL"
					},
					new Column
					{
						Name = "poster_country",
						DataType = "varchar(2)",
						IsNullable = true,
						DefaultValue = "NULL"
					},
					new Column
					{
						Name = "exif",
						DataType = "text",
						IsNullable = true
					}
				},
				Indexes = {
					new Index
					{
						Type = IndexType.Primary,
						Columns = { "doc_id" }
					},
					new Index
					{
						Name = "num_subnum_index",
						Type = IndexType.Unique,
						Columns = { "num", "subnum" }
					},
					new Index
					{
						Name = "thread_num_subnum_index",
						Type = IndexType.Regular,
						Columns = { "thread_num", "num", "subnum" }
					},
					new Index
					{
						Name = "subnum_index",
						Type = IndexType.Regular,
						Columns = { "subnum" }
					},
					new Index
					{
						Name = "op_index",
						Type = IndexType.Regular,
						Columns = { "op" }
					},
					new Index
					{
						Name = "media_id_index",
						Type = IndexType.Regular,
						Columns = { "media_id" }
					},
					new Index
					{
						Name = "media_hash_index",
						Type = IndexType.Regular,
						Columns = { "media_hash" }
					},
					new Index
					{
						Name = "media_orig_index",
						Type = IndexType.Regular,
						Columns = { "media_orig" }
					},
					new Index
					{
						Name = "name_trip_index",
						Type = IndexType.Regular,
						Columns = { "name", "trip" }
					},
					new Index
					{
						Name = "trip_index",
						Type = IndexType.Regular,
						Columns = { "trip" }
					},
					new Index
					{
						Name = "email_index",
						Type = IndexType.Regular,
						Columns = { "email" }
					},
					new Index
					{
						Name = "poster_ip_index",
						Type = IndexType.Regular,
						Columns = { "poster_ip" }
					},
					new Index
					{
						Name = "timestamp_index",
						Type = IndexType.Regular,
						Columns = { "timestamp" }
					}
				},
				ForeignKeys = {},
				Options = {
					{ "ENGINE", "TokuDB" },
					{ "AUTO_INCREMENT", "227642182" },
					{ "DEFAULT CHARSET", "utf8mb4" },
					{ "COLLATE", "utf8mb4_0900_ai_ci" }
				}
			}),
		new TableTestcase("table1v2",
			@"CREATE TABLE IF NOT EXISTS `table1v2` (
				`doc_id` int unsigned NOT NULL AUTO_INCREMENT,
				`media_id` int unsigned NOT NULL DEFAULT '0',
				`poster_ip` decimal(39,0) unsigned NOT NULL DEFAULT '0',
				`num` int unsigned NOT NULL,
				`subnum` int unsigned NOT NULL,
				`thread_num` int unsigned NOT NULL DEFAULT '0',
				`op` tinyint(1) NOT NULL DEFAULT '0',
				`timestamp` int unsigned NOT NULL,
				`timestamp_expired` int unsigned NOT NULL,
				`preview_orig` varchar(20) DEFAULT NULL,
				`preview_w` smallint unsigned NOT NULL DEFAULT '0',
				`preview_h` smallint unsigned NOT NULL DEFAULT '0',
				`media_filename` text,
				`media_w` smallint unsigned NOT NULL DEFAULT '0',
				`media_h` smallint unsigned NOT NULL DEFAULT '0',
				`media_size` int unsigned NOT NULL DEFAULT '0',
				`media_hash` varchar(25) DEFAULT NULL,
				`media_orig` varchar(20) DEFAULT NULL,
				`spoiler` tinyint(1) NOT NULL DEFAULT '0',
				`deleted` tinyint(1) NOT NULL DEFAULT '0',
				`capcode` varchar(1) NOT NULL DEFAULT 'N',
				`email` varchar(100) DEFAULT NULL,
				`name` varchar(100) DEFAULT NULL,
				`trip` varchar(25) DEFAULT NULL,
				`title` varchar(100) DEFAULT NULL,
				`comment` text,
				`delpass` tinytext,
				`sticky` tinyint(1) NOT NULL DEFAULT '0',
				`locked` tinyint(1) NOT NULL DEFAULT '0',
				`poster_hash` varchar(8) DEFAULT NULL,
				`poster_country` varchar(2) DEFAULT NULL,
				`exif` text,
				PRIMARY KEY (`doc_id`),
				UNIQUE KEY `num_subnum_index` (`num`,`subnum`),
				INDEX `thread_num_subnum_index` (`thread_num`,`num`,`subnum`),
				INDEX `subnum_index` (`subnum`),
				INDEX `op_index` (`op`),
				INDEX `media_id_index` (`media_id`),
				INDEX `media_hash_index` (`media_hash`),
				INDEX `media_orig_index` (`media_orig`),
				INDEX `name_trip_index` (`name`,`trip`),
				INDEX `trip_index` (`trip`),
				INDEX `email_index` (`email`),
				INDEX `poster_ip_index` (`poster_ip`),
				INDEX `timestamp_index` (`timestamp`)
			) ENGINE=TokuDB AUTO_INCREMENT=227642182 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			new Table
			{
				Name = "table1v2",
				Columns = {
					new Column
					{
						Name = "doc_id",
						DataType = "int",
						IsAutoIncrement = true,
						Unsigned = true
					},
					new Column
					{
						Name = "media_id",
						DataType = "int",
						DefaultValue = "\'0\'",
						Unsigned = true
					},
					new Column
					{
						Name = "poster_ip",
						DataType = "decimal(39,0)",
						DefaultValue = "\'0\'",
						Unsigned = true
					},
					new Column
					{
						Name = "num",
						DataType = "int",
						Unsigned = true
					},
					new Column
					{
						Name = "subnum",
						DataType = "int",
						Unsigned = true
					},
					new Column
					{
						Name = "thread_num",
						DataType = "int",
						DefaultValue = "\'0\'",
						Unsigned = true
					},
					new Column
					{
						Name = "op",
						DataType = "tinyint(1)",
						DefaultValue = "\'0\'"
					},
					new Column
					{
						Name = "timestamp",
						DataType = "int",
						Unsigned = true
					},
					new Column
					{
						Name = "timestamp_expired",
						DataType = "int",
						Unsigned = true
					},
					new Column
					{
						Name = "preview_orig",
						DataType = "varchar(20)",
						IsNullable = true,
						DefaultValue = "NULL"
					},
					new Column
					{
						Name = "preview_w",
						DataType = "smallint",
						DefaultValue = "\'0\'",
						Unsigned = true
					},
					new Column
					{
						Name = "preview_h",
						DataType = "smallint",
						DefaultValue = "\'0\'",
						Unsigned = true
					},
					new Column
					{
						Name = "media_filename",
						DataType = "text",
						IsNullable = true
					},
					new Column
					{
						Name = "media_w",
						DataType = "smallint",
						DefaultValue = "\'0\'",
						Unsigned = true
					},
					new Column
					{
						Name = "media_h",
						DataType = "smallint",
						DefaultValue = "\'0\'",
						Unsigned = true
					},
					new Column
					{
						Name = "media_size",
						DataType = "int",
						DefaultValue = "\'0\'",
						Unsigned = true
					},
					new Column
					{
						Name = "media_hash",
						DataType = "varchar(25)",
						IsNullable = true,
						DefaultValue = "NULL"
					},
					new Column
					{
						Name = "media_orig",
						DataType = "varchar(20)",
						IsNullable = true,
						DefaultValue = "NULL"
					},
					new Column
					{
						Name = "spoiler",
						DataType = "tinyint(1)",
						DefaultValue = "\'0\'"
					},
					new Column
					{
						Name = "deleted",
						DataType = "tinyint(1)",
						DefaultValue = "\'0\'"
					},
					new Column
					{
						Name = "capcode",
						DataType = "varchar(1)",
						DefaultValue = "\'N\'"
					},
					new Column
					{
						Name = "email",
						DataType = "varchar(100)",
						IsNullable = true,
						DefaultValue = "NULL"
					},
					new Column
					{
						Name = "name",
						DataType = "varchar(100)",
						IsNullable = true,
						DefaultValue = "NULL"
					},
					new Column
					{
						Name = "trip",
						DataType = "varchar(25)",
						IsNullable = true,
						DefaultValue = "NULL"
					},
					new Column
					{
						Name = "title",
						DataType = "varchar(100)",
						IsNullable = true,
						DefaultValue = "NULL"
					},
					new Column
					{
						Name = "comment",
						DataType = "text",
						IsNullable = true
					},
					new Column
					{
						Name = "delpass",
						DataType = "tinytext",
						IsNullable = true
					},
					new Column
					{
						Name = "sticky",
						DataType = "tinyint(1)",
						DefaultValue = "\'0\'"
					},
					new Column
					{
						Name = "locked",
						DataType = "tinyint(1)",
						DefaultValue = "\'0\'"
					},
					new Column
					{
						Name = "poster_hash",
						DataType = "varchar(8)",
						IsNullable = true,
						DefaultValue = "NULL"
					},
					new Column
					{
						Name = "poster_country",
						DataType = "varchar(2)",
						IsNullable = true,
						DefaultValue = "NULL"
					},
					new Column
					{
						Name = "exif",
						DataType = "text",
						IsNullable = true
					}
				},
				Indexes = {
					new Index
					{
						Type = IndexType.Primary,
						Columns = { "doc_id" }
					},
					new Index
					{
						Name = "num_subnum_index",
						Type = IndexType.Unique,
						Columns = { "num", "subnum" }
					},
					new Index
					{
						Name = "thread_num_subnum_index",
						Type = IndexType.Regular,
						Columns = { "thread_num", "num", "subnum" }
					},
					new Index
					{
						Name = "subnum_index",
						Type = IndexType.Regular,
						Columns = { "subnum" }
					},
					new Index
					{
						Name = "op_index",
						Type = IndexType.Regular,
						Columns = { "op" }
					},
					new Index
					{
						Name = "media_id_index",
						Type = IndexType.Regular,
						Columns = { "media_id" }
					},
					new Index
					{
						Name = "media_hash_index",
						Type = IndexType.Regular,
						Columns = { "media_hash" }
					},
					new Index
					{
						Name = "media_orig_index",
						Type = IndexType.Regular,
						Columns = { "media_orig" }
					},
					new Index
					{
						Name = "name_trip_index",
						Type = IndexType.Regular,
						Columns = { "name", "trip" }
					},
					new Index
					{
						Name = "trip_index",
						Type = IndexType.Regular,
						Columns = { "trip" }
					},
					new Index
					{
						Name = "email_index",
						Type = IndexType.Regular,
						Columns = { "email" }
					},
					new Index
					{
						Name = "poster_ip_index",
						Type = IndexType.Regular,
						Columns = { "poster_ip" }
					},
					new Index
					{
						Name = "timestamp_index",
						Type = IndexType.Regular,
						Columns = { "timestamp" }
					}
				},
				ForeignKeys = {},
				Options = {
					{ "ENGINE", "TokuDB" },
					{ "AUTO_INCREMENT", "227642182" },
					{ "DEFAULT CHARSET", "utf8mb4" },
					{ "COLLATE", "utf8mb4_0900_ai_ci" }
				}
			}),
		new TableTestcase("table2",
			@"CREATE TABLE IF NOT EXISTS `table2` (
				`doc_id` int unsigned NOT NULL AUTO_INCREMENT,  /* test */
			) ENGINE=InnoDB AUTO_INCREMENT=227642182 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci /* `COMPRESSION`=TOKUDB_ZLIB */",
			new Table
			{
				Name = "table2",
				Columns = {
					new Column
					{
						Name = "doc_id",
						DataType = "int",
						IsAutoIncrement = true,
						Unsigned = true
					}
				},
				Indexes = { },
				ForeignKeys = {},
				Options = {
					{ "ENGINE", "InnoDB" },
					{ "AUTO_INCREMENT", "227642182" },
					{ "DEFAULT CHARSET", "utf8mb4" },
					{ "COLLATE", "utf8mb4_0900_ai_ci" }
				}
			}),
		new TableTestcase("table3",
			@"CREATE TABLE IF NOT EXISTS `table3` (
				`doc_id` int unsigned NOT NULL AUTO_INCREMENT,  /* test */
				 KEY `ff_plugin_ff_articles_title_index` (`title`(191))
			) ENGINE=InnoDB AUTO_INCREMENT=227642182 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci /* `COMPRESSION`=TOKUDB_ZLIB */",
			new Table
			{
				Name = "table3",
				Columns = {
					new Column
					{
						Name = "doc_id",
						DataType = "int",
						IsAutoIncrement = true,
						Unsigned = true
					}
				},
				Indexes = {
					new Index {
						Name = "ff_plugin_ff_articles_title_index",
						Type = IndexType.Regular,
						Columns = {
							new Index.IndexColumn("title", 191)
						},
					}
				},
				ForeignKeys = {},
				Options = {
					{ "ENGINE", "InnoDB" },
					{ "AUTO_INCREMENT", "227642182" },
					{ "DEFAULT CHARSET", "utf8mb4" },
					{ "COLLATE", "utf8mb4_0900_ai_ci" }
				}
			}),
		new TableTestcase("table4",
			@"CREATE TABLE IF NOT EXISTS `table4` (
				`doc_id` int(10) unsigned NOT NULL AUTO_INCREMENT,
				`num` int(10) unsigned NOT NULL,
				`subnum` int(10) unsigned NOT NULL,
				`thread_num` int(10) unsigned NOT NULL DEFAULT '0',
				`media_filename` text,
				`comment` text,
				PRIMARY KEY (`doc_id`),
				KEY `num_index` (`num`),
				KEY `subnum_index` (`subnum`),
				KEY `thread_num_index` (`thread_num`),
				FULLTEXT KEY `media_filename_fulltext` (`media_filename`),
				FULLTEXT KEY `comment_fulltext` (`comment`)
			) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4",
			new Table
			{
				Name = "table4",
				Columns =
				{
					new Column
					{
						Name = "doc_id",
						DataType = "int(10)",
						IsAutoIncrement = true,
						Unsigned = true
					},
					new Column
					{
						Name = "num",
						DataType = "int(10)",
						Unsigned = true
					},
					new Column
					{
						Name = "subnum",
						DataType = "int(10)",
						Unsigned = true
					},
					new Column
					{
						Name = "thread_num",
						DataType = "int(10)",
						DefaultValue = "\'0\'",
						Unsigned = true
					},
					new Column
					{
						Name = "media_filename",
						DataType = "text",
						IsNullable = true
					},
					new Column
					{
						Name = "comment",
						DataType = "text",
						IsNullable = true
					}
				},
				Indexes =
				{
					new Index
					{
						Type = IndexType.Primary,
						Columns = { "doc_id" }
					},
					new Index
					{
						Name = "num_index",
						Columns = { "num" }
					},
					new Index
					{
						Name = "subnum_index",
						Columns = { "subnum" }
					},
					new Index
					{
						Name = "thread_num_index",
						Columns = { "thread_num" }
					},
					new Index
					{
						Name = "media_filename_fulltext",
						Type = IndexType.Fulltext,
						Columns = { "media_filename" }
					},
					new Index
					{
						Name = "comment_fulltext",
						Type = IndexType.Fulltext,
						Columns = { "comment" }
					}
				},
				Options =
				{
					{ "ENGINE", "MyISAM" },
					{ "DEFAULT CHARSET", "utf8mb4" }
				}
			}),
		new TableTestcase("table5",
			@"CREATE TABLE IF NOT EXISTS `table5` (
  `BoardId` smallint unsigned NOT NULL,
  `PostId` bigint unsigned NOT NULL,
  `Index` tinyint unsigned NOT NULL,
  `FileId` int unsigned DEFAULT NULL,
  `Filename` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `IsSpoiler` tinyint(1) NOT NULL,
  `IsDeleted` tinyint(1) NOT NULL,
  `AdditionalMetadata` json DEFAULT NULL,
  PRIMARY KEY (`BoardId`,`PostId`,`Index`),
  KEY `IX_file_mappings_FileId` (`FileId`),
  CONSTRAINT `FK_file_mappings_boards_BoardId` FOREIGN KEY (`BoardId`) REFERENCES `boards` (`Id`) ON DELETE CASCADE,
  CONSTRAINT `FK_file_mappings_files_FileId` FOREIGN KEY (`FileId`) REFERENCES `files` (`Id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci",
			new Table
			{
				Name = "table5",
				Columns =
				{
					new Column
					{
						Name = "BoardId",
						DataType = "smallint",
						Unsigned = true
					},
					new Column
					{
						Name = "PostId",
						DataType = "bigint",
						Unsigned = true
					},
					new Column
					{
						Name = "Index",
						DataType = "tinyint",
						Unsigned = true
					},
					new Column
					{
						Name = "FileId",
						DataType = "int",
						IsNullable = true,
						DefaultValue = "NULL",
						Unsigned = true
					},
					new Column
					{
						Name = "Filename",
						DataType = "varchar(255)",
						CharacterSet = "utf8mb4",
						Collation = "utf8mb4_0900_ai_ci"
					},
					new Column
					{
						Name = "IsSpoiler",
						DataType = "tinyint(1)"
					},
					new Column
					{
						Name = "IsDeleted",
						DataType = "tinyint(1)"
					},
					new Column
					{
						Name = "AdditionalMetadata",
						DataType = "json",
						IsNullable = true,
						DefaultValue = "NULL"
					}
				},
				Indexes =
				{
					new Index
					{
						Type = IndexType.Primary,
						Columns =
						{
							new IndexColumn("BoardId"),
							new IndexColumn("PostId"),
							new IndexColumn("Index")
						}
					},
					new Index
					{
						Name = "IX_file_mappings_FileId",
						Columns =
						{
							new IndexColumn("FileId")
						}
					}
				},
				ForeignKeys =
				{
					new ForeignKey
					{
						Name = "FK_file_mappings_boards_BoardId",
						Columns = { "BoardId" },
						ReferenceTable = "boards",
						ReferenceColumns = { "Id" },
						OnDelete = "CASCADE"
					},
					new ForeignKey
					{
						Name = "FK_file_mappings_files_FileId",
						Columns = { "FileId" },
						ReferenceTable = "files",
						ReferenceColumns = { "Id" }
					}
				},
				Options =
				{
					{ "ENGINE", "InnoDB" },
					{ "DEFAULT CHARSET", "utf8mb4" },
					{ "COLLATE", "utf8mb4_general_ci" }
				}
			}),
	];

	private static IEnumerable<TestCaseData> GetTableTestcases()
	{
		foreach (var table in TestcaseData)
		{
			yield return new TestCaseData(table)
			{
				TestName = $"ParseTable - {table.Title}"
			};
		}
	}

#pragma warning disable NUnit2005

	[TestCaseSource(nameof(GetTableTestcases))]
	public void ParseTable(TableTestcase tableTestcase)
	{
		var table = CreateTableParser.Parse(tableTestcase.CreateTable);

		Console.WriteLine(table.Dump(new DumpOptions() { IgnoreDefaultValues = true, DumpStyle = DumpStyle.CSharp }));

		Assert.AreEqual(tableTestcase.ExpectedTable.Name, table.Name, "Table name didn't match");

		var dumpOptions = new DumpOptions() { IgnoreDefaultValues = true, DumpStyle = DumpStyle.Console };

		Assert.AreEqual(tableTestcase.ExpectedTable.Columns.Count, table.Columns.Count, "Table column count mismatch");

		for (int i = 0; i < table.Columns.Count; i++)
		{
			Column column = table.Columns[i];
			Column expectedColumn = tableTestcase.ExpectedTable.Columns[i];

			if (!JToken.DeepEquals(JObject.FromObject(column), JObject.FromObject(expectedColumn)))
			{
				Assert.Fail($"Column assertion failed.\nExpected: {expectedColumn.Dump(dumpOptions)}\nActual: {column.Dump(dumpOptions)}");
			}
		}

		Assert.AreEqual(tableTestcase.ExpectedTable.Indexes.Count, table.Indexes.Count, "Table index count mismatch");

		for (int i = 0; i < table.Indexes.Count; i++)
		{
			Index index = table.Indexes[i];
			Index expectedIndex = tableTestcase.ExpectedTable.Indexes[i];

			if (!JToken.DeepEquals(JObject.FromObject(index), JObject.FromObject(expectedIndex)))
			{
				Assert.Fail($"Index assertion failed.\nExpected: {expectedIndex.Dump(dumpOptions)}\nActual: {index.Dump(dumpOptions)}");
			}
		}

		CollectionAssert.AreEqual(tableTestcase.ExpectedTable.Options, table.Options, "Table option assertion failed");

		Console.WriteLine(table.ToCreateTableSql());
	}
}