using MySqlConnector;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using System.Threading.Channels;
using System.IO.Pipelines;

namespace mysqlchump.Import;

internal class JsonImporter : BaseImporter
{
	public async Task ImportAsync(Stream dataStream, Func<MySqlConnection> createConnection, ImportOptions options)
	{
		var indexQueue = Channel.CreateUnbounded<(string text, string tableName, string indexName, bool isFk, string sql)>();

		Task reindexTask = null;

		if (options.DeferIndexes)
		{
			reindexTask = Task.Run(async () =>
			{
				await using (var connection = createConnection())
				{
					using var command = connection.CreateCommand();
					command.CommandTimeout = 9999999;

					command.CommandText = "SET FOREIGN_KEY_CHECKS = 0;";
					await command.ExecuteNonQueryAsync();
					
					await foreach (var (text, tableName, indexName, isFk, sql) in indexQueue.Reader.ReadAllAsync())
					{
						command.CommandTimeout = 9999999;

						// check if index exists first
						if (isFk)
						{
							command.CommandText = $@"
SELECT COUNT(*)
FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS
WHERE CONSTRAINT_SCHEMA = '{connection.Database}'
	AND TABLE_NAME = '{tableName}'
	AND CONSTRAINT_NAME = '{indexName}';";
						}
						else
						{
							command.CommandText = $@"
SELECT COUNT(*)
FROM INFORMATION_SCHEMA.STATISTICS
WHERE table_schema = '{connection.Database}'
	AND table_name = '{tableName}'
	AND INDEX_NAME = '{indexName}';";
						}

							var exists = (long)await command.ExecuteScalarAsync();

						if (exists <= 0)
						{
							Console.Error.WriteLine(text);
							command.CommandText = sql;

							await command.ExecuteNonQueryAsync();
						}
					}

					command.CommandText = "SET FOREIGN_KEY_CHECKS = 1;";
					await command.ExecuteNonQueryAsync();
				}
			});
		}

		using var reader = new StreamReader(dataStream, Encoding.UTF8, leaveOpen: true);
		using var jsonReader = new JsonTextReader(reader);

		void AssertToken(JsonToken tokenType, object value = null)
		{
			if (!jsonReader.Read() || jsonReader.TokenType != tokenType || (value != null && !jsonReader.Value.Equals(value)))
				throw new InvalidOperationException(
					$"Token type {jsonReader.TokenType} invalid at this position L{jsonReader.LineNumber}:{jsonReader.LinePosition} (expected {tokenType}{(value != null ? $", {value}" : "")})");
		}

		AssertToken(JsonToken.StartObject);
		AssertToken(JsonToken.PropertyName, "version");
		AssertToken(JsonToken.Integer, 2L);
		AssertToken(JsonToken.PropertyName, "tables");
		AssertToken(JsonToken.StartArray);

		async Task<bool> ProcessTable(JsonTextReader jsonReader)
		{
			if (!jsonReader.Read() || jsonReader.TokenType == JsonToken.EndArray)
				return false;

			AssertToken(JsonToken.PropertyName, "name");
			AssertToken(JsonToken.String);

			string tableName = (string)jsonReader.Value;

			AssertToken(JsonToken.PropertyName, "create_statement");
			AssertToken(JsonToken.String);

			string createStatement = (string)jsonReader.Value;

			//Console.Error.WriteLine(createStatement);

			var parsedTable = CreateTableParser.Parse(createStatement);

			if (options.SetInnoDB)
			{
				parsedTable.Options["ENGINE"] = "InnoDB";

				parsedTable.Options.Remove("COMPRESSION"); // tokudb
				parsedTable.Options["ROW_FORMAT"] = "DYNAMIC";

				createStatement = parsedTable.ToCreateTableSql();
			}

			if (options.SetCompressed)
			{
				parsedTable.Options["ROW_FORMAT"] = "COMPRESSED";

				createStatement = parsedTable.ToCreateTableSql();
			}

			var removedIndexes = new List<Index>();
			var removedFks = new List<ForeignKey>();

			if (options.DeferIndexes || options.StripIndexes)
			{
				removedIndexes.AddRange(parsedTable.Indexes.Where(x => x.Type != IndexType.Primary));
				parsedTable.Indexes.RemoveAll(x => x.Type != IndexType.Primary);

				removedFks.AddRange(parsedTable.ForeignKeys);
				parsedTable.ForeignKeys.Clear();

				createStatement = parsedTable.ToCreateTableSql();
			}

			AssertToken(JsonToken.PropertyName, "columns");
			AssertToken(JsonToken.StartObject);

			var columns = new List<(string columnName, string type)>();

			while (await jsonReader.ReadAsync() && jsonReader.TokenType == JsonToken.PropertyName)
			{
				var columnName = (string)jsonReader.Value;
				AssertToken(JsonToken.String);

				columns.Add((columnName, (string)jsonReader.Value));
			}

			AssertToken(JsonToken.PropertyName, "approx_count");
			await jsonReader.ReadAsync();
			ulong? approxCount = jsonReader.Value == null ? null : (ulong?)(long)jsonReader.Value;

			AssertToken(JsonToken.PropertyName, "rows");
			AssertToken(JsonToken.StartArray);

			if (options.SourceTables == null
				|| options.SourceTables.Length == 0
				|| options.SourceTables.Any(x => x == "*")
				|| options.SourceTables.Any(x => tableName.Equals(x, StringComparison.OrdinalIgnoreCase)))
			{
				if (!options.NoCreate)
					await using (var connection = createConnection())
					{
						using var command = connection.CreateCommand();

						command.CommandTimeout = 9999999;

						// check if table exists first
						command.CommandText = $@"SELECT COUNT(*)
							FROM information_schema.TABLES
							WHERE TABLE_SCHEMA = '{connection.Database}' AND TABLE_NAME = '{tableName}'";

						var exists = (long)await command.ExecuteScalarAsync();

						if (exists != 0)
						{
							Console.Error.WriteLine($"Table '{tableName}' already exists, so it will not be created.");
						}
						else
						{
							command.CommandText = createStatement;
							await command.ExecuteNonQueryAsync();
						}
					}

				if (options.ImportMechanism == ImportMechanism.SqlStatements)
				{
					await DoParallelSqlInserts(options.ParallelThreads, approxCount, tableName, createConnection, async (channel, reportRowCount) =>
					{
						try
						{
							while (true)
							{
								var (canContinue, insertQuery, rowCount) = GetNextInsertBatch(jsonReader, tableName, columns, options.InsertIgnore);

								reportRowCount(rowCount);

								if (insertQuery == null)
									break;

								await channel.Writer.WriteAsync(insertQuery);

								if (!canContinue)
									break;
							}
						}
						catch (Exception ex)
						{
							Logging.WriteLine($"Error when reading data source @ L{jsonReader.LineNumber}:{jsonReader.LinePosition}");
							Logging.WriteLine(ex.ToString());
						}

						channel.Writer.Complete();
					});
				}
				else if (options.ImportMechanism == ImportMechanism.LoadDataInfile)
				{
					await DoParallelCsvImports(options, approxCount, tableName, columns, createConnection, async (pipeWriters, reportRowCount) =>
					{
						var flushTasks = new Task[pipeWriters.Length];

						try
						{
							while (true)
							{
								int writerIndex = 0;
								PipeWriter writer;

								while (true)
								{
									bool found = false;
									for (int i = 0; i < pipeWriters.Length; i++)
									{
										if (flushTasks[i] != null && flushTasks[i].IsFaulted)
											Console.WriteLine(flushTasks[i].Exception);

										if (flushTasks[i] == null || flushTasks[i].IsCompleted)
										{
											writerIndex = i;
											found = true;
											break;
										}
									}

									if (found)
									{
										writer = pipeWriters[writerIndex];
										break;
									}

									// all writers are currently busy.
									await Task.Delay(2);
								}

								var (rowCount, canContinue) = WriteBatchCsvPipe(jsonReader, writer, tableName, columns);

								if (rowCount == 0)
									break;

								//await writer.FlushAsync();
								flushTasks[writerIndex] = Task.Run(async () => await writer.FlushAsync());

								reportRowCount(rowCount);

								if (!canContinue)
									break;
							}

							foreach (var writer in pipeWriters)
								await writer.CompleteAsync();
						}
						catch (Exception ex)
						{
							Logging.WriteLine($"Error when reading data source @ L{jsonReader.LineNumber}:{jsonReader.LinePosition}");
							Logging.WriteLine(ex.ToString());

							foreach (var writer in pipeWriters)
								await writer.CompleteAsync(ex);
						}
					});
				}

				if (options.DeferIndexes)
				{
					foreach (var index in removedIndexes)
					{
						await indexQueue.Writer.WriteAsync((
							$"Creating index {tableName}.{index.Name}...",
							tableName,
							index.Name,
							false,
							$"ALTER TABLE `{tableName}` ADD {index.ToSql()};"));
					}

					foreach (var fk in removedFks)
					{
						await indexQueue.Writer.WriteAsync((
							$"Creating foreign key {tableName}.{fk.Name}...",
							tableName,
							fk.Name,
							true,
							$"ALTER TABLE `{tableName}` ADD {fk.ToSql()};"));
					}
				}
			}
			else
			{
				Console.Error.WriteLine($"Skipping table '{tableName}' ({(approxCount.HasValue ? $"~{approxCount}" : "?")} rows)");

				int layer = 0;

				while (jsonReader.Read() && layer >= 0)
				{
					if (jsonReader.TokenType == JsonToken.StartArray)
						layer++;
					else if (jsonReader.TokenType == JsonToken.EndArray)
						layer--;

					if (layer < 0)
						break;
				}
			}
			
			AssertToken(JsonToken.PropertyName, "actual_count");
			AssertToken(JsonToken.Integer);

			AssertToken(JsonToken.EndObject);

			return true;
		}

		while (await ProcessTable(jsonReader)) { }

		indexQueue.Writer.Complete();

		if (reindexTask != null)
			await reindexTask;
	}

	private StringBuilder queryBuilder = new StringBuilder(1_000_000);

	private (bool canContinue, string insertQuery, int rowCount) GetNextInsertBatch(JsonTextReader jsonReader, string tableName, List<(string columnName, string type)> columns, bool insertIgnore)
	{
		const int insertLimit = 2000;
		int count = 0;

		queryBuilder.Clear();
		queryBuilder.Append($"INSERT{(insertIgnore ? " IGNORE" : "")} INTO `{tableName}` ({string.Join(',', columns.Select(x => $"`{x.columnName}`"))}) VALUES ");
		
		bool needsComma = false;
		bool canContinue = true;

		for (; count < insertLimit; count++)
		{
			if (!jsonReader.Read() || jsonReader.TokenType != JsonToken.StartArray)
			{
				canContinue = false;
				break;
			}
			
			queryBuilder.Append(needsComma ? ",(" : "(");
			needsComma = true;

			for (int columnNum = 0; columnNum < columns.Count; columnNum++)
			{
				if (!jsonReader.Read() || jsonReader.TokenType == JsonToken.EndArray)
					throw new InvalidDataException("Row ends prematurely");

				if (columnNum > 0)
					queryBuilder.Append(',');
				
				object value = jsonReader.Value;
				string writtenValue;
				
				if (jsonReader.TokenType == JsonToken.Null)
					writtenValue = "NULL";
				else if (columns[columnNum].type == "BLOB")
					writtenValue = "_binary 0x" + Utility.ByteArrayToString(Convert.FromBase64String((string)value));
				else if (jsonReader.TokenType == JsonToken.String)
					writtenValue = $"'{((string)value).Replace("\\", "\\\\").Replace("'", "''")}'";
				else if (jsonReader.TokenType == JsonToken.Boolean)
					writtenValue = (bool)value ? "TRUE": "FALSE";
				else if (jsonReader.TokenType == JsonToken.Date)
					writtenValue = $"'{(DateTime)value:yyyy-MM-dd HH:mm:ss}'";
				else if (jsonReader.TokenType == JsonToken.Integer
				         || jsonReader.TokenType == JsonToken.Float)
					writtenValue = value.ToString();
				else
					throw new InvalidDataException($"Unknown token type: {jsonReader.TokenType}");

				queryBuilder.Append(writtenValue);
			}

			if (!jsonReader.Read() || jsonReader.TokenType != JsonToken.EndArray)
				throw new InvalidDataException("Row ends prematurely");
			
			queryBuilder.Append(')');
		}

		if (count == 0)
			return (false, null, 0);

		queryBuilder.Append(';');

		return (canContinue, queryBuilder.ToString(), count);
	}

	private (int rows, bool canContinue) WriteBatchCsvPipe(JsonTextReader jsonReader, PipeWriter pipeWriter, string tableName, List<(string columnName, string type)> columns)
	{
		bool canContinue = true;

		const int minimumSpanSize = 4 * 1024 * 1024;

		var span = pipeWriter.GetSpan(minimumSpanSize);
		var currentPosition = 0;
		int rows = 0;

		var encoding = new UTF8Encoding(false);

		void WriteString(ReadOnlySpan<char> data, Span<byte> span)
		{
			// UTF-8 max bytes per character is 4
			var maxByteLength = data.Length * 4;

			if (span.Length - currentPosition < maxByteLength)
			{
				if (currentPosition > 0)
					pipeWriter.Advance(currentPosition);

				span = pipeWriter.GetSpan(minimumSpanSize);
				//writtenSinceFlush += currentPosition;
				currentPosition = 0;

				//if (writtenSinceFlush > 10 * 1024 * 1024)
				//	Task.Run(() => pipeWriter.FlushAsync()).Wait();
			}

			currentPosition += encoding.GetBytes(data, span.Slice(currentPosition));
		}

		void SanitizeAndWrite(string input, Span<byte> span)
		{
			bool requiresEscaping = false;
			int len = input.Length;
			for (int i = 0; i < len; i++)
			{
				char c = input[i];
				if (c == '"' || c == '\\' || c == '\0' || c == '\n' || c == ',')
				{
					requiresEscaping = true;
					break;
				}
			}

			if (!requiresEscaping)
			{
				WriteString(input, span);
				return;
			}

			WriteString("\"", span);

			int start = 0;

			void Flush(int i, Span<byte> span)
			{
				if (i > start)
					WriteString(input.AsSpan(start, i - start), span);
			}

			for (int i = 0; i < len; i++)
			{
				char c = input[i];

				switch (c)
				{
					case '\r':
						if (i + 1 < len && input[i + 1] == '\n')
						{
							Flush(i, span);
							WriteString("\\\n", span);
							i++;
							start = i + 1;
						}
						break;
					case '\n':
						Flush(i, span);
						WriteString("\\\n", span);
						start = i + 1;
						break;
					case '"':
						Flush(i, span);
						WriteString("\\\"", span);
						start = i + 1;
						break;
					case '\0':
						Flush(i, span);
						WriteString("\\0", span);
						start = i + 1;
						break;
					case '\\':
						Flush(i, span);
						WriteString("\\\\", span);
						start = i + 1;
						break;
				}
			}

			Flush(len, span);
			WriteString("\"", span);
		}

		while (true)
		{
			if (currentPosition >= 3 * 1024 * 1024)
				break;
			//if (rows >= 2000)
			//	break;

			if (!jsonReader.Read() || jsonReader.TokenType != JsonToken.StartArray)
			{
				canContinue = false;
				break;
			}
			
			WriteString("\n", span);

			for (int columnNum = 0; columnNum < columns.Count; columnNum++)
			{
				if (!jsonReader.Read() || jsonReader.TokenType == JsonToken.EndArray)
					throw new InvalidDataException("Row ends prematurely");

				if (columnNum > 0)
					WriteString(",", span);

				object value = jsonReader.Value;

				switch (jsonReader.TokenType)
				{
					case JsonToken.Null:
						WriteString("\\N", span);
						break;
					case JsonToken.String:
						// if (columns[columnNum].type == "BLOB")
						SanitizeAndWrite((string)value, span);
						break;
					case JsonToken.Boolean:
						WriteString((bool)value ? "1" : "0", span);
						break;
					case JsonToken.Date:
						WriteString(((DateTime)value).ToString("yyyy-MM-dd HH:mm:ss"), span);
						break;
					case JsonToken.Integer:
					case JsonToken.Float:
						// TODO: int.TryFormat
						WriteString(value.ToString(), span);
						break;
					default:
						if (columns[columnNum].type == "BLOB")
						{
							//WriteString(Utility.ByteArrayToString(Convert.FromBase64String((string)value)), span);
							WriteString((string)value, span);
							break;
						}
						else
							throw new InvalidDataException($"Unknown token type: {jsonReader.TokenType}");
				}
			}

			if (!jsonReader.Read() || jsonReader.TokenType != JsonToken.EndArray)
				throw new InvalidDataException("Row ends prematurely");

			rows++;
		}

		if (currentPosition > 0)
			pipeWriter.Advance(currentPosition);

		return (rows, canContinue);
	}
}