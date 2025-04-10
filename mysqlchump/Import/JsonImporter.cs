using MySqlConnector;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
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

		using var jsonReader = new JsonTokenizer(dataStream);

		void AssertToken(JsonTokenType tokenType, object value = null)
		{
			var token = jsonReader.Read();

			if (token == JsonTokenType.EndOfFile || token != tokenType)
				goto failure;

			if (value == null)
				return;

			if ((token == JsonTokenType.String || token == JsonTokenType.PropertyName) && ((string)value).AsMemory().Equals(jsonReader.ValueString))
				goto failure;

			if (token == JsonTokenType.NumberLong && Convert.ToInt64(value) != jsonReader.ValueLong)
				goto failure;

			if (token == JsonTokenType.NumberDouble && Convert.ToDouble(value) != jsonReader.ValueDouble)
				goto failure;

			if (token == JsonTokenType.Boolean && (bool)value != jsonReader.ValueBoolean)
				goto failure;

			return;

failure:
				throw new InvalidOperationException(
					$"Token type {jsonReader.TokenType} invalid at this position (expected {tokenType}{(value != null ? $", {value}" : "")})");
		}

		AssertToken(JsonTokenType.StartObject);
		AssertToken(JsonTokenType.PropertyName, "version");
		AssertToken(JsonTokenType.NumberLong, 2L);
		AssertToken(JsonTokenType.PropertyName, "tables");
		AssertToken(JsonTokenType.StartArray);

		async Task<bool> ProcessTable(JsonTokenizer jsonReader)
		{
			if (jsonReader.Read() == JsonTokenType.EndOfFile || jsonReader.TokenType == JsonTokenType.EndArray)
				return false;

			AssertToken(JsonTokenType.PropertyName, "name");
			AssertToken(JsonTokenType.String);

			string tableName = jsonReader.ValueString.ToString();

			AssertToken(JsonTokenType.PropertyName, "create_statement");
			AssertToken(JsonTokenType.String);

			string createStatement = jsonReader.ValueString.ToString();

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

			AssertToken(JsonTokenType.PropertyName, "columns");
			AssertToken(JsonTokenType.StartObject);

			var columns = new List<(string columnName, string type)>();

			while (jsonReader.Read() != JsonTokenType.EndOfFile && jsonReader.TokenType == JsonTokenType.PropertyName)
			{
				var columnName = jsonReader.ValueString.ToString();
				AssertToken(JsonTokenType.String);

				columns.Add((columnName, jsonReader.ValueString.ToString()));
			}

			AssertToken(JsonTokenType.PropertyName, "approx_count");
			
			ulong? approxCount = jsonReader.Read() == JsonTokenType.Null ? null : (ulong?)jsonReader.ValueLong;

			AssertToken(JsonTokenType.PropertyName, "rows");
			AssertToken(JsonTokenType.StartArray);

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
							Console.Error.WriteLine($"Error when reading data source");
							Console.Error.WriteLine(ex.ToString());
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
							Console.Error.WriteLine($"Error when reading data source");
							Console.Error.WriteLine(ex.ToString());

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

				while (jsonReader.Read() != JsonTokenType.EndOfFile && layer >= 0)
				{
					if (jsonReader.TokenType == JsonTokenType.StartArray)
						layer++;
					else if (jsonReader.TokenType == JsonTokenType.EndArray)
						layer--;

					if (layer < 0)
						break;
				}
			}
			
			AssertToken(JsonTokenType.PropertyName, "actual_count");
			AssertToken(JsonTokenType.NumberLong);

			AssertToken(JsonTokenType.EndObject);

			return true;
		}

		while (await ProcessTable(jsonReader)) { }

		indexQueue.Writer.Complete();

		if (reindexTask != null)
			await reindexTask;
	}

	private StringBuilder queryBuilder = new StringBuilder(1_000_000);

	private (bool canContinue, string insertQuery, int rowCount) GetNextInsertBatch(JsonTokenizer jsonReader, string tableName, List<(string columnName, string type)> columns, bool insertIgnore)
	{
		const int insertLimit = 2000;
		int count = 0;

		queryBuilder.Clear();
		queryBuilder.Append($"INSERT{(insertIgnore ? " IGNORE" : "")} INTO `{tableName}` ({string.Join(',', columns.Select(x => $"`{x.columnName}`"))}) VALUES ");
		
		bool needsComma = false;
		bool canContinue = true;

		byte[] b64Buffer = new byte[64];

		for (; count < insertLimit; count++)
		{
			if (jsonReader.Read() == JsonTokenType.EndOfFile || jsonReader.TokenType != JsonTokenType.StartArray)
			{
				canContinue = false;
				break;
			}
			
			queryBuilder.Append(needsComma ? ",(" : "(");
			needsComma = true;

			for (int columnNum = 0; columnNum < columns.Count; columnNum++)
			{
				if (jsonReader.Read() == JsonTokenType.EndOfFile || jsonReader.TokenType == JsonTokenType.EndArray)
					throw new InvalidDataException("Row ends prematurely");

				if (columnNum > 0)
					queryBuilder.Append(',');
				
				string writtenValue = null;

				if (jsonReader.TokenType == JsonTokenType.Null)
					writtenValue = "NULL";
				else if (columns[columnNum].type == "BLOB")
				{
					if (b64Buffer.Length < jsonReader.ValueString.Length)
						b64Buffer = new byte[jsonReader.ValueString.Length];

					if (!Convert.TryFromBase64Chars(jsonReader.ValueString.Span, b64Buffer, out var decodeLength))
						throw new Exception($"Failed to decode base64 string: {jsonReader.ValueString.Span}");

					queryBuilder.Append("_binary 0x");

					writtenValue = Utility.ByteArrayToString(b64Buffer.AsSpan(0, decodeLength));
				}
				else if (jsonReader.TokenType == JsonTokenType.String)
					writtenValue = $"'{jsonReader.ValueString.ToString().Replace("\\", "\\\\").Replace("'", "''")}'";
				else if (jsonReader.TokenType == JsonTokenType.Boolean)
					writtenValue = jsonReader.ValueBoolean ? "TRUE" : "FALSE";
				//else if (jsonReader.TokenType == JsonTokenType.Date)
				//	writtenValue = $"'{(DateTime)value:yyyy-MM-dd HH:mm:ss}'";
				else if (jsonReader.TokenType == JsonTokenType.NumberLong)
					writtenValue = jsonReader.ValueLong.ToString();
				else if (jsonReader.TokenType == JsonTokenType.NumberDouble)
					writtenValue = jsonReader.ValueDouble.ToString();
				else
					throw new InvalidDataException($"Unknown token type: {jsonReader.TokenType}");

				if (writtenValue != null)
					queryBuilder.Append(writtenValue);
			}

			if (jsonReader.Read() == JsonTokenType.EndOfFile || jsonReader.TokenType != JsonTokenType.EndArray)
				throw new InvalidDataException("Row ends prematurely");
			
			queryBuilder.Append(')');
		}

		if (count == 0)
			return (false, null, 0);

		queryBuilder.Append(';');

		return (canContinue, queryBuilder.ToString(), count);
	}

	private (int rows, bool canContinue) WriteBatchCsvPipe(JsonTokenizer jsonReader, PipeWriter pipeWriter, string tableName, List<(string columnName, string type)> columns)
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

		void SanitizeAndWrite(ReadOnlySpan<char> input, Span<byte> span)
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

			void Flush(int i, ReadOnlySpan<char> input, Span<byte> span)
			{
				if (i > start)
					WriteString(input.Slice(start, i - start), span);
			}

			for (int i = 0; i < len; i++)
			{
				char c = input[i];

				switch (c)
				{
					case '\r':
						if (i + 1 < len && input[i + 1] == '\n')
						{
							Flush(i, input, span);
							WriteString("\\\n", span);
							i++;
							start = i + 1;
						}
						break;
					case '\n':
						Flush(i, input, span);
						WriteString("\\\n", span);
						start = i + 1;
						break;
					case '"':
						Flush(i, input, span);
						WriteString("\\\"", span);
						start = i + 1;
						break;
					case '\0':
						Flush(i, input, span);
						WriteString("\\0", span);
						start = i + 1;
						break;
					case '\\':
						Flush(i, input, span);
						WriteString("\\\\", span);
						start = i + 1;
						break;
				}
			}

			Flush(len, input, span);
			WriteString("\"", span);
		}

		while (true)
		{
			if (currentPosition >= 3 * 1024 * 1024)
				break;
			//if (rows >= 2000)
			//	break;

			if (jsonReader.Read() == JsonTokenType.EndOfFile || jsonReader.TokenType != JsonTokenType.StartArray)
			{
				canContinue = false;
				break;
			}
			
			WriteString("\n", span);

			for (int columnNum = 0; columnNum < columns.Count; columnNum++)
			{
				if (jsonReader.Read() == JsonTokenType.EndOfFile || jsonReader.TokenType == JsonTokenType.EndArray)
					throw new InvalidDataException("Row ends prematurely");

				if (columnNum > 0)
					WriteString(",", span);

				switch (jsonReader.TokenType)
				{
					case JsonTokenType.Null:
						WriteString("\\N", span);
						break;
					case JsonTokenType.String:
						// if (columns[columnNum].type == "BLOB")
						SanitizeAndWrite(jsonReader.ValueString.Span, span);
						break;
					case JsonTokenType.Boolean:
						WriteString(jsonReader.ValueBoolean ? "1" : "0", span);
						break;
					//case JsonToken.Date:
					//	WriteString(((DateTime)value).ToString("yyyy-MM-dd HH:mm:ss"), span);
					//	break;
					case JsonTokenType.NumberLong:
						// TODO: int.TryFormat
						WriteString(jsonReader.ValueLong.ToString(), span);
						break;
					case JsonTokenType.NumberDouble:
						// TODO: double.TryFormat
						WriteString(jsonReader.ValueDouble.ToString(), span);
						break;
					default:
						if (columns[columnNum].type == "BLOB")
						{
							//WriteString(Utility.ByteArrayToString(Convert.FromBase64String((string)value)), span);
							WriteString(jsonReader.ValueString.Span, span);
							break;
						}
						else
							throw new InvalidDataException($"Unknown token type: {jsonReader.TokenType}");
				}
			}

			if (jsonReader.Read() == JsonTokenType.EndOfFile || jsonReader.TokenType != JsonTokenType.EndArray)
				throw new InvalidDataException("Row ends prematurely");

			rows++;
		}

		if (currentPosition > 0)
			pipeWriter.Advance(currentPosition);

		return (rows, canContinue);
	}
}