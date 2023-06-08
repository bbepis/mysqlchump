using MySqlConnector;
using Nito.AsyncEx;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace mysqlchump.Import;

internal class JsonImporter
{
	public async Task ImportAsync(Stream dataStream, bool insertIgnore, Func<MySqlConnection> createConnection)
	{
		using var reader = new StreamReader(dataStream, Encoding.UTF8, leaveOpen: true);
		using var jsonReader = new JsonTextReader(reader);

		void AssertToken(JsonToken tokenType, object value = null)
		{
			if (!jsonReader.Read() || jsonReader.TokenType != tokenType || (value != null && !jsonReader.Value.Equals(value)))
				throw new InvalidOperationException(
					$"Token type {jsonReader.TokenType} invalid at this position L{jsonReader.LineNumber}:{jsonReader.LinePosition} (expected {tokenType})");
		}

		AssertToken(JsonToken.StartObject);
		AssertToken(JsonToken.PropertyName, "version");
		AssertToken(JsonToken.Integer, 1L);
		AssertToken(JsonToken.PropertyName, "tables");
		AssertToken(JsonToken.StartArray);

		async Task<bool> ProcessTable(JsonTextReader jsonReader)
		{
			const int concurrentLimit = 12;

			if (!jsonReader.Read() || jsonReader.TokenType == JsonToken.EndArray)
				return false;

			AssertToken(JsonToken.PropertyName, "name");
			AssertToken(JsonToken.String);

			string tableName = (string)jsonReader.Value;

			AssertToken(JsonToken.PropertyName, "create_statement");
			AssertToken(JsonToken.String);

			string createStatement = (string)jsonReader.Value;

			await using (var connection = createConnection())
			{
				connection.Open();
				
				using var command = connection.CreateCommand();

				command.CommandTimeout = 9999999;
				command.CommandText = createStatement;

				await command.ExecuteNonQueryAsync();
			}

			AssertToken(JsonToken.PropertyName, "columns");
			AssertToken(JsonToken.StartObject);

			var columns = new Dictionary<string, string>();

			while (await jsonReader.ReadAsync() && jsonReader.TokenType == JsonToken.PropertyName)
			{
				var columnName = (string)jsonReader.Value;
				AssertToken(JsonToken.String);

				columns[columnName] = (string)jsonReader.Value;
			}

			AssertToken(JsonToken.PropertyName, "approx_count");
			await jsonReader.ReadAsync();
			ulong? approxCount = jsonReader.Value == null ? null : (ulong?)(long)jsonReader.Value;

			AssertToken(JsonToken.PropertyName, "rows");
			AssertToken(JsonToken.StartArray);

			var channel = Channel.CreateBounded<string>(2);
			var transactionSemaphore = new AsyncSemaphore(1);

			int processedRowCount = 0;

			void writeProgress()
			{
				if (OperatingSystem.IsWindows())
					Console.CursorLeft = 0;
				else
					Console.Error.Write("\u001b[1000D"); // move cursor to the left
				
				double percentage = 100d * processedRowCount / approxCount.GetValueOrDefault();

				if (percentage > 100 || double.IsNaN(percentage))
					percentage = 100;

				Console.Error.Write($"{tableName} - {processedRowCount:N0} / ~{approxCount?.ToString("N0") ?? "?"} - ({percentage:N2}%)");
			}

			var cts = new CancellationTokenSource();

			bool printProgress = !Console.IsErrorRedirected;

			if (printProgress)
				_ = Task.Run(async () =>
				{
					while (!cts.IsCancellationRequested)
					{
						writeProgress();
						await Task.Delay(1000);
					}
				});

			var enqueueTask = Task.Run(async () =>
			{
				try
				{
					while (true)
					{
						var (canContinue, insertQuery, rowCount) = GetNextInsertBatch(jsonReader, tableName, columns, insertIgnore);

						processedRowCount += rowCount;

						if (insertQuery == null)
							break;

						await channel.Writer.WriteAsync(insertQuery);

						if (!canContinue)
							break;
					}
				}
				catch (Exception ex)
				{
					Console.Error.WriteLine($"Error when reading data source @ L{jsonReader.LineNumber}:{jsonReader.LinePosition}");
					Console.Error.WriteLine(ex.ToString());
				}

				channel.Writer.Complete();
			});

			var sendTasks = Enumerable.Range(0, concurrentLimit).Select(async _ =>
			{
				await using var connection = createConnection();
				connection.Open();

				Task sendCommand(string commandText)
				{
					using var command = connection.CreateCommand();

					command.CommandTimeout = 9999999;
					command.CommandText = commandText;

					return command.ExecuteNonQueryAsync();
				}

				string commandText = null;

				try
				{
					using (var @lock = await transactionSemaphore.LockAsync())
						await sendCommand("SET SESSION time_zone = \"+00:00\"; SET autocommit=0; SET UNIQUE_CHECKS=0; START TRANSACTION;");
					
					await foreach (var text in channel.Reader.ReadAllAsync())
					{
						commandText = text;

						await sendCommand(commandText);
					}

					commandText = null;

					using (var @lock = await transactionSemaphore.LockAsync())
						await sendCommand("COMMIT;");
				}
				catch (Exception ex)
				{
					if (commandText != null)
						Console.Error.WriteLine(commandText);

					Console.Error.WriteLine(ex);
				}
			}).ToArray();

			await Task.WhenAll(sendTasks.Append(enqueueTask));
			
			if (printProgress)
			{
				cts.Cancel();
				writeProgress();
				Console.Error.WriteLine();
			}

			AssertToken(JsonToken.PropertyName, "actual_count");
			AssertToken(JsonToken.Integer);

			AssertToken(JsonToken.EndObject);

			return true;
		}

		while (await ProcessTable(jsonReader)) { }
	}

	private StringBuilder queryBuilder = new StringBuilder(1_000_000);

	private (bool canContinue, string insertQuery, int rowCount) GetNextInsertBatch(JsonTextReader jsonReader, string tableName, Dictionary<string, string> columns, bool insertIgnore)
	{
		const int insertLimit = 2000;
		int count = 0;

		Dictionary<string, int> columnOrdering =
			new(columns.Keys.OrderBy(x => x).Select((x, i) => new KeyValuePair<string, int>(x, i)));

		var orderedColumnNames = columns.Keys.OrderBy(x => columnOrdering[x]).ToArray();

		queryBuilder.Clear();
		queryBuilder.Append($"INSERT{(insertIgnore ? " IGNORE" : "")} INTO `{tableName}` ({string.Join(',', orderedColumnNames)}) VALUES ");

		var insertArray = new string[columns.Count];

		bool needsComma = false;
		bool canContinue = true;

		for (; count < insertLimit; count++)
		{
			if (!jsonReader.Read() || jsonReader.TokenType != JsonToken.StartObject)
			{
				canContinue = false;
				break;
			}
			
			queryBuilder.Append(needsComma ? ",(" : "(");
			needsComma = true;

			while (jsonReader.Read() && jsonReader.TokenType == JsonToken.PropertyName)
			{
				var columnName = (string)jsonReader.Value;

				if (!columnOrdering.TryGetValue(columnName, out int index))
					throw new InvalidDataException($"Unknown row column: {jsonReader.Value}");

				jsonReader.Read();
				object value = jsonReader.Value;
				
				if (jsonReader.TokenType == JsonToken.Null)
					insertArray[index] = "NULL";
				else if (columns[columnName] == "BLOB")
					insertArray[index] = "_binary 0x" + Utility.ByteArrayToString(Convert.FromBase64String((string)value));
				else if (jsonReader.TokenType == JsonToken.String)
					insertArray[index] = $"'{((string)value).Replace("\\", "\\\\").Replace("'", "''")}'";
				else if (jsonReader.TokenType == JsonToken.Boolean)
					insertArray[index] = (bool)value ? "TRUE": "FALSE";
				else if (jsonReader.TokenType == JsonToken.Date)
					insertArray[index] = $"'{(DateTime)value:yyyy-MM-dd HH:mm:ss}'";
				else if (jsonReader.TokenType == JsonToken.Integer
				         || jsonReader.TokenType == JsonToken.Float)
					insertArray[index] = value.ToString();
				else
					throw new InvalidDataException($"Unknown token type: {jsonReader.TokenType}");
			}

			queryBuilder.Append(string.Join(',', insertArray));

			queryBuilder.Append(")");
		}

		if (count == 0)
			return (false, null, 0);

		queryBuilder.Append(";");

		return (canContinue, queryBuilder.ToString(), count);
	}
}