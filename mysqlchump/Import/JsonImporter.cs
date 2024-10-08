using MySqlConnector;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using System.Text.RegularExpressions;
using System.Text.Json;

namespace mysqlchump.Import;

internal class JsonImporter : BaseImporter
{
	public async Task ImportAsync(Stream dataStream, Func<MySqlConnection> createConnection, string[] sourceTables,
		bool insertIgnore, bool noCreate, bool upgradeTokuDb, bool cleanAsagiIndexes)
	{
		var jsonReader = new JsonStreamReader(dataStream, new byte[4096]);

		void AssertToken(JsonTokenType tokenType, object value = null)
		{
			var token = jsonReader.ReadToken();

			if (token == null)
				throw new EndOfStreamException("Reached end of stream abruptly");

			if (token != tokenType)
				throw new InvalidOperationException(
					$"Token type {tokenType} invalid at this position L{jsonReader.GetReader().}:{jsonReader.LinePosition} (expected {token}{(value != null ? $", {value}" : "")})");

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

			if (upgradeTokuDb)
			{
				// TODO: replace with more generic regex
				createStatement = createStatement
					.Replace("ENGINE=TokuDB", "ENGINE=InnoDB");

				var regex = new Regex(@"`?compression`?='?tokudb_[a-z]+'?", RegexOptions.IgnoreCase);

				createStatement = regex.Replace(createStatement, "ROW_FORMAT=DYNAMIC");
			}

			if (cleanAsagiIndexes)
			{
				var primaryKeyRegex = new Regex(@"PRIMARY KEY \([^)]+\),");
				var keyRegex = new Regex(@"(?:UNIQUE )?KEY `?([a-z_]+)`?\s*.+$", RegexOptions.Multiline);

				bool isAsagi = keyRegex.Matches(createStatement).Any(x => x.Groups[1].Value == "num_subnum_index");

				if (isAsagi)
				{
					createStatement = keyRegex.Replace(createStatement, "");
					// we can't change primary key because AUTO_INCREMENT has to be on the primary key iirc
					// createStatement = primaryKeyRegex.Replace(createStatement, "PRIMARY KEY (`num`, `subnum`)");
					createStatement = primaryKeyRegex.Replace(createStatement, "PRIMARY KEY (`doc_id`), KEY `custom_num` (`num`), KEY `custom_threadnum_num` (`thread_num`, `num`)");
				}
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

			if (sourceTables == null
				|| sourceTables.Length == 0
				|| sourceTables.Any(x => x == "*")
				|| sourceTables.Any(x => tableName.Equals(x, StringComparison.OrdinalIgnoreCase)))
			{
				if (!noCreate)
					await using (var connection = createConnection())
					{
						using var command = connection.CreateCommand();

						command.CommandTimeout = 9999999;
						command.CommandText = createStatement;

						await command.ExecuteNonQueryAsync();
					}

				await DoParallelInserts(12, approxCount, tableName, createConnection, async (channel, reportRowCount) =>
				{
					try
					{
						while (true)
						{
							var (canContinue, insertQuery, rowCount) = GetNextInsertBatch(jsonReader, tableName, columns, insertIgnore);

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
						Console.Error.WriteLine($"Error when reading data source @ L{jsonReader.LineNumber}:{jsonReader.LinePosition}");
						Console.Error.WriteLine(ex.ToString());
					}

					channel.Writer.Complete();
				});
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
}