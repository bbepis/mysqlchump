using MySqlConnector;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace mysqlchump.Import;

internal class JsonImporter : BaseImporter
{
	public async Task ImportAsync(Stream dataStream, bool insertIgnore, Func<MySqlConnection> createConnection)
	{
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
		queryBuilder.Append($"INSERT{(insertIgnore ? " IGNORE" : "")} INTO `{tableName}` ({string.Join(',', columns.Select(x => x.columnName))}) VALUES ");
		
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