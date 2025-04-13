using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO.Pipelines;

namespace mysqlchump.Import;

public class JsonImporter : BaseImporter
{
	protected JsonTokenizer JsonTokenizer { get; }
	private bool HasReadStart { get; set; } = false;

	public JsonImporter(ImportOptions options, Stream dataStream) : base(options, dataStream)
	{
		JsonTokenizer = new JsonTokenizer(DataStream);
	}

	protected override async Task<(bool foundAnotherTable, string createTableSql, ulong? approxRows)> ReadToNextTable()
	{
		void AssertToken(JsonTokenType tokenType, object value = null)
		{
			var token = JsonTokenizer.Read();

			if (token == JsonTokenType.EndOfFile || token != tokenType)
				goto failure;

			if (value == null)
				return;

			if ((token == JsonTokenType.String || token == JsonTokenType.PropertyName) && ((string)value).AsMemory().Equals(JsonTokenizer.ValueString))
				goto failure;

			if (token == JsonTokenType.NumberLong && Convert.ToInt64(value) != JsonTokenizer.ValueLong)
				goto failure;

			if (token == JsonTokenType.NumberDouble && Convert.ToDouble(value) != JsonTokenizer.ValueDouble)
				goto failure;

			if (token == JsonTokenType.Boolean && (bool)value != JsonTokenizer.ValueBoolean)
				goto failure;

			return;
		failure:
			throw new InvalidOperationException(
				$"Token type {JsonTokenizer.TokenType} invalid at this position (expected {tokenType}{(value != null ? $", {value}" : "")})");
		}

		if (!HasReadStart)
		{
			AssertToken(JsonTokenType.StartObject);
			AssertToken(JsonTokenType.PropertyName, "version");
			AssertToken(JsonTokenType.NumberLong, 2L);
			AssertToken(JsonTokenType.PropertyName, "tables");
			AssertToken(JsonTokenType.StartArray);

			HasReadStart = true;
		}
		else
		{
			if (JsonTokenizer.TokenType == JsonTokenType.StartArray)
			{
				// skip to the end of the rows section
				int layer = 0;
				while (JsonTokenizer.Read() != JsonTokenType.EndOfFile && layer >= 0)
				{
					if (JsonTokenizer.TokenType == JsonTokenType.StartArray)
						layer++;
					else if (JsonTokenizer.TokenType == JsonTokenType.EndArray)
						layer--;

					if (layer < 0)
						break;
				}
			}

			AssertToken(JsonTokenType.PropertyName, "actual_count");
			AssertToken(JsonTokenType.NumberLong);

			AssertToken(JsonTokenType.EndObject);
		}

		if (JsonTokenizer.Read() == JsonTokenType.EndOfFile || JsonTokenizer.TokenType == JsonTokenType.EndArray)
			return (false, null, null);

		AssertToken(JsonTokenType.PropertyName, "name");
		AssertToken(JsonTokenType.String);

		string tableName = JsonTokenizer.ValueString.ToString();

		AssertToken(JsonTokenType.PropertyName, "create_statement");
		AssertToken(JsonTokenType.String);

		string createStatement = JsonTokenizer.ValueString.ToString();

		//Console.Error.WriteLine(createStatement);



		AssertToken(JsonTokenType.PropertyName, "columns");
		AssertToken(JsonTokenType.StartObject);

		var columns = new List<(string columnName, string type)>();

		while (JsonTokenizer.Read() != JsonTokenType.EndOfFile && JsonTokenizer.TokenType == JsonTokenType.PropertyName)
		{
			var columnName = JsonTokenizer.ValueString.ToString();
			AssertToken(JsonTokenType.String);

			columns.Add((columnName, JsonTokenizer.ValueString.ToString()));
		}

		AssertToken(JsonTokenType.PropertyName, "approx_count");

		ulong? approxCount = JsonTokenizer.Read() == JsonTokenType.Null ? null : (ulong?)JsonTokenizer.ValueLong;

		AssertToken(JsonTokenType.PropertyName, "rows");
		AssertToken(JsonTokenType.StartArray);

		return (true, createStatement, approxCount);
	}

	private StringBuilder queryBuilder = new StringBuilder(1_000_000);
	protected override (int rows, bool canContinue, string sqlCommand) ReadDataSql(string tableName, ColumnInfo[] columns)
	{
		const int insertLimit = 2000;
		int count = 0;

		queryBuilder.Clear();
		queryBuilder.Append($"INSERT{(ImportOptions.InsertIgnore ? " IGNORE" : "")} INTO `{tableName}` ({string.Join(',', columns.Select(x => $"`{x.name}`"))}) VALUES ");

		bool needsComma = false;
		bool canContinue = true;

		byte[] b64Buffer = new byte[64];

		for (; count < insertLimit; count++)
		{
			if (JsonTokenizer.Read() == JsonTokenType.EndOfFile || JsonTokenizer.TokenType != JsonTokenType.StartArray)
			{
				canContinue = false;
				break;
			}

			queryBuilder.Append(needsComma ? ",(" : "(");
			needsComma = true;

			for (int columnNum = 0; columnNum < columns.Length; columnNum++)
			{
				if (JsonTokenizer.Read() == JsonTokenType.EndOfFile || JsonTokenizer.TokenType == JsonTokenType.EndArray)
					throw new InvalidDataException("Row ends prematurely");

				if (columnNum > 0)
					queryBuilder.Append(',');

				string writtenValue = null;

				if (JsonTokenizer.TokenType == JsonTokenType.Null)
					writtenValue = "NULL";
				else if (columns[columnNum].type == "BLOB")
				{
					if (b64Buffer.Length < JsonTokenizer.ValueString.Length)
						b64Buffer = new byte[JsonTokenizer.ValueString.Length];

					if (!Convert.TryFromBase64Chars(JsonTokenizer.ValueString.Span, b64Buffer, out var decodeLength))
						throw new Exception($"Failed to decode base64 string: {JsonTokenizer.ValueString.Span}");

					queryBuilder.Append("_binary 0x");

					writtenValue = Utility.ByteArrayToString(b64Buffer.AsSpan(0, decodeLength));
				}
				else if (JsonTokenizer.TokenType == JsonTokenType.String)
					writtenValue = $"'{JsonTokenizer.ValueString.ToString().Replace("\\", "\\\\").Replace("'", "''")}'";
				else if (JsonTokenizer.TokenType == JsonTokenType.Boolean)
					writtenValue = JsonTokenizer.ValueBoolean ? "TRUE" : "FALSE";
				//else if (jsonReader.TokenType == JsonTokenType.Date)
				//	writtenValue = $"'{(DateTime)value:yyyy-MM-dd HH:mm:ss}'";
				else if (JsonTokenizer.TokenType == JsonTokenType.NumberLong)
					writtenValue = JsonTokenizer.ValueLong.ToString();
				else if (JsonTokenizer.TokenType == JsonTokenType.NumberDouble)
					writtenValue = JsonTokenizer.ValueDouble.ToString();
				else
					throw new InvalidDataException($"Unknown token type: {JsonTokenizer.TokenType}");

				if (writtenValue != null)
					queryBuilder.Append(writtenValue);
			}
			if (JsonTokenizer.Read() == JsonTokenType.EndOfFile || JsonTokenizer.TokenType != JsonTokenType.EndArray)
				throw new InvalidDataException("Row ends prematurely");

			queryBuilder.Append(')');
		}

		if (count == 0)
			return (0, false, null);

		queryBuilder.Append(';');

		return (count, canContinue, queryBuilder.ToString());
	}

	protected override (int rows, bool canContinue) ReadDataCsv(PipeWriter pipeWriter, string tableName, ColumnInfo[] columns)
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

			if (JsonTokenizer.Read() == JsonTokenType.EndOfFile || JsonTokenizer.TokenType != JsonTokenType.StartArray)
			{
				canContinue = false;
				break;
			}
			
			WriteString("\n", span);

			for (int columnNum = 0; columnNum < columns.Length; columnNum++)
			{
				if (JsonTokenizer.Read() == JsonTokenType.EndOfFile || JsonTokenizer.TokenType == JsonTokenType.EndArray)
					throw new InvalidDataException("Row ends prematurely");

				if (columnNum > 0)
					WriteString(",", span);

				switch (JsonTokenizer.TokenType)
				{
					case JsonTokenType.Null:
						WriteString("\\N", span);
						break;
					case JsonTokenType.String:
						// if (columns[columnNum].type == "BLOB")
						SanitizeAndWrite(JsonTokenizer.ValueString.Span, span);
						break;
					case JsonTokenType.Boolean:
						WriteString(JsonTokenizer.ValueBoolean ? "1" : "0", span);
						break;
					//case JsonToken.Date:
					//	WriteString(((DateTime)value).ToString("yyyy-MM-dd HH:mm:ss"), span);
					//	break;
					case JsonTokenType.NumberLong:
						// TODO: int.TryFormat
						WriteString(JsonTokenizer.ValueLong.ToString(), span);
						break;
					case JsonTokenType.NumberDouble:
						// TODO: double.TryFormat
						WriteString(JsonTokenizer.ValueDouble.ToString(), span);
						break;
					default:
						if (columns[columnNum].type == "BLOB")
						{
							//WriteString(Utility.ByteArrayToString(Convert.FromBase64String((string)value)), span);
							WriteString(JsonTokenizer.ValueString.Span, span);
							break;
						}
						else
							throw new InvalidDataException($"Unknown token type: {JsonTokenizer.TokenType}");
				}
			}

			if (JsonTokenizer.Read() == JsonTokenType.EndOfFile || JsonTokenizer.TokenType != JsonTokenType.EndArray)
				throw new InvalidDataException("Row ends prematurely");

			rows++;
		}

		if (currentPosition > 0)
			pipeWriter.Advance(currentPosition);

		return (rows, canContinue);
	}

	public override void Dispose() => JsonTokenizer.Dispose();
}