using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO.Pipelines;
using System.Globalization;

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
		int count = 0;

		queryBuilder.Clear();
		queryBuilder.Append($"INSERT{(ImportOptions.InsertIgnore ? " IGNORE" : "")} INTO `{tableName}` ({string.Join(',', columns.Select(x => $"`{x.name}`"))}) VALUES ");

		bool needsComma = false;
		bool canContinue = true;

		byte[] b64Buffer = new byte[64];
		char[] conversionBuffer = new char[64];

		for (; count < ImportOptions.InsertBatchSize; count++)
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
				
				var column = columns[columnNum];
				
				string writtenValue = null;

				if (JsonTokenizer.TokenType == JsonTokenType.Null)
					writtenValue = "NULL";
				else if (column.type == ColumnDataType.Binary)
				{
					if (JsonTokenizer.ValueString.Length == 0)
					{
						writtenValue = "''";
					}
					else
					{
						if (b64Buffer.Length < JsonTokenizer.ValueString.Length)
							b64Buffer = new byte[JsonTokenizer.ValueString.Length];

						if (!Convert.TryFromBase64Chars(JsonTokenizer.ValueString.Span, b64Buffer, out var decodeLength))
							throw new Exception($"Failed to decode base64 string: {JsonTokenizer.ValueString.Span}");

						queryBuilder.Append("_binary 0x");

						writtenValue = Utility.ByteArrayToString(b64Buffer.AsSpan(0, decodeLength));
					}
				}
				else if (column.type == ColumnDataType.Date)
				{
					var date = DateTime.ParseExact(JsonTokenizer.ValueString.Span, "yyyy-MM-ddTH:mm:ss.fffZ", CultureInfo.InvariantCulture,
						DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal);

					if (!date.TryFormat(conversionBuffer, out int writtenChars, "yyyy-MM-dd HH:mm:ss"))
						throw new Exception($"Failed to convert date: {JsonTokenizer.ValueString.Span}");

					queryBuilder.Append("'");
					queryBuilder.Append(conversionBuffer.AsSpan(0, writtenChars));
					queryBuilder.Append("'");
				}
				else if (JsonTokenizer.TokenType == JsonTokenType.String)
				{
					queryBuilder.Append('\'');
					if (!JsonTokenizer.ValueString.Span.ContainsAny("\'\\"))
						queryBuilder.Append(JsonTokenizer.ValueString);
					else
						queryBuilder.Append(JsonTokenizer.ValueString.ToString().Replace("\\", "\\\\").Replace("'", "''"));
					queryBuilder.Append('\'');
				}
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

	//private byte[] base64Buffer = new byte[512 * 1024];

	protected override (int rows, bool canContinue) ReadDataCsv(PipeWriter pipeWriter, string tableName, ColumnInfo[] columns)
	{
		Span<char> intFormatBuffer = stackalloc char[64];

		bool canContinue = true;

		const int minimumSpanSize = 4 * 1024 * 1024;

		var span = pipeWriter.GetSpan(minimumSpanSize);
		var currentPosition = 0;
		int rows = 0;

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

			currentPosition += Utility.NoBomUtf8.GetBytes(data, span.Slice(currentPosition));
		}

		byte[] HexAlphabetSpan = new[]
		{
			(byte)'0', (byte)'1', (byte)'2', (byte)'3',
			(byte)'4', (byte)'5', (byte)'6', (byte)'7',
			(byte)'8', (byte)'9', (byte)'A', (byte)'B',
			(byte)'C', (byte)'D', (byte)'E', (byte)'F'
		};

		void WriteHex(ReadOnlySpan<byte> data, Span<byte> span)
		{
			for (var i = 0; i < data.Length; ++i)
			{
				int j = currentPosition + i * 2;

				span[j] = HexAlphabetSpan[data[i] >> 4];
				span[j + 1] = HexAlphabetSpan[data[i] & 0xF];

				currentPosition += 2;
			}
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
						// Dates, strings and binary blobs are processed through here
						// - Strings get handled as strings
						// - Binary blobs are read from JSON as Base64, and inserted via LOAD DATA as Base64
						// - Dates are handled as strings by MySQL
						// Therefore just pass as-is
						SanitizeAndWrite(JsonTokenizer.ValueString.Span, span);
						break;
					case JsonTokenType.Boolean:
						WriteString(JsonTokenizer.ValueBoolean ? "1" : "0", span);
						break;
					case JsonTokenType.NumberLong:
						if (!JsonTokenizer.ValueLong.TryFormat(intFormatBuffer, out int written))
							throw new Exception("Integer format buffer too small");

						WriteString(intFormatBuffer.Slice(0, written), span);
						break;
					case JsonTokenType.NumberDouble:
						if (!JsonTokenizer.ValueDouble.TryFormat(intFormatBuffer, out int dWritten))
							throw new Exception("Integer format buffer too small");

						WriteString(intFormatBuffer.Slice(0, dWritten), span);
						break;
					default:
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