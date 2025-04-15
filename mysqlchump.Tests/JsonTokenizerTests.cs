using System.Diagnostics;
using System.Text;
using Newtonsoft.Json;

namespace mysqlchump.Tests;

class JsonTokenizerTests
{
	[Test]
	public void DoJsonTest()
	{
		using var fileStream = new FileStream(@"C:\Temp\archived.moe-foolfuuka-20250331.full.json", FileMode.Open);
		var reader = new StreamReader(fileStream, Utility.NoBomUtf8, leaveOpen: true);

		var stopwatch = Stopwatch.StartNew();

		using var jsonReader = new JsonTextReader(reader);
		stopwatch.Restart();
		while (jsonReader.Read()) { }
		Console.WriteLine(stopwatch.ToString());
		jsonReader.Close();
		reader.Close();

		fileStream.Position = 0;
		reader = new StreamReader(fileStream, Utility.NoBomUtf8, leaveOpen: true);
		var tokenizer = new JsonTokenizer(fileStream);

		stopwatch.Restart();
		while (tokenizer.Read() != JsonTokenType.EndOfFile) { };
		Console.WriteLine(stopwatch.ToString());
	}

	[Test]
	public void DoSmallJsonTest()
	{
		using var jsonStream = new MemoryStream(Utility.NoBomUtf8.GetBytes("{\"propName\": [1,2,3]}"));

		var tokenizer = new JsonTokenizer(jsonStream);

		while (true) {
			var token = tokenizer.Read();

			switch (token)
			{
				case JsonTokenType.None:
				case JsonTokenType.StartObject:
				case JsonTokenType.EndObject:
				case JsonTokenType.StartArray:
				case JsonTokenType.EndArray:
				case JsonTokenType.Null:
				case JsonTokenType.Comma:
				case JsonTokenType.Colon:
				case JsonTokenType.EndOfFile:
					Console.WriteLine(token.ToString());
					break;
				case JsonTokenType.String:
				case JsonTokenType.PropertyName:
					Console.WriteLine($"{token}: {tokenizer.ValueString}");
					break;
				case JsonTokenType.NumberLong:
					Console.WriteLine($"{token}: {tokenizer.ValueLong}");
					break;
				case JsonTokenType.NumberDouble:
					Console.WriteLine($"{token}: {tokenizer.ValueDouble}");
					break;
				case JsonTokenType.Boolean:
					Console.WriteLine($"{token}: {tokenizer.ValueBoolean}");
					break;
			}

			if (token == JsonTokenType.EndOfFile)
				break;
		}
	}
}