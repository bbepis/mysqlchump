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
		var reader = new StreamReader(fileStream, Encoding.UTF8, leaveOpen: true);

		var stopwatch = Stopwatch.StartNew();

		using var jsonReader = new JsonTextReader(reader);
		stopwatch.Restart();
		while (jsonReader.Read()) { }
		Console.WriteLine(stopwatch.ToString());
		jsonReader.Close();
		reader.Close();

		fileStream.Position = 0;
		reader = new StreamReader(fileStream, Encoding.UTF8, leaveOpen: true);
		var tokenizer = new JsonTokenizer(fileStream);

		stopwatch.Restart();
		while (tokenizer.Read() != JsonTokenType.EndOfFile) { };
		Console.WriteLine(stopwatch.ToString());
	}
}