using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MySqlConnector;

namespace mysqlchump.Tests;

[TestFixture]
internal class FullTest
{
	internal class TestRowData
	{
		public int Id;
		public string? TextData;
		public byte[]? BinaryData;
		public DateTime? Date;
		public decimal? DecimalData;
	}

	private static readonly DateTime BaseDateTime = DateTime.Parse("2008-01-10 12:34:56");
	private TestRowData GenerateTestRowData(int id)
	{
		var random = new Random(id + 1000);

		string? textData = null;
		byte[]? binaryData = null;
		DateTime? date = null;
		decimal? decimalData = null;

		if (id % 509 != 0)
		{
			var stringLength = (int)Math.Floor(random.NextDouble() * 1025);

			Span<char> stringChars = stackalloc char[stringLength];

			const int asciiMin = 32;
			const int asciiMax = 126;
			const int asciiRange = (asciiMax - asciiMin) + 1;

			for (int i = 0; i < stringLength; i++)
				stringChars[i] = (char)(byte)Math.Floor(asciiMin + random.NextDouble() * asciiRange);

			textData = new string(stringChars);
		}

		if (id % 659 != 0)
		{
			var binaryLength = (int)Math.Floor(random.NextDouble() * 1025);

			binaryData = new byte[binaryLength];

			for (int i = 0; i < binaryLength; i++)
				binaryData[i] = (byte)Math.Floor(random.NextDouble() * 256);
		}

		if (id % 719 != 0)
		{
			const int maxRange = 60 * 60 * 24 * 365 * 10;
			date = BaseDateTime + TimeSpan.FromSeconds(random.NextDouble() * maxRange);
		}

		if (id % 109 != 0)
		{
			decimalData = (decimal)random.NextDouble();
		}

		return new()
		{
			Id = id,
			TextData = textData,
			BinaryData = binaryData,
			Date = date,
			DecimalData = decimalData
		};
	}

	[Test]
	public async Task CreateTestTable()
	{
		var connection = new MySqlConnection("Server=localhost;uid=root;database=test");

		await connection.OpenAsync();

		var command = new MySqlCommand(@"
DROP TABLE `data`;
CREATE TABLE `data` (
	`id` INT NOT NULL,
	`textdata` VARCHAR(1024) NULL DEFAULT NULL COLLATE 'utf8mb4_0900_ai_ci',
	`binarydata` VARBINARY(1024) NULL DEFAULT NULL,
	`date` DATETIME NULL DEFAULT NULL,
	`decimaldata` DECIMAL(20,6) NULL DEFAULT NULL,
	PRIMARY KEY (`id`) USING BTREE
)
COLLATE='utf8mb4_0900_ai_ci'
ENGINE=InnoDB
;", connection);

		await command.ExecuteNonQueryAsync();

		try
		{
			int counter = 0;
			var stringbuilder = new StringBuilder();
			stringbuilder.Append("INSERT INTO data VALUES ");

			async Task submitText(string str)
			{
				if (counter++ > 0)
				{
					stringbuilder.Append(",");
				}
				stringbuilder.Append(str);

				if (++counter == 200)
				{
					stringbuilder.Append(";");
					command.CommandText = stringbuilder.ToString();
					await command.ExecuteScalarAsync();

					stringbuilder.Clear();
					stringbuilder.Append("INSERT INTO data VALUES ");
					counter = 0;
				}
			}

			for (int i = 1; i < 10001; i++)
			{
				var row = GenerateTestRowData(i);

				await submitText($"({i},{
					(row.TextData != null ? $"'{MySqlHelper.EscapeString(row.TextData)}'" : "NULL")
				},{
					(row.BinaryData != null
						? row.BinaryData.Length > 0 ? "_binary 0x" + Convert.ToHexString(row.BinaryData) : "''"
						: "NULL")
				},{	
					(row.Date != null ? $"'{row.Date.Value:yyyy-MM-dd HH:mm:ss}'" : "NULL")
				},{
					(row.DecimalData != null ? row.DecimalData.Value : "NULL")
				})");
			}

			if (counter > 0)
			{
				stringbuilder.Append(";");
				await command.ExecuteScalarAsync();
			}
		}
		catch
		{
			Console.WriteLine(command.CommandText);
			throw;
		}
	}
}