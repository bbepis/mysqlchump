using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MySqlConnector;
using Newtonsoft.Json;

namespace mysqlchump.Export;

public class JsonDumper : BaseDumper
{
	public JsonDumper(MySqlConnection connection) : base(connection) { }

	public override bool CompatibleWithMultiTableStdout => true;

	private JsonTextWriter JsonWriter { get; set; } = null;

	public override async Task WriteStartTableAsync(string table, Stream outputStream, bool writeSchema, bool truncate)
	{
		if (JsonWriter == null)
		{
			// start writing new file
			JsonWriter = new JsonTextWriter(new StreamWriter(outputStream, Encoding.UTF8, 20 * 1024 * 1024));
			JsonWriter.Formatting = Formatting.None;

			await JsonWriter.WriteStartObjectAsync();              // {
			await JsonWriter.WritePropertyNameAsync("version");    //    "version"
			await JsonWriter.WriteValueAsync(1);                   //             : 1,
			await JsonWriter.WritePropertyNameAsync("tables");     //    "tables"
			await JsonWriter.WriteStartArrayAsync();               //            : [
		}

		await JsonWriter.WriteStartObjectAsync();
		await JsonWriter.WritePropertyNameAsync("name");
		await JsonWriter.WriteValueAsync(table);
		await JsonWriter.WritePropertyNameAsync("create_statement");
		await JsonWriter.WriteValueAsync(await GetCreateSql(table));
	}

	public override async Task WriteInsertQueries(string table, string query, Stream outputStream, MySqlTransaction transaction = null)
	{
		await using var selectCommand = new MySqlCommand(query, Connection, transaction);

		selectCommand.CommandTimeout = 3600;

		ulong? totalRowCount;

		await using (var rowCountCommand = new MySqlCommand(query, Connection, transaction))
		{
			rowCountCommand.CommandText =
				$"SELECT TABLE_ROWS FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table}' AND TABLE_SCHEMA = '{Connection.Database}';";
			totalRowCount = (ulong?)await rowCountCommand.ExecuteScalarAsync();
		}

		await using var reader = await selectCommand.ExecuteReaderAsync();
			
		await JsonWriter.WritePropertyNameAsync("columns");
		await JsonWriter.WriteStartObjectAsync();

		Columns = (await reader.GetColumnSchemaAsync()).AsEnumerable().ToArray();

		foreach (var column in Columns)
		{
			await JsonWriter.WritePropertyNameAsync(column.ColumnName);
			await JsonWriter.WriteValueAsync(column.DataTypeName);
		}

		await JsonWriter.WriteEndObjectAsync();
		
		await JsonWriter.WritePropertyNameAsync("approx_count");
		await JsonWriter.WriteValueAsync(totalRowCount);

		await JsonWriter.WritePropertyNameAsync("rows");
		await JsonWriter.WriteStartArrayAsync();

		int rowCounter = 0;
			
		var dumpTimer = new Stopwatch();
		dumpTimer.Start();
			
		void WriteProgress(long currentRow)
		{
			if (Console.IsErrorRedirected)
				return;

			if (OperatingSystem.IsWindows())
				Console.CursorLeft = 0;
			else
				Console.Error.Write("\u001b[1000D"); // move cursor to the left

			double percentage = totalRowCount.HasValue ?100 * currentRow / (double)totalRowCount.Value : 0;
			if (percentage > 100 || double.IsNaN(percentage))
			{
				// mysql stats are untrustworthy
				percentage = 100;
			}

			Console.Error.Write($"{table} - {currentRow:N0} / {(totalRowCount.HasValue ? $"~{totalRowCount:N0}" : "?")} ({(totalRowCount.HasValue ? percentage.ToString("N2") : "?")} %)");
		}
		
		

		try
		{
			while (await reader.ReadAsync())
			{
				if (dumpTimer.ElapsedMilliseconds >= 1000)
				{
					WriteProgress(rowCounter);
					dumpTimer.Restart();
				}

				JsonWriter.WriteStartObject();

				for (int i = 0; i < Columns.Length; i++)
				{
					var column = Columns[i];

					JsonWriter.WritePropertyName(column.ColumnName);
					JsonWriter.WriteValue(GetJsonMySqlValue(column, reader[i]));
				}

				JsonWriter.WriteEndObject();
				rowCounter++;
			}
		}
		catch
		{
			List<string> columnValues = new List<string>();

			for (int i = 0; i < reader.FieldCount; i++)
			{
				try
				{
					columnValues.Add(reader[i]?.ToString() ?? "<NULL>");
				}
				catch
				{
					columnValues.Add("<ERROR>");
				}
			}

			Console.Error.WriteLine();
			Console.Error.WriteLine(string.Join(", ", Columns.Select(x => x.ColumnName)));
			Console.Error.WriteLine(string.Join(", ", columnValues));

			throw;
		}
			
		WriteProgress(rowCounter);

		if (!Console.IsErrorRedirected)
			Console.Error.WriteLine();
			
		await JsonWriter.WriteEndArrayAsync();

		await JsonWriter.WritePropertyNameAsync("actual_count");
		await JsonWriter.WriteValueAsync(rowCounter);

		await JsonWriter.WriteEndObjectAsync();
	}

	public override async Task WriteEndTableAsync(string table, Stream outputStream, bool tablesRemainingForStream)
	{
		if (!tablesRemainingForStream)
		{
			await JsonWriter.CloseAsync();
			JsonWriter = null;
		}
	}

	private static object GetJsonMySqlValue(DbColumn column, object value)
	{
		if (value == null || value == DBNull.Value)
			return null;

		return value;

		//throw new SqlTypeException($"Could not represent type: {column.DataTypeName}");
	}
}