using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.SqlTypes;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MySqlConnector;

namespace mysqlchump.Export;

public abstract class BaseTextDumper : BaseDumper
{
	protected BaseTextDumper(MySqlConnection connection) : base(connection) { }

	protected virtual void StartInsertBatch(string table, DbDataReader reader, StringBuilder builder) { }
	protected virtual void EndInsertBatch(DbDataReader reader, StringBuilder builder) { }

	protected abstract void CreateInsertLine(DbDataReader reader, StringBuilder builder);

	protected virtual void WriteInsertEnd(StringBuilder builder) { }

	protected virtual void WriteInsertContinuation(StringBuilder builder) { }

	public override async Task WriteInsertQueries(string table, string query, Stream outputStream, MySqlTransaction transaction = null)
	{
		int counter = 0;
		int splitCounter = 1024;

		var bodyBuilder = new StringBuilder();
		//Memory<char> bufferString = new char[1024 * 1024];

		var dumpTimer = new Stopwatch();
		dumpTimer.Start();

		Task ioTask = null;

		ulong? rowCount = null;

		await using (var rowCountCommand = new MySqlCommand(query, Connection, transaction))
		{
			rowCountCommand.CommandText =
				$"SELECT TABLE_ROWS FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table}' AND TABLE_SCHEMA = '{Connection.Database}';";
			rowCount = (ulong)await rowCountCommand.ExecuteScalarAsync();
		}


		using var writer = new StreamWriter(outputStream, Utility.NoBomUtf8, 4096, true);
		using var selectCommand = new MySqlCommand(query, Connection, transaction);

		selectCommand.CommandTimeout = 3600;

		using var reader = await selectCommand.ExecuteReaderAsync();

		async Task FlushStringBuilderToDisk()
		{
			if (ioTask != null)
				await ioTask;

			//bodyBuilder.CopyTo(0, bufferString.Span, bodyBuilder.Length);
			//ioTask = writer.WriteAsync(bufferString.Slice(0, bodyBuilder.Length));
			ioTask = writer.WriteAsync(bodyBuilder.ToString());
			bodyBuilder.Clear();
		}

		void writeProgress(long currentRow)
		{
			if (OperatingSystem.IsWindows())
				Console.CursorLeft = 0;
			else
				Console.Error.Write("\u001b[1000D"); // move cursor to the left

			double percentage = 100 * currentRow / (double)rowCount.Value;
			if (percentage > 100)
			{
				// mysql stats are untrustworthy
				percentage = 100;
			}

			Console.Error.Write($"{table} - {currentRow:N0} / {(rowCount.HasValue ? $"~{rowCount:N0}" : "?")} ({(rowCount.HasValue ? percentage.ToString("N2") : "?")} %)");
		}

		bool start = true;
		try
		{
			while (await reader.ReadAsync())
			{
				if (counter++ % splitCounter == 0 || start)
				{
					//Console.WriteLine($"Flushed: {counter}");

					if (!Console.IsErrorRedirected && dumpTimer.ElapsedMilliseconds >= 1000)
					{
						writeProgress(counter);
						dumpTimer.Restart();
					}

					if (bodyBuilder.Length > 0)
					{
						await FlushStringBuilderToDisk();
					}

					if (start)
					{
						// Read schema
						Columns = (await reader.GetColumnSchemaAsync()).AsEnumerable().ToArray();
					}
					else
					{
						EndInsertBatch(reader, bodyBuilder);
					}

					StartInsertBatch(table, reader, bodyBuilder);

					start = false;
				}
				else
				{
					WriteInsertContinuation(bodyBuilder);
				}

				CreateInsertLine(reader, bodyBuilder);
			}
		}
		catch (Exception)
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

		WriteInsertEnd(bodyBuilder);

		await FlushStringBuilderToDisk();

		writeProgress(counter);
		Console.Error.WriteLine();

		if (ioTask != null)
			await ioTask;
	}
}