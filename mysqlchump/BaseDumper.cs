using System;
using System.Data.Common;
using System.Data.SqlTypes;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MySqlConnector;

namespace mysqlchump
{
	public abstract class BaseDumper
	{
		public MySqlConnection Connection { get; }

		protected BaseDumper(MySqlConnection connection)
		{
			Connection = connection;
		}

		protected DbColumn[] Columns { get; set; }

		protected virtual void StartInsertBatch(string table, DbDataReader reader, StringBuilder builder) { }
		protected virtual void EndInsertBatch(DbDataReader reader, StringBuilder builder) { }

		protected abstract void CreateInsertLine(DbDataReader reader, StringBuilder builder);

		public virtual Task WriteTableSchemaAsync(string table, Stream outputStream) => Task.CompletedTask;

		protected virtual void WriteInsertEnd(StringBuilder builder) { }

		protected virtual void WriteInsertContinuation(StringBuilder builder) { }

		public async Task WriteInsertQueries(string table, string query, Stream outputStream, MySqlTransaction transaction = null)
		{
			int counter = 0;
			int splitCounter = 1024;

			var bodyBuilder = new StringBuilder();
			//Memory<char> bufferString = new char[1024 * 1024];

			Task ioTask = null;

			using (var writer = new StreamWriter(outputStream, new UTF8Encoding(false), 4096, true))
			using (var selectCommand = new MySqlCommand(query, Connection, transaction))
			using (var reader = await selectCommand.ExecuteReaderAsync())
			{
				async Task FlushStringBuilderToDisk()
				{
					if (ioTask != null)
						await ioTask;

					//bodyBuilder.CopyTo(0, bufferString.Span, bodyBuilder.Length);
					//ioTask = writer.WriteAsync(bufferString.Slice(0, bodyBuilder.Length));
					ioTask = writer.WriteAsync(bodyBuilder.ToString());
					bodyBuilder.Clear();
				}

				bool start = true;

				while (await reader.ReadAsync())
				{
					if (counter++ % splitCounter == 0 || start)
					{
						if (bodyBuilder.Length > 0)
						{
							await FlushStringBuilderToDisk();
						}

						if (start)
						{
							// Read schema
							Columns = reader.GetColumnSchema().AsEnumerable().ToArray();
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

				WriteInsertEnd(bodyBuilder);

				await FlushStringBuilderToDisk();

				if (ioTask != null)
					await ioTask;
			}
		}

		protected static string GetMySqlStringRepresentation(DbColumn column, object value)
		{
			if (value == null || value == DBNull.Value)
				return "NULL";

			var columnType = column.DataType;

			if (columnType == typeof(byte)
				|| columnType == typeof(sbyte)
				|| columnType == typeof(ushort)
				|| columnType == typeof(short)
				|| columnType == typeof(uint)
				|| columnType == typeof(int)
				|| columnType == typeof(ulong)
				|| columnType == typeof(long)
				|| columnType == typeof(float)
				|| columnType == typeof(double)
				|| columnType == typeof(decimal))
			{
				return value.ToString();
			}

			if (columnType == typeof(bool))
				return (bool)value ? "1" : "0";

			if (columnType == typeof(string))
				return "'" + MySqlHelper.EscapeString(value.ToString())
										.Replace("\r", "\\r")
										.Replace("\n", "\\n")
						   + "'";

			if (columnType == typeof(byte[]))
				return "_binary 0x" + ByteArrayToString((byte[])value);

			throw new SqlTypeException($"Could not represent type: {column.DataTypeName}");
		}

		private static string ByteArrayToString(byte[] ba)
		{
			StringBuilder hex = new StringBuilder(ba.Length * 2);
			foreach (byte b in ba)
				hex.AppendFormat("{0:x2}", b);
			return hex.ToString();
		}
	}
}