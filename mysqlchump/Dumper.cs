using System;
using System.Data.Common;
using System.Data.SqlTypes;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;

namespace mysqlchump
{
	public class Dumper
	{
		public MySqlConnection Connection { get; }

		public Dumper(MySqlConnection connection)
		{
			Connection = connection;
		}

		public async Task WriteTableSchemaAsync(string table, Stream outputStream)
		{
			using (var writer = new StreamWriter(outputStream, Encoding.UTF8, 4096, true))
			using (var createTableCommand = new MySqlCommand($"SHOW CREATE TABLE `{table}`", Connection))
			using (var reader = await createTableCommand.ExecuteReaderAsync())
			{
				if (!await reader.ReadAsync())
					throw new ArgumentException($"Could not find table `{table}`");

				string createSql = (string)reader["Create Table"];

				createSql = createSql.Replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS");

				await writer.WriteAsync(createSql);
				await writer.WriteAsync(";\n\n\n");
			}
		}

		public async Task WriteInsertQueries(string table, string query, Stream outputStream, MySqlTransaction transaction = null)
		{
			int counter = 0;
			int splitCounter = 1024;

			var bodyBuilder = new StringBuilder();
			Task ioTask = null;

			using (var writer = new StreamWriter(outputStream, Encoding.UTF8, 4096, true))
			using (var selectCommand = new MySqlCommand(query, Connection, transaction))
			using (var reader = await selectCommand.ExecuteReaderAsync())
			{
				bool start = true;

				DbColumn[] columns = null;

				while (await reader.ReadAsync())
				{
					if (counter++ % splitCounter == 0 || start)
					{
						if (bodyBuilder.Length > 0)
						{
							if (ioTask != null)
								await ioTask;

							ioTask = writer.WriteAsync(bodyBuilder.ToString());
							bodyBuilder.Clear();
						}

						if (start)
						{
							// Read schema
							columns = reader.GetColumnSchema().AsEnumerable().ToArray();
						}
						else
						{
							bodyBuilder.AppendLine(";");
						}

						bodyBuilder.AppendLine($"INSERT INTO `{table}` ({string.Join(", ", columns.Select(column => $"`{column.ColumnName}`"))}) VALUES");

						start = false;
					}
					else
					{
						bodyBuilder.AppendLine(",");
					}

					bodyBuilder.Append("(");

					bool rowStart = true;
					foreach (var column in columns)
					{
						if (!rowStart)
							bodyBuilder.Append(", ");

						bodyBuilder.Append(GetMySqlStringRepresentation(column, reader[column.ColumnName]));

						rowStart = false;
					}

					bodyBuilder.Append(")");
				}

				bodyBuilder.Append(";\n\n");

				if (ioTask != null)
					await ioTask;

				await writer.WriteAsync(bodyBuilder.ToString());
			}
		}

		private static string GetMySqlStringRepresentation(DbColumn column, object value)
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