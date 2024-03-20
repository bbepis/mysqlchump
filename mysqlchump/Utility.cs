using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using MySqlConnector;

namespace mysqlchump
{
	public static class Utility
	{
		public static UTF8Encoding NoBomUtf8 { get; } = new UTF8Encoding(false);

		public static bool TryFirst<T>(this IEnumerable<T> enumerable, Func<T, bool> predicate, out T value)
		{
			foreach (var item in enumerable)
			{
				if (predicate(item))
				{
					value = item;
					return true;
				}
			}

			value = default;
			return false;
		}

		public static MySqlCommand SetParam(this MySqlCommand command, string name, object value)
		{
			var param = command.CreateParameter();
			param.ParameterName = name;
			param.Value = value;
			command.Parameters.Add(param);

			return command;
		}

		public static async Task WriteToStreamAsync(this Stream stream, string text)
		{
			await using var writer = new StreamWriter(stream, Utility.NoBomUtf8, 4096, true);

			await writer.WriteAsync(text);
		}

		public static string ByteArrayToString(byte[] ba)
		{
			StringBuilder hex = new StringBuilder(ba.Length * 2);
			foreach (byte b in ba)
				hex.AppendFormat("{0:x2}", b);
			return hex.ToString();
		}

		// Adapted from https://stackoverflow.com/a/8094334
		public static bool Glob(string value, string pattern)
		{
			int pos = 0;

			while (pos < pattern.Length)
			{
				switch (pattern[pos])
				{
					case '?': break;

					case '*':
					{
						for (int i = value.Length; i >= pos; i--)
						{
							if (Glob(value.Substring(i), pattern.Substring(pos + 1)))
							{
								return true;
							}
						}

						return false;
					}

					default:
					{
						if (value.Length == pos || char.ToUpper(pattern[pos]) != char.ToUpper(value[pos]))
						{
							return false;
						}
						break;
					}
				}

				pos++;
			}

			return value.Length == pos;
		}
	}
}