using System;
using System.Collections.Generic;
using MySqlConnector;

namespace mysqlchump
{
	public static class Utility
	{
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
	}
}