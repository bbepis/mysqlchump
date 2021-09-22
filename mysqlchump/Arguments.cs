using System;
using System.Collections.Generic;
using System.Reflection;

namespace mysqlchump
{
	public static class Arguments
	{
		public static T Parse<T>(string[] args) where T : IArgumentCollection, new()
		{
			Dictionary<CommandDefinitionAttribute, Action<string>> valueSwitches = new Dictionary<CommandDefinitionAttribute, Action<string>>();
			Dictionary<CommandDefinitionAttribute, Action<bool>> boolSwitches = new Dictionary<CommandDefinitionAttribute, Action<bool>>();

			var config = new T();
			config.Values = new List<string>();

			foreach (var prop in typeof(T).GetProperties(BindingFlags.Instance | BindingFlags.Public))
			{
				var def = prop.GetCustomAttribute<CommandDefinitionAttribute>();

				if (def == null)
					continue;

				if (prop.PropertyType == typeof(bool))
				{
					boolSwitches.Add(def, x => prop.SetValue(config, x));
				}
				else if (prop.PropertyType == typeof(string))
				{
					valueSwitches.Add(def, x => prop.SetValue(config, x));
				}
				else if (typeof(IList<string>).IsAssignableFrom(prop.PropertyType))
				{
					if (prop.GetValue(config) == null)
					{
						prop.SetValue(config, new List<string>());
					}

					valueSwitches.Add(def, x =>
					{
						var list = (IList<string>)prop.GetValue(config);
						list.Add(x);
					});
				}
			}

			CommandDefinitionAttribute previousSwitchDefinition = null;
			bool valuesOnly = false;

			foreach (string arg in args)
			{
				if (arg == "--")
				{
					// no more switches, only values
					valuesOnly = true;

					continue;
				}

				if (valuesOnly)
				{
					config.Values.Add(arg);
					continue;
				}

				if (arg.StartsWith("-")
					|| arg.StartsWith("--"))
				{
					string previousSwitch;

					if (arg.StartsWith("--"))
						previousSwitch = arg.Substring(2);
					else
						previousSwitch = arg.Substring(1);

					if (boolSwitches.Keys.TryFirst(x
						=> x.LongArg.Equals(previousSwitch, StringComparison.InvariantCultureIgnoreCase)
						|| x.ShortArg?.Equals(previousSwitch, StringComparison.InvariantCultureIgnoreCase) == true,
						out var definition))
					{
						boolSwitches[definition](true);
						previousSwitch = null;

						continue;
					}

					if (valueSwitches.Keys.TryFirst(x
						=> x.LongArg.Equals(previousSwitch, StringComparison.InvariantCultureIgnoreCase)
						|| x.ShortArg?.Equals(previousSwitch, StringComparison.InvariantCultureIgnoreCase) == true,
						out definition))
					{
						previousSwitchDefinition = definition;

						continue;
					}

					Console.WriteLine("Unrecognized command line option: " + arg);
					throw new Exception();
				}

				if (previousSwitchDefinition != null)
				{
					valueSwitches[previousSwitchDefinition](arg);
					previousSwitchDefinition = null;
				}
				else
				{
					config.Values.Add(arg);
				}
			}

			return config;
		}
	}

	public interface IArgumentCollection
	{
		IList<string> Values { get; set; }
	}

	public class CommandDefinitionAttribute : Attribute
	{
		public string DisplayName { get; set; }
		public string ShortArg { get; set; }
		public string LongArg { get; set; }

		public CommandDefinitionAttribute(string displayName, string shortArg, string longArg)
		{
			DisplayName = displayName;
			ShortArg = shortArg;
			LongArg = longArg;
		}
	}
}