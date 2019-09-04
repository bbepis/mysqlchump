using System;
using System.Collections.Generic;

namespace mysqlchump
{
  public class Arguments
  {
    public IReadOnlyDictionary<string, string> Switches { get; protected set; }

    public IList<string> Values { get; protected set; }

    public string this[string switchKey] => 
      Switches.ContainsKey(switchKey)
        ? Switches[switchKey] 
        : null;

    public string this[int valueIndex] => Values[valueIndex];

    public Arguments(Dictionary<string, string> switches, List<string> values)
    {
      Values = values;
      Switches = switches;
    }

    public static Arguments Parse(string[] args)
    {
      Dictionary<string, string> switches = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
      List<string> values = new List<string>();

      string previousSwitch = null;

      foreach (string arg in args)
      {
        if (arg.StartsWith("-")
            || arg.StartsWith("--")
            || arg.StartsWith("/"))
        {
          if (previousSwitch != null)
            switches.Add(previousSwitch, string.Empty);

          if (arg.StartsWith("--"))
            previousSwitch = arg.Substring(2);
          else
            previousSwitch = arg.Substring(1);

          continue;
        }

        if (previousSwitch != null)
        {
          switches.Add(previousSwitch, arg);
          previousSwitch = null;
        }
        else
        {
          values.Add(arg);
        }
      }

      if (previousSwitch != null)
        switches.Add(previousSwitch, string.Empty);

      return new Arguments(switches, values);
    }
  }
}