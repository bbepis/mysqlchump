using System;
using System.Collections.Generic;
using System.Linq;

namespace mysqlchump;

internal class TopologicalItem
{
	public string Name { get; set; }
	public string[] Dependencies { get; set; }
}

internal static class TopologicalSort
{
	public static string[] SortItems(TopologicalItem[] items)
	{
		foreach (var item in items)
			Console.Error.WriteLine($"{item.Name} ({string.Join(',', item.Dependencies)})");

		var dict = items.ToDictionary(x => x.Name);
		var edgeCount = items.ToDictionary(x => x.Name, x => 
			x.Dependencies
				.Distinct()
				.Count(dep => dep != x.Name));

		var sortedList = new List<string>();
		var queue = new Queue<TopologicalItem>(items.Where(x => edgeCount[x.Name] == 0));

		while (queue.Count > 0)
		{
			var nextItem = queue.Dequeue();

			sortedList.Add(nextItem.Name);
			edgeCount[nextItem.Name] = -1;

			foreach (var dep in dict.Values)
			{
				if (dep.Dependencies.Contains(nextItem.Name))
					if (--edgeCount[dep.Name] == 0)
						queue.Enqueue(dep);
			}
		}

		Console.Error.WriteLine(string.Join(",", sortedList));

		if (sortedList.Count != items.Length)
			throw new Exception("Circular dependency detected");

		return sortedList.ToArray();
	}
}