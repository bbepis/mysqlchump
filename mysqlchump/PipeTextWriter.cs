using System;
using System.IO.Pipelines;

namespace mysqlchump;

public class PipeTextWriter
{
	public PipeWriter Writer { get; }

	private int FlushThreshold = 512 * 1024;
	private int CurrentWritten = 0;

	public PipeTextWriter(PipeWriter writer)
	{
		Writer = writer;
	}

	public void Write(ReadOnlySpan<char> str)
	{
		Span<byte> buffer = Writer.GetSpan(str.Length * 4); // UTF-8 max byte estimate

		if (!Utility.NoBomUtf8.TryGetBytes(str, buffer, out int bytesWritten))
			throw new Exception($"Could not encode string: {str}");

		Writer.Advance(bytesWritten);

		CurrentWritten += bytesWritten;

		if (CurrentWritten >= FlushThreshold)
		{
			Writer.FlushAsync().AsTask().Wait();
		}
	}
}