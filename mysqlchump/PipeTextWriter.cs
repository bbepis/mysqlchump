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

	private byte[] Buffer = new byte[512 * 1024];

	public void Write(ReadOnlySpan<char> str)
	{
	tryWrite:
		if (!Utility.NoBomUtf8.TryGetBytes(str, Buffer.AsSpan(CurrentWritten), out int bytesWritten))
		{
			Flush();
			goto tryWrite;
		}

		CurrentWritten += bytesWritten;
	}

	public void Flush()
	{
		Writer.WriteAsync(Buffer.AsMemory(0, CurrentWritten)).AsTask().Wait();
		CurrentWritten = 0;
	}
}