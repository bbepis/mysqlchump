using System;
using System.IO.Pipelines;
using System.Threading.Tasks;

namespace mysqlchump;

public class PipeTextWriter
{
	public PipeWriter Writer { get; }

	private char[] Buffer;
	private int CurrentWritten = 0;
	private Task flushTask = null;

	public PipeTextWriter(PipeWriter writer, int bufferSize = 512 * 1024)
	{
		Writer = writer;
		Buffer = new char[bufferSize];
	}

	public void Write(ReadOnlySpan<char> str)
	{
		int bytesWritten;

		if (str.Length > Buffer.Length)
		{
			// got ourselves a biggun'

			if (CurrentWritten > 0)
				Flush();

			var span = Writer.GetSpan(str.Length * 4);

			if (!Utility.NoBomUtf8.TryGetBytes(Buffer.AsSpan(0, CurrentWritten), span, out bytesWritten))
				throw new Exception("Failed to UTF-8 encode string to pipe writer");

			Writer.Advance(bytesWritten);

			flushTask = Task.Run(() => Writer.FlushAsync());
			return;
		}

		if (str.Length + CurrentWritten > Buffer.Length)
			Flush(true);

		str.CopyTo(Buffer.AsSpan(CurrentWritten));

		CurrentWritten += str.Length;
	}

	// https://gist.github.com/antoninkriz/915364de7f264dd14a572936abd5228b
	// ... declared ReadOnlySpan<byte> methods returning a constant array initialization - new byte {1, 2, 3, ...}
	//   - are compiled as the program's static data, therefore omitting a redundant memcpy.
	private static ReadOnlySpan<byte> HexAlphabetSpan => new[]
	{
		(byte)'0', (byte)'1', (byte)'2', (byte)'3',
		(byte)'4', (byte)'5', (byte)'6', (byte)'7',
		(byte)'8', (byte)'9', (byte)'A', (byte)'B',
		(byte)'C', (byte)'D', (byte)'E', (byte)'F'
	};

	public void WriteHex(byte[] data)
	{
		for (var i = 0; i < data.Length; ++i)
		{
			if (Buffer.Length - CurrentWritten < 2)
				Flush();

			int j = CurrentWritten + i * 2;

			Buffer[j] = (char)HexAlphabetSpan[data[i] >> 4];
			Buffer[j + 1] = (char)HexAlphabetSpan[data[i] & 0xF];

			CurrentWritten += 2;
		}
	}


	public void Flush(bool softFlush = false)
	{
		if (flushTask != null && !flushTask.IsCompleted)
			flushTask.Wait();

		var span = Writer.GetSpan(CurrentWritten * 4);

		if (!Utility.NoBomUtf8.TryGetBytes(Buffer.AsSpan(0, CurrentWritten), span, out int bytesWritten))
			throw new Exception("Failed to UTF-8 encode string to pipe writer");

		Writer.Advance(bytesWritten);

		if (softFlush)
			flushTask = Task.Run(() => Writer.FlushAsync());
		else
			Writer.FlushAsync().AsTask().Wait();
		
		CurrentWritten = 0;
	}
}