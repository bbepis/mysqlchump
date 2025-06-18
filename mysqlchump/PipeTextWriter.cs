using System;
using System.Buffers;
using System.IO.Pipelines;
using System.Threading.Tasks;

namespace mysqlchump;

public class PipeTextWriter : IDisposable
{
	public PipeWriter Writer { get; }

	private IMemoryOwner<char> _memoryOwner;
	private Memory<char> Buffer;
	private int CurrentWritten = 0;
	private Task<FlushResult> flushTask = null;

	public PipeTextWriter(PipeWriter writer, int minBufferSize = 512 * 1024)
	{
		Writer = writer;

		_memoryOwner = MemoryPool<char>.Shared.Rent(minBufferSize);
		Buffer = _memoryOwner.Memory;
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

			if (!Utility.NoBomUtf8.TryGetBytes(Buffer.Span.Slice(0, CurrentWritten), span, out bytesWritten))
				throw new Exception("Failed to UTF-8 encode string to pipe writer");

			Writer.Advance(bytesWritten);

			flushTask = Task.Run(async () => await Writer.FlushAsync());
			return;
		}

		if (str.Length + CurrentWritten > Buffer.Length)
			Flush(true);

		str.CopyTo(Buffer.Span.Slice(CurrentWritten));

		CurrentWritten += str.Length;
	}

	public void Write<T>(T value, ReadOnlySpan<char> format = default) where T : struct, ISpanFormattable
	{
		if (CurrentWritten + 64 > Buffer.Length)
			Flush(true);

		var targetSpan = Buffer.Span.Slice(CurrentWritten);

		value.TryFormat(targetSpan, out int written, format, null);

		CurrentWritten += written;
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

	public void WriteHex(ReadOnlySpan<byte> data)
	{
		var span = Buffer.Span;

		for (var i = 0; i < data.Length; ++i)
		{
			if (Buffer.Length - (CurrentWritten + 2) < 2)
				Flush();

			span[CurrentWritten] = (char)HexAlphabetSpan[data[i] >> 4];
			span[CurrentWritten + 1] = (char)HexAlphabetSpan[data[i] & 0xF];

			CurrentWritten += 2;
		}
	}

	public void WriteBase64(ReadOnlySpan<byte> data)
	{
		int currentIndex = 0;

		while (currentIndex < data.Length)
		{
			int remainingBuffer = Buffer.Length - CurrentWritten;

			if (remainingBuffer <= 4)
			{
				Flush();
				remainingBuffer = Buffer.Length - CurrentWritten;
			}

			int base64Space = 3 * (remainingBuffer / 4);

			int conversionSize = Math.Min(base64Space, data.Length - currentIndex);

			if (!Convert.TryToBase64Chars(data.Slice(currentIndex, conversionSize), Buffer.Span.Slice(CurrentWritten), out int charsWritten))
				throw new Exception("Could not convert data to base 64");

			currentIndex += conversionSize;
			CurrentWritten += charsWritten;
		}
	}

	public void WriteJsonString(ReadOnlySpan<char> input)
	{
		Write("\"");

		int runStart = 0;

		for (int i = 0; i < input.Length; i++)
		{
			char c = input[i];
			string escapeSequence = null;

			switch (c)
			{
				case '\"':
					escapeSequence = "\\\"";
					break;
				case '\\':
					escapeSequence = "\\\\";
					break;
				case '\b':
					escapeSequence = "\\b";
					break;
				case '\f':
					escapeSequence = "\\f";
					break;
				case '\n':
					escapeSequence = "\\n";
					break;
				case '\r':
					escapeSequence = "\\r";
					break;
				case '\t':
					escapeSequence = "\\t";
					break;
				default:
					if (c < ' ')
					{
						escapeSequence = "\\u" + ((int)c).ToString("x4");
					}
					break;
			}

			if (escapeSequence != null)
			{
				if (i > runStart)
				{
					Write(input.Slice(runStart, i - runStart));
				}
				Write(escapeSequence);
				runStart = i + 1;
			}
		}

		if (runStart < input.Length)
		{
			Write(input.Slice(runStart, input.Length - runStart));
		}

		Write("\"");
	}

	public void WriteCsvString(ReadOnlySpan<char> input, bool mysqlInvalidFormat)
	{
		bool requiresEscaping = false;
		int len = input.Length;
		for (int i = 0; i < len; i++)
		{
			char c = input[i];
			if (c == '"' || c == '\\' || c == '\0' || c == '\n' || c == ',')
			{
				requiresEscaping = true;
				break;
			}
		}

		if (!requiresEscaping)
		{
			Write(input);
			return;
		}

		Write("\"");

		int start = 0;

		void FlushChunk(int i, ReadOnlySpan<char> input)
		{
			if (i > start)
				Write(input.Slice(start, i - start));
		}

		if (mysqlInvalidFormat)
		{
			for (int i = 0; i < len; i++)
			{
				char c = input[i];

				switch (c)
				{
					case '\r':
						if (i + 1 < len && input[i + 1] == '\n')
						{
							FlushChunk(i, input);
							Write("\\\n");
							i++;
							start = i + 1;
						}
						break;
					case '\n':
						FlushChunk(i, input);
						Write("\\\n");
						start = i + 1;
						break;
					case '"':
						FlushChunk(i, input);
						Write("\\\"");
						start = i + 1;
						break;
					case '\0':
						FlushChunk(i, input);
						Write("\\0");
						start = i + 1;
						break;
					case '\\':
						FlushChunk(i, input);
						Write("\\\\");
						start = i + 1;
						break;
				}
			}
		}
		else
		{
			for (int i = 0; i < len; i++)
			{
				char c = input[i];

				switch (c)
				{
					case '"':
						FlushChunk(i, input);
						Write("\"\"");
						start = i + 1;
						break;
				}
			}
		}

		FlushChunk(len, input);
		Write("\"");
	}

	public void Flush(bool softFlush = false)
	{
		if (flushTask != null && !flushTask.IsCompleted)
			flushTask.Wait();

		if (CurrentWritten == 0)
			return;

		var span = Writer.GetSpan(CurrentWritten * 4);

		if (!Utility.NoBomUtf8.TryGetBytes(Buffer.Span.Slice(0, CurrentWritten), span, out int bytesWritten))
			throw new Exception("Failed to UTF-8 encode string to pipe writer");

		Writer.Advance(bytesWritten);

		if (softFlush)
			flushTask = Task.Run(async () => await Writer.FlushAsync());
		else
			_ = Writer.FlushAsync().AsTask().Result;
		
		CurrentWritten = 0;
	}

	public void Dispose()
	{
		_memoryOwner.Dispose();
		_memoryOwner = null;
	}

	~PipeTextWriter()
	{
		if (_memoryOwner != null)
			Dispose();
	}
}