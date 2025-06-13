using mysqlchump.SqlParsing;
using Newtonsoft.Json.Linq;
using NUnit.Framework.Legacy;
using Assert = NUnit.Framework.Legacy.ClassicAssert;

namespace mysqlchump.Tests;

public class SqlTests
{
	[Test, Explicit]
	public void PerformanceTest()
	{
		using var fs = new FileStream(@"C:\Temp\mysqldump-test.sql", FileMode.Open);
		var tokenizer = new SqlTokenizer(fs);

		while (true)
		{
			var token = tokenizer.Read();
			if (token == SqlTokenType.EOF)
				break;
		}
	}

	public class InfiniteStream : Stream
	{
		private readonly byte[] _buffer;
		private long _position;  // The logical position (i.e., grows indefinitely)
		private long _actualIndex;  // The index into the byte[] buffer

		// Constructor that accepts the byte array
		public InfiniteStream(byte[] buffer)
		{
			if (buffer == null)
				throw new ArgumentNullException(nameof(buffer));

			_buffer = buffer;
			_position = 0;
			_actualIndex = 0;
		}

		public override long Length => _buffer.Length;

		// The current position in the byte array
		public override long Position
		{
			get => _position;
			set
			{
				if (value < 0)
					throw new ArgumentOutOfRangeException(nameof(Position));
				_position = value;
				_actualIndex = value % _buffer.Length;
			}
		}

		// This is a read-only stream, so we just return the number of available bytes in the array
		public override bool CanRead => true;

		// We don't allow writing to this stream
		public override bool CanWrite => false;

		// This stream cannot seek, so we return false
		public override bool CanSeek => true;

		public override void Flush()
		{
			throw new NotImplementedException();
		}

		// Read operation
		public override int Read(byte[] buffer, int offset, int count)
		{
			if (buffer == null)
				throw new ArgumentNullException(nameof(buffer));
			if (offset < 0 || offset >= buffer.Length)
				throw new ArgumentOutOfRangeException(nameof(offset));
			if (count < 0 || offset + count > buffer.Length)
				throw new ArgumentException("Invalid offset and count combination.");

			int bytesRead = 0;

			while (count > 0)
			{
				// Get the index in the buffer using modulo for infinite looping
				int currentIndex = (int)(_actualIndex % _buffer.Length);
				int bytesToRead = Math.Min(count, _buffer.Length - currentIndex);

				Array.Copy(_buffer, currentIndex, buffer, offset + bytesRead, bytesToRead);

				bytesRead += bytesToRead;
				count -= bytesToRead;
				_position += bytesToRead;  // logical position increases
				_actualIndex += bytesToRead;  // actual index into the buffer increases

				// Wrap the actual index around when we exceed the buffer length
				if (_actualIndex >= _buffer.Length)
					_actualIndex = 0;
			}

			return bytesRead;
		}

		// Seeking is not supported in this implementation, so throws an exception
		public override long Seek(long offset, SeekOrigin origin)
		{
			throw new NotSupportedException("Seeking is not supported in this stream.");
		}

		// Set the length is not supported because it's a read-only stream
		public override void SetLength(long value)
		{
			throw new NotSupportedException("Setting the length is not supported in this stream.");
		}

		// Write operation is not supported because it's a read-only stream
		public override void Write(byte[] buffer, int offset, int count)
		{
			throw new NotSupportedException("Writing is not supported in this stream.");
		}
	}
}