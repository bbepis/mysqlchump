using System;
using System.Diagnostics;
using System.IO;
using System.Text;

namespace mysqlchump;

/// <summary>
/// Represents the type of a token.
/// </summary>
public enum TokenType
{
	Comma,
	Semicolon,
	LeftBrace,    // '('
	RightBrace,   // ')'
	Equals,
	String,
	Number,
	Null,
	BinaryBlob,
	Identifier,
	EOF
}

/// <summary>
/// Represents a token with its type and value.
/// </summary>
public class Token
{
	public TokenType Type { get; }
	public string Value { get; } // For tokens that carry a value

	public Token(TokenType type, string value = null)
	{
		Type = type;
		Value = value;
	}

	public override string ToString()
	{
		return Type switch
		{
			TokenType.Comma => "Comma",
			TokenType.Semicolon => "Semicolon",
			TokenType.LeftBrace => "LeftBrace",
			TokenType.RightBrace => "RightBrace",
			TokenType.String => $"String(\"{Value}\")",
			TokenType.Number => $"Number({Value})",
			TokenType.Null => "Null",
			TokenType.BinaryBlob => $"BinaryBlob({Value})",
			TokenType.Identifier => $"Identifier({Value})",
			TokenType.EOF => "EOF",
			_ => "Unknown"
		};
	}
}

/// <summary>
/// A tokenizer for SQL statements.
/// </summary>
public class Tokenizer : IDisposable
{
	private readonly StreamReader _reader;
	private readonly char[] _buffer;
	private int _bufferSize;
	private int _bufferPos;
	private bool _endOfStream;

	private const int DefaultBufferSize = 4096;

	public Tokenizer(Stream stream, int bufferSize = DefaultBufferSize)
	{
		_reader = new StreamReader(stream, Encoding.UTF8);
		_buffer = new char[bufferSize];
		_bufferSize = 0;
		_bufferPos = 0;
		_endOfStream = false;
		FillBuffer();
	}

	/// <summary>
	/// Fills the buffer with more data from the stream.
	/// </summary>
	private void FillBuffer()
	{
		if (_endOfStream)
			return;

		// Shift unread characters to the beginning
		if (_bufferPos > 0 && _bufferPos < _bufferSize)
		{
			Array.Copy(_buffer, _bufferPos, _buffer, 0, _bufferSize - _bufferPos);
			_bufferSize -= _bufferPos;
			_bufferPos = 0;
		}
		else if (_bufferPos >= _bufferSize)
		{
			_bufferSize = 0;
			_bufferPos = 0;
		}

		int read = _reader.Read(_buffer, _bufferSize, _buffer.Length - _bufferSize);
		if (read == 0)
		{
			_endOfStream = true;
		}
		else
		{
			_bufferSize += read;
		}
	}

	/// <summary>
	/// Peeks the next character without advancing the position.
	/// Returns null if end of stream is reached.
	/// </summary>
	private char? PeekChar(int offset = 0)
	{
		while (_bufferPos + offset >= _bufferSize && !_endOfStream)
		{
			FillBuffer();
			if (_bufferPos + offset >= _bufferSize && _endOfStream)
				break;
		}

		if (_bufferPos + offset >= _bufferSize)
			return null;

		return _buffer[_bufferPos + offset];
	}

	/// <summary>
	/// Reads the next character and advances the position.
	/// Returns null if end of stream is reached.
	/// </summary>
	private char? ReadChar()
	{
		char? ch = PeekChar();
		if (ch.HasValue)
			_bufferPos++;
		return ch;
	}

	/// <summary>
	/// Consumes characters while the predicate is true.
	/// </summary>
	private string ConsumeWhile(Func<char, bool> predicate)
	{
		StringBuilder sb = new StringBuilder();
		while (true)
		{
			var ch = PeekChar();
			if (!ch.HasValue || !predicate(ch.Value))
				break;

			sb.Append(ch.Value);
			_bufferPos++;
		}
		return sb.ToString();
	}

	/// <summary>
	/// Skips whitespace characters.
	/// </summary>
	private void SkipWhitespace()
	{
		ConsumeWhile(char.IsWhiteSpace);
	}

	/// <summary>
	/// Retrieves the next token from the input.
	/// </summary>
	public Token GetNextToken()
	{
start:
		SkipWhitespace();

		var current = PeekChar();
		if (!current.HasValue)
			return new Token(TokenType.EOF);

		// Single-character tokens
		switch (current.Value)
		{
			case ',':
				ReadChar();
				return new Token(TokenType.Comma);
			case ';':
				ReadChar();
				return new Token(TokenType.Semicolon);
			case '(':
				ReadChar();
				return new Token(TokenType.LeftBrace);
			case ')':
				ReadChar();
				return new Token(TokenType.RightBrace);
			case '=':
				ReadChar();
				return new Token(TokenType.Equals);
			case '\'':
				return ReadString();
			// Handle other cases...
			default:
				if (current.Value == '-' && PeekChar(1) == '-')
				{
					while (true)
					{
						var read = ReadChar();
						if (read == null ||  read.Value == '\n')
						{
							ReadChar();
							break;
						}
					}

					goto start;
				}

				if (current.Value == '`')
				{
					ReadChar();
					var identifier = ConsumeWhile(x => x != '`');
					ReadChar();
					return new Token(TokenType.Identifier, identifier);
				}
				
				if (char.IsDigit(current.Value))
					return ReadNumber();

				// Handle binary blobs starting with X' or _binary
				if (current.Value == 'X' || current.Value == 'x' || current.Value == '_')
				{
					var binaryToken = TryReadBinaryBlob();
					if (binaryToken != null)
						return binaryToken;
				}

				if (IsIdentifierStart(current.Value))
					return ReadIdentifierOrKeyword();

				throw new Exception($"Unexpected character: '{current.Value}' at position {_bufferPos}");
		}
	}

	/// <summary>
	/// Determines if a character is a valid start for an identifier.
	/// SQL identifiers typically start with letters or underscores.
	/// </summary>
	private bool IsIdentifierStart(char ch)
	{
		return char.IsLetter(ch) || ch == '_';
	}

	/// <summary>
	/// Determines if a character is valid within an identifier.
	/// </summary>
	private bool IsIdentifierPart(char ch)
	{
		return char.IsLetterOrDigit(ch) || ch == '_';
	}

	/// <summary>
	/// Reads an identifier or a keyword.
	/// </summary>
	private Token ReadIdentifierOrKeyword()
	{
		string identifier = ConsumeWhile(IsIdentifierPart);
		if (string.Equals(identifier, "NULL", StringComparison.OrdinalIgnoreCase))
			return new Token(TokenType.Null);
		return new Token(TokenType.Identifier, identifier);
	}

	/// <summary>
	/// Reads a number token.
	/// </summary>
	private Token ReadNumber()
	{
		StringBuilder sb = new StringBuilder();
		bool hasDecimal = false;

		while (true)
		{
			var ch = PeekChar();
			if (!ch.HasValue)
				break;

			if (char.IsDigit(ch.Value))
			{
				sb.Append(ch.Value);
				ReadChar();
			}
			else if (ch.Value == '.' && !hasDecimal)
			{
				hasDecimal = true;
				sb.Append(ch.Value);
				ReadChar();
			}
			else
			{
				break;
			}
		}

		return new Token(TokenType.Number, sb.ToString());
	}

	/// <summary>
	/// Reads a string token, handling escaped single quotes.
	/// </summary>
	private Token ReadString()
	{
		StringBuilder sb = new StringBuilder();
		ReadChar(); // Consume the opening quote

		while (true)
		{
			var ch = ReadChar();
			if (!ch.HasValue)
				throw new Exception("Unterminated string literal.");

			if (ch.Value == '\'')
			{
				if (PeekChar() == '\'') // Escaped single quote
				{
					sb.Append('\'');
					ReadChar(); // Consume the escaped quote
				}
				else
				{
					break; // End of string
				}
			}
			else
			{
				sb.Append(ch.Value);
			}
		}

		return new Token(TokenType.String, sb.ToString());
	}

	/// <summary>
	/// Tries to read a binary blob token.
	/// Supports X'FFF' and _binary 0xFFF formats.
	/// Returns null if not a binary blob.
	/// </summary>
	private Token TryReadBinaryBlob()
	{
		var start = PeekChar();
		if (!start.HasValue)
			return null;

		if (start.Value == 'X' || start.Value == 'x')
		{
			// Expect X'FFF'
			ReadChar(); // Consume 'X' or 'x'
			if (PeekChar() != '\'')
				throw new Exception("Invalid binary blob format. Expected single quote after X.");

			ReadChar(); // Consume opening quote
			StringBuilder sb = new StringBuilder();
			while (true)
			{
				var ch = ReadChar();
				if (!ch.HasValue)
					throw new Exception("Unterminated binary blob.");

				if (ch.Value == '\'')
					break;

				if (!IsHexChar(ch.Value))
					throw new Exception($"Invalid character '{ch.Value}' in binary blob.");

				sb.Append(ch.Value);
			}

			return new Token(TokenType.BinaryBlob, $"X'{sb.ToString()}'");
		}
		else if (start.Value == '_')
		{
			// Expect _binary 0xFFF
			// Read '_binary'


			string prefix = ConsumeWhile(x => char.IsLetter(x) || x == '_');
			if (!string.Equals(prefix, "_binary", StringComparison.OrdinalIgnoreCase))
			{
				// Not a binary blob, treat as identifier
				return new Token(TokenType.Identifier, prefix);
			}

			SkipWhitespace();

			// Expect '0x' prefix
			var first = ReadChar();
			var second = PeekChar();
			if (!first.HasValue || !second.HasValue || first.Value != '0' || (second.Value != 'x' && second.Value != 'X'))
				throw new Exception("Invalid binary blob format. Expected '0x'.");

			// ReadChar(); // Consume '0'
			ReadChar(); // Consume 'x' or 'X'

			StringBuilder sb = new StringBuilder();
			while (true)
			{
				var ch = PeekChar();
				if (!ch.HasValue)
					break;

				if (!IsHexChar(ch.Value))
					break;

				sb.Append(ch.Value);
				ReadChar();
			}

			if (sb.Length == 0)
				throw new Exception("Invalid binary blob format. No hexadecimal digits found.");

			return new Token(TokenType.BinaryBlob, $"_binary 0x{sb.ToString()}");
		}

		return null;
	}

	/// <summary>
	/// Checks if a character is a valid hexadecimal digit.
	/// </summary>
	private bool IsHexChar(char ch)
	{
		return (ch >= '0' && ch <= '9') ||
				(ch >= 'a' && ch <= 'f') ||
				(ch >= 'A' && ch <= 'F');
	}

	public void Dispose()
	{
		_reader?.Dispose();
	}
}

/// <summary>
/// Example usage of the Tokenizer.
/// </summary>
static class SqlTest
{
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