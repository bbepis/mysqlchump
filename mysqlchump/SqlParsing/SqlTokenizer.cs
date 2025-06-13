using System;
using System.IO;
using System.Text;

namespace mysqlchump.SqlParsing;

/// <summary>
/// Represents the type of a token.
/// </summary>
public enum SqlTokenType
{
	Comma, // ','
	Semicolon, // ';'
	LeftBrace,    // '('
	RightBrace,   // ')'
	Equals, // '='
	String, // 'value'
	Integer, // 64-bit long
	Double, // 64-bit floating point number
	Null, // NULL
	BinaryBlob, // Supports X'FFF' and _binary 0xFFF formats.
	Identifier, // An identifier, e.g. 'SELECT' or 'FROM'
	EOF
}

public class SqlTokenizer
{
	// Adjust the buffer size as needed.
	private const int BufferSize = 4096;
	private StringBuilder stringBuilder = new StringBuilder();
	private readonly StreamReader _reader;
	private readonly char[] _buffer = new char[BufferSize];
	private int _bufferLength = 0;
	// _position indexes into _buffer – always valid when _bufferLength>0.
	private int _position = 0;
	private bool _isEOF = false;

	// Properties for the token’s “value.”
	public SqlTokenType TokenType { get; private set; }
	public bool IdentifierWasEscaped { get; set; }
	public ReadOnlyMemory<char> ValueString { get; private set; }
	public long ValueLong { get; private set; }
	public double ValueDouble { get; private set; }
	public ReadOnlyMemory<char> ValueBinaryHex { get; private set; }

	public SqlTokenizer(Stream stream)
	{
		// Use the default encoding (or specify one). The StreamReader
		// will read from the stream using a fixed‐size buffer.
		_reader = new StreamReader(stream);
		FillBufferInitial();
	}

	// Called once at construction.
	private void FillBufferInitial()
	{
		_bufferLength = _reader.Read(_buffer, 0, BufferSize);
		_position = 0;
		if (_bufferLength == 0)
		{
			_isEOF = true;
		}
	}

	// When we have consumed all characters in the buffer we “refill.”
	// This method copies leftover characters (if any) from _buffer[_position..]
	// into the beginning of the buffer then reads more from the stream.
	private bool RefillBuffer()
	{
		if (_isEOF) return false;

		int remaining = Math.Max(_bufferLength - _position, 0);
		if (remaining > 0)
		{
			// Move any leftover characters to the beginning.
			Array.Copy(_buffer, _position, _buffer, 0, remaining);
		}
		int read = _reader.Read(_buffer, remaining, BufferSize - remaining);
		_bufferLength = remaining + read;
		_position = 0;
		if (read == 0)
		{
			_isEOF = true;
		}
		return _bufferLength > 0;
	}

	// Ensure there is at least one char available in _buffer.
	private bool EnsureCharAvailable(int offset = 0)
	{
		if (_position + offset >= _bufferLength)
		{
			return RefillBuffer();
		}
		return true;
	}

	// Peek at the next char (without advancing) or return null at end‐of‐file.
	private char? Peek(int offset = 0)
	{
		if (!EnsureCharAvailable(offset))
			return null;
		return _buffer[_position + offset];
	}

	// Consume (advance past) any whitespace.
	private void SkipWhiteSpace()
	{
		while (true)
		{
			var ch = Peek();
			if (!ch.HasValue)
				break;

			if (ch == '-' && Peek(1) == '-')
			{
				while (ch != null && ch != '\n')
				{
					_position++;
					ch = Peek();
				}
				continue;
			}

			if (ch == '/' && Peek(1) == '*')
			{
				while (ch != null)
				{
					_position++;
					ch = Peek();

					if (ch == '*' && Peek(1) == '/')
					{
						_position += 2;
						break;
					}
				}
				continue;
			}

			if (!char.IsWhiteSpace(ch.Value))
				break;

			_position++;
			continue;
		}
	}

	// Returns true if after a '-' we see at least one digit.
	private bool NextIsDigit()
	{
		char? next = Peek(1);
		return next.HasValue && char.IsDigit(next.Value);
	}

	// The public method. Each call reads the next token from the Stream.
	// It also sets the TokenType property and the appropriate value property.
	public SqlTokenType Read()
	{
		// Clear previous token’s values.
		ValueString = ReadOnlyMemory<char>.Empty;
		ValueBinaryHex = ReadOnlyMemory<char>.Empty;
		ValueLong = 0;
		ValueDouble = 0;
		IdentifierWasEscaped = false;

	restart:
		SkipWhiteSpace();

		var ch = Peek();
		if (!ch.HasValue)
		{
			TokenType = SqlTokenType.EOF;
			return TokenType;
		}

		// Simple one‐character tokens.
		switch (ch)
		{
			case ',':
				_position++;
				TokenType = SqlTokenType.Comma;
				return TokenType;
			case ';':
				_position++;
				TokenType = SqlTokenType.Semicolon;
				return TokenType;
			case '(':
				_position++;
				TokenType = SqlTokenType.LeftBrace;
				return TokenType;
			case ')':
				_position++;
				TokenType = SqlTokenType.RightBrace;
				return TokenType;
			case '=':
				_position++;
				TokenType = SqlTokenType.Equals;
				return TokenType;
			case '+':
			case '-':
			case '*':
			case '/':
			case '<':
			case '>':
				// math operators. we're not parsing for these
				_position++;
				goto restart;
		}

		// String literal: starts with a single quote.
		if (ch == '\'')
		{
			ReadStringLiteral();
			return TokenType = SqlTokenType.String;
		}

		// 0x literal: binary blob without the _binary prefix
		char? nextCh = Peek(1);
		if (ch == '0' && (nextCh == 'x' || nextCh == 'X'))
		{
			_position += 2; // consume 0x
			return ReadBinaryBlobFor0x();
		}

		// Number literal: digit or a '-' sign followed by a digit.
		if (char.IsDigit(ch.Value) || (ch == '-' && NextIsDigit()))
		{
			return ReadNumber();
		}

		// If the token starts with an X (or x) and is immediately followed by a single quote,
		// then it is a binary blob of the form X'AAA'.
		if ((ch == 'X' || ch == 'x') && nextCh == '\'')
		{
			_position++; // consume X
			_position++; // consume the opening '
			return ReadBinaryBlob('\'');
		}

		// Identifier or keyword. Also handle _binary. If an identifier starts with '_' then we
		// read the complete identifier and, if it equals "_binary" (case–insensitive) we
		// potentially treat an immediately following 0x prefix as a binary blob.
		ReadOnlyMemory<char> ident;
		if (ch == '_')
		{
			ident = ReadIdentifierToken();
			if (ident.Span.Equals("_binary", StringComparison.OrdinalIgnoreCase))
			{
				SkipWhiteSpace();
				if (Peek() == '0' && (Peek(1) == 'x' || Peek(1) == 'X'))
				{
					// Consume the 0 and the x.
					_position++;
					_position++;
					return ReadBinaryBlobFor0x();
				}

				if (Peek() == '\'' && Peek(1) == '\'')
				{
					// _binary ''
					// apparently how empty binary strings are represented in --skip-opt

					_position += 2;
					TokenType = SqlTokenType.BinaryBlob;
					return TokenType;
				}

				else
				{
					ValueString = ident;
					TokenType = SqlTokenType.Identifier;
					return TokenType;
				}
			}
			else
			{
				ValueString = ident;
				TokenType = IsNullKeyword(ident) ? SqlTokenType.Null : SqlTokenType.Identifier;
				return TokenType;
			}
		}

		// Otherwise process an identifier.
		ident = ReadIdentifierToken();

		if (ident.Length == 0)
		{
			var debug = _buffer.AsSpan(_position - 10);
			throw new Exception("Found invalid identifier");
		}

		if (IsNullKeyword(ident))
		{
			TokenType = SqlTokenType.Null;
		}
		else
		{
			TokenType = SqlTokenType.Identifier;
		}
		ValueString = ident;
		return TokenType;
	}

	// Read a string literal token. This method assumes that the current character was a
	// starting quote ('). It consumes the complete literal including the closing quote.
	// It also recognizes doubled-up quotes as an escaped single quote.
	private void ReadStringLiteral()
	{
		// Skip the starting single quote.
		_position++;
		int tokenStart = _position;

		// We use a flag to indicate when we have seen escape sequences.
		bool isStringBuilding = false;

		void stringBuilderFlush()
		{
			if (!isStringBuilding)
			{
				isStringBuilding = true;
				stringBuilder.Clear();
			}
			stringBuilder.Append(_buffer, tokenStart, _position - tokenStart);
			tokenStart = 0;
		}

		// Loop until we hit an unescaped closing quote.
		while (true)
		{
			if (_position >= _bufferLength)
			{
				stringBuilderFlush();

				if (!EnsureCharAvailable())
					throw new Exception("Unterminated string literal");
			}

			char c = _buffer[_position];

			if (c == '\'')
			{
				if (_position + 1 >= _bufferLength)
				{
					stringBuilderFlush();

					if (!EnsureCharAvailable(1))
						throw new Exception("Unterminated string literal");
				}

				// If the next character is also a quote then treat it as an escape.
				if (Peek(1) == '\'')
				{
					_position++;
					if (!isStringBuilding)
					{
						isStringBuilding = true;
						stringBuilder.Clear();
					}
					stringBuilder.Append(_buffer, tokenStart, _bufferLength - tokenStart);
					// skip over the escape
					_position++;
					tokenStart = _position;
					continue;
				}

				var finishPosition = _position;
				if (finishPosition == 0 && tokenStart > 0)
					// we got unlucky and a buffer refresh happened right as the string was supposed to end
					finishPosition = 4096;

				var result = new ReadOnlyMemory<char>(_buffer, tokenStart, finishPosition - tokenStart);

				if (isStringBuilding)
				{
					isStringBuilding = true;
					stringBuilder.Append(result);
					result = stringBuilder.ToString().AsMemory();
				}

				ValueString = result;
				_position++;
				return;
			}
			else if (c == '\\')
			{
				if (!isStringBuilding)
				{
					isStringBuilding = true;
					stringBuilder.Clear();
				}
				stringBuilder.Append(_buffer, tokenStart, _position - tokenStart);

				var nextChar = Peek(1);

				var appendChar = nextChar switch
				{
					'r' => '\r',
					'n' => '\n',
					't' => '\t', 
					'0' => '\0',
					_ => nextChar
				};

				stringBuilder.Append(appendChar);

				_position++;
				tokenStart = _position + 1;
			}

			_position++;
		}
	}

	// Read a number literal. It may be an integer (64-bit) or a double.
	// We support an optional '-' sign, a fractional part (if a dot is seen)
	// and an exponent (if an 'e' or 'E' is present).
	private SqlTokenType ReadNumber()
	{
		int tokenStart = _position;
		bool isDouble = false;
		bool isStringBuilding = false;

		// Consume an optional '-' sign.
		if (Peek() == '-')
		{
			_position++;
		}

		// Loop reading the digits and optional dot.
		while (true)
		{
			if (_position >= _bufferLength)
			{
				if (!isStringBuilding)
				{
					isStringBuilding = true;
					stringBuilder.Clear();
				}
				stringBuilder.Append(_buffer, tokenStart, _bufferLength - tokenStart);
				tokenStart = 0;
			}
			var ch = Peek();
			if (!ch.HasValue)
				break;
			if (char.IsDigit(ch.Value))
			{
				_position++;
				continue;
			}
			if (ch == '.' && !isDouble)
			{
				isDouble = true;
				_position++;
				continue;
			}
			// Support exponent: e or E followed by an optional sign then digits.
			if ((ch == 'e' || ch == 'E'))
			{
				isDouble = true;
				_position++;
				ch = Peek();
				if (ch.HasValue && (ch == '+' || ch == '-'))
				{
					_position++;
				}
				continue;
			}
			break;
		}

		int tokenEnd = _position;
		var tokenSpan = new ReadOnlySpan<char>(_buffer, tokenStart, tokenEnd - tokenStart);

		if (isStringBuilding)
		{
			stringBuilder.Append(tokenSpan);
			tokenSpan = stringBuilder.ToString();
		}

		if (!isDouble)
		{
			if (long.TryParse(tokenSpan, out long lvalue))
				ValueLong = lvalue;
			else
				ValueLong = 0;
			TokenType = SqlTokenType.Integer;
		}
		else
		{
			if (double.TryParse(tokenSpan, out double dvalue))
				ValueDouble = dvalue;
			else
				ValueDouble = 0;
			TokenType = SqlTokenType.Double;
		}
		return TokenType;
	}

	// Read a binary blob literal whose bytes are specified
	// inside a pair of delimiters. (For X'ABC', the delimiter is the single–quote.)
	private SqlTokenType ReadBinaryBlob(char delimiter)
	{
		int tokenStart = _position;
		while (true)
		{
			if (!EnsureCharAvailable())
				break;
			char c = _buffer[_position];
			if (c == delimiter)
			{
				int tokenEnd = _position;
				ValueBinaryHex = new ReadOnlyMemory<char>(_buffer, tokenStart, tokenEnd - tokenStart);
				_position++; // Consume the closing delimiter.
				TokenType = SqlTokenType.BinaryBlob;
				return TokenType;
			}
			_position++;
		}
		// If we get here, the blob was unterminated.
		TokenType = SqlTokenType.BinaryBlob;
		ValueBinaryHex = ReadOnlyMemory<char>.Empty;
		return TokenType;
	}

	// After reading _binary 0x the remainder is the hexadecimal digits.
	private SqlTokenType ReadBinaryBlobFor0x()
	{
		int tokenStart = _position;
		bool isStringBuilding = false;

		while (true)
		{
			if (_position >= _bufferLength)
			{
				if (!isStringBuilding)
				{
					isStringBuilding = true;
					stringBuilder.Clear();
				}
				stringBuilder.Append(_buffer, tokenStart, _bufferLength - tokenStart);
				tokenStart = 0;
			}

			if (!EnsureCharAvailable())
				break;

			char c = _buffer[_position];
			// Only allow hex digits.
			if (Uri.IsHexDigit(c))
			{
				_position++;
				continue;
			}
			break;
		}

		int tokenEnd = _position;
		var remaining = new ReadOnlyMemory<char>(_buffer, tokenStart, tokenEnd - tokenStart);

		if (isStringBuilding)
		{
			stringBuilder.Append(remaining);
			ValueBinaryHex = stringBuilder.ToString().AsMemory();
		}
		else
		{
			ValueBinaryHex = remaining;
		}

		TokenType = SqlTokenType.BinaryBlob;
		return TokenType;
	}

	// Reads an identifier from the stream. An identifier is defined as a
	// contiguous set of letters, digits and underscores.
	// (In MySQL more is allowed; adjust as needed.)
	private ReadOnlyMemory<char> ReadIdentifierToken()
	{
		int tokenStart = _position;
		bool isEscaped = false;

		bool isStringBuilding = false;

		while (true)
		{
			if (_position >= _bufferLength)
			{
				if (!isStringBuilding)
				{
					isStringBuilding = true;
					stringBuilder.Clear();
				}
				stringBuilder.Append(_buffer, tokenStart, _bufferLength - tokenStart);
				tokenStart = 0;
			}

			if (!EnsureCharAvailable())
			{
				break;
			}

			char c = _buffer[_position];
			if (c == '`')
			{
				if (isEscaped)
				{
					break;
				}
				else
				{
					tokenStart++;
					_position++;
					isEscaped = true;
				}
			}
			else if (isEscaped || char.IsLetterOrDigit(c) || c == '_' || c == '.' || c == '"' || c == '@')
			{
				_position++;
			}
			else
			{
				break;
			}
		}
		int tokenEnd = _position;

		if (isEscaped)
			_position++;

		var remaining = new ReadOnlyMemory<char>(_buffer, tokenStart, tokenEnd - tokenStart);

		if (isStringBuilding)
		{
			stringBuilder.Append(remaining);
			return stringBuilder.ToString().AsMemory();
		}

		IdentifierWasEscaped = isEscaped;

		return remaining;
	}

	// Returns true if the given identifier token equals "NULL" (ignoring case).
	private bool IsNullKeyword(ReadOnlyMemory<char> token)
	{
		if (token.Length != 4) return false;
		var span = token.Span;
		return (span[0] == 'N' || span[0] == 'n') &&
			   (span[1] == 'U' || span[1] == 'u') &&
			   (span[2] == 'L' || span[2] == 'l') &&
			   (span[3] == 'L' || span[3] == 'l');
	}
}