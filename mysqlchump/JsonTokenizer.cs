using System;
using System.Globalization;
using System.IO;
using System.Text;

namespace mysqlchump;

// The token types the tokenizer produces.
public enum JsonTokenType
{
    None,
    StartObject,  // {
    EndObject,    // }
    StartArray,   // [
    EndArray,     // ]
    PropertyName, // a string used as a property name in an object
    String,       // a string literal
    NumberLong,   // an integer value
    NumberDouble, // a floating–point value (if the literal contains a fractional part or exponent)
    Boolean,      // true/false
    Null,         // null literal
    Comma,        // ,
    Colon,        // :
    EndOfFile
}

// A minimal JSON exception.
public class JsonException : Exception
{
    public JsonException(string message) : base(message) { }
}

// The high–performance, synchronous JSON tokenizer.
public class JsonTokenizer : IDisposable
{
    // Underlying stream reader for decoding the stream.
    private readonly StreamReader StreamReader;
    // A buffer that holds characters read from the stream.
    private readonly char[] Buffer;
    // _bufferPos is the current position and _bufferLen is the number
    // of valid chars currently in _buffer.
    private int BufferPos;
    private int BufferLen;

    // Token type for the last token read.
    public JsonTokenType TokenType { get; private set; }
    // Value properties; only one of these is valid for a given token.
    public ReadOnlyMemory<char> ValueString { get; private set; }
    public long ValueLong { get; private set; }
    public double ValueDouble { get; private set; }
    public bool ValueBoolean { get; private set; }

    // Create a new tokenizer reading from the given Stream.
    // bufferSize is the size (in characters) of the internal buffer.
    public JsonTokenizer(Stream stream, int bufferSize = 4096)
    {
        if (stream == null)
            throw new ArgumentNullException(nameof(stream));

        Buffer = new char[bufferSize];

        // Construct a StreamReader (using UTF-8; adjust as needed).
        StreamReader = new StreamReader(stream, Encoding.UTF8, detectEncodingFromByteOrderMarks: true, bufferSize: bufferSize, leaveOpen: true);
        // Prime the buffer.
        FillBuffer();
    }

    // Read the next JSON token from the stream. The method returns the token type.
    // It advances the internal buffer and decodes tokens such as '{', '}', strings,
    // numbers, booleans, and null.
    public JsonTokenType Read()
    {
        SkipWhiteSpace();
        if (!EnsureBuffer(1))
        {
            TokenType = JsonTokenType.EndOfFile;
            return TokenType;
        }

        char c = Buffer[BufferPos];

        // Punctuation tokens and structural characters.
        switch (c)
        {
            case '{':
                BufferPos++;
                TokenType = JsonTokenType.StartObject;
                //_context.Push(JsonContainerType.Object);
                //_expectPropertyName = true;
                return TokenType;

            case '}':
                BufferPos++;
                //if (_context.Count == 0 || _context.Peek() != JsonContainerType.Object)
                //    throw new JsonException("Unexpected '}' encountered.");
                //_context.Pop();
                TokenType = JsonTokenType.EndObject;
                return TokenType;

            case '[':
                BufferPos++;
                TokenType = JsonTokenType.StartArray;
                //_context.Push(JsonContainerType.Array);
                //_expectPropertyName = false;
                return TokenType;

            case ']':
                BufferPos++;
                //if (_context.Count == 0 || _context.Peek() != JsonContainerType.Array)
                //    throw new JsonException("Unexpected ']' encountered.");
                //_context.Pop();
                TokenType = JsonTokenType.EndArray;
                return TokenType;

            case ',':
                BufferPos++;
                // In an object a comma means the next string is a property name.
                //if (_context.Count > 0 && _context.Peek() == JsonContainerType.Object)
                //    _expectPropertyName = true;
                TokenType = JsonTokenType.Comma;
                return Read();

            case ':':
                BufferPos++;
                TokenType = JsonTokenType.Colon;
                return TokenType;

            case '"':
                return ReadString();

            case 't':
            case 'f':
                return ReadBoolean();

            case 'n':
                return ReadNull();

            case '-':
            case '0':
            case '1':
            case '2':
            case '3':
            case '4':
            case '5':
            case '6':
            case '7':
            case '8':
            case '9':
                return ReadNumber();

            default:
                throw new JsonException($"Unexpected character '{c}' at buffer position {BufferPos}.");
        }
    }

    // Skip any whitespace characters.
    private void SkipWhiteSpace()
    {
        while (true)
        {
            if (BufferPos >= BufferLen)
            {
                if (!FillBuffer())
                    break;
            }
            char ch = Buffer[BufferPos];
            if (ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r')
                BufferPos++;
            else
                break;
        }
    }

    // Fill _buffer with more characters. Before reading, any unconsumed characters 
    // (from _bufferPos to _bufferLen) are shifted to the start.
    private bool FillBuffer()
    {
        if (BufferPos < BufferLen)
        {
            int remaining = BufferLen - BufferPos;
            if (remaining > 0)
            {
                // Use a span copy for speed.
                ReadOnlySpan<char> src = Buffer.AsSpan(BufferPos, remaining);
                src.CopyTo(Buffer);
            }
            BufferLen = remaining;
            BufferPos = 0;
        }
        else
        {
            BufferLen = 0;
            BufferPos = 0;
        }
        int read = StreamReader.Read(Buffer, BufferLen, Buffer.Length - BufferLen);
        if (read == 0)
            return false;
        BufferLen += read;
        return true;
    }

    // Ensure there are at least minChars available in _buffer (from _bufferPos onward). 
    // Returns true if that condition holds.
    private bool EnsureBuffer(int minChars)
    {
        while (BufferLen - BufferPos < minChars)
        {
            if (!FillBuffer())
                break;
        }
        return BufferLen - BufferPos >= minChars;
    }

    // Read a JSON string (which may be escaped). When inside an object and
    // _expectPropertyName is true the token is marked as PropertyName.
    private readonly StringBuilder tempBuilder = new StringBuilder();
    private JsonTokenType ReadString()
    {
        // Skip the opening quote.
        BufferPos++;
        int start = BufferPos;
        StringBuilder sb = tempBuilder;
        sb.Clear();
        bool hadEscape = false;

        while (true)
        {
            // we need an extra character buffer at the end to check for colons
            if (BufferPos + 1 >= BufferLen)
            {
                hadEscape = true;
                int length = BufferPos - start;
                sb ??= new StringBuilder();
                sb.Append(Buffer, start, length);

                if (!EnsureBuffer(2))
                    throw new JsonException("Unterminated string literal.");

                start = 0;
            }

            char c = Buffer[BufferPos];
            if (c == '"')
            {
                int length = BufferPos - start;
                ReadOnlyMemory<char> result;
                if (hadEscape)
                {
                    // Append any chars after the last escape.
                    sb!.Append(Buffer, start, length);
                    result = sb!.ToString().AsMemory();
                }
                else
                {
                    // Fast–path: no escapes encountered.
                    result = new Memory<char>(Buffer, start, length);
                }
                BufferPos++; // Skip closing quote.

				if (Buffer[BufferPos] == ':')
				{
					TokenType = JsonTokenType.PropertyName;
					BufferPos++;
				}
				else
					TokenType = JsonTokenType.String;

				ValueString = result;
                return TokenType;
            }
            else if (c == '\\')
            {
                hadEscape = true;
                sb ??= new StringBuilder();
                // Append the literal characters so far.
                sb.Append(Buffer, start, BufferPos - start);
                BufferPos++; // Skip the backslash.
                if (!EnsureBuffer(1))
                    throw new JsonException("Incomplete escape sequence in string literal.");
                char escape = Buffer[BufferPos++];
                switch (escape)
                {
                    case '"': sb.Append('"'); break;
                    case '\\': sb.Append('\\'); break;
                    case '/': sb.Append('/'); break;
                    case 'b': sb.Append('\b'); break;
                    case 'f': sb.Append('\f'); break;
                    case 'n': sb.Append('\n'); break;
                    case 'r': sb.Append('\r'); break;
                    case 't': sb.Append('\t'); break;
                    case 'u':
                        if (!EnsureBuffer(4))
                            throw new JsonException("Incomplete Unicode escape sequence.");
                        int code = 0;
                        for (int i = 0; i < 4; i++)
                        {
                            char hex = Buffer[BufferPos++];
                            code = (code << 4) | HexToInt(hex);
                        }
                        sb.Append((char)code);
                        break;
                    default:
                        throw new JsonException($"Invalid escape sequence: \\{escape}");
                }
                start = BufferPos;
            }
            else
            {
                BufferPos++;
            }
        }
    }

    // Convert a hex digit into its integer value.
    private int HexToInt(char c)
    {
        if (c >= '0' && c <= '9')
            return c - '0';
        if (c >= 'A' && c <= 'F')
            return c - 'A' + 10;
        if (c >= 'a' && c <= 'f')
            return c - 'a' + 10;
        throw new JsonException($"Invalid hexadecimal character '{c}' in Unicode escape.");
    }

    // Read a literal “true” or “false” from the stream.
    private JsonTokenType ReadBoolean()
    {
        if (Buffer[BufferPos] == 't')
        {
            const string trueLiteral = "true";
            for (int i = 0; i < trueLiteral.Length; i++)
            {
                if (!EnsureBuffer(1))
                    throw new JsonException("Unexpected end of input reading literal 'true'.");
                if (Buffer[BufferPos] != trueLiteral[i])
                    throw new JsonException("Invalid token -- expected literal 'true'.");
                BufferPos++;
            }
            TokenType = JsonTokenType.Boolean;
            ValueBoolean = true;
        }
        else
        {
            const string falseLiteral = "false";
            for (int i = 0; i < falseLiteral.Length; i++)
            {
                if (!EnsureBuffer(1))
                    throw new JsonException("Unexpected end of input reading literal 'false'.");
                if (Buffer[BufferPos] != falseLiteral[i])
                    throw new JsonException("Invalid token -- expected literal 'false'.");
                BufferPos++;
            }
            TokenType = JsonTokenType.Boolean;
            ValueBoolean = false;
        }
        return TokenType;
    }

    // Read a “null” literal.
    private JsonTokenType ReadNull()
    {
        const string nullLiteral = "null";
        for (int i = 0; i < nullLiteral.Length; i++)
        {
            if (!EnsureBuffer(1))
                throw new JsonException("Unexpected end of input reading literal 'null'.");
            if (Buffer[BufferPos] != nullLiteral[i])
                throw new JsonException("Invalid token -- expected literal 'null'.");
            BufferPos++;
        }
        TokenType = JsonTokenType.Null;
        return TokenType;
    }

    // Read a numeric literal. The algorithm reads an optional minus,
    // then an integer part; if a '.' or exponent is found the number is treated as a double,
    // otherwise the literal is parsed to a long.
    private JsonTokenType ReadNumber()
    {
        EnsureBuffer(30);

        int start = BufferPos;
        bool isFraction = false, isExponent = false;

        // optional minus sign.
        if (Buffer[BufferPos] == '-')
        {
            BufferPos++;
            if (!EnsureBuffer(1))
                throw new JsonException("Unexpected end after '-' in number literal.");
        }

        // integer part
        if (Buffer[BufferPos] == '0')
        {
            BufferPos++;
        }
        else if (Buffer[BufferPos] >= '1' && Buffer[BufferPos] <= '9')
        {
            while (EnsureBuffer(1) && Buffer[BufferPos] >= '0' && Buffer[BufferPos] <= '9')
            {
                BufferPos++;
                if (BufferPos >= BufferLen)
                    break;
            }
        }
        else
        {
            throw new JsonException("Invalid number literal; expected digit.");
        }

        // fraction part
        if (EnsureBuffer(1) && Buffer[BufferPos] == '.')
        {
            isFraction = true;
            BufferPos++; // skip '.'
            if (!EnsureBuffer(1) || !(Buffer[BufferPos] >= '0' && Buffer[BufferPos] <= '9'))
                throw new JsonException("Invalid number literal; expected digit after decimal point.");
            while (EnsureBuffer(1) && Buffer[BufferPos] >= '0' && Buffer[BufferPos] <= '9')
            {
                BufferPos++;
                if (BufferPos >= BufferLen)
                    break;
            }
        }

        // exponent part
        if (EnsureBuffer(1) && (Buffer[BufferPos] == 'e' || Buffer[BufferPos] == 'E'))
        {
            isExponent = true;
            BufferPos++; // skip e/E
            if (EnsureBuffer(1) && (Buffer[BufferPos] == '+' || Buffer[BufferPos] == '-'))
                BufferPos++;
            if (!EnsureBuffer(1) || !(Buffer[BufferPos] >= '0' && Buffer[BufferPos] <= '9'))
                throw new JsonException("Invalid number literal; expected digit in exponent.");
            while (EnsureBuffer(1) && Buffer[BufferPos] >= '0' && Buffer[BufferPos] <= '9')
            {
                BufferPos++;
                if (BufferPos >= BufferLen)
                    break;
            }
        }

        int length = BufferPos - start;
        // Create a temporary string from the numeric literal.
        var numStr = new ReadOnlySpan<char>(Buffer, start, length);
        if (!isFraction && !isExponent && long.TryParse(numStr, out long l))
        {
            TokenType = JsonTokenType.NumberLong;
            ValueLong = l;
        }
        else
        {
            TokenType = JsonTokenType.NumberDouble;
            ValueDouble = double.Parse(numStr, CultureInfo.InvariantCulture);
        }
        return TokenType;
    }

	public void Dispose() => ((IDisposable)StreamReader).Dispose();
}