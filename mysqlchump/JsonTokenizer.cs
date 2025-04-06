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
public class JsonTokenizer
{
    // Underlying stream reader for decoding the stream.
    private readonly StreamReader _reader;
    // A buffer that holds characters read from the stream.
    private readonly char[] _buffer;
    // _bufferPos is the current position and _bufferLen is the number
    // of valid chars currently in _buffer.
    private int _bufferPos;
    private int _bufferLen;

    // Stack that tracks our container (object or array) context.
    //private readonly Stack<JsonContainerType> _context = new Stack<JsonContainerType>();
    // When inside an object, this flag tells us whether the next
    // quoted string should be interpreted as a property name.
    private bool _expectPropertyName;

    // The possible container types.
    //private enum JsonContainerType { Object, Array }

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

        _buffer = new char[bufferSize];

        // Construct a StreamReader (using UTF-8; adjust as needed).
        _reader = new StreamReader(stream, Encoding.UTF8, detectEncodingFromByteOrderMarks: true, bufferSize: bufferSize, leaveOpen: true);
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

        char c = _buffer[_bufferPos];

        // Punctuation tokens and structural characters.
        switch (c)
        {
            case '{':
                _bufferPos++;
                TokenType = JsonTokenType.StartObject;
                //_context.Push(JsonContainerType.Object);
                //_expectPropertyName = true;
                return TokenType;

            case '}':
                _bufferPos++;
                //if (_context.Count == 0 || _context.Peek() != JsonContainerType.Object)
                //    throw new JsonException("Unexpected '}' encountered.");
                //_context.Pop();
                TokenType = JsonTokenType.EndObject;
                return TokenType;

            case '[':
                _bufferPos++;
                TokenType = JsonTokenType.StartArray;
                //_context.Push(JsonContainerType.Array);
                //_expectPropertyName = false;
                return TokenType;

            case ']':
                _bufferPos++;
                //if (_context.Count == 0 || _context.Peek() != JsonContainerType.Array)
                //    throw new JsonException("Unexpected ']' encountered.");
                //_context.Pop();
                TokenType = JsonTokenType.EndArray;
                return TokenType;

            case ',':
                _bufferPos++;
                // In an object a comma means the next string is a property name.
                //if (_context.Count > 0 && _context.Peek() == JsonContainerType.Object)
                //    _expectPropertyName = true;
                TokenType = JsonTokenType.Comma;
                return TokenType;

            case ':':
                _bufferPos++;
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
                throw new JsonException($"Unexpected character '{c}' at buffer position {_bufferPos}.");
        }
    }

    // Skip any whitespace characters.
    private void SkipWhiteSpace()
    {
        while (true)
        {
            if (_bufferPos >= _bufferLen)
            {
                if (!FillBuffer())
                    break;
            }
            char ch = _buffer[_bufferPos];
            if (ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r')
                _bufferPos++;
            else
                break;
        }
    }

    // Fill _buffer with more characters. Before reading, any unconsumed characters 
    // (from _bufferPos to _bufferLen) are shifted to the start.
    private bool FillBuffer()
    {
        if (_bufferPos < _bufferLen)
        {
            int remaining = _bufferLen - _bufferPos;
            if (remaining > 0)
            {
                // Use a span copy for speed.
                ReadOnlySpan<char> src = _buffer.AsSpan(_bufferPos, remaining);
                src.CopyTo(_buffer);
            }
            _bufferLen = remaining;
            _bufferPos = 0;
        }
        else
        {
            _bufferLen = 0;
            _bufferPos = 0;
        }
        int read = _reader.Read(_buffer, _bufferLen, _buffer.Length - _bufferLen);
        if (read == 0)
            return false;
        _bufferLen += read;
        return true;
    }

    // Ensure there are at least minChars available in _buffer (from _bufferPos onward). 
    // Returns true if that condition holds.
    private bool EnsureBuffer(int minChars)
    {
        while (_bufferLen - _bufferPos < minChars)
        {
            if (!FillBuffer())
                break;
        }
        return _bufferLen - _bufferPos >= minChars;
    }

    // Read a JSON string (which may be escaped). When inside an object and
    // _expectPropertyName is true the token is marked as PropertyName.
    private readonly StringBuilder tempBuilder = new StringBuilder();
    private JsonTokenType ReadString()
    {
        // Skip the opening quote.
        _bufferPos++;
        int start = _bufferPos;
        StringBuilder sb = tempBuilder;
        sb.Clear();
        bool hadEscape = false;

        while (true)
        {
            // we need an extra character buffer at the end to check for colons
            if (_bufferPos + 1 >= _bufferLen)
            {
                hadEscape = true;
                int length = _bufferPos - start;
                sb ??= new StringBuilder();
                sb.Append(_buffer, start, length);

                if (!EnsureBuffer(2))
                    throw new JsonException("Unterminated string literal.");

                start = 0;
            }

            char c = _buffer[_bufferPos];
            if (c == '"')
            {
                int length = _bufferPos - start;
                ReadOnlyMemory<char> result;
                if (hadEscape)
                {
                    // Append any chars after the last escape.
                    sb!.Append(_buffer, start, length);
                    result = sb!.ToString().AsMemory();
                }
                else
                {
                    // Fast–path: no escapes encountered.
                    result = new Memory<char>(_buffer, start, length);
                }
                _bufferPos++; // Skip closing quote.
                // Depending on the state, either this string is a property name or a literal.
                //if (_context.Count > 0 && _context.Peek() == JsonContainerType.Object && _expectPropertyName)
                //{
                //    TokenType = JsonTokenType.PropertyName;
                //    _expectPropertyName = false;
                //}
                //else
                //{
                    TokenType = _buffer[_bufferPos] == ':' ? JsonTokenType.PropertyName : JsonTokenType.String;
                //}
                ValueString = result;
                return TokenType;
            }
            else if (c == '\\')
            {
                hadEscape = true;
                sb ??= new StringBuilder();
                // Append the literal characters so far.
                sb.Append(_buffer, start, _bufferPos - start);
                _bufferPos++; // Skip the backslash.
                if (!EnsureBuffer(1))
                    throw new JsonException("Incomplete escape sequence in string literal.");
                char escape = _buffer[_bufferPos++];
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
                            char hex = _buffer[_bufferPos++];
                            code = (code << 4) | HexToInt(hex);
                        }
                        sb.Append((char)code);
                        break;
                    default:
                        throw new JsonException($"Invalid escape sequence: \\{escape}");
                }
                start = _bufferPos;
            }
            else
            {
                _bufferPos++;
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
        if (_buffer[_bufferPos] == 't')
        {
            const string trueLiteral = "true";
            for (int i = 0; i < trueLiteral.Length; i++)
            {
                if (!EnsureBuffer(1))
                    throw new JsonException("Unexpected end of input reading literal 'true'.");
                if (_buffer[_bufferPos] != trueLiteral[i])
                    throw new JsonException("Invalid token -- expected literal 'true'.");
                _bufferPos++;
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
                if (_buffer[_bufferPos] != falseLiteral[i])
                    throw new JsonException("Invalid token -- expected literal 'false'.");
                _bufferPos++;
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
            if (_buffer[_bufferPos] != nullLiteral[i])
                throw new JsonException("Invalid token -- expected literal 'null'.");
            _bufferPos++;
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

        int start = _bufferPos;
        bool isFraction = false, isExponent = false;

        // optional minus sign.
        if (_buffer[_bufferPos] == '-')
        {
            _bufferPos++;
            if (!EnsureBuffer(1))
                throw new JsonException("Unexpected end after '-' in number literal.");
        }

        // integer part
        if (_buffer[_bufferPos] == '0')
        {
            _bufferPos++;
        }
        else if (_buffer[_bufferPos] >= '1' && _buffer[_bufferPos] <= '9')
        {
            while (EnsureBuffer(1) && _buffer[_bufferPos] >= '0' && _buffer[_bufferPos] <= '9')
            {
                _bufferPos++;
                if (_bufferPos >= _bufferLen)
                    break;
            }
        }
        else
        {
            throw new JsonException("Invalid number literal; expected digit.");
        }

        // fraction part
        if (EnsureBuffer(1) && _buffer[_bufferPos] == '.')
        {
            isFraction = true;
            _bufferPos++; // skip '.'
            if (!EnsureBuffer(1) || !(_buffer[_bufferPos] >= '0' && _buffer[_bufferPos] <= '9'))
                throw new JsonException("Invalid number literal; expected digit after decimal point.");
            while (EnsureBuffer(1) && _buffer[_bufferPos] >= '0' && _buffer[_bufferPos] <= '9')
            {
                _bufferPos++;
                if (_bufferPos >= _bufferLen)
                    break;
            }
        }

        // exponent part
        if (EnsureBuffer(1) && (_buffer[_bufferPos] == 'e' || _buffer[_bufferPos] == 'E'))
        {
            isExponent = true;
            _bufferPos++; // skip e/E
            if (EnsureBuffer(1) && (_buffer[_bufferPos] == '+' || _buffer[_bufferPos] == '-'))
                _bufferPos++;
            if (!EnsureBuffer(1) || !(_buffer[_bufferPos] >= '0' && _buffer[_bufferPos] <= '9'))
                throw new JsonException("Invalid number literal; expected digit in exponent.");
            while (EnsureBuffer(1) && _buffer[_bufferPos] >= '0' && _buffer[_bufferPos] <= '9')
            {
                _bufferPos++;
                if (_bufferPos >= _bufferLen)
                    break;
            }
        }

        int length = _bufferPos - start;
        // Create a temporary string from the numeric literal.
        var numStr = new ReadOnlySpan<char>(_buffer, start, length);
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
}