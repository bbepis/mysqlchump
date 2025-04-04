using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace mysqlchump;

/// <summary>
/// Represents a database table.
/// </summary>
public class Table
{
	public string Name { get; set; }
	public List<Column> Columns { get; } = new List<Column>();
	public List<Index> Indexes { get; } = new List<Index>();
	public List<ForeignKey> ForeignKeys { get; } = new List<ForeignKey>();
	public Dictionary<string, string> Options { get; } = new(StringComparer.OrdinalIgnoreCase);

	/// <summary>
	/// Converts the Table object back into a CREATE TABLE SQL statement.
	/// </summary>
	public string ToCreateTableSql()
	{
		StringBuilder sb = new StringBuilder();
		sb.AppendLine($"CREATE TABLE `{Name}` (");

		List<string> columnDefinitions = new List<string>();
		foreach (var column in Columns)
		{
			columnDefinitions.Add($"\t{column.ToSql()}");
		}

		foreach (var index in Indexes)
		{
			columnDefinitions.Add($"\t{index.ToSql()}");
		}

		foreach (var fk in ForeignKeys)
		{
			columnDefinitions.Add($"\t{fk.ToSql()}");
		}

		sb.Append(string.Join(",\n", columnDefinitions));
		sb.Append("\n)");

		foreach (var option in Options)
		{
			sb.Append($"\n{option.Key}={option.Value}");
		}

		sb.Append(';');

		return sb.ToString();
	}
}

/// <summary>
/// Represents a column in a table.
/// </summary>
public class Column
{
	public string Name { get; set; }
	public string DataType { get; set; } // e.g., VARCHAR(255), ENUM('a','b')
	public bool IsNullable { get; set; } = true;
	public bool IsPrimaryKey { get; set; } = false;
	public string DefaultValue { get; set; } = null;
	public bool IsAutoIncrement { get; set; } = false;
	public bool Unsigned { get; set; } = false;
	public string CharacterSet { get; set; } = null;
	public string Collation { get; set; } = null;
	public string Extra { get; set; } = null; // Additional column options

	/// <summary>
	/// Converts the Column object to its SQL definition.
	/// </summary>
	public string ToSql()
	{
		StringBuilder sb = new StringBuilder();
		sb.Append($"`{Name}` {DataType}");

		if (Unsigned)
		{
			sb.Append(" UNSIGNED");
		}

		if (IsAutoIncrement)
		{
			sb.Append(" AUTO_INCREMENT");
		}

		if (!string.IsNullOrEmpty(CharacterSet))
		{
			sb.Append($" CHARACTER SET '{CharacterSet}'");
		}

		if (!string.IsNullOrEmpty(Collation))
		{
			sb.Append($" COLLATE '{Collation}'");
		}

		sb.Append(IsNullable ? " NULL" : " NOT NULL");

		if (DefaultValue != null)
		{
			sb.Append($" DEFAULT {DefaultValue}");
		}

		if (!string.IsNullOrEmpty(Extra))
		{
			sb.Append($" {Extra}");
		}

		return sb.ToString().TrimEnd();
	}
}

/// <summary>
/// Represents an index on a table.
/// </summary>
public class Index
{
	public string Name { get; set; } // e.g., PRIMARY, UNIQUE key name
	public IndexType Type { get; set; }
	public List<string> Columns { get; } = new List<string>();

	public string ToSql()
	{
		StringBuilder sb = new StringBuilder();
		if (Type == IndexType.Primary)
		{
			sb.Append("PRIMARY KEY ");
		}
		else if (Type == IndexType.Unique)
		{
			sb.Append($"UNIQUE KEY `{Name}` ");
		}
		else
		{
			sb.Append($"INDEX `{Name}` ");
		}

		sb.Append("(");
		sb.Append(string.Join(", ", Columns.Select(c => $"`{c}`")));
		sb.Append(")");
		return sb.ToString();
	}
}

/// <summary>
/// Represents a foreign key constraint.
/// </summary>
public class ForeignKey
{
	public string Name { get; set; }
	public List<string> Columns { get; } = new List<string>();
	public string ReferenceTable { get; set; }
	public List<string> ReferenceColumns { get; } = new List<string>();
	public string OnUpdate { get; set; } = null; // e.g., CASCADE
	public string OnDelete { get; set; } = null; // e.g., SET NULL

	public string ToSql()
	{
		StringBuilder sb = new StringBuilder();
		sb.Append($"CONSTRAINT `{Name}` FOREIGN KEY (");
		sb.Append(string.Join(", ", Columns.ConvertAll(c => $"`{c}`")));
		sb.Append($") REFERENCES `{ReferenceTable}` (");
		sb.Append(string.Join(", ", ReferenceColumns.ConvertAll(c => $"`{c}`")));
		sb.Append(")");

		if (!string.IsNullOrEmpty(OnUpdate))
		{
			sb.Append($" ON UPDATE {OnUpdate}");
		}

		if (!string.IsNullOrEmpty(OnDelete))
		{
			sb.Append($" ON DELETE {OnDelete}");
		}

		return sb.ToString();
	}
}

/// <summary>
/// Enum representing the type of an index.
/// </summary>
public enum IndexType
{
	Primary,
	Unique,
	Index
}

public class CreateTableParser
{
	private readonly Tokenizer _tokenizer;
	private Token _currentToken;

	public CreateTableParser(Tokenizer tokenizer)
	{
		_tokenizer = tokenizer;
		_currentToken = _tokenizer.GetNextToken();
	}

	public static Table Parse(string tableSql)
	{
		try
		{
			return new CreateTableParser(new Tokenizer(new MemoryStream(Encoding.UTF8.GetBytes(tableSql)))).Parse();
		}
		catch
		{
			Console.Error.WriteLine($"Failed to parse SQL:");
			Console.Error.WriteLine(tableSql);
			throw;
		}
	}

	/// <summary>
	/// Parses the tokens to extract a Table object.
	/// </summary>
	public Table Parse()
	{
		Expect(TokenType.Identifier, "CREATE");
		_currentToken = _tokenizer.GetNextToken();

		Expect(TokenType.Identifier, "TABLE");
		_currentToken = _tokenizer.GetNextToken();

		while (_currentToken.Type == TokenType.Identifier
			&& (_currentToken.Value == "IF" || _currentToken.Value == "NOT" || _currentToken.Value == "EXISTS"))
		{
			_currentToken = _tokenizer.GetNextToken();
		}

		// Table name could be optionally quoted
		string tableName = ParseIdentifier();
		var table = new Table { Name = tableName };

		Expect(TokenType.LeftBrace);
		_currentToken = _tokenizer.GetNextToken();

		// Parse columns and constraints
		while (_currentToken.Type != TokenType.RightBrace && _currentToken.Type != TokenType.EOF)
		{
			if (_currentToken.Type == TokenType.Identifier)
			{
				string identifier = _currentToken.Value.ToUpperInvariant();
				if (identifier == "PRIMARY" || identifier == "UNIQUE" || identifier == "KEY" || identifier == "CONSTRAINT" || identifier == "FOREIGN")
				{
					ParseTableConstraint(table);
				}
				else
				{
					ParseColumnDefinition(table);
				}
			}
			else
			{
				throw new Exception($"Unexpected token {_currentToken} while parsing table definition.");
			}

			// After each column or constraint, expect a comma or closing parenthesis
			if (_currentToken.Type == TokenType.Comma)
			{
				_currentToken = _tokenizer.GetNextToken();
			}
		}

		Expect(TokenType.RightBrace);
		_currentToken = _tokenizer.GetNextToken();

		while (_currentToken.Type == TokenType.Identifier)
		{
			var tableOption = _currentToken.Value;

			_currentToken = _tokenizer.GetNextToken();

			while (_currentToken.Type == TokenType.Identifier)
			{
				tableOption += " " + _currentToken.Value;
				_currentToken = _tokenizer.GetNextToken();
			}

			Expect(TokenType.Equals);

			_currentToken = _tokenizer.GetNextToken();

			if (_currentToken.Type != TokenType.Identifier
				&& _currentToken.Type != TokenType.String
				&& _currentToken.Type != TokenType.Number)
				Expect(TokenType.Identifier);

			string value = _currentToken.Value;

			if (_currentToken.Type == TokenType.String)
				value = $"'{value.Replace("'", "\\'").Replace("\\", "\\\\")}'";

			table.Options[tableOption] = value;
			_currentToken = _tokenizer.GetNextToken();
		}

		if (_currentToken.Type != TokenType.Semicolon && _currentToken.Type != TokenType.EOF)
			Expect(TokenType.Semicolon);

		_currentToken = _tokenizer.GetNextToken();

		return table;
	}

	/// <summary>
	/// Parses a column definition and adds it to the table.
	/// </summary>
	private void ParseColumnDefinition(Table table)
	{
		string columnName = ParseIdentifier();

		// Get data type, which might consist of multiple tokens (e.g., ENUM definitions)
		string dataType = ParseDataType();

		var column = new Column
		{
			Name = columnName,
			DataType = dataType
		};

		// Parse column constraints (NULL/NOT NULL, DEFAULT, AUTO_INCREMENT, etc.)
		while (_currentToken.Type == TokenType.Identifier
			|| _currentToken.Type == TokenType.BinaryBlob
			|| _currentToken.Type == TokenType.Null)
		{
			if (_currentToken.Type == TokenType.Null)
			{
				column.IsNullable = true;
				_currentToken = _tokenizer.GetNextToken();
				continue;
			}

			string constraint = _currentToken.Value.ToUpperInvariant();
			switch (constraint)
			{
				case "NOT":
					_currentToken = _tokenizer.GetNextToken();
					if (_currentToken.Type != TokenType.Null)
						Expect(TokenType.Identifier, "NULL");
					column.IsNullable = false;
					_currentToken = _tokenizer.GetNextToken();
					break;
				case "NULL":
					column.IsNullable = true;
					_currentToken = _tokenizer.GetNextToken();
					break;
				case "DEFAULT":
					_currentToken = _tokenizer.GetNextToken();

					bool checkEndBrace = false;
					if (_currentToken.Type == TokenType.LeftBrace)
					{
						checkEndBrace = true;
						_currentToken = _tokenizer.GetNextToken();						
					}

					column.DefaultValue = ParseDefaultValue();

					if (checkEndBrace && _currentToken.Type == TokenType.RightBrace)
						_currentToken = _tokenizer.GetNextToken();
					break;
				case "AUTO_INCREMENT":
					column.IsAutoIncrement = true;
					_currentToken = _tokenizer.GetNextToken();
					break;
				case "PRIMARY":
					// Handle PRIMARY KEY specified per column
					_currentToken = _tokenizer.GetNextToken();
					Expect(TokenType.Identifier, "KEY");
					column.IsPrimaryKey = true;
					_currentToken = _tokenizer.GetNextToken();
					break;
				case "UNIQUE":
					// Handle UNIQUE per column
					_currentToken = _tokenizer.GetNextToken();
					column.Extra = "UNIQUE";
					break;
				case "UNSIGNED":
					// Handle UNIQUE per column
					_currentToken = _tokenizer.GetNextToken();
					column.Unsigned = true;
					break;
				case "COLLATE":
					_currentToken = _tokenizer.GetNextToken();

					if (_currentToken.Type != TokenType.Identifier && _currentToken.Type != TokenType.String)
						Expect(TokenType.String);

					column.Collation = _currentToken.Value;
					_currentToken = _tokenizer.GetNextToken();
					break;
				case "CHARACTER":
					_currentToken = _tokenizer.GetNextToken();
					Expect(TokenType.Identifier, "SET");
					_currentToken = _tokenizer.GetNextToken();

					if (_currentToken.Type != TokenType.Identifier && _currentToken.Type != TokenType.String)
						Expect(TokenType.String);

					column.CharacterSet = _currentToken.Value;
					_currentToken = _tokenizer.GetNextToken();
					break;
				default:
					// Handle other constraints or extras as needed
					// For simplicity, ignore unhandled constraints
					_currentToken = _tokenizer.GetNextToken();
					break;
			}
		}

		table.Columns.Add(column);
	}

	/// <summary>
	/// Parses a table-level constraint (PRIMARY KEY, UNIQUE, FOREIGN KEY, etc.).
	/// </summary>
	private void ParseTableConstraint(Table table)
	{
		string constraintType = _currentToken.Value.ToUpperInvariant();

		if (constraintType == "PRIMARY")
		{
			// PRIMARY KEY (`col1`, `col2`)
			_currentToken = _tokenizer.GetNextToken();
			Expect(TokenType.Identifier, "KEY");
			_currentToken = _tokenizer.GetNextToken();

			var index = new Index
			{
				Type = IndexType.Primary
			};

			index.Columns.AddRange(ParseColumnList());

			table.Indexes.Add(index);
		}
		else if (constraintType == "UNIQUE")
		{
			// UNIQUE KEY `key_name` (`col1`, `col2`)
			_currentToken = _tokenizer.GetNextToken();
			Expect(TokenType.Identifier, "KEY");
			_currentToken = _tokenizer.GetNextToken();

			string keyName = null;
			if (_currentToken.Type == TokenType.Identifier)
			{
				keyName = ParseIdentifier();
			}

			var index = new Index
			{
				Type = IndexType.Unique,
				Name = keyName
			};

			Expect(TokenType.LeftBrace);

			index.Columns.AddRange(ParseColumnList());

			table.Indexes.Add(index);
		}
		else if (constraintType == "KEY")
		{
			// KEY `key_name` (`col1`, `col2`)
			_currentToken = _tokenizer.GetNextToken();
			string keyName = null;
			if (_currentToken.Type == TokenType.Identifier)
			{
				keyName = ParseIdentifier();
			}

			Expect(TokenType.LeftBrace);
			//_currentToken = _tokenizer.GetNextToken();

			var index = new Index
			{
				Type = IndexType.Index,
				Name = keyName
			};

			index.Columns.AddRange(ParseColumnList());

			table.Indexes.Add(index);
		}
		else if (constraintType == "CONSTRAINT")
		{
			// CONSTRAINT `fk_name` FOREIGN KEY (`col1`) REFERENCES `ref_table` (`ref_col1`) ON DELETE CASCADE ON UPDATE RESTRICT
			_currentToken = _tokenizer.GetNextToken();
			string constraintName = ParseIdentifier();

			Expect(TokenType.Identifier, "FOREIGN");
			_currentToken = _tokenizer.GetNextToken();
			Expect(TokenType.Identifier, "KEY");
			_currentToken = _tokenizer.GetNextToken();

			var fk = new ForeignKey
			{
				Name = constraintName
			};

			fk.Columns.AddRange(ParseColumnList());

			Expect(TokenType.Identifier, "REFERENCES");
			_currentToken = _tokenizer.GetNextToken();

			string refTable = ParseIdentifier();
			fk.ReferenceTable = refTable;

			Expect(TokenType.LeftBrace);
			//_currentToken = _tokenizer.GetNextToken();
			fk.ReferenceColumns.AddRange(ParseColumnList());
			//Expect(TokenType.RightBrace);
			//_currentToken = _tokenizer.GetNextToken();

			// Optional ON DELETE and ON UPDATE clauses
			while (_currentToken.Type == TokenType.Identifier)
			{
				string action = _currentToken.Value.ToUpperInvariant();
				if (action == "ON")
				{
					_currentToken = _tokenizer.GetNextToken();
					string eventType = _currentToken.Value.ToUpperInvariant();
					if (eventType == "DELETE")
					{
						_currentToken = _tokenizer.GetNextToken();
						if (_currentToken.Type == TokenType.Identifier)
						{
							fk.OnDelete = _currentToken.Value.ToUpperInvariant();

							if (fk.OnDelete == "NO")
							{
								_currentToken = _tokenizer.GetNextToken();
								fk.OnDelete += " " + _currentToken.Value.ToUpperInvariant();
							}
							_currentToken = _tokenizer.GetNextToken();
						}
					}
					else if (eventType == "UPDATE")
					{
						_currentToken = _tokenizer.GetNextToken();
						if (_currentToken.Type == TokenType.Identifier)
						{
							fk.OnUpdate = _currentToken.Value.ToUpperInvariant();

							if (fk.OnUpdate == "NO")
							{
								_currentToken = _tokenizer.GetNextToken();
								fk.OnUpdate += " " + _currentToken.Value.ToUpperInvariant();
							}
							_currentToken = _tokenizer.GetNextToken();
						}
					}
				}
				else
				{
					break;
				}
			}

			table.ForeignKeys.Add(fk);
		}
		else
		{
			throw new Exception($"Unsupported table constraint type: {constraintType}");
		}

		if (_currentToken.Type == TokenType.Identifier && _currentToken.Value == "USING")
		{
			_currentToken = _tokenizer.GetNextToken();
			_currentToken = _tokenizer.GetNextToken();
		}
	}

	/// <summary>
	/// Parses a list of column names enclosed in parentheses.
	/// </summary>
	private List<string> ParseColumnList()
	{
		var columns = new List<string>();
		Expect(TokenType.LeftBrace);
		_currentToken = _tokenizer.GetNextToken();

		while (_currentToken.Type != TokenType.RightBrace && _currentToken.Type != TokenType.EOF)
		{
			string column = ParseIdentifier();
			columns.Add(column);

			if (_currentToken.Type == TokenType.Comma)
			{
				_currentToken = _tokenizer.GetNextToken();
			}
			else
			{
				break;
			}
		}

		Expect(TokenType.RightBrace);
		_currentToken = _tokenizer.GetNextToken();

		return columns;
	}

	/// <summary>
	/// Parses the data type of a column, handling complex types like ENUM.
	/// </summary>
	private string ParseDataType()
	{
		StringBuilder sb = new StringBuilder();

		// Data type can be multiple tokens, e.g., ENUM('a','b'), VARCHAR(255), etc.
		if (_currentToken.Type != TokenType.Identifier && _currentToken.Type != TokenType.BinaryBlob)
			throw new Exception($"Expected data type identifier, found {_currentToken}");

		sb.Append(_currentToken.Value);
		_currentToken = _tokenizer.GetNextToken();

		// Handle type parameters, e.g., VARCHAR(255), DECIMAL(10,2), ENUM('a','b')
		if (_currentToken.Type == TokenType.LeftBrace)
		{
			sb.Append("(");
			_currentToken = _tokenizer.GetNextToken();

			int parenCount = 1;
			while (parenCount > 0 && _currentToken.Type != TokenType.EOF)
			{
				if (_currentToken.Type == TokenType.RightBrace)
				{
					parenCount--;
					sb.Append(")");
					_currentToken = _tokenizer.GetNextToken();
					break;
				}
				else if (_currentToken.Type == TokenType.LeftBrace)
				{
					parenCount++;
					sb.Append("(");
					_currentToken = _tokenizer.GetNextToken();
				}
				else if (_currentToken.Type == TokenType.String)
				{
					sb.Append($"'{_currentToken.Value}'");
					_currentToken = _tokenizer.GetNextToken();
				}
				else
				{
					sb.Append(_currentToken.Value);
					if (_currentToken.Type == TokenType.Comma)
						sb.Append(",");
					//	sb.Append(", ");
					//else
					//	sb.Append(" ");
					_currentToken = _tokenizer.GetNextToken();
				}
			}
		}

		return sb.ToString();
	}

	/// <summary>
	/// Parses an identifier, handling quoted identifiers.
	/// </summary>
	private string ParseIdentifier()
	{
		if (_currentToken.Type == TokenType.Identifier)
		{
			string identifier = _currentToken.Value;
			_currentToken = _tokenizer.GetNextToken();
			return identifier;
		}
		else
		{
			throw new Exception($"Expected identifier, found {_currentToken}");
		}
	}

	/// <summary>
	/// Parses a default value for a column.
	/// </summary>
	private string ParseDefaultValue()
	{
		switch (_currentToken.Type)
		{
			case TokenType.String:
				string escaped = _currentToken.Value.Replace("'", "''");
				string stringValue = $"'{escaped}'";
				_currentToken = _tokenizer.GetNextToken();
				return stringValue;
			case TokenType.Number:
				string number = _currentToken.Value;
				_currentToken = _tokenizer.GetNextToken();
				return number;
			case TokenType.Null:
				_currentToken = _tokenizer.GetNextToken();
				return "NULL";
			case TokenType.Identifier:
				string identifier = _currentToken.Value.ToUpperInvariant();
				if (identifier == "CURRENT_TIMESTAMP")
				{
					_currentToken = _tokenizer.GetNextToken();
					return "CURRENT_TIMESTAMP";
				}
				else
				{
					// Handle other expressions if necessary
					_currentToken = _tokenizer.GetNextToken();
					return identifier;
				}
			default:
				throw new Exception($"Unexpected token type {_currentToken.Type} for default value.");
		}
	}

	/// <summary>
	/// Expects the current token to be of a specific type and optionally a specific value.
	/// Throws an exception if not matched.
	/// </summary>
	private void Expect(TokenType type, string value = null)
	{
		if (_currentToken.Type != type)
			throw new Exception($"Expected token type {type}, found {_currentToken.Type}.");

		if (value != null && !string.Equals(_currentToken.Value, value, StringComparison.OrdinalIgnoreCase))
			throw new Exception($"Expected token value '{value}', found '{_currentToken.Value}'.");
	}
}