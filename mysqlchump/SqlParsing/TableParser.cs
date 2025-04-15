using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace mysqlchump.SqlParsing;

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
	public bool IsNullable { get; set; }
	public bool IsPrimaryKey { get; set; }
	public string DefaultValue { get; set; }
	public bool IsAutoIncrement { get; set; }
	public bool Unsigned { get; set; }
	public string CharacterSet { get; set; }
	public string Collation { get; set; }
	public string Extra { get; set; } // Additional column options

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
	public class IndexColumn
	{
		public string ColumnName { get; set; }
		public int? PrefixLength { get; set; }

		public IndexColumn() { }

		public IndexColumn(string name, int? prefixLength = null)
		{
			ColumnName = name;
			PrefixLength = prefixLength;
		}

		public override string ToString()
		{
			return $"`{ColumnName}`{(PrefixLength.HasValue ? $"({PrefixLength})" : "")}";
		}

		public static implicit operator IndexColumn(string columnName) => new IndexColumn(columnName);
	}

	public string Name { get; set; } // e.g., PRIMARY, UNIQUE key name
	public IndexType Type { get; set; }
	public List<IndexColumn> Columns { get; } = new List<IndexColumn>();

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
		sb.Append(string.Join(", ", Columns.Select(x => x.ToString())));
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
	Regular,
	Primary,
	Unique,
	Fulltext
}

public class CreateTableParser
{
	private readonly SqlTokenizer _tokenizer;
	private SqlTokenType _currentToken;

	public CreateTableParser(SqlTokenizer tokenizer, bool shouldRead = true)
	{
		_tokenizer = tokenizer;

		_currentToken = shouldRead ? _tokenizer.Read() : _tokenizer.TokenType;
	}

	public static Table Parse(string tableSql)
	{
		try
		{
			return new CreateTableParser(new SqlTokenizer(new MemoryStream(Utility.NoBomUtf8.GetBytes(tableSql)))).Parse();
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
		if (_currentToken != SqlTokenType.Identifier || !_tokenizer.ValueString.Span.Equals("TABLE", StringComparison.OrdinalIgnoreCase))
		{
			Expect(SqlTokenType.Identifier, "CREATE");
			_currentToken = _tokenizer.Read();

			Expect(SqlTokenType.Identifier, "TABLE");
		}
		_currentToken = _tokenizer.Read();

		while (_tokenizer.TokenType == SqlTokenType.Identifier
			&& (
				_tokenizer.ValueString.Span.Equals("IF", StringComparison.OrdinalIgnoreCase)
				|| _tokenizer.ValueString.Span.Equals("NOT", StringComparison.OrdinalIgnoreCase)
				|| _tokenizer.ValueString.Span.Equals("EXISTS", StringComparison.OrdinalIgnoreCase)
				))
		{
			_currentToken = _tokenizer.Read();
		}

		// Table name could be optionally quoted
		string tableName = _tokenizer.ValueString.ToString();
		var table = new Table { Name = tableName };

		_currentToken = _tokenizer.Read();
		Expect(SqlTokenType.LeftBrace);
		_currentToken = _tokenizer.Read();

		// Parse columns and constraints
		while (_currentToken != SqlTokenType.RightBrace && _currentToken != SqlTokenType.EOF)
		{
			if (_currentToken == SqlTokenType.Identifier)
			{
				string identifier = _tokenizer.ValueString.ToString().ToUpperInvariant();
				if (!_tokenizer.IdentifierWasEscaped
					&& (identifier == "PRIMARY"
					|| identifier == "UNIQUE"
					|| identifier == "KEY"
					|| identifier == "INDEX"
					|| identifier == "CONSTRAINT"
					|| identifier == "FOREIGN"
					|| identifier == "FULLTEXT"))
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
			if (_currentToken == SqlTokenType.Comma)
			{
				_currentToken = _tokenizer.Read();
			}
		}

		Expect(SqlTokenType.RightBrace);
		_currentToken = _tokenizer.Read();

		while (_currentToken == SqlTokenType.Identifier)
		{
			var tableOption = _tokenizer.ValueString.ToString();

			_currentToken = _tokenizer.Read();

			while (_currentToken == SqlTokenType.Identifier)
			{
				tableOption += " " + _tokenizer.ValueString.ToString();
				_currentToken = _tokenizer.Read();
			}

			Expect(SqlTokenType.Equals);

			_currentToken = _tokenizer.Read();

			if (_currentToken != SqlTokenType.Identifier
				&& _currentToken != SqlTokenType.String
				&& _currentToken != SqlTokenType.Integer
				&& _currentToken != SqlTokenType.Double)
				Expect(SqlTokenType.Identifier);

			string value = _currentToken switch
			{
				SqlTokenType.Identifier => _tokenizer.ValueString.ToString(),
				SqlTokenType.String => _tokenizer.ValueString.ToString(),
				SqlTokenType.Integer => _tokenizer.ValueLong.ToString(),
				SqlTokenType.Double => _tokenizer.ValueDouble.ToString(),
				_ => throw new ArgumentOutOfRangeException(nameof(_currentToken)),
			};

			if (_currentToken == SqlTokenType.String)
				value = $"'{value.Replace("'", "\\'").Replace("\\", "\\\\")}'";

			table.Options[tableOption] = value;
			_currentToken = _tokenizer.Read();
		}

		if (_currentToken != SqlTokenType.Semicolon && _currentToken != SqlTokenType.EOF)
			Expect(SqlTokenType.Semicolon);

		_currentToken = _tokenizer.Read();

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
			DataType = dataType,
			IsNullable = true
		};

		// Parse column constraints (NULL/NOT NULL, DEFAULT, AUTO_INCREMENT, etc.)
		while (_currentToken == SqlTokenType.Identifier
			|| _currentToken == SqlTokenType.BinaryBlob
			|| _currentToken == SqlTokenType.Null)
		{
			if (_currentToken == SqlTokenType.Null)
			{
				// redundant
				column.IsNullable = true;
				_currentToken = _tokenizer.Read();
				continue;
			}

			string constraint = _tokenizer.ValueString.ToString().ToUpperInvariant();
			switch (constraint)
			{
				case "NOT":
					_currentToken = _tokenizer.Read();
					if (_currentToken != SqlTokenType.Null)
						Expect(SqlTokenType.Identifier, "NULL");
					column.IsNullable = false;
					_currentToken = _tokenizer.Read();
					break;
				case "NULL":
					column.IsNullable = true;
					_currentToken = _tokenizer.Read();
					break;
				case "DEFAULT":
					_currentToken = _tokenizer.Read();

					bool checkEndBrace = false;
					if (_currentToken == SqlTokenType.LeftBrace)
					{
						checkEndBrace = true;
						_currentToken = _tokenizer.Read();						
					}

					column.DefaultValue = ParseDefaultValue();

					if (checkEndBrace && _currentToken == SqlTokenType.RightBrace)
						_currentToken = _tokenizer.Read();
					break;
				case "AUTO_INCREMENT":
					column.IsAutoIncrement = true;
					_currentToken = _tokenizer.Read();
					break;
				case "PRIMARY":
					// Handle PRIMARY KEY specified per column
					_currentToken = _tokenizer.Read();
					Expect(SqlTokenType.Identifier, "KEY");
					column.IsPrimaryKey = true;
					_currentToken = _tokenizer.Read();
					break;
				case "UNIQUE":
					// Handle UNIQUE per column
					_currentToken = _tokenizer.Read();
					column.Extra = "UNIQUE";
					break;
				case "UNSIGNED":
					// Handle UNIQUE per column
					_currentToken = _tokenizer.Read();
					column.Unsigned = true;
					break;
				case "COLLATE":
					_currentToken = _tokenizer.Read();

					if (_currentToken != SqlTokenType.Identifier && _currentToken != SqlTokenType.String)
						Expect(SqlTokenType.String);

					column.Collation = _tokenizer.ValueString.ToString();
					_currentToken = _tokenizer.Read();
					break;
				case "CHARACTER":
					_currentToken = _tokenizer.Read();
					Expect(SqlTokenType.Identifier, "SET");
					_currentToken = _tokenizer.Read();

					if (_currentToken != SqlTokenType.Identifier && _currentToken != SqlTokenType.String)
						Expect(SqlTokenType.String);

					column.CharacterSet = _tokenizer.ValueString.ToString();
					_currentToken = _tokenizer.Read();
					break;
				default:
					// Handle other constraints or extras as needed
					// For simplicity, ignore unhandled constraints
					_currentToken = _tokenizer.Read();
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
		string constraintType = _tokenizer.ValueString.ToString().ToUpperInvariant();

		if (constraintType == "PRIMARY")
		{
			// PRIMARY KEY (`col1`, `col2`)
			_currentToken = _tokenizer.Read();
			Expect(SqlTokenType.Identifier, "KEY");
			_currentToken = _tokenizer.Read();

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
			_currentToken = _tokenizer.Read();
			Expect(SqlTokenType.Identifier, "KEY");
			_currentToken = _tokenizer.Read();

			string keyName = null;
			if (_currentToken == SqlTokenType.Identifier)
			{
				keyName = ParseIdentifier();
			}

			var index = new Index
			{
				Type = IndexType.Unique,
				Name = keyName
			};

			Expect(SqlTokenType.LeftBrace);

			index.Columns.AddRange(ParseColumnList());

			table.Indexes.Add(index);
		}
		else if (constraintType == "FULLTEXT")
		{
			// FULLTEXT KEY `key_name` (`col1`)
			_currentToken = _tokenizer.Read();
			Expect(SqlTokenType.Identifier, "KEY");
			_currentToken = _tokenizer.Read();

			string keyName = null;
			if (_currentToken == SqlTokenType.Identifier)
			{
				keyName = ParseIdentifier();
			}

			var index = new Index
			{
				Type = IndexType.Fulltext,
				Name = keyName
			};

			Expect(SqlTokenType.LeftBrace);

			index.Columns.AddRange(ParseColumnList());

			table.Indexes.Add(index);
		}
		else if (constraintType == "KEY" || constraintType == "INDEX")
		{
			// KEY `key_name` (`col1`, `col2`)
			_currentToken = _tokenizer.Read();
			string keyName = null;
			if (_currentToken == SqlTokenType.Identifier)
			{
				keyName = ParseIdentifier();
			}

			Expect(SqlTokenType.LeftBrace);
			//_currentToken = _tokenizer.Read();

			var index = new Index
			{
				Type = IndexType.Regular,
				Name = keyName
			};

			index.Columns.AddRange(ParseColumnList());

			table.Indexes.Add(index);
		}
		else if (constraintType == "CONSTRAINT")
		{
			// CONSTRAINT `fk_name` FOREIGN KEY (`col1`) REFERENCES `ref_table` (`ref_col1`) ON DELETE CASCADE ON UPDATE RESTRICT
			_currentToken = _tokenizer.Read();
			string constraintName = ParseIdentifier();

			Expect(SqlTokenType.Identifier, "FOREIGN");
			_currentToken = _tokenizer.Read();
			Expect(SqlTokenType.Identifier, "KEY");
			_currentToken = _tokenizer.Read();

			var fk = new ForeignKey
			{
				Name = constraintName
			};

			fk.Columns.AddRange(ParseColumnList().Select(x => x.ColumnName));

			Expect(SqlTokenType.Identifier, "REFERENCES");
			_currentToken = _tokenizer.Read();

			string refTable = ParseIdentifier();
			fk.ReferenceTable = refTable;

			Expect(SqlTokenType.LeftBrace);
			//_currentToken = _tokenizer.Read();
			fk.ReferenceColumns.AddRange(ParseColumnList().Select(x => x.ColumnName));
			//Expect(TokenType.RightBrace);
			//_currentToken = _tokenizer.Read();

			// Optional ON DELETE and ON UPDATE clauses
			while (_currentToken == SqlTokenType.Identifier)
			{
				string action = _tokenizer.ValueString.ToString().ToUpperInvariant();
				if (action == "ON")
				{
					_currentToken = _tokenizer.Read();
					string eventType = _tokenizer.ValueString.ToString().ToUpperInvariant();
					if (eventType == "DELETE")
					{
						_currentToken = _tokenizer.Read();
						if (_currentToken == SqlTokenType.Identifier)
						{
							fk.OnDelete = _tokenizer.ValueString.ToString().ToUpperInvariant();

							if (fk.OnDelete == "NO")
							{
								_currentToken = _tokenizer.Read();
								fk.OnDelete += " " + _tokenizer.ValueString.ToString().ToUpperInvariant();
							}
							_currentToken = _tokenizer.Read();
						}
					}
					else if (eventType == "UPDATE")
					{
						_currentToken = _tokenizer.Read();
						if (_currentToken == SqlTokenType.Identifier)
						{
							fk.OnUpdate = _tokenizer.ValueString.ToString().ToUpperInvariant();

							if (fk.OnUpdate == "NO")
							{
								_currentToken = _tokenizer.Read();
								fk.OnUpdate += " " + _tokenizer.ValueString.ToString().ToUpperInvariant();
							}
							_currentToken = _tokenizer.Read();
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

		if (_currentToken == SqlTokenType.Identifier && _tokenizer.ValueString.ToString() == "USING")
		{
			_currentToken = _tokenizer.Read();
			_currentToken = _tokenizer.Read();
		}
	}

	/// <summary>
	/// Parses a list of column names enclosed in parentheses.
	/// </summary>
	private List<Index.IndexColumn> ParseColumnList()
	{
		var columns = new List<Index.IndexColumn>();
		Expect(SqlTokenType.LeftBrace);
		_currentToken = _tokenizer.Read();

		while (_currentToken != SqlTokenType.RightBrace && _currentToken != SqlTokenType.EOF)
		{
			Index.IndexColumn column = ParseIdentifier();
			columns.Add(column);

			// check for prefix length
			if (_currentToken == SqlTokenType.LeftBrace)
			{
				_currentToken = _tokenizer.Read();
				Expect(SqlTokenType.Integer);

				column.PrefixLength = (int)_tokenizer.ValueLong;

				_currentToken = _tokenizer.Read();
				Expect(SqlTokenType.RightBrace);
				_currentToken = _tokenizer.Read();
			}

			if (_currentToken == SqlTokenType.Comma)
			{
				_currentToken = _tokenizer.Read();
			}
			else
			{
				break;
			}
		}

		Expect(SqlTokenType.RightBrace);
		_currentToken = _tokenizer.Read();

		return columns;
	}

	/// <summary>
	/// Parses the data type of a column, handling complex types like ENUM.
	/// </summary>
	private string ParseDataType()
	{
		StringBuilder sb = new StringBuilder();

		// Data type can be multiple tokens, e.g., ENUM('a','b'), VARCHAR(255), etc.
		if (_currentToken != SqlTokenType.Identifier && _currentToken != SqlTokenType.BinaryBlob)
			throw new Exception($"Expected data type identifier, found {_currentToken}");

		sb.Append(_tokenizer.ValueString.ToString());
		_currentToken = _tokenizer.Read();

		// Handle type parameters, e.g., VARCHAR(255), DECIMAL(10,2), ENUM('a','b')
		if (_currentToken == SqlTokenType.LeftBrace)
		{
			sb.Append("(");
			_currentToken = _tokenizer.Read();

			int parenCount = 1;
			while (parenCount > 0 && _currentToken != SqlTokenType.EOF)
			{
				if (_currentToken == SqlTokenType.RightBrace)
				{
					parenCount--;
					sb.Append(")");
					_currentToken = _tokenizer.Read();
					break;
				}
				else if (_currentToken == SqlTokenType.LeftBrace)
				{
					parenCount++;
					sb.Append("(");
					_currentToken = _tokenizer.Read();
				}
				else if (_currentToken == SqlTokenType.String)
				{
					sb.Append($"'{_tokenizer.ValueString.ToString()}'");
					_currentToken = _tokenizer.Read();
				}
				else if (_currentToken == SqlTokenType.Integer)
				{
					sb.Append(_tokenizer.ValueLong.ToString());
					_currentToken = _tokenizer.Read();
				}
				else if (_currentToken == SqlTokenType.Double)
				{
					sb.Append(_tokenizer.ValueDouble.ToString());
					_currentToken = _tokenizer.Read();
				}
				else
				{
					sb.Append(_tokenizer.ValueString.ToString());
					if (_currentToken == SqlTokenType.Comma)
						sb.Append(",");
					//	sb.Append(", ");
					//else
					//	sb.Append(" ");
					_currentToken = _tokenizer.Read();
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
		if (_currentToken == SqlTokenType.Identifier)
		{
			string identifier = _tokenizer.ValueString.ToString();
			_currentToken = _tokenizer.Read();
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
		switch (_currentToken)
		{
			case SqlTokenType.String:
				string escaped = _tokenizer.ValueString.ToString().Replace("'", "''");
				string stringValue = $"'{escaped}'";
				_currentToken = _tokenizer.Read();
				return stringValue;
			case SqlTokenType.Integer:
				string number = _tokenizer.ValueLong.ToString();
				_currentToken = _tokenizer.Read();
				return number;
			case SqlTokenType.Double:
				string numberDouble = _tokenizer.ValueDouble.ToString();
				_currentToken = _tokenizer.Read();
				return numberDouble;
			case SqlTokenType.Null:
				_currentToken = _tokenizer.Read();
				return "NULL";
			case SqlTokenType.Identifier:
				string identifier = _tokenizer.ValueString.ToString().ToUpperInvariant();
				if (identifier == "CURRENT_TIMESTAMP")
				{
					_currentToken = _tokenizer.Read();
					return "CURRENT_TIMESTAMP";
				}
				else
				{
					// Handle other expressions if necessary
					_currentToken = _tokenizer.Read();
					return identifier;
				}
			default:
				throw new Exception($"Unexpected token type {_currentToken} for default value.");
		}
	}

	/// <summary>
	/// Expects the current token to be of a specific type and optionally a specific value.
	/// Throws an exception if not matched.
	/// </summary>
	private void Expect(SqlTokenType type, string value = null)
	{
		if (_currentToken != type)
			throw new Exception($"Expected token type {type}, found {_currentToken}.");

		if (value != null && !string.Equals(_tokenizer.ValueString.ToString(), value, StringComparison.OrdinalIgnoreCase))
			throw new Exception($"Expected token value '{value}', found '{_tokenizer.ValueString.ToString()}'.");
	}
}