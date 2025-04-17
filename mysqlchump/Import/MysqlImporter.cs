using System;
using System.IO;
using System.IO.Pipelines;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using mysqlchump.SqlParsing;

namespace mysqlchump.Import;

public class MysqlImporter : BaseImporter
{
	protected SqlTokenizer SqlTokenizer { get; }
	private bool ImmediateCreateTable { get; set; }

	public MysqlImporter(ImportOptions options, Stream dataStream) : base(options, dataStream)
	{
		SqlTokenizer = new SqlTokenizer(dataStream);
	}

	private void TrySkipOverStoredProcedure()
	{
		if (AssertIdentifier("DELIMITER", false))
		{
			// assuming we're about to create a procedure or trigger until the next DELIMITER appears

			while (true)
			{
				var token = SqlTokenizer.Read();

				if (token == SqlTokenType.EOF || AssertIdentifier("DELIMITER", false))
					break;
			}
		}
	}

	protected override async Task<(bool foundAnotherTable, string createTableSql, ulong? approxRows)> ReadToNextTable()
	{
		if (ImmediateCreateTable)
		{
			ImmediateCreateTable = false;
			goto parseTable;
		}

		var token = SqlTokenizer.Read();

		while (true)
		{
			if (token == SqlTokenType.EOF)
				return (false, null, null);

			TrySkipOverStoredProcedure();
			token = SqlTokenizer.TokenType;

			if (AssertIdentifier("CREATE", false))
			{
				token = SqlTokenizer.Read();

				if (AssertIdentifier("TABLE", false))
				{
					goto parseTable;
				}
			}

			token = SqlTokenizer.Read();
		}

	parseTable:
		var table = new CreateTableParser(SqlTokenizer, false).Parse().ToCreateTableSql();

		return (true, table, null);
	}

	private bool AssertToken(SqlTokenType tokenType, bool throwOnFailure = true)
	{
		if (SqlTokenizer.TokenType != tokenType)
		{
			if (throwOnFailure)
				throw new Exception($"Token assertion failed. Expected {tokenType} and got {SqlTokenizer.TokenType}");

			return false;
		}

		return true;
	}

	private bool AssertIdentifier(string identifier, bool throwOnFailure = true)
	{
		if (SqlTokenizer.TokenType != SqlTokenType.Identifier
			|| !SqlTokenizer.ValueString.Span.Equals(identifier, StringComparison.OrdinalIgnoreCase))
		{
			if (throwOnFailure)
				throw new Exception($"Identifier assertion failed. Expected '{identifier}' and got {SqlTokenizer.TokenType} '{SqlTokenizer.ValueString}'");

			return false;
		}

		return true;
	}

	protected StringBuilder queryBuilder = new StringBuilder();
	protected override (int rows, bool canContinue, string sqlCommand) ReadDataSql(string tableName, ColumnInfo[] columns)
	{
		const int insertLimit = 8000;
		int count = 0;

		queryBuilder.Clear();

		string[] currentColumnList = columns.Select(x => x.name).ToArray();
		string[] testColumnList = new string[128];

		queryBuilder.Append($"INSERT{(ImportOptions.InsertIgnore ? " IGNORE" : "")} INTO `{tableName}` (`{string.Join("`,`", currentColumnList)}`) VALUES ");

		bool needsComma = false;
		bool canContinue = true;

		var token = SqlTokenizer.Read();
		while (true)
		{
			if (token == SqlTokenType.EOF)
			{
				canContinue = false;
				break;
			}

			TrySkipOverStoredProcedure();
			token = SqlTokenizer.TokenType;

			if (AssertIdentifier("INSERT", false))
			{
				token = SqlTokenizer.Read();
				
				if (AssertIdentifier("IGNORE", false))
					token = SqlTokenizer.Read();

				AssertIdentifier("INTO");

				token = SqlTokenizer.Read();
				if (!SqlTokenizer.ValueString.Span.Equals(tableName, StringComparison.Ordinal))
					throw new Exception($"Table name incorrect, was expecting `{tableName}` but got `{SqlTokenizer.ValueString}`");

				token = SqlTokenizer.Read();

				if (token == SqlTokenType.LeftBrace)
				{
					int testColumnCount = 0;
					while (true)
					{
						token = SqlTokenizer.Read();
						
						if (token == SqlTokenType.RightBrace)
							break;

						if (token == SqlTokenType.Identifier)
							testColumnList[testColumnCount++] = SqlTokenizer.ValueString.ToString();

						token = SqlTokenizer.Read();

						if (token == SqlTokenType.RightBrace)
							break;
					}

					if (testColumnCount != currentColumnList.Length || !testColumnList.AsSpan(0, testColumnCount).SequenceEqual(currentColumnList))
						throw new NotImplementedException("Need to handle custom column specification");

					token = SqlTokenizer.Read();
				}

				AssertIdentifier("VALUES");

				if (needsComma)
					queryBuilder.Append(',');

				while (true)
				{
					token = SqlTokenizer.Read();

					switch (token)
					{
						case SqlTokenType.Comma:
							queryBuilder.Append(',');
							break;
						case SqlTokenType.LeftBrace:
							queryBuilder.Append('(');
							break;
						case SqlTokenType.RightBrace:
							queryBuilder.Append(')');
							count++;
							break;
						case SqlTokenType.Equals:
							queryBuilder.Append('=');
							break;
						case SqlTokenType.String:
							queryBuilder.Append('\'');
							queryBuilder.Append(SqlTokenizer.ValueString.Span.ToString().Replace("\\", "\\\\").Replace("'", "''"));
							queryBuilder.Append('\'');
							break;
						case SqlTokenType.Integer:
							queryBuilder.Append(SqlTokenizer.ValueLong);
							break;
						case SqlTokenType.Double:
							queryBuilder.Append(SqlTokenizer.ValueDouble);
							break;
						case SqlTokenType.Null:
							queryBuilder.Append("NULL");
							break;
						case SqlTokenType.BinaryBlob:
							throw new NotImplementedException("Binaryblob");
						case SqlTokenType.Identifier:
							throw new Exception($"Unexpected identifier '{SqlTokenizer.ValueString}'");
						case SqlTokenType.EOF:
							throw new Exception($"Unexpected end of file");
					}

					if (token == SqlTokenType.Semicolon)
						break;
				}

				needsComma = true;
			}

			if (AssertIdentifier("CREATE", false))
			{
				token = SqlTokenizer.Read();

				if (AssertIdentifier("TABLE", false))
				{
					ImmediateCreateTable = true;
					canContinue = false;
					break;
				}
			}

			if (count >= insertLimit)
			{
				break;
			}

			token = SqlTokenizer.Read();
		}

		if (count > 0)
		{
			queryBuilder.Append(";");
			return (count, canContinue, queryBuilder.ToString());
		}

		return (0, false, null);
	}


	protected override (int rows, bool canContinue) ReadDataCsv(PipeWriter pipeWriter, string tableName, ColumnInfo[] columns) => throw new NotImplementedException();
	public override void Dispose() => throw new NotImplementedException();
}