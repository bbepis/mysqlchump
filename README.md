# mysqlchump
mysqldump rewrite, focused on performance

Uses connection multiplexing to bypass MySQL bottlenecks related to single-threading (in cases where there the disk is not the bottleneck).

I've personally seen export/import speed increases of up to 16x, but your mileage may vary.

### Exporting

`mysqlchump` supports exporting to a multitude of formats:
- `mysql` for .sql statements. Combines 1000 rows into each statement, for efficient reimporting.
- `postgres`: Experimental converter from mysql data to postgres data, for importing to a postgres database. See limitations detailed in PostgresDumper.cs
- `csv` to .csv files. One table per file. Note that this will provide *valid* csv files, as opposed to existing mysql tools (`INTO OUTFILE file.csv`)
- `json` to mysqlchump-specific .json files. Most efficient format

All exports are multithreaded, i.e. will buffer writes while waiting for additional data to return from the database, such that there's minimal downtime.

Combined with better locking policies, exporting with this tool is significantly faster than mysqldump.

```
Usage:
  mysqlchump export [<output location>] [options]

Arguments:
  <output location>  Specify either a file or a folder to output to. '-' for stdout, otherwise defaults to creating
                     files in the current directory

Options:
  -t, --table <table>                            The table to be dumped. Can be specified multiple times, or passed '*'
                                                 to dump all tables.
  --tables <tables>                              A comma-separated list of tables to dump.
  --connection-string <connection-string>        A connection string to use to connect to the database. Not required if
                                                 -s -d -u -p have been specified
  -s, --server <server>                          The server to connect to. [default: localhost]
  -o, --port <port>                              The port of the server to connect to. [default: 3306]
  -d, --database <database>                      The database to use when dumping.
  -u, --username <username>                      The username to connect with.
  -p, --password <password>                      The password to connect with.
  -f, --output-format <csv|json|mysql|postgres>  The output format to create when dumping. [default: mysql]
  -q, --select <select>                          The select query to use when filtering rows/columns. If not specified,
                                                 will dump the entire table.
                                                 Table being examined is specified with "{table}". [default: SELECT *
                                                 FROM `{table}`]
  --no-creation                                  Don't output table creation statements.
  --truncate                                     Prepend data insertions with a TRUNCATE command.
  --append                                       If specified, will append to the specified file instead of overwriting.
  -?, -h, --help                                 Show help and usage information
```

### Importing

As mentioned before imports are multiplexed over different connections, instead of being fully synchronous. This helps in many ways:
- For cases where the CPU is the bottleneck (each connection is handled by only a single thread, meaning that each query runs on a single thread), this performs significantly faster as it allows much more writes to the disk than normal
- For cases where the disk is the bottleneck, it still helps as it means that inserts are queued instantaneously instead of waiting for network latency for every single chunk of inserts

Available import formats:
- `mysqlForceBatch`: .sql files produced by mysqldump that insert one row per query. They're normally abhorrent for insert performance (roughly 100's of rows / second), so this import format handler will rewrite them into much more efficient multi-insert statements. This will **not** work with any .sql file structured otherwise.
- `mysqlRaw`: Just runs the raw .sql files as-is. Useful for when you don't have access to a mysql CLI interpreter
- `csv`: .csv files. Requires schemas to already exist
- `json`: .json files produced by mysqlchump, will create tables/databases for you if needed

```
Usage:
  mysqlchump import <input file> [options]

Arguments:
  <input file>  Specify a file to read from. Otherwise - for stdin

Options:
  --connection-string <connection-string>                 A connection string to use to connect to the database. Not
                                                          required if -s -d -u -p have been specified
  -s, --server <server>                                   The server to connect to. [default: localhost]
  -o, --port <port>                                       The port of the server to connect to. [default: 3306]
  -d, --database <database>                               The database to use when dumping.
  -u, --username <username>                               The username to connect with.
  -p, --password <password>                               The password to connect with.
  -f, --input-format <csv|json|mysqlForceBatch|mysqlRaw>  The input format to use when importing. [default:
                                                          mysqlForceBatch]
  -t, --table <table>                                     The table to import to. Only relevant for CSV data
  --insert-ignore                                         Changes INSERT to INSERT IGNORE. Useful for loading into
                                                          existing datasets, but can be slower
  --csv-columns <csv-columns>                             A comma-separated list of columns that the CSV corresponds
                                                          to. Ignored if --csv-use-headers is specified
  --csv-use-headers                                       Use the first row in the CSV as header data to determine
                                                          column names.
  --csv-fix-mysql                                         Enables a pre-processor to fix invalid CSV files generated by
                                                          MySQL. Note that enabling this will break functional CSV
                                                          files
  --no-creation                                           (JSON only) Don't run CREATE TABLE statement.
  -?, -h, --help                                          Show help and usage information                        Show help and usage information
```
