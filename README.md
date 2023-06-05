# mysqlchump
mysqldump alternative

Alternatively named `mysqlC#ump`

#### Usage:
```
mysqlchump [command] [options]

Commands:
  export <output location>  Exports data from a database
  import <input file>       Imports data to a database
```

`export`
```
Description:
  Exports data from a database

Usage:
  mysqlchump export [<output location>] [options]

Arguments:
  <output location>  Specify either a file or a folder to output to. '-' for stdout, otherwise defaults to creating
                     files in the current directory

Options:
  -t, --table <table>                       The table to be dumped. Can be specified multiple times, or passed '*' to
                                            dump all tables.
  --tables <tables>                         A comma-separated list of tables to dump.
  --connection-string <connection-string>   A connection string to use to connect to the database. Not required if -s
                                            -d -u -p have been specified
  -s, --server <server>                     The server to connect to. Defaults to localhost. [default: localhost]
  -o, --port <port>                         The port of the server to connect to. Defaults to 3306. [default: 3306]
  -d, --database <database>                 The database to use when dumping.
  -u, --username <username>                 The username to connect with.
  -p, --password <password>                 The password to connect with.
  -f, --output-format <csv|mysql|postgres>  The output format to create when dumping. [default: mysql]
  -q, --select <select>                     The select query to use when filtering rows/columns. If not specified, will 
                                            dump the entire table.
                                            Table being examined is specified with "{table}". [default: SELECT * FROM
                                            `{table}`]
  --no-creation                             Don't output table creation statements.
  --truncate                                Prepend data insertions with a TRUNCATE command.
  --append                                  If specified, will append to the specified file instead of overwriting.
  -?, -h, --help                            Show help and usage information
```

`import` (experimental, only for regular mysqldump imports)
```
Description:
  Imports data to a database

Usage:
  mysqlchump import <input file> [options]

Arguments:
  <input file>  Specify a file to read from. Otherwise - for stdin

Options:
  --connection-string <connection-string>  A connection string to use to connect to the database. Not required if -s -d 
                                           -u -p have been specified
  -s, --server <server>                    The server to connect to. Defaults to localhost. [default: localhost]
  -o, --port <port>                        The port of the server to connect to. Defaults to 3306. [default: 3306]
  -d, --database <database>                The database to use when dumping.
  -u, --username <username>                The username to connect with.
  -p, --password <password>                The password to connect with.
  -f, --input-format <mysqlForceBatch>     The input format to use when importing. [default: mysqlForceBatch]
  -?, -h, --help                           Show help and usage information
```
