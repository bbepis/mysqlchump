# mysqlchump
mysqldump alternative

Alternatively named `mysqlC#ump`

#### Features:
- Can use a custom select query to drop columns or apply transformations to outputted data

#### Limitations:
- Currently doesn't support all data types
- Can only do a single table at a time

#### Usage:
```
mysqlchump --table <table> --connectionString <connection string> [--select <select sql statement>] [--append] <output file>
```
