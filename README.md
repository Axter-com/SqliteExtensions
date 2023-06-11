# SqliteExtensions
CSharp SQLite extensions which works with System.Data.SQLite and Microsoft.Data.Sqlite package

## Added functions
- API's using common SQL terms
  - Insert
  - DropTable
  - DeleteFrom
  - Query
  - Execute
- A single CreateConnection call which performs the following:
  - Creates DB file if it doesn't exist.
  - Create tables and schema if DB file newly created.
  - Formats connection string if input string is just the file path.
  - Opens the connection.
  - Executes pragma command to optimize SQLite writes.
- Table API's
  - GetTables (Returns query results for all the DB table namesw)
  - GetTableList (Returns string list of all the table names in the DB)
  - CopyTable
  - InsertTable
  - InsertOrReplaceTable
