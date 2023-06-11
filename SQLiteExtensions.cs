/* ***************************************************************************
 * Copyright © 2023 David Maisonave	(https://axter.com)	All rights reserved.
 * ******************************************************************************************************************************************************
 * The is free software. You can redistribute it and/or modify it under the terms of the MIT License. For more information, please see License file 
 * distributed with this package.
 * ******************************************************************************************************************************************************
 * <summary>
 *      SQLiteExtensions extends either of the following packages:
 *          System.Data.SQLite package. (https://system.data.sqlite.org/index.html/doc/trunk/www/index.wiki)
 *          Microsoft.Data.Sqlite package. (https://docs.microsoft.com/en-us/dotnet/standard/data/sqlite)
 *      It adds the following:
 *          1. Adds copy table functions.
 *          2. Adds wrapper functions using common SQL terms.
 *          3. Adds ability to use either SQLite packages without having to include package details in source code.
 *              a) Package details only have to be defined in this file.
 *              b) Usage code only needs using statement for SQLiteExtensions.
 *                  using SQLiteExtensions;
 *              c) Use "var" variable types and the Create??? API's to avoid using package variable types details in source code.
 *                  Examples:
 *                          using ( var connection = SqliteExt.CreateConnection($"Data Source={AppDbUpdate};Mode=ReadWriteCreate") )
 *                          {
 *                              var transaction = connection.BeginTransaction();
 *                              var command = connection.CreateCommand();
 *                              var reader = command.CreateReader("SELECT name FROM sqlite_master WHERE type='table' AND name='Languages';");
 *                          }
 * Setup Instructions:
 *     To add System.Data.SQLite or Microsoft.Data.Sqlite package to a VS .Net project, 
 *     open DOS command prompt, and change to the directory containing the project (*.csproj) file. 
 *     Enter ONLY ONE of the following command:  
 *          dotnet add package System.Data.SQLite
 *          dotnet add package Microsoft.Data.Sqlite
 *     
 *     FYI: The fully qualified path for dotnet is usually the following: C:\Program Files\dotnet\dotnet.exe 
 *     
 *     By default, SQLiteExtensions supports System.Data.SQLite. To use Microsoft.Data.Sqlite, add a define MICROSOFT_DATA_SQLITE
 *          #define MICROSOFT_DATA_SQLITE
 *  Usage:
 *     An instance of SQLiteConnection or SQLiteCommand is required to use most of the SQLiteExtensions API's.
 *     Example Syntax:
 *         conn.InsertTable(conn_dest, "myTableName"); // Where conn is of type SQLiteConnection
 *         int results = command.Execute($"INSERT INTO VersionInfo (Name, VerInfo) VALUES ('{version.Key}', '{version.Value}')"); // Where command is of type SQLiteCommand
 *         var reader = command.CreateReader("SELECT name FROM sqlite_master WHERE type='table' AND name='Languages';");
 * </summary>
*/

// To use Microsoft.Data.Sqlite, uncomment the next line.
// #define MICROSOFT_DATA_SQLITE

namespace SQLiteExtensions
{
#if MICROSOFT_DATA_SQLITE
    using Microsoft.Data.Sqlite;
    using SQLiteConnection = Microsoft.Data.Sqlite.SqliteConnection;
    using SQLiteTransaction = Microsoft.Data.Sqlite.SqliteTransaction;
    using SQLiteCommand = Microsoft.Data.Sqlite.SqliteCommand;
    using SQLiteDataReader = Microsoft.Data.Sqlite.SqliteDataReader;
#else //SYSTEM_DATA_SQLITE
    using System.Data.SQLite;
#endif

    using System.Diagnostics;

    // using Windows.Storage;

    /// Extension methods for SQLite. (SQLiteConnection and SQLiteCommand)
    ///Copy table data
    ///  Options:
    ///      Copy between DB (connections)
    ///      Copy table to another table within same DB (connection)
    ///      Execute either "INSERT" or "INSERT OR REPLACE"
    ///      Execute in transaction mode
    ///      Delete destination table content before copy
    ///      Delete (drop) table before copy
    ///      Create destination table schema
    /// <usage>
    ///     Example #1:
    ///         sqliteConnection_SrcDb.CopyTable(sqliteConnection_DestinationDb, "myTableName");
    ///     Example #2:
    ///         SqliteExt.CopyTable(sqliteConnection_SrcDb, sqliteConnection_DestinationDb, "myTableName");
    /// </usage>
    public static class SqliteExt
    {
        ///////////////////////////////////////////////////////////////////////////
        // API's using common SQL terms
        static public int Insert(this SQLiteConnection conn, string InsertQueryCmd) => Execute(conn, InsertQueryCmd);
        static public int Insert(this SQLiteCommand sql_cmd, string InsertQueryCmd) => sql_cmd.Execute(InsertQueryCmd);
        static public int DropTable(this SQLiteCommand sql_cmd, string TableName) => sql_cmd.Execute($"DROP TABLE if EXISTS '{TableName}';");
        static public int DeleteFrom(this SQLiteCommand sql_cmd, string TableName) => sql_cmd.Execute($"DELETE from {TableName}");
        static public SQLiteDataReader Query(this SQLiteCommand sql_cmd, string selectQuery) => sql_cmd.CreateReader(selectQuery);

        ///////////////////////////////////////////////////////////////////////////
        // API's to create SQLiteConnection, SQLiteCommand, and SQLiteDataReader

        /// <summary>
        ///     Creates SQLiteConnection using input connection string
        /// </summary>
        /// <param name="dbFilePath">File path to DB file to open</param>
        /// <param name="CreateTablesCmd">If this value is NOT null and DB file does NOT exists, DB file is created using create table command</param>
        /// <param name="AutoOpenConnection">If true, opens the connection</param>
        /// <param name="CkStartsWithDataSource">If true, checks if connections string starts with "Data Source", and if not, inserts it.</param>
        /// <param name="FastConnection">If true, passes pragma value to increase SQLite connection performance.</param>
        /// <param name="DoRethrow">If true, re-throws any exception to let calling function capture it.</param>
        ///  <returns>
        ///      A SQLiteConnection
        ///      Returns null on failure
        ///  </returns>      
        /// <usage>
        ///     Example #1:
        ///         using ( SQLiteConnection? conn = SqliteExt.CreateConnection(".\\myDbFile.db") )
        ///         {
        ///             SQLiteCommand sql_cmd = conn.CreateCommand();
        ///             foreach ( FileInfo file in fileInfos )
        ///                 sql_cmd.Execute($"INSERT OR REPLACE  into FileProperties (ParentDir, Name, Ext, Size) values (\"{file.Name}\", \"{file.DirectoryName}\", \"{file.Extension}\", {file.Length})");
        ///         }
        ///     Example #2:
        ///         string createTableCmd = "CREATE TABLE \"FileProperties\" (\r\n\t\"ParentDir\"\tTEXT NOT NULL,\r\n\t\"Name\"\tTEXT NOT NULL,\r\n\t\"Ext\"\tTEXT NOT NULL,\r\n\t\"MediaType\"\tINTEGER NOT NULL DEFAULT 0,\r\n\t\"Size\"\tINTEGER NOT NULL DEFAULT 0,\r\n\t\"Duration\"\tINTEGER NOT NULL DEFAULT 0,\r\n\t\"CheckSum\"\tINTEGER NOT NULL DEFAULT 0,\r\n\t\"FingerPrint\"\tBLOB,\r\n\tPRIMARY KEY(\"Name\",\"ParentDir\")\r\n);";
        ///         using ( SQLiteConnection? conn = SqliteExt.CreateConnection(".\\Database\\myDbFile.db", createTableCmd) )
        /// </usage>
        static public SQLiteConnection CreateConnection(string dbFilePath, string CreateTablesCmd = null, bool AutoOpenConnection = true, bool CkStartsWithDataSource = true, bool FastConnection = true, bool DoRethrow = false)
        {
            string UsingConnectionString = ( CkStartsWithDataSource && dbFilePath.ToLower().StartsWith("data source") == false ) ? "Data Source=" + dbFilePath: dbFilePath;
            try
            {
                bool CreateTables = false;
                if ( CreateTablesCmd != null && dbFilePath.ToLower().StartsWith("data source") == false && !File.Exists(dbFilePath) )
                {
                    SQLiteConnection.CreateFile(dbFilePath);
                    CreateTables = true;
                }
                SQLiteConnection conn = new (UsingConnectionString);
                if ( AutoOpenConnection && conn != null )
        {
            conn.Open();
                    if ( FastConnection )
                        conn.Execute(@"PRAGMA journal_mode = 'wal'", true);
                    if ( CreateTables )
                        conn.Execute(CreateTablesCmd, true);
                }
            return conn;
        }
            catch(Exception ex)
        {
                Debug.WriteLine($"Error: SQL CreateConnection failure using connection string '{UsingConnectionString}'; Input param:'{dbFilePath}', {CreateTablesCmd}, {AutoOpenConnection}, {CkStartsWithDataSource}, {FastConnection}\nErrMsg: {ex.Message}");
                if ( DoRethrow )
                    throw ex;
        }
            return null;
        }

        /// <summary>
        ///     Overrides the default behavior of DbDataReader to return a specialized SQLiteDataReader class
        /// </summary>
        /// <param name="sql_cmd">SQL DB SQLiteCommand</param>
        /// <param name="selectQuery">Select query command</param>
        /// <param name="DoRethrow">If true, re-throws any exception to let calling function capture it.</param>
        ///  <returns>
        ///      A SQLiteDataReader
        ///      Returns null on failure
        ///  </returns>
        /// <usage>
        ///     Example #1:
        ///         SQLiteCommand sql_cmd = conn.CreateCommand();
        ///         SQLiteDataReader queryResults = sql_cmd.CreateReader("select * from fooTable");
        ///     Example #2:
        ///         See GetTables && GetTableList implementation
        /// </usage>
        static public SQLiteDataReader CreateReader(this SQLiteCommand sql_cmd, string selectQuery, bool DoRethrow = false)
        {
            try
            {
                sql_cmd.CommandText = selectQuery;
                return sql_cmd.ExecuteReader();
            }
            catch ( Exception ex )
            {
                Debug.WriteLine($"Error: SQL CreateReader failure for query '{selectQuery}'\nErrMsg: {ex.Message}");
                if ( DoRethrow )
                    throw ex;
            }
            return null;
        }

        /// <summary>
        /// Execute Non-Query. Example: Insert, Insert or Replace, Drop, Create, etc...
        /// </summary>
        /// <param name="conn">SQL DB connection</param>
        /// <param name="NonQueryCmd">SQL Non-Query (Insert, Insert or Replace, Drop, Create)</param>
        /// <param name="DoRethrow">If true, re-throws any exception to let calling function capture it.</param>
        ///  <returns>
        ///      The number of rows inserted/updated affected by it.
        ///      Returns negative number on failure
        ///  </returns>
        /// <usage>
        ///     Example #1:
        ///         SQLiteConnection? conn = SqliteExt.CreateConnection(".\\myDbFile.db");
        ///         conn.Execute($"DELETE from {TableName}");
        ///     Example #2:
        ///         SQLiteConnection? conn = SqliteExt.CreateConnection(".\\myDbFile.sqlite");
        ///         conn.Execute($"INSERT OR REPLACE into foofoo (Name, Size) values (\"{foo_Name}\", {foo_Size})");
        /// </usage>
        static public int Execute(this SQLiteConnection conn, string NonQueryCmd, bool DoRethrow = false)
        {
            try
            {
                SQLiteCommand sql_cmd = conn.CreateCommand();
                sql_cmd.CommandText = NonQueryCmd;
                return sql_cmd.ExecuteNonQuery();
            }
            catch ( Exception ex )
            {
                Debug.WriteLine($"Error: SQL Execute failure for query '{NonQueryCmd}'\nErrMsg: {ex.Message}");
                if ( DoRethrow )
                    throw ex;
            }
            return -1;
        }
        /// <summary>
        /// Execute Non-Query. Example: Insert, Insert or Replace, Drop, Create, etc... 
        /// </summary>
        /// <param name="sql_cmd">SQL DB connection</param>
        /// <param name="NonQueryCmd">SQL Non-Query (Insert, Insert or Replace, Drop, Create)</param>
        /// <param name="DoRethrow">If true, re-throws any exception to let calling function capture it.</param>
        ///  <returns>
        ///      The number of rows inserted/updated affected by it.
        ///      Returns negative number on failure
        ///  </returns>
        /// <usage>
        ///     Example #1:
        ///         SQLiteCommand sql_cmd = conn.CreateCommand();
        ///         sql_cmd.Execute($"DELETE from {TableName}");
        ///     Example #2:
        ///         sql_cmd.Execute($"INSERT into foofoo (Name, Size) values (\"{foo_Name}\", {foo_Size})"");
        /// </usage>
        static public int Execute(this SQLiteCommand sql_cmd, string NonQueryCmd, bool DoRethrow = false)
        {
            try
            {
                sql_cmd.CommandText = NonQueryCmd;
                return sql_cmd.ExecuteNonQuery();
            }
            catch(Exception ex)
        {
                Debug.WriteLine($"Error: SQL Execute failure for query '{sql_cmd.CommandText}'\nErrMsg: {ex.Message}");
                if ( DoRethrow )
                    throw ex;
        }
            return -1;
        }

        ///////////////////////////////////////////////////////////////////////////
        // Table API's

        /// <summary>
        ///     Executes query for tales in DB schema
        /// </summary>
        ///  <returns>
        ///     Returns query results with all the tables in the DB schema
        ///  </returns>
        static public SQLiteDataReader GetTables(this SQLiteCommand sql_cmd) => sql_cmd.CreateReader("select name from sqlite_schema WHERE type ='table' AND name NOT LIKE 'sqlite_%';");
        
        /// <summary>
        ///     Executes query for tales in DB schema
        /// </summary>
        ///  <returns>
        ///     Returns a list of all the tables in the DB schema
        ///  </returns>
        static public List<string> GetTableList(this SQLiteCommand sql_cmd)
        {
            List<string> tableList = new();
            SQLiteDataReader reader = sql_cmd.GetTables();
            while ( reader.Read() )
            {
                tableList.Add(reader.GetString(0));
            }
            return tableList;
        }

        /// <summary>
        /// Create table and insert table data from one DB (connection) to another.
        /// The table name is the same in source and destination DB (connection).
        /// </summary>
        /// <param name="sourceConnection">SQL DB connection to the (from) source DB</param>
        /// <param name="destinationConnection">SQL DB connection to the (target) destination DB</param>
        /// <param name="tableName">Name of the table in the source and destination</param>
        /// <param name="TransactionMode">If true, execute in transaction mode</param>
        /// <param name="deleteDestinationTableFirst">If true, deletes destination table before executing insert.</param>
        static public bool CopyTable(this SQLiteConnection sourceConnection, SQLiteConnection destinationConnection, string tableName, bool deleteDestinationTableFirst = false, bool TransactionMode = true) => CopyTable(sourceConnection, tableName, tableName, destinationConnection, "INSERT", deleteDestinationTableFirst, true, TransactionMode);
        /// <summary>
        /// Insert table data from one DB (connection) to another.
        /// The table name is the same in source and destination DB (connection).
        /// </summary>
        /// <param name="sourceConnection">SQL DB connection to the (from) source DB</param>
        /// <param name="destinationConnection">SQL DB connection to the (target) destination DB</param>
        /// <param name="tableName">Name of the table in the source and destination</param>
        /// <param name="TransactionMode">If true, execute in transaction mode</param>
        /// <param name="deleteDestinationTableFirst">If true, deletes destination table content before executing insert.</param>
        static public bool InsertTable(this SQLiteConnection sourceConnection, SQLiteConnection destinationConnection, string tableName, bool deleteDestinationTableFirst = false, bool TransactionMode = true) => CopyTable(sourceConnection, tableName, tableName, destinationConnection, "INSERT", deleteDestinationTableFirst, TransactionMode);
        /// <summary>
        /// Insert table data from one table to another.
        /// The source and destination table can be in the same DB (connection) or in two different DB.
        /// If the source and destination DB are the same, then the table name must be different.
        /// </summary>
        /// <param name="sourceConnection">SQL DB connection to the (from) source DB</param>
        /// <param name="sourceTableName">Name of the table to copy</param>
        /// <param name="destinationTableName">Name of the table to copy</param>
        /// <param name="destinationConnection">SQL DB connection to the (target) destination DB</param>
        /// <param name="TransactionMode">If true, execute in transaction mode</param>
        /// <param name="deleteDestinationTableFirst">If true, deletes destination table content before executing insert.</param>
        static public bool InsertTable(this SQLiteConnection sourceConnection, string sourceTableName, string destinationTableName, SQLiteConnection destinationConnection = null, bool deleteDestinationTableFirst = false, bool TransactionMode = true) => CopyTable(sourceConnection, sourceTableName, destinationTableName, destinationConnection, "INSERT", deleteDestinationTableFirst, TransactionMode);
        /// <summary>
        /// Insert or Replace table data from one DB (connection) to another.
        /// The table name is the same in source and destination DB (connection).
        /// </summary>
        /// <param name="sourceConnection">SQL DB connection to the (from) source DB</param>
        /// <param name="destinationConnection">SQL DB connection to the (target) destination DB</param>
        /// <param name="tableName">Name of the table in the source and destination</param>
        /// <param name="TransactionMode">If true, execute in transaction mode</param>
        /// <param name="deleteDestinationTableFirst">If true, deletes destination table content before executing insert.</param>
        static public bool InsertOrReplaceTable(this SQLiteConnection sourceConnection, SQLiteConnection destinationConnection, string tableName, bool deleteDestinationTableFirst = false, bool TransactionMode = true) => CopyTable(sourceConnection, tableName, tableName, destinationConnection, "INSERT OR REPLACE", deleteDestinationTableFirst, TransactionMode);
        /// <summary>
        /// Insert or Replace table data from one table to another.
        /// The source and destination table can be in the same DB (connection) or in two different DB.
        /// If the source and destination DB are the same, then the table name must be different.
        /// </summary>
        /// <param name="sourceConnection">SQL DB connection to the (from) source DB</param>
        /// <param name="sourceTableName">Name of the table to copy</param>
        /// <param name="destinationTableName">Name of the table to copy</param>
        /// <param name="destinationConnection">SQL DB connection to the (target) destination DB</param>
        /// <param name="TransactionMode">If true, execute in transaction mode</param>
        /// <param name="deleteDestinationTableFirst">If true, deletes destination table content before executing insert.</param>
        static public bool InsertOrReplaceTable(this SQLiteConnection sourceConnection, string sourceTableName, string destinationTableName, SQLiteConnection destinationConnection = null, bool deleteDestinationTableFirst = false, bool TransactionMode = true) => CopyTable(sourceConnection, sourceTableName, destinationTableName, destinationConnection, "INSERT OR REPLACE", deleteDestinationTableFirst, TransactionMode);
        /// <summary>
        /// Insert or InsertOrReplace table data from one DB (connection) to another
        /// </summary>
        /// <param name="sourceTableName">Name of the table to copy</param>
        /// <param name="sourceConnection">SQL DB connection to the (from) source DB</param>
        /// <param name="destinationTableName">Name of the table to copy</param>
        /// <param name="destinationConnection">SQL DB connection to the (target) destination DB</param>
        /// <param name="TransactionMode">If true, execute in transaction mode</param>
        /// <param name="SqlInsertCmd">Used this to overwrite insert command. Can be one of the following: "INSERT", "INSERT OR REPLACE" </param>
        /// <param name="deleteDestinationTableFirst">If true, deletes destination table content before executing insert.</param>
        static public bool CopyTable(this SQLiteConnection sourceConnection, string sourceTableName, string destinationTableName = null, SQLiteConnection destinationConnection = null, string SqlInsertCmd = "INSERT", bool deleteDestinationTableFirst = false, bool createTableFirst = false, bool TransactionMode = true) => CopyTableImpl(sourceConnection, sourceTableName, destinationTableName, destinationConnection, SqlInsertCmd, deleteDestinationTableFirst, createTableFirst, TransactionMode);
        static public string defaultAppDbFile
        {
            get
            {
                String AppPath = System.IO.Path.GetDirectoryName(System.Diagnostics.Process.GetCurrentProcess().MainModule.FileName);
                String ProductName = System.Diagnostics.Process.GetCurrentProcess().MainModule.FileName;
                if (!Directory.Exists(AppPath))
                    Directory.CreateDirectory(AppPath);
                if ( !Directory.Exists($"{AppPath}\\{DbPath}") )
                    Directory.CreateDirectory($"{AppPath}\\{DbPath}");
                Debug.Assert(Directory.Exists($"{AppPath}\\{DbPath}"));
                return $"{AppPath}\\{DbPath}\\{ProductName}.db";
            }
        }
        static public string DbPath { get; } = "Database";
        
        #region SqliteExt Private methods
        static private bool CopyTableImpl(this SQLiteConnection sourceConnection, string sourceTableName, string destinationTableName = null, SQLiteConnection destinationConnection = null, string SqlInsertCmd = "INSERT", bool deleteDestinationTableFirst = false, bool createTableFirst = false, bool TransactionMode = true)
        {
            destinationTableName ??= sourceTableName;
            destinationConnection ??= sourceConnection;
            Debug.Assert(!sourceConnection.Equals(destinationConnection) || !sourceTableName.Equals(destinationTableName));
            if ( sourceConnection.Equals(destinationConnection) && sourceTableName.Equals(destinationTableName) )
                return false;
            int results = 0;
            SQLiteTransaction? transaction = TransactionMode ? destinationConnection.BeginTransaction() : null;
            SQLiteCommand toDbCommand = destinationConnection.CreateCommand();
            SQLiteCommand? fromDbCommand = sourceConnection.CreateCommand();
            if ( createTableFirst )
            {
                if ( deleteDestinationTableFirst )
                {
                    results = toDbCommand.Execute($"DROP TABLE if EXISTS '{destinationTableName}';");
                }
                SQLiteDataReader ? readSchema = fromDbCommand.CreateReader($"SELECT sql FROM sqlite_master WHERE type='table' AND name='{destinationTableName}';");
                if ( readSchema.Read() )
                {
                    string createTableSchema = readSchema.GetString(0);
                    results = toDbCommand.Execute(createTableSchema.Replace($"CREATE TABLE \"{sourceTableName}\"", $"CREATE  TABLE \"{destinationTableName}\" "));
                }
                readSchema.Close();
            }
            else if ( deleteDestinationTableFirst )
            {
                toDbCommand.Execute($"delete from {destinationTableName}");
            }
            SQLiteDataReader ? sourceReader = fromDbCommand.CreateReader($"SELECT * FROM {sourceTableName};");
            string Columns = "";
            while ( sourceReader.Read() )
            {
                if ( string.IsNullOrEmpty(Columns) )
                {
                    for ( int i = 0 ; i < sourceReader.FieldCount ; ++i )
                    {
                        Columns += sourceReader.GetName(i) + ",";
                    }
                    Columns = Columns.TrimEnd(',');
                }
                string Values = "";
                for ( int i = 0 ; i < sourceReader.FieldCount ; ++i )
                {
                    Values += $"'{sourceReader.GetString(i)}',";
                }
                Values = Values.TrimEnd(',');
                results = toDbCommand.Execute($"{SqlInsertCmd} INTO {destinationTableName} ({Columns}) VALUES({Values});");
                Debug.Assert(results == 1, $"Failed to insert into table {destinationTableName} from table {sourceTableName}");
            }
            sourceReader.Close();
            if ( transaction != null )
                transaction.Commit();
            return true;
        }
#endregion SqliteExt Private methods
    }
}
