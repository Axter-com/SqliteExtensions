/* ***************************************************************************
 * Copyright © 2022 David Maisonave	(https://axter.com)	All rights reserved.
 * ******************************************************************************************************************************************************
 * The is free software. You can redistribute it and/or modify it under the terms of the MIT License. For more information, please see License file 
 * distributed with this package.
 * ******************************************************************************************************************************************************
 * <summary>
 *      SQLiteExtensions extends either of the following packages:
 *          System.Data.SQLite package. (https://system.data.sqlite.org/index.html/doc/trunk/www/index.wiki)
 *          Microsoft.Data.Sqlite package. (https://docs.microsoft.com/en-us/dotnet/standard/data/sqlite) 
 * Setup Instructions:
 *     To add System.Data.SQLite or Microsoft.Data.Sqlite package to a VS .Net project, 
 *     open DOS command prompt, and change to the directory containing the project (*.csproj) file. 
 *     Enter ONE of the following command:  
 *          dotnet add package System.Data.SQLite
 *          dotnet add package Microsoft.Data.Sqlite
 *     
 *     FYI: The fully qualified path for dotnet is usually the following: C:\Program Files\dotnet\dotnet.exe 
 *     
 *     By default, SQLiteExtensions supports System.Data.SQLite. To use it with Microsoft.Data.Sqlite, add a define MICROSOFT_DATA_SQLITE
 *          #define MICROSOFT_DATA_SQLITE
 *  Usage:
 *     An instance of SQLiteConnection is required to use the SQLiteExtensions API's.
 *     Example Syntax:
 *         conn.InsertTable(conn_dest, "myTableName"); // Where conn is of type SQLiteConnection
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

    /// Extension methods for SQLite connections (SQLiteConnection).
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
        // API's using common SQL terms
        static public int Insert(this SQLiteConnection conn, string InsertQueryCmd) => Execute(conn, InsertQueryCmd);
        static public int Insert(this SQLiteCommand sqliteCommand, string InsertQueryCmd) => Execute(sqliteCommand, InsertQueryCmd);
        static public int DropTable(this SQLiteCommand sqliteCommand, string TableName)=>sqliteCommand.Execute($"DROP TABLE if EXISTS '{TableName}';");
        static public int DeleteFrom(this SQLiteCommand sqliteCommand, string TableName)=> sqliteCommand.Execute($"DELETE from {TableName}");

        // API's to create SQLiteConnection, SQLiteCommand, and SQLiteDataReader
        static public SQLiteConnection CreateConnection(string connectionString)  // This is the only API which requires SqliteExt to be used.  Example:  SqliteExt.CreateConnection(@"Data Source=C:\mydatabase.db;Mode=ReadOnly")
        {
            var conn = new SQLiteConnection(connectionString);
            conn.Open();
            return conn;
        }
        static public SQLiteCommand CreateCommand(this SQLiteConnection conn, string cmd = "") => conn.CreateCommand(cmd);
        static public SQLiteDataReader CreateReader(this SQLiteConnection conn, string cmd)=> conn.CreateCommand(cmd).ExecuteReader();
        //{
        //    SQLiteCommand sqliteCommand = conn.CreateCommand(cmd);
        //    return sqliteCommand.ExecuteReader();
        //}
        static public SQLiteDataReader CreateReader(this SQLiteCommand sqliteCommand, string cmd)
        {
            sqliteCommand.CommandText = cmd;
            return sqliteCommand.ExecuteReader();
        }
        /// <summary>
        /// Execute Non-Query. Example: Insert, Insert or Replace, Drop, Create, etc...
        /// </summary>
        /// <param name="conn">SQL DB connection</param>
        /// <param name="NonQueryCmd">SQL Non-Query (Insert, Insert or Replace, Drop, Create)</param>
        static public int Execute(this SQLiteConnection conn, string NonQueryCmd)
        {
            SQLiteCommand sqliteCommand = conn.CreateCommand(NonQueryCmd);
            return sqliteCommand.ExecuteNonQuery();
        }
        /// <summary>
        /// Execute Non-Query. Example: Insert, Insert or Replace, Drop, Create, etc... 
        /// </summary>
        /// <param name="sqliteCommand">SQL DB connection</param>
        /// <param name="NonQueryCmd">SQL Non-Query (Insert, Insert or Replace, Drop, Create)</param>
        static public int Execute(this SQLiteCommand sqliteCommand, string NonQueryCmd)
        {
            sqliteCommand.CommandText = NonQueryCmd;
            return sqliteCommand.ExecuteNonQuery();
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
                    results = toDbCommand.Execute($"DROP TABLE if EXISTS '{destinationTableName}';);
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
