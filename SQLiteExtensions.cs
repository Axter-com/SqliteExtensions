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

namespace SQLiteExtensions
{
#if MICROSOFT_DATA_SQLITE
    using Microsoft.Data.Sqlite;
    using SQLiteConnection = Microsoft.Data.Sqlite.SqliteConnection;
    using SQLiteTransaction = Microsoft.Data.Sqlite.SqliteTransaction;
    using SQLiteCommand = Microsoft.Data.Sqlite.SqliteCommand;
    using SQLiteDataReader = Microsoft.Data.Sqlite.SqliteDataReader;
#else //SYSTEM_DATA_SQLITE_
    using System.Data.SQLite;
#endif

    using System.Diagnostics;

    public static class Sqlite
    {
        static public SQLiteConnection CreateConnection(string connectionString)
        {
            return new SQLiteConnection(connectionString);
        }
    }

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
    ///         sqliteconnectionSrcDb.CopyTable(sqliteconnectionDestinationDb, "myTableName");
    ///     Example #2:
    ///         SqliteConnectionExtensions.CopyTable(sqliteconnectionSrcDb, sqliteconnectionDestinationDb, "myTableName");
    /// </usage>
    public static class SqliteConnectionExtensions // Public API's
    {
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

#region SqliteConnectionExtensions Private methods
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
                    toDbCommand.CommandText = $"DROP TABLE if EXISTS '{destinationTableName}';";
                    results = toDbCommand.ExecuteNonQuery();
                }
                fromDbCommand.CommandText = $"SELECT sql FROM sqlite_master WHERE type='table' AND name='{destinationTableName}';";
                SQLiteDataReader ? readSchema = fromDbCommand.ExecuteReader();
                if ( readSchema.Read() )
                {
                    string createTableSchema = readSchema.GetString(0);
                    toDbCommand.CommandText = createTableSchema.Replace($"CREATE TABLE \"{sourceTableName}\"", $"CREATE  TABLE \"{destinationTableName}\" ");
                    results = toDbCommand.ExecuteNonQuery();
                }
                readSchema.Close();
            }
            else if ( deleteDestinationTableFirst )
            {
                toDbCommand.CommandText = $"delete from {destinationTableName}";
                toDbCommand.ExecuteNonQuery();
            }
            fromDbCommand.CommandText = $"SELECT * FROM {sourceTableName};";
            SQLiteDataReader ? sourceReader = fromDbCommand.ExecuteReader();
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
                toDbCommand.CommandText = $"{SqlInsertCmd} INTO {destinationTableName} ({Columns}) VALUES({Values});";
                results = toDbCommand.ExecuteNonQuery();
                Debug.Assert(results == 1, $"Failed to insert into table {destinationTableName} from table {sourceTableName}");
            }
            sourceReader.Close();
            if ( transaction != null )
                transaction.Commit();
            return true;
        }
#endregion SqliteConnectionExtensions Private methods
    }

#if !MICROSOFT_DATA_SQLITE
    public class SqliteException : Exception
    {
        /// <summary>
        ///     Initializes a new instance of the <see cref="SqliteException" /> class.
        /// </summary>
        /// <param name="message">The message to display for the exception. Can be null.</param>
        /// <param name="errorCode">The SQLite error code.</param>
        public SqliteException(string message, int errorCode = 0)
            : base($"{message}; SQL-Error:{errorCode}")
        {
        }
    }
#endif // !MICROSOFT_DATA_SQLITE
}
