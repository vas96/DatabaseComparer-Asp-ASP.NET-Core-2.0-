using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Data.SQLite;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using Comparer.Models;
using Microsoft.AspNetCore.Http;
using Microsoft.EntityFrameworkCore.Internal;
using MySql.Data.MySqlClient;
using Npgsql;

namespace DbComparer
{
    internal interface IDatabase
    {
        /// <summary>
        ///     Підключення до сервера
        /// </summary>
        /// <returns>True якщо підключення встановлено</returns>
        bool ConnectToServer(int port = int.MinValue);

        /// <summary>
        ///     Підключення до конктретної бази даних на сервері
        /// </summary>
        /// <param name="databaseName">Ім'я бази даних</param>
        /// <returns>True якщо підключення встановлено</returns>
        bool ConnectToDatabase(string databaseName, int port = int.MinValue);

        /// <summary>
        ///     Підключення до файлу бази данних
        /// </summary>
        /// <param name="location">Шлях до файлу. Якщо не заданий - заново відкривається підключення</param>
        /// <returns>True, якщо підключення встановлено</returns>
        bool ConnectToFile(string location = null);

        /// <summary>
        ///     Віддалене підключення
        /// </summary>
        /// <param name="ip">IP</param>
        /// <param name="port">Порт</param>
        /// <param name="db">Назва БД (за потребою)</param>
        /// <returns></returns>
        bool RemoteConnection(Dictionary<string, string> dictionary);

        /// <summary>
        ///     Повертає список баз даних на сервері
        /// </summary>
        /// <returns></returns>
        List<string> GetDatabasesList();

        /// <summary>
        ///     Повертає список таблиць у базі даних (файлі чи сервері)
        /// </summary>
        /// <param name="database"></param>
        /// <returns></returns>
        List<string> GetTablesList(string database = null);

        /// <summary>
        ///     Повертає всю інформацію про колонки у таблиці
        /// </summary>
        /// <param name="tableName">Назва таблиці. Якщо null - по замовчуванню обирається SelectedTable</param>
        /// <returns></returns>
        DataTable GetTableInfo(string tableName);

        /// <summary>
        ///     Зчитує дані з таблиці (для селектора по string)
        /// </summary>
        /// <typeparam name="Selected">String</typeparam>
        /// <param name="query">Запит</param>
        /// <param name="selector">Селектор (міститься у БД)</param>
        /// <returns></returns>
        List<string[]> Read(string query, Func<IDataRecord, string[]> selector);

        /// <summary>
        ///     Закриває підключення
        /// </summary>
        void CloseConnection();

        /// <summary>
        ///     Будує стандартний запит на вибірку
        /// </summary>
        /// <param name="table">Ім'я таблиці. Якщо null - використовується SelectedTable</param>
        /// <returns></returns>
        string BuildSelectQuery(string table = null);
    }

    public abstract class Database : IDatabase
    {
        /// <summary>
        ///     Види підключення
        /// </summary>
        public enum Connection_Type
        {
            File,
            Remote,
            Server
        }

        /// <summary>
        ///     Можливі типи БД
        /// </summary>
        public enum Database_Type
        {
            SqlServer,
            MySql,
            Oracle,
            SQLite,
            XML,
            PostgreSQL,
            NONE
        }

        /// <summary>
        ///     Підлючення
        /// </summary>
        public DbConnection connection;

        /// <summary>
        ///     Вид активного підключення
        /// </summary>
        public Connection_Type ConType;

        /// <summary>
        ///     Рядок підключення
        /// </summary>
        public string DataConnectionString;

        /// <summary>
        ///     Тип активної БД
        /// </summary>
        public Database_Type DbType;

        /// <summary>
        ///     Назва файлу
        /// </summary>
        public string FileName;

        /// <summary>
        ///     Селектор
        ///     Повертає дані у вигляді масиву рядків
        /// </summary>
        public Func<IDataRecord, string[]> FullStringArraySelector = delegate(IDataRecord s)
        {
            var counter = s.FieldCount;

            var array = new string[counter];
            for (var i = 0; i < counter; i++) array[i] = s[i].ToString();
            return array;
        };

        /// <summary>
        ///     Селектор
        ///     Повертає дані у вигляді суцільного рядка
        /// </summary>
        public Func<IDataRecord, string> FullStringSelector = delegate(IDataRecord s)
        {
            var obj = new object[s.FieldCount];
            for (var i = 0; i < s.FieldCount; i++) obj[i] = s[i];
            return string.Join(" ", obj);
        };

        /// <summary>
        ///     Колонки, обрані для порівняння
        /// </summary>
        public List<Column> SelectedColumns;

        /// <summary>
        ///     Обрана база даних
        /// </summary>
        public string SelectedDatabase;

        /// <summary>
        ///     Обрана таблиця
        /// </summary>
        public string SelectedTable;

        /// <summary>
        ///     Інформація про колонки активної таблиці
        /// </summary>
        public List<Column> TableColumns;

        /// <summary>
        ///     ІНІЦІАЛІЗАЦІЯ...
        /// </summary>
        public Database()
        {
            DataConnectionString = null;
            SelectedTable = null;
            SelectedDatabase = null;
            connection = null;
            TableColumns = new List<Column>();
            SelectedColumns = new List<Column>();
            FileName = null;
        }

        public abstract bool ConnectToServer(int port = int.MinValue);
        public abstract bool ConnectToDatabase(string databaseName, int port = int.MinValue);
        public abstract bool ConnectToFile(string location = null);
        public abstract bool RemoteConnection(Dictionary<string, string> dictionary);
        public abstract List<string> GetDatabasesList();
        public abstract List<string> GetTablesList(string database = null);
        public abstract DataTable GetTableInfo(string tableName = null);
        public abstract List<string[]> Read(string query, Func<IDataRecord, string[]> selector);

        public abstract void CloseConnection();

        /// <summary>
        ///     Будує запит до БД на основі SelectedColumns
        /// </summary>
        /// <param name="table"></param>
        /// <returns></returns>
        public string BuildSelectQuery(string table = null)
        {
            var Select = "SELECT ";
            foreach (var item in SelectedColumns) Select += item.Name + ", ";
            Select = Select.Remove(Select.Length - 2, 2);
            Select += " FROM " + (table == null ? SelectedTable : table);
            return Select;
            /*
             INSERT INTO table_name (column1, column2, column3, ...)
             VALUES (value1, value2, value3, ...);
             */
        }

        /// <summary>
        ///     Створює запит на додавання даних
        /// </summary>
        /// <param name="stringses">собсно, по яких даних будувати запит</param>
        /// <returns></returns>
        public string[] BuildInsert(List<string[]> stringses, string[] selected)
        {
            var Result = new string[selected.Length];
            var arr = selected.Select(i => int.Parse(i)).ToArray();
            var columns = "(" + SelectedColumns.Select(i => i.Name).Join(",") + ")";
            for (var i = 0; i < selected.Length; i++)
            {
                var Insert = "INSERT INTO "
                             + SelectedTable
                             + columns
                             + " VALUES(";
                for (var j = 0; j < SelectedColumns.Count; j++)
                    if (SelectedColumns[j].Type != "int")
                        Insert += "'" + stringses[arr[i]][j] + "',";
                    else
                        Insert += stringses[arr[i]][j] + ",";
                Insert = Insert.Remove(Insert.Length - 1, 1);
                Insert += ");";
                Result[i] = Insert;
            }

            return Result;
        }

        /// <summary>
        ///     Створює запит(и) на оновлення даних в таблиці
        /// </summary>
        /// <param name="stringsTo">Куда</param>
        /// <param name="stringsFrom">Звідки</param>
        /// <returns>масівчик</returns>
        public string[] BuildUpdate(List<string[]> stringsTo, List<string[]> stringsFrom, string[] selected)
        {
            var Result = new string[selected.Length];
            var arr = selected.Select(i => int.Parse(i)).ToArray();
            for (var i = 0; i < selected.Length; i++)
            {
                var Update = "UPDATE "
                             + SelectedTable
                             + " SET "
                             + SelectedColumns[0].Name
                             + "=";
                if (SelectedColumns[0].Type != "int")
                    Update += "'" + stringsFrom[arr[i]][0] + "' ";
                else Update += stringsFrom[arr[i]][0] + " ";
                for (var j = 1; j < SelectedColumns.Count; j++)
                    if (SelectedColumns[j].Type != "int")
                        Update += ", " + SelectedColumns[j].Name + "='" + stringsFrom[arr[i]][j] + "'";
                    else
                        Update += ", " + SelectedColumns[j].Name + "=" + stringsFrom[arr[i]][j];
                Update += " WHERE "
                          + SelectedColumns[0].Name
                          + "=";
                if (SelectedColumns[0].Type != "int")
                    Update += "'" + stringsTo[arr[i]][0] + "' ";
                else Update += stringsTo[arr[i]][0] + " ";
                for (var j = 1; j < SelectedColumns.Count; j++)
                    if (SelectedColumns[j].Type != "int")
                        Update += ", " + SelectedColumns[j].Name + "='" + stringsTo[arr[i]][j] + "'";
                    else
                        Update += ", " + SelectedColumns[j].Name + "=" + stringsTo[arr[i]][j];
                Update += ";";
                Result[i] = Update;
            }

            return Result;
            /*
                UPDATE table_name
                SET column1 = value1, column2 = value2, ...
                WHERE condition;
             */
        }

        /// <summary>
        ///     Визначає тип БД
        /// </summary>
        /// <param name="file"></param>
        /// <returns></returns>
        public static Database_Type GetDBType(IFormFile file)
        {
            var ext = Path.GetExtension(file.FileName).Replace(".", "").ToLower();
            switch (ext)
            {
                case "mdf":
                {
                    return Database_Type.SqlServer;
                    break;
                }
                case "sql":
                {
                    return Database_Type.MySql;
                    break;
                }
                case "xml":
                {
                    return Database_Type.XML;
                    break;
                }
                case "db":
                {
                    return Database_Type.SQLite;
                    break;
                }
                default:
                {
                    return Database_Type.NONE;
                    break;
                }
            }
        }

        /// <summary>
        ///     Визначає тип БД та виконує присвоєння
        /// </summary>
        /// <param name="file"></param>
        /// <returns></returns>
        public static Database InitializeType(IFormFile file)
        {
            var type = GetDBType(file);
            switch (type)
            {
                case Database_Type.MySql:
                {
                    return new MySqlDataBaseConnector {DbType = Database_Type.MySql};
                    break;
                }
                case Database_Type.SqlServer:
                {
                    return new SqlDataBaseConnector {DbType = Database_Type.SqlServer};
                    ;
                    break;
                }
                case Database_Type.SQLite:
                {
                    return new SQLiteDatabaseConnector {DbType = Database_Type.SQLite};
                    ;
                    break;
                }
                default:
                {
                    return null;
                    break;
                }
            }
        }

        /// <summary>
        ///     Визначає тип БД та виконує присвоєння
        /// </summary>
        /// <param name="file"></param>
        /// <returns></returns>
        public static Database InitializeType(string type)
        {
            switch (type)
            {
                case "MySQL":
                {
                    return new MySqlDataBaseConnector {DbType = Database_Type.MySql};
                    break;
                }
                case "SQLServer":
                {
                    return new SqlDataBaseConnector {DbType = Database_Type.SqlServer};
                    ;
                    break;
                }
                case "SQLite":
                {
                    return new SQLiteDatabaseConnector {DbType = Database_Type.SQLite};
                    ;
                    break;
                }
                case "PostgreSQL":
                {
                    return new PostGreSQLDatabaseConnector {DbType = Database_Type.PostgreSQL};
                    ;
                    break;
                }
                default:
                {
                    return null;
                    break;
                }
            }
        }

        /// <summary>
        ///     Дані про колонки таблиці
        /// </summary>
        public class Column
        {
            public bool ISKey;
            public string Length;
            public string Name;
            public int Position;
            public string Type;

            /// <summary>
            ///     Витянує такі дані про колонку як:
            ///     Позиція, Імя, Тип, Довжина, ЧиЄКлючем
            /// </summary>
            /// <param name="dr"></param>
            public Column(DataRow dr, Database_Type type)
            {
                switch (type)
                {
                    case Database_Type.SqlServer:
                    case Database_Type.MySql:
                    {
                        var array = dr.ItemArray;
                        Position = int.Parse(array[4].ToString()) - 1; //Позиція
                        Name = array[3].ToString(); //Імя
                        Type = array[7].ToString(); //Тип
                        Length = array[8].ToString(); //Довжина
                        if (array[15] != "PRI")
                            ISKey = false; //Ключ
                        else ISKey = true;
                        break;
                    }
                    case Database_Type.SQLite:
                    {
                        var array = dr.ItemArray;
                        Position = int.Parse(array[6].ToString()); //Позиція
                        Name = array[3].ToString(); //Імя
                        Type = array[11].ToString(); //Тип
                        Length = array[13].ToString(); //Довжина
                        if (array[27].ToString() != "True")
                            ISKey = false; //Ключ
                        else ISKey = true;
                        break;
                    }
                    case Database_Type.PostgreSQL:
                    {
                        var array = dr.ItemArray;
                        Position = int.Parse(array[4].ToString()) - 1; //Позиція
                        Name = array[3].ToString(); //Імя
                        Type = array[7].ToString(); //Тип
                        Length = array[8].ToString(); //Довжина
                        ISKey = false; //Ключ
                        break;
                    }
                }
            }
        }
    }

    public class SqlDataBaseConnector : Database
    {
        public SqlDataBaseConnector()
        {
            DataConnectionString = "Data Source=.; Integrated Security=True;";
            DbType = Database_Type.SqlServer;
        }


        public override bool ConnectToServer(int port = int.MinValue)
        {
            try
            {
                connection = new SqlConnection(DataConnectionString);
                connection.Open();
            }
            catch (Exception e)
            {
                return false;
            }

            return true;
        }

        public override bool ConnectToDatabase(string databaseName, int port = int.MinValue)
        {
            try
            {
                if (databaseName != null)
                    DataConnectionString += "Initial Catalog =" + databaseName + ";";
                ;
                connection = new SqlConnection(DataConnectionString);
                connection.Open();
            }
            catch (Exception EX_NAME)
            {
                return false;
            }

            return true;
        }

        public override bool ConnectToFile(string location = null)
        {
            try
            {
                if (location != null)
                    DataConnectionString =
                        "Data Source=(localdb)\\MSSQLLocalDB;AttachDbFilename=" + location +
                        ";Integrated Security=True;Connect Timeout=30;";
                connection = new SqlConnection(DataConnectionString);
                connection.Open();
                ConType = Connection_Type.File;
                return true;
            }
            catch (Exception e)
            {
                return false;
            }
        }

        public override bool RemoteConnection(Dictionary<string, string> param)
        {
            try
            {
                DataConnectionString =
                    $"Data Source={param["ip"]},{param["port"]};Network Library=DBMSSOCN;User ID={param["user"]};Password={param["pass"]};Connect Timeout=30;";
                DataConnectionString += "Initial Catalog =" + param["db"] + ";";
                connection = new SqlConnection(DataConnectionString);
                connection.Open();
                ConType = Connection_Type.Remote;
                return true;
            }
            catch (Exception e)
            {
                return false;
            }
        }

        public override List<string> GetDatabasesList()
        {
            var list = new List<string>();

            using (var cmd = new SqlCommand("SELECT name from sys.databases", connection as SqlConnection))
            {
                using (IDataReader dr = cmd.ExecuteReader())
                {
                    while (dr.Read()) list.Add(dr[0].ToString());
                }
            }

            return list;
        }

        public override List<string> GetTablesList(string database = null)
        {
            var list = new List<string>();
            var schema = connection.GetSchema("Tables");
            foreach (DataRow row in schema.Rows) list.Add(row[2].ToString());

            return list;
        }

        public override DataTable GetTableInfo(string tableName = null)
        {
            try
            {
                var columnRestrictions = new string[4];
                if (tableName == null)
                    columnRestrictions[2] = SelectedTable;
                else
                    columnRestrictions[2] = tableName;

                var departmentIDSchemaTable = connection.GetSchema("Columns", columnRestrictions);
                TableColumns.Clear();
                var dv = departmentIDSchemaTable.DefaultView;
                departmentIDSchemaTable.DefaultView.Sort = "ORDINAL_POSITION Asc";
                var SortedView = dv.ToTable();
                foreach (DataRow row in SortedView.Rows) TableColumns.Add(new Column(row, DbType));

                /////////
                var _table = tableName == null ? SelectedTable : tableName;
                var adapter = new
                    SqlDataAdapter("SELECT TOP 1 * FROM " + _table,
                        connection as SqlConnection);
                adapter.MissingSchemaAction = MissingSchemaAction.AddWithKey;
                var table = new DataTable();
                adapter.Fill(table);
                using (var reader = new DataTableReader(table))
                {
                    var ordinal = 0;
                    var schemaTable = reader.GetSchemaTable();
                    for (var i = 0; i < schemaTable.Rows.Count; i++)
                        if ((bool) schemaTable.Rows[i].ItemArray[12])
                            TableColumns[i].ISKey = true;
                }
                ///////////////

                return departmentIDSchemaTable;
            }
            catch (Exception ex)
            {
                return null;
            }
        }

        public override List<string[]> Read(string query, Func<IDataRecord, string[]> selector)
        {
            try
            {
                using (var cmd = connection.CreateCommand())
                {
                    cmd.CommandText = query;
                    using (var r = cmd.ExecuteReader())
                    {
                        return r.Cast<IDataRecord>().Select(selector).ToList();
                    }
                }
            }
            catch (Exception EX_NAME)
            {
                return null;
            }
        }


        public override void CloseConnection()
        {
            if (connection != null && connection.State != ConnectionState.Closed)
            {
                connection.Close();
                SqlConnection.ClearPool(connection as SqlConnection);
            }
        }
    }

    public class MySqlDataBaseConnector : Database
    {
        private string LocalInstance = "";
        private int port;

        public MySqlDataBaseConnector()
        {
            DbType = Database_Type.MySql;
        }

        public override bool ConnectToServer(int port = int.MinValue)
        {
            try
            {
                if (port == int.MinValue)
                {
                    DataConnectionString = "SERVER=localhost;UID='root';PASSWORD='user';Pooling=True";
                }
                else
                {
                    Thread.Sleep(1000);
                    DataConnectionString = $"SERVER=localhost;Port={port};UID='root';PASSWORD='user';Pooling=True";
                }

                connection = new MySqlConnection(DataConnectionString);
                connection.Open();
                return true;
            }
            catch (Exception e)
            {
                return false;
            }
        }

        public override bool ConnectToDatabase(string databaseName, int port = int.MinValue)
        {
            try
            {
                if (databaseName != null)
                    DataConnectionString += $"DATABASE={databaseName};";
                if (port != int.MinValue)
                    DataConnectionString += $"Port={port};";
                connection = new MySqlConnection(DataConnectionString);
                connection.Open();
                return true;
            }
            catch (Exception EX_NAME)
            {
                return false;
            }
        }

        public override bool ConnectToFile(string location = null)
        {
            try
            {
                port = AdditionalFunctions.GetAvailablePort(3306);
                var DestinationPath = Path.GetDirectoryName(location) + $"\\data{port}\\";
                LocalInstance = Directory.GetParent(DestinationPath).Parent.Parent.Parent.FullName +
                                "\\MySQLInstance\\data\\";
                //Now Create all of the directories
                foreach (var dirPath in Directory.GetDirectories(LocalInstance, "*",
                    SearchOption.AllDirectories))
                    Directory.CreateDirectory(dirPath.Replace(LocalInstance, DestinationPath));
                //Copy all the files & Replaces any files with the same name
                foreach (var newPath in Directory.GetFiles(LocalInstance, "*.*",
                    SearchOption.AllDirectories))
                    File.Copy(newPath, newPath.Replace(LocalInstance, DestinationPath), true);
                var fileName = Directory.GetParent(LocalInstance).Parent.FullName + "\\my.ini";
                var file = File.ReadAllText(fileName);
                file = file.Replace("MY_PORT", port.ToString());
                file = file.Replace("MY_LOCATION", DestinationPath.Replace("\\", "/"));
                File.WriteAllText(DestinationPath + @"\my.ini", file);

                Process.Start(new ProcessStartInfo
                {
                    FileName = Directory.GetParent(LocalInstance).Parent.FullName + @"\bin\mysqld.exe",
                    Arguments = $"--install MySQL{port} --defaults-file=" + '"' + DestinationPath.Replace(@"\", "/") +
                                "my.ini" + '"',
                    Verb = "runas",
                    UseShellExecute = true,
                    WindowStyle = ProcessWindowStyle.Hidden
                }).WaitForExit();
                Process.Start(new ProcessStartInfo
                {
                    FileName = "net.exe",
                    Arguments = $"start MYSQL{port}",
                    Verb = "runas",
                    UseShellExecute = true,
                    WindowStyle = ProcessWindowStyle.Hidden
                }).WaitForExit();

                if (ConnectToServer(port))
                {
                    var DbList = GetDatabasesList();
                    var script = new MySqlScript(connection as MySqlConnection, File.ReadAllText(location));
                    script.Delimiter = ";";
                    var sw = new Stopwatch();
                    sw.Start();
                    script.Execute();
                    sw.Stop();
                    var NewDbName = GetDatabasesList().Except(DbList).First();
                    ConnectToDatabase(NewDbName, port);
                    SelectedDatabase = NewDbName;
                    ConType = Connection_Type.File;
                    return true;
                }

                throw new Exception();
            }
            catch (Exception e)
            {
                return false;
            }
        }

        public override bool RemoteConnection(Dictionary<string, string> param)
        {
            try
            {
                DataConnectionString =
                    $"Server={param["ip"]};" +
                    $"User = {param["user"]}; Password = {param["pass"]};";

                DataConnectionString += $"DATABASE={param["db"]};";
                connection = new MySqlConnection(DataConnectionString);
                connection.Open();
                ConType = Connection_Type.Remote;
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                return false;
            }

            return true;
        }

        public override List<string> GetDatabasesList()
        {
            try
            {
                var command = (connection as MySqlConnection).CreateCommand();
                command.CommandText = "SHOW DATABASES;";
                using (var Reader = command.ExecuteReader())
                {
                    var rows = new List<string>();
                    while (Reader.Read())
                        for (var i = 0; i < Reader.FieldCount; i++)
                            rows.Add(Reader.GetValue(i).ToString());
                    return rows;
                }
            }
            catch (Exception)
            {
                return null;
            }
        }

        public override List<string> GetTablesList(string database)
        {
            try
            {
                if (database != null)
                {
                    var connected = ConnectToDatabase(database);
                    if (!connected)
                        return null;
                }

                var command = (connection as MySqlConnection).CreateCommand();
                command.CommandText = "SHOW TABLES;";
                using (var Reader = command.ExecuteReader())
                {
                    var rows = new List<string>();
                    while (Reader.Read())
                        for (var i = 0; i < Reader.FieldCount; i++)
                            rows.Add(Reader.GetValue(i).ToString());
                    return rows;
                }
            }
            catch (Exception e)
            {
                return null;
            }
        }

        public override DataTable GetTableInfo(string tableName)
        {
            try
            {
                var columnRestrictions = new string[4];
                if (tableName == null)
                    columnRestrictions[2] = SelectedTable;
                else
                    columnRestrictions[2] = tableName;

                var departmentIDSchemaTable = connection.GetSchema("Columns", columnRestrictions);
                TableColumns.Clear();
                var dv = departmentIDSchemaTable.DefaultView;
                departmentIDSchemaTable.DefaultView.Sort = "ORDINAL_POSITION Asc";
                var SortedView = dv.ToTable();
                foreach (DataRow row in SortedView.Rows) TableColumns.Add(new Column(row, DbType));
                return departmentIDSchemaTable;
            }
            catch (Exception ex)
            {
                return null;
            }
        }

        public override List<string[]> Read(string query, Func<IDataRecord, string[]> selector)
        {
            try
            {
                using (var cmd = (connection as MySqlConnection).CreateCommand())
                {
                    cmd.CommandText = query;
                    using (var r = cmd.ExecuteReader())
                    {
                        return r.Cast<IDataRecord>().Select(selector).ToList();
                    }
                }
            }
            catch (Exception EX_NAME)
            {
                Console.WriteLine(EX_NAME.Message);
                return null;
            }
        }

        public override void CloseConnection()
        {
            if (connection != null && connection.State != ConnectionState.Closed)
            {
                connection.Close();
                if (ConType == Connection_Type.File)
                {
                    Process.Start(new ProcessStartInfo
                    {
                        FileName = "net.exe",
                        Arguments = $"stop MYSQL{port}",
                        Verb = "runas",
                        UseShellExecute = true,
                        WindowStyle = ProcessWindowStyle.Hidden
                    }).WaitForExit();
                    Process.Start(new ProcessStartInfo
                    {
                        FileName = Directory.GetParent(LocalInstance).Parent.FullName + @"\bin\mysqld.exe",
                        Arguments = $"--remove MYSQL{port}",
                        Verb = "runas",
                        UseShellExecute = true,
                        WindowStyle = ProcessWindowStyle.Hidden
                    }).WaitForExit();
                }

                MySqlConnection.ClearPool(connection as MySqlConnection);
            }
        }
    }


    public class SQLiteDatabaseConnector : Database
    {
        public SQLiteDatabaseConnector()
        {
            DbType = Database_Type.SQLite;
        }

        public override bool ConnectToServer(int port = int.MinValue)
        {
            throw new NotImplementedException();
        }

        public override bool ConnectToDatabase(string databaseName, int port = int.MinValue)
        {
            throw new NotImplementedException();
        }

        public override bool ConnectToFile(string location = null)
        {
            try
            {
                connection = new SQLiteConnection($"Data Source={location};Version=3;");
                connection.Open();
                ConType = Connection_Type.File;
                return true;
            }
            catch (Exception e)
            {
                return false;
            }
        }

        public override bool RemoteConnection(Dictionary<string, string> dictionary)
        {
            throw new NotImplementedException();
        }

        public override List<string> GetDatabasesList()
        {
            throw new NotImplementedException();
        }

        public override List<string> GetTablesList(string database = null)
        {
            var tables = new List<string>();
            try
            {
                var dt = connection.GetSchema("Tables");
                foreach (DataRow row in dt.Rows)
                {
                    var tablename = (string) row[2];
                    tables.Add(tablename);
                }

                return tables;
            }
            catch (Exception e)
            {
                return null;
            }
        }

        public override DataTable GetTableInfo(string tableName = null)
        {
            try
            {
                var columnRestrictions = new string[4];
                if (tableName == null)
                    columnRestrictions[2] = SelectedTable;
                else
                    columnRestrictions[2] = tableName;

                var departmentIDSchemaTable = connection.GetSchema("Columns", columnRestrictions);
                TableColumns.Clear();
                var dv = departmentIDSchemaTable.DefaultView;
                departmentIDSchemaTable.DefaultView.Sort = "ORDINAL_POSITION Asc";
                var SortedView = dv.ToTable();
                foreach (DataRow row in SortedView.Rows) TableColumns.Add(new Column(row, DbType));
                return departmentIDSchemaTable;
            }
            catch (Exception ex)
            {
                return null;
            }
        }

        public override List<string[]> Read(string query, Func<IDataRecord, string[]> selector)
        {
            try
            {
                using (var cmd = (connection as SQLiteConnection).CreateCommand())
                {
                    cmd.CommandText = query;
                    using (var r = cmd.ExecuteReader())
                    {
                        return r.Cast<IDataRecord>().Select(selector).ToList();
                    }
                }
            }
            catch (Exception EX_NAME)
            {
                Console.WriteLine(EX_NAME.Message);
                return null;
            }
        }

        public override void CloseConnection()
        {
            if (connection != null && connection.State != ConnectionState.Closed)
            {
                connection.Close();
                SQLiteConnection.ClearPool(connection as SQLiteConnection);
            }
        }
    }

    public class PostGreSQLDatabaseConnector : Database
    {
        private string OnServerName = "";

        ///IMPORTANT!!!!!!
        private readonly string path_to_bin = @"C:\Program Files\PostgreSQL\10\bin";

        ///IMPORTANT!!!!!!
        public PostGreSQLDatabaseConnector()
        {
            DataConnectionString =
                "Server=127.0.0.1; Port=5432; User Id=root; Password=user;";
        }

        public override bool ConnectToServer(int port = 5432)
        {
            if (port != int.MinValue)
                DataConnectionString = $"Server=127.0.0.1; Port={port}; User Id=root; Password=user; Database=postgres";
            try
            {
                connection = new NpgsqlConnection(DataConnectionString);
                connection.Open();
            }
            catch (Exception e)
            {
                return false;
            }

            return true;
        }

        public override bool ConnectToDatabase(string databaseName, int port = 5432)
        {
            DataConnectionString =
                $"Server=127.0.0.1; Port={port}; User Id=root; Password=user; Database={databaseName};";
            try
            {
                connection = new NpgsqlConnection(DataConnectionString);
                connection.Open();
            }
            catch (Exception e)
            {
                return false;
            }

            return true;
        }

        public override bool ConnectToFile(string location = null)
        {
            try
            {
                var db_list = GetDatabasesList();
                OnServerName = "";
                for (var i = 0; i < 65536; i++)
                    if (!db_list.Contains("PostUser" + i))
                    {
                        OnServerName = "PostUser" + i;
                        break;
                    }

                var DestinationPath = Path.GetDirectoryName(location);
                var LocalInstance = Directory.GetParent(DestinationPath).Parent.FullName + @"\PostgreSQL\";
                var fileName = LocalInstance + "Import.bat";
                var file = File.ReadAllText(fileName);
                file = file.Replace("FROM", path_to_bin);
                file = file.Replace("IMPORT_PARAM", $"-U root {OnServerName} < {location}");
                File.WriteAllText(DestinationPath + $"\\{OnServerName}.bat", file);
                Process.Start(new ProcessStartInfo
                {
                    FileName = path_to_bin + @"\createdb.exe",
                    Arguments = $"-U root {OnServerName}",
                    Verb = "runas",
                    UseShellExecute = true,
                    WindowStyle = ProcessWindowStyle.Hidden
                }).WaitForExit();
                Process.Start(new ProcessStartInfo
                {
                    FileName = DestinationPath + $"\\{OnServerName}.bat",
                    Verb = "runas",
                    UseShellExecute = true
                }).WaitForExit();
                if (ConnectToDatabase(OnServerName))
                    ConType = Connection_Type.File;
                else return false;
                return true;
            }
            catch (Exception e)
            {
                return false;
            }
        }

        public override bool RemoteConnection(Dictionary<string, string> dictionary)
        {
            throw new NotImplementedException();
        }

        public override List<string> GetDatabasesList()
        {
            try
            {
                if (connection == null || connection.State != ConnectionState.Open)
                    ConnectToServer();
                var command = (connection as NpgsqlConnection).CreateCommand();
                command.CommandText = "SELECT datname FROM pg_database;";
                using (var Reader = command.ExecuteReader())
                {
                    var rows = new List<string>();
                    while (Reader.Read())
                        for (var i = 0; i < Reader.FieldCount; i++)
                            rows.Add(Reader.GetValue(i).ToString());
                    return rows;
                }
            }
            catch (Exception)
            {
                return null;
            }
        }

        public override List<string> GetTablesList(string database = null)
        {
            var tables = new List<string>();
            try
            {
                var dt = connection.GetSchema("Tables");
                foreach (DataRow row in dt.Rows)
                    if (row[1].ToString() == "public")
                    {
                        var tablename = (string) row[2];
                        tables.Add(tablename);
                    }

                tables.Sort();
                return tables;
            }
            catch (Exception e)
            {
                return null;
            }
        }

        public override DataTable GetTableInfo(string tableName = null)
        {
            try
            {
                var columnRestrictions = new string[4];
                if (tableName == null)
                    columnRestrictions[2] = SelectedTable;
                else
                    columnRestrictions[2] = tableName;

                var departmentIDSchemaTable = connection.GetSchema("Columns", columnRestrictions);
                TableColumns.Clear();
                var dv = departmentIDSchemaTable.DefaultView;
                departmentIDSchemaTable.DefaultView.Sort = "ORDINAL_POSITION Asc";
                var SortedView = dv.ToTable();
                foreach (DataRow row in SortedView.Rows) TableColumns.Add(new Column(row, DbType));
                return departmentIDSchemaTable;
            }
            catch (Exception ex)
            {
                return null;
            }
        }

        public override List<string[]> Read(string query, Func<IDataRecord, string[]> selector)
        {
            try
            {
                using (var cmd = (connection as NpgsqlConnection).CreateCommand())
                {
                    cmd.CommandText = query;
                    using (var r = cmd.ExecuteReader())
                    {
                        return r.Cast<IDataRecord>().Select(selector).ToList();
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                return null;
            }
        }

        public override void CloseConnection()
        {
            if (connection != null && connection.State != ConnectionState.Closed)
            {
                Process.Start(new ProcessStartInfo
                {
                    FileName = path_to_bin + @"\dropdb.exe",
                    Arguments = $"-U root {OnServerName}",
                    Verb = "runas",
                    UseShellExecute = true,
                    WindowStyle = ProcessWindowStyle.Hidden
                }).WaitForExit();
                connection.Close();
            }
        }
    }
}