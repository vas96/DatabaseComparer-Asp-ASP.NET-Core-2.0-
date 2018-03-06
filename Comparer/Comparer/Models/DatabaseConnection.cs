using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using Jdforsythe.MySQLConnection;
using Microsoft.AspNetCore.Http;
using MySql.Data.MySqlClient;

namespace DbComparer
{
    interface IDatabase
    {
        /// <summary>
        /// Підключення до сервера
        /// </summary>
        /// <returns>True якщо підключення встановлено</returns>
        bool ConnectToServer();

        /// <summary>
        /// Підключення до конктретної бази даних на сервері
        /// </summary>
        /// <param name="databaseName">Ім'я бази даних</param>
        /// <returns>True якщо підключення встановлено</returns>
        bool ConnectToDatabase(string databaseName);

        /// <summary>
        /// Підключення до файлу бази данних
        /// </summary> 
        /// <param name="location">Шлях до файлу. Якщо не заданий - заново відкривається підключення</param>
        /// <returns>True, якщо підключення встановлено</returns>
        bool ConnectToFile(string location = null);

        /// <summary>
        /// Повертає список баз даних на сервері
        /// </summary>
        /// <returns></returns>
        List<string> GetDatabasesList();

        /// <summary>
        /// Повертає список таблиць у базі даних (файлі чи сервері)
        /// </summary>
        /// <param name="database"></param>
        /// <returns></returns>
        List<string> GetTablesList(string database=null);

        /// <summary>
        /// Повертає всю інформацію про колонки у таблиці
        /// </summary>
        /// <param name="tableName">Назва таблиці. Якщо null - по замовчуванню обирається SelectedTable</param>
        /// <returns></returns>
        DataTable GetTableInfo(string tableName);

        /// <summary>
        /// Зчитує дані з таблиці
        /// </summary>
        /// <typeparam name="Selected">String</typeparam>
        /// <param name="query">Запит</param>
        /// <param name="selector">Селектор (міститься у БД)</param>
        /// <returns></returns>
        Selected[] Read<Selected>(string query, Func<IDataRecord, Selected> selector);

        /// <summary>
        /// Закриває підключення
        /// </summary>
        void CloseConnection();

        /// <summary>
        /// Будує стандартний запит на вибірку
        /// </summary>
        /// <param name="table">Ім'я таблиці. Якщо null - використовується SelectedTable</param>
        /// <returns></returns>
        string BuildSelectQuery(string table = null);

    }

    public abstract class Database : IDatabase
    {
        /// <summary>
        /// ІНІЦІАЛІЗАЦІЯ...
        /// </summary>
        public Database()
        {
            DataConnectionString = null;
            SelectedTable = null;
            SelectedDatabase = null;
            connection = null;
            TableColumns = new List<Column>();
            SelectedColumns = new List<string>();
        }
        public string DataConnectionString;

        public string SelectedDatabase;

        public string SelectedTable;

        public DbConnection connection;

        public List<Column> TableColumns;

        public List<string> SelectedColumns;

        public Func<IDataRecord, string> ParticallSelector = delegate (IDataRecord s)
        {
            return String.Format("{0} {1}", s[0], s[1]);
        };

        protected static int counter;

        public Func<IDataRecord, string> FullSelector = delegate (IDataRecord s)
        {
            string str = "";
            for (int i = 0; i < counter; i++)
            {
                str += s[i] + " ";
            }

            return str.Remove(str.Length - 2, 2);
        };

        public Database_Type DbType;

        public abstract bool ConnectToServer();
        public abstract bool ConnectToDatabase(string databaseName);
        public abstract bool ConnectToFile(string location = null);
        public abstract List<string> GetDatabasesList();
        public abstract List<string> GetTablesList(string database=null);
        public abstract DataTable GetTableInfo(string tableName=null);

        public abstract Selected[] Read<Selected>(string query, Func<IDataRecord, Selected> selector);

        public abstract void CloseConnection();

        public string BuildSelectQuery(string table = null)
        {
            string Select = "SELECT ";
            foreach (var item in SelectedColumns)
            {
                Select += item + ", ";
            }
            Select = Select.Remove(Select.Length - 2, 2);
            Select += " FROM " + ((table == null) ? SelectedTable : table);
            return Select;
        }

        public static Database_Type GetDBType(IFormFile file)
        {
            string ext = Path.GetExtension(file.FileName).Replace(".", "").ToLower();
            switch (ext)
            {
                case "mdf":
                    {
                        return Database_Type.SqlServer; break;
                    }
                case "sql":
                    {
                        return Database_Type.MySql; break;
                    }
                case "xml":
                    {
                        return Database_Type.XML; break;
                    }
                default:
                    {
                        return Database_Type.NONE; break;
                    }
            }
        }


        public static Database InitializeType(IFormFile file)
        {

            Database.Database_Type type = Database.GetDBType(file);
            switch (type)
            {
                case Database_Type.MySql:
                    { return new MySqlDataBaseConnector() { DbType = Database_Type.MySql }; break; }
                case Database_Type.SqlServer:
                    { return new SqlDataBaseConnector() { DbType = Database_Type.SqlServer }; ; break; }
                default:
                    {
                        return null;
                        break;
                    }
            }
        }


        public enum Database_Type
        {
            SqlServer,
            MySql,
            Oracle,
            XML,
            NONE
        }

        public class Column
        {
            public string Name;
            public string Type;
            public string Length;

            public Column(DataRow dr)
            {
                var array = dr.ItemArray;
                Name = array[3].ToString();
                Type = array[7].ToString();
                Length = array[8].ToString();
            }
        }
    }

    public class SqlDataBaseConnector : Database
    {
        public SqlDataBaseConnector():base()
        {
            DataConnectionString = "Data Source=.; Integrated Security=True;";
        }

        public override bool ConnectToServer()
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

        public override bool ConnectToDatabase(string databaseName)
        {
            try
            {
                DataConnectionString = "Data Source=.; Integrated Security=True; Initial Catalog =" + databaseName + ";";
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
                        "Data Source=(localdb)\\MSSQLLocalDB;AttachDbFilename=" + location + ";Integrated Security=True;Connect Timeout=30;Encrypt=False;TrustServerCertificate=True;ApplicationIntent=ReadWrite;MultiSubnetFailover=False;";
                connection = new SqlConnection(DataConnectionString);
                connection.Open();
                return true;
            }
            catch (Exception e)
            {
                return false;
            }
        }

        public override List<string> GetDatabasesList()
        {
            List<string> list = new List<string>();

            using (connection)
            {
                using (SqlCommand cmd = new SqlCommand("SELECT name from sys.databases", (connection as SqlConnection)))
                {
                    using (IDataReader dr = cmd.ExecuteReader())
                    {
                        while (dr.Read())
                        {
                            list.Add(dr[0].ToString());
                        }
                    }
                }
            }
            return list;
        }

        public override List<string> GetTablesList(string database=null)
        {
            List<string> list = new List<string>();
            DataTable schema = connection.GetSchema("Tables");
            foreach (DataRow row in schema.Rows)
            {
                list.Add(row[2].ToString());
            }
            return list;
        }

        public override DataTable GetTableInfo(string tableName = null)
        {

            try
            {
                String[] columnRestrictions = new String[4];

                // For the array, 0-member represents Catalog; 1-member represents Schema; 
                // 2-member represents Table Name; 3-member represents Column Name. 
                // Now we specify the Table_Name and Column_Name of the columns what we want to get schema information.
                if (tableName == null)
                    columnRestrictions[2] = SelectedTable;
                else
                    columnRestrictions[2] = tableName;

                DataTable departmentIDSchemaTable = connection.GetSchema("Columns", columnRestrictions);
                TableColumns.Clear();
                DataView dv = departmentIDSchemaTable.DefaultView;
                departmentIDSchemaTable.DefaultView.Sort = "ORDINAL_POSITION Asc";
                var SortedView = dv.ToTable();
                foreach (DataRow row in SortedView.Rows)
                {
                    TableColumns.Add(new Column(row));
                }
                return departmentIDSchemaTable;
            }
            catch (Exception ex)
            {
                return null;
            }
        }

        public override Selected[] Read<Selected>(string query, Func<IDataRecord, Selected> selector)
        {
            try
            {
                using (var cmd = connection.CreateCommand())
                {
                    if (SelectedColumns.Count == 0)
                    {
                        counter = GetTableInfo(SelectedTable).Rows.Count;
                    }
                    else
                    {
                        counter = SelectedColumns.Count;
                    }
                    cmd.CommandText = query;
                    using (var r = cmd.ExecuteReader())
                        return ((DbDataReader)r).Cast<IDataRecord>().Select(selector).ToArray();
                }
            }
            catch (Exception EX_NAME)
            {
                return null;
            }
        }

        public override void CloseConnection()
        {
            if (connection!=null && connection.State!=ConnectionState.Closed)
            {
                connection.Close();
                SqlConnection.ClearPool(connection as SqlConnection);
            }
        }
    }

    public class MySqlDataBaseConnector : Database
    {
        public MySqlDataBaseConnector():base()
        {
            DataConnectionString = "SERVER=localhost;UID='root';" + "PASSWORD='';";
        }

        public override bool ConnectToServer()
        {
            try
            {
                connection = new MySqlConnection(DataConnectionString);
                connection.Open();
                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }

        public override bool ConnectToDatabase(string databaseName)
        {
            try
            {
                DataConnectionString = "SERVER=localhost;DATABASE=" + databaseName +
                     ";UID=root; PASSWORD='';";
                connection = new MySqlConnection(DataConnectionString);
                connection.Open();
                return true;
            }
            catch (Exception EX_NAME)
            {
                return false;
            }

        }

        /// <summary>
        /// TODO
        /// </summary>
        /// <returns></returns>
        public override bool ConnectToFile(string location = null)
        {
            throw new NotImplementedException();
        }

        public override List<string> GetDatabasesList()
        {
            try
            {
                if (connection == null || connection.State != ConnectionState.Open)
                    ConnectToServer();
                MySqlCommand command = (connection as MySqlConnection).CreateCommand();
                command.CommandText = "SHOW DATABASES;";
                using (MySqlDataReader Reader = command.ExecuteReader())
                {
                    List<string> rows = new List<string>();
                    while (Reader.Read())
                    {
                        for (int i = 0; i < Reader.FieldCount; i++)
                            rows.Add(Reader.GetValue(i).ToString());
                    }
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
                ConnectToDatabase(database);
                MySqlCommand command = (connection as MySqlConnection).CreateCommand();
                command.CommandText = "SHOW TABLES;";
                using (MySqlDataReader Reader = command.ExecuteReader())
                {
                    List<string> rows = new List<string>();
                    while (Reader.Read())
                    {
                        for (int i = 0; i < Reader.FieldCount; i++)
                            rows.Add(Reader.GetValue(i).ToString());
                    }
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
                String[] columnRestrictions = new String[4];
                columnRestrictions[2] = tableName;

                DataTable departmentIDSchemaTable = connection.GetSchema("Columns", columnRestrictions);
                return departmentIDSchemaTable;
            }
            catch (Exception ex)
            {
                return null;
            }
        }

        public override Selected[] Read<Selected>(string query, Func<IDataRecord, Selected> selector)
        {
            try
            {
                using (var cmd = (connection as MySqlConnection).CreateCommand())
                {
                    counter = GetTableInfo(SelectedTable).Rows.Count;
                    cmd.CommandText = query;
                    using (MySqlDataReader r = cmd.ExecuteReader())
                        return ((DbDataReader)r).Cast<IDataRecord>().Select(selector).ToArray();
                }
            }
            catch (Exception EX_NAME)
            {
                System.Console.WriteLine(EX_NAME.Message);
                return null;
            }
        }

        public override void CloseConnection()
        {
            if (connection != null && connection.State != ConnectionState.Closed)
            {
                connection.Close();
                MySqlConnection.ClearPool(connection as MySqlConnection);
            }
        }
    }
}