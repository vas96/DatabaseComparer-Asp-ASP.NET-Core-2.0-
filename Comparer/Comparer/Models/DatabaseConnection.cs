using System;
using System.CodeDom;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DBTest;
using Jdforsythe.MySQLConnection;
using Microsoft.AspNetCore.Http;
using Microsoft.EntityFrameworkCore.Query.Expressions;
using MySql.Data.MySqlClient;
using ObjectsComparer;

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
        List<string> GetTablesList(string database = null);

        /// <summary>
        /// Повертає всю інформацію про колонки у таблиці
        /// </summary>
        /// <param name="tableName">Назва таблиці. Якщо null - по замовчуванню обирається SelectedTable</param>
        /// <returns></returns>
        DataTable GetTableInfo(string tableName);

        /// <summary>
        /// Зчитує дані з таблиці (для селектора по string)
        /// </summary>
        /// <typeparam name="Selected">String</typeparam>
        /// <param name="query">Запит</param>
        /// <param name="selector">Селектор (міститься у БД)</param>
        /// <returns></returns>
        List<string[]> Read(string query, Func<IDataRecord, string[]> selector);
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
            FileName = null;
        }

        public string FileName;
        /// <summary>
        /// Рядок підключення
        /// </summary>
        public string DataConnectionString;

        /// <summary>
        /// Обрана база даних
        /// </summary>
        public string SelectedDatabase;

        /// <summary>
        /// Обрана таблиця
        /// </summary>
        public string SelectedTable;

        /// <summary>
        /// Підлючення
        /// </summary>
        public DbConnection connection;

        /// <summary>
        /// Інформація про колонки активної таблиці
        /// </summary>
        public List<Column> TableColumns;

        /// <summary>
        /// Колонки, обрані для порівняння
        /// </summary>
        public List<string> SelectedColumns;

        /// <summary>
        /// Селектор
        /// Повертає дані у вигляді суцільного рядка
        /// </summary>
        public Func<IDataRecord, string> FullStringSelector = delegate (IDataRecord s)
        {
            object[] obj = new object[s.FieldCount];
            for (int i = 0; i < s.FieldCount; i++)
            {
                obj[i] = s[i];
            }
            return String.Join(" ", obj);
        };

        /// <summary>
        /// Селектор
        /// Повертає дані у вигляді масиву рядків
        /// </summary>
        public Func<IDataRecord, string[]> FullStringArraySelector = delegate (IDataRecord s)
        {
            int counter = s.FieldCount;

            string[] array = new string[counter];
            for (int i = 0; i < counter; i++)
            {
                array[i] = s[i].ToString();
            }
            return array;
        };

        /// <summary>
        /// Тип активної БД
        /// </summary>
        public Database_Type DbType;

        public abstract bool ConnectToServer();
        public abstract bool ConnectToDatabase(string databaseName);
        public abstract bool ConnectToFile(string location = null);
        public abstract List<string> GetDatabasesList();
        public abstract List<string> GetTablesList(string database = null);
        public abstract DataTable GetTableInfo(string tableName = null);
        public abstract List<string[]> Read(string query, Func<IDataRecord, string[]> selector);

        public abstract void CloseConnection();

        /// <summary>
        /// Будує запит до БД на основі SelectedColumns
        /// </summary>
        /// <param name="table"></param>
        /// <returns></returns>
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

        /// <summary>
        /// Визначає тип БД
        /// </summary>
        /// <param name="file"></param>
        /// <returns></returns>
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

        /// <summary>
        /// Визначає тип БД та виконує присвоєння
        /// </summary>
        /// <param name="file"></param>
        /// <returns></returns>
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

        /// <summary>
        /// Можливі типи БД
        /// </summary>
        public enum Database_Type
        {
            SqlServer,
            MySql,
            Oracle,
            XML,
            NONE
        }

        /// <summary>
        /// Дані про колонки таблиці
        /// </summary>
        public class Column
        {
            public int Position;
            public string Name;
            public string Type;
            public string Length;
            public bool ISKey;

            /// <summary>
            /// Витянує такі дані про колонку як:
            /// Позиція, Імя, Тип, Довжина, ЧиЄКлючем
            /// </summary>
            /// <param name="dr"></param>
            public Column(DataRow dr)
            {
                var array = dr.ItemArray;
                Position = Int32.Parse(array[4].ToString()) - 1;//Позиція
                Name = array[3].ToString();//Імя
                Type = array[7].ToString();//Тип
                Length = array[8].ToString();//Довжина
                ISKey = false;//Ключ
            }
        }
    }

    public class SqlDataBaseConnector : Database
    {
        public SqlDataBaseConnector() : base()
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
                DataConnectionString =
                    "Data Source=.; Integrated Security=True; Initial Catalog =" + databaseName + ";";
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
                        ";Integrated Security=True;Connect Timeout=30;Encrypt=False;TrustServerCertificate=True;ApplicationIntent=ReadWrite;MultiSubnetFailover=False;";
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

        public override List<string> GetTablesList(string database = null)
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

                /////////
                string _table = (tableName == null) ? SelectedTable : tableName;
                SqlDataAdapter adapter = new
                    SqlDataAdapter("SELECT TOP 1 * FROM " + _table,
                        (connection as SqlConnection));
                adapter.MissingSchemaAction = MissingSchemaAction.AddWithKey;
                DataTable table = new DataTable();
                adapter.Fill(table);
                using (DataTableReader reader = new DataTableReader(table))
                {
                    int ordinal = 0;
                    DataTable schemaTable = reader.GetSchemaTable();
                    for (int i = 0; i < schemaTable.Rows.Count; i++)
                    {
                        if ((bool)schemaTable.Rows[i].ItemArray[12])
                        {
                            TableColumns[i].ISKey = true;
                        }
                    }
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
                        return ((DbDataReader)r).Cast<IDataRecord>().Select(selector).ToList();
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

        public void Test()
        {
            DataTable dtMaths = new DataTable("Maths");
            var a = typeof(int);
            dtMaths.Columns.Add("StudID1", typeof(string));
            dtMaths.Columns.Add("StudName1", typeof(string));
            dtMaths.Columns.Add("Fuck1", typeof(string));
            dtMaths.Rows.Add(1, "Mike", "Fuck");
            dtMaths.Rows.Add(2, "Mukesh", "Fuck");
            dtMaths.Rows.Add(3, "Erin", "Fuck");
            dtMaths.Rows.Add(4, "Roshni", "Fuck");
            dtMaths.Rows.Add(5, "Ajay", "Fuck");

            DataTable dtEnglish = new DataTable("English");
            dtEnglish.Columns.Add("StudID", typeof(string));
            dtEnglish.Columns.Add("StudName", typeof(string));
            dtEnglish.Columns.Add("Fuck", typeof(string));
            dtEnglish.Rows.Add(6, "Arjun", "Fuck");
            dtEnglish.Rows.Add(2, "Mukesh", "Fuck");
            dtEnglish.Rows.Add(7, "John", "Fuck");
            dtEnglish.Rows.Add(4, "Roshni", "Fuck");
            dtEnglish.Rows.Add(8, "Kumar", "Fuck");

            var d1 = dtMaths.Rows.Cast<DataRow>().Select(i => i.ItemArray).ToArray();
            var d2 = dtEnglish.Rows.Cast<DataRow>().Select(i => i.ItemArray).ToArray();
            var d11 = new string[3][]
                {new string[3] {"1", "2", "4"}, new string[3] {"1", "2", "3"}, new string[3] {"1", "2", "0"}};
            var d22 = new string[3][]
                {new string[3] {"1", "2", "4"}, new string[3] {"1", "2", "5"}, new string[3] {"1", "2", "6"}};

            SqlDataBaseConnector sql = new SqlDataBaseConnector();
            var c1 = sql.ConnectToServer();
            var c2 = sql.ConnectToDatabase("Repair");
            sql.SelectedTable = "NewEmployees";
            sql.GetTableInfo("NewEmployees");
            foreach (var item in sql.TableColumns)
            {
                sql.SelectedColumns.Add(item.Name);
            }

            SqlDataBaseConnector sql2 = new SqlDataBaseConnector();
            var c12 = sql2.ConnectToServer();
            var c22 = sql2.ConnectToDatabase("Repair");
            sql2.SelectedTable = "NewEmployees2";
            sql2.GetTableInfo();
            foreach (var item in sql2.TableColumns)
            {
                sql2.SelectedColumns.Add(item.Name);
            }
            Stopwatch sw1 = new Stopwatch();
            Stopwatch sw2 = new Stopwatch();
            sw1.Start();
            IEnumerable<string[]> table1 = null, table2 = null;
            var table11 = new Task(() => { table1 = sql.Read(sql.BuildSelectQuery(), FullStringArraySelector); });
            table11.Start();
            var table22 = new Task(() => { table2 = sql2.Read(sql2.BuildSelectQuery(), FullStringArraySelector); });
            sw1.Stop();
            table22.Start();
            Task.WaitAll(table11, table22);
            sw2.Start();
            var dasda = table1.Except(table2, new StringArrayComparer());
            sw2.Stop();
            System.Console.WriteLine(1);
            //            IQueryable<DataRow> dr1 = dtMaths.Rows.Cast<DataRow>().AsQueryable();
            //            IQueryable<DataRow> dr2 = dtEnglish.Rows.Cast<DataRow>().AsQueryable();


            //            System.Data.DataView view = new System.Data.DataView(dtMaths);
            //            System.Data.DataTable selected = view.ToTable("Selected", false, dtMaths.Columns[0].ToString(),
            //                dtMaths.Columns[2].ToString());
            //            var dtOnlyMaths = dtMaths.Rows.Cast<DataRow>().Except(dtEnglish.Rows.Cast<DataRow>());

        }
    }

    public class MySqlDataBaseConnector : Database
    {
        public MySqlDataBaseConnector() : base()
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

        public override List<string[]> Read(string query, Func<IDataRecord, string[]> selector)
        {
            try
            {
                using (var cmd = (connection as MySqlConnection).CreateCommand())
                {
                    cmd.CommandText = query;
                    using (MySqlDataReader r = cmd.ExecuteReader())
                        return ((DbDataReader)r).Cast<IDataRecord>().Select(selector).ToList();
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