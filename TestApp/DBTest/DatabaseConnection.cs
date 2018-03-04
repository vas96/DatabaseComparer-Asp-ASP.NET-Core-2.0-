using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Windows.Forms;
using MySql.Data.MySqlClient;

namespace DbComparer
{
    interface IDatabase
    {
        bool ConnectToServer();

        bool ConnectToDatabase(string databaseName);

        bool ConnectToFile(string location = null);

        List<string> GetDatabasesList();

        List<string> GetTablesList(string database);

        DataSet GetTableInfo(string tableName);

        Selected[] Read<Selected>(string query, Func<IDataRecord, Selected> selector);

        void CloseConnection();

        string BuildSelectQuery(string table = null);

    }

    public abstract class Database : IDatabase
    {
        protected string DataConnectionString = null;

        public string SelectedDatabase;

        public string SelectedTable;

        protected DbConnection connection;

        public List<string> CollumnList;

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

        public enum SourceType { Server, File }

        public SourceType Source = SourceType.Server;

        public abstract bool ConnectToServer();
        public abstract bool ConnectToDatabase(string databaseName);
        public abstract bool ConnectToFile(string location = null);
        public abstract List<string> GetDatabasesList();
        public abstract List<string> GetTablesList(string database);
        public abstract DataSet GetTableInfo(string tableName);

        public abstract Selected[] Read<Selected>(string query, Func<IDataRecord, Selected> selector);

        public abstract void CloseConnection();

        public string BuildSelectQuery(string table = null)
        {
            string Select = "SELECT ";
            foreach (var item in CollumnList)
            {
                Select += item + ", ";
            }
            Select=Select.Remove(Select.Length - 2, 2);
            Select += " FROM " + ((table == null) ? SelectedTable : table);
            return Select;
        }
    }

    public class SqlDataBaseConnector : Database
    {
        public SqlDataBaseConnector()
        {
            DataConnectionString = "Data Source=.; Integrated Security=True;";
            CollumnList = new List<string>();
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
                List<string> list = new List<string>();
                if (connection == null || connection.State != ConnectionState.Open)
                    ConnectToServer();
                DataTable databases = connection.GetSchema("Databases");
                foreach (DataRow database in databases.Rows)
                {
                    String databaseName = database.Field<String>("database_name");
                    list.Add(databaseName);
                }
                return list;
            }
            catch (Exception e)
            {
                return null;
            }
        }

        public override List<string> GetTablesList(string database)
        {
            ConnectToDatabase(database);
            List<string> list = new List<string>();
            DataTable schema = connection.GetSchema("Tables");
            foreach (DataRow row in schema.Rows)
            {
                list.Add(row[2].ToString());
            }
            return list;
        }

        public override DataSet GetTableInfo(string tableName)
        {
            var sql = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS " +
                      "WHERE TABLE_NAME = '" + tableName +
                      "' ORDER BY ORDINAL_POSITION";
            try
            {
                SqlConnection conn = new SqlConnection(DataConnectionString);
                SqlCommand cmd = new SqlCommand(sql, conn);
                SqlDataAdapter da = new SqlDataAdapter(cmd);
                DataSet customerOrders = new DataSet();
                da.Fill(customerOrders, tableName);
                return customerOrders;
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
                    if (CollumnList.Count == 0)
                    {
                        counter = GetTableInfo(SelectedTable).Tables[0].Rows.Count;
                    }
                    else
                    {
                        counter = CollumnList.Count;
                    }
                    cmd.CommandText = query;
                    using (var r = cmd.ExecuteReader())
                        return ((DbDataReader)r).Cast<IDataRecord>().Select(selector).ToArray();
                }
            }
            catch (Exception EX_NAME)
            {
                MessageBox.Show(EX_NAME.Message);
                return null;
            }
        }

        public override void CloseConnection()
        {
            connection.Close();
        }
    }

    public class MySqlDataBaseConnector : Database
    {
        public MySqlDataBaseConnector()
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

        public override DataSet GetTableInfo(string tableName)
        {
            var sql = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS " +
                         "WHERE TABLE_NAME = '" + tableName +
                         "' ORDER BY ORDINAL_POSITION";
            try
            {
                MySqlConnection conn = new MySqlConnection(DataConnectionString);
                MySqlCommand cmd = new MySqlCommand(sql, conn);
                MySqlDataAdapter da = new MySqlDataAdapter(cmd);
                DataSet customerOrders = new DataSet();
                da.Fill(customerOrders, tableName);
                return customerOrders;
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
                    counter = GetTableInfo(SelectedTable).Tables[0].Rows.Count;
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
            connection.Close();
        }
    }
}
