using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using MySql.Data.MySqlClient;

namespace DbComparer
{
    static class Functions
    {
        static public DbColumnInfo SQLgetColumnsName(SqlConnection Connection, string TableName)
        {
            DbColumnInfo db = new DbColumnInfo();
            using (SqlCommand command = Connection.CreateCommand())
            {
                command.CommandText = "select c.name from sys.columns c inner join sys.tables t on t.object_id = c.object_id and t.name = '"+ TableName+"' and t.type = 'U'";
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        db.Name.Add(reader.GetString(0));
                        db.SqlDataType.Add(reader.GetFieldType(0));
                    }
                }
            }
            return db;
        }
        

        static public string[] MySQLgetColumnsName(string Connection, string TableName)
        {
            List<string> listacolumnas = new List<string>();
            MySqlConnection con = new MySqlConnection(Connection);
            con.Open();
            using (MySqlCommand command = new MySqlCommand("SELECT * FROM "+ TableName, con))
            {
                MySqlDataReader reader = command.ExecuteReader();
                reader.Read();
                {
                    for (int i = 0; i < reader.FieldCount; i++)
                        listacolumnas.Add(reader.GetName(i));
                }
            }
            con.Close();
            return listacolumnas.ToArray();
        }
    }

    public class DbColumnInfo
    {
        public DbColumnInfo()
        {
            Name=new List<string>();
            SqlDataType=new List<Type>();
        }

        public List<string> Name { get; set; }
        public List<Type> SqlDataType { get; set; }
    }
}
