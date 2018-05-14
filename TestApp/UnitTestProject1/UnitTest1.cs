using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Data.SQLite;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using DbComparer;
using DBTest;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using MySql.Data.MySqlClient;

namespace UnitTestProject1
{
    [TestClass]
    public class UnitTest1
    {
        private Stopwatch sw;
        [TestMethod]
        public void SQLConnection()
        {
            Database db = new SqlDataBaseConnector();
            if (db.ConnectToServer())
            {
                var l1 = db.GetDatabasesList();
                Assert.IsNotNull(l1);
                var l2 = db.GetTablesList(l1[8]);
                Assert.IsNotNull(l2);
                DataTable dtMaths = new DataTable("Maths");
                dtMaths.Columns.Add("StudID", typeof(int));
                dtMaths.Columns.Add("StudName", typeof(string));
                Func<IDataRecord, string[]> select = delegate (IDataRecord s)
                {
                    return new string[] { s[0].ToString(), s[1].ToString() };
                };
                var l3 = db.GetTableInfo(l2[3]);
                Assert.IsNotNull(l3);
                sw = new Stopwatch();
                sw.Start();
                var l4 = db.Read("Select * from " + l2[17], select);
                sw.Stop();
                var time = sw.Elapsed;
                Assert.IsNotNull(l4);
                db.CloseConnection();
            }
        }

        [TestMethod]
        public void MySQLConnection()
        {
            Database db = new MySqlDataBaseConnector();
            if (db.ConnectToServer())
            {
                var l1 = db.GetDatabasesList();
                Assert.IsNotNull(l1);
                var l2 = db.GetTablesList(l1[3]);
                Assert.IsNotNull(l2);
                Func<IDataRecord, string[]> select = delegate (IDataRecord s)
                {
                    return new string[] { s[0].ToString(), s[1].ToString() };
                };
                var l3 = db.GetTableInfo(l2[17]);
                Assert.IsNotNull(l3);
                sw = new Stopwatch();
                sw.Start();
                var l4 = db.Read("Select * from " + l2[17], select);
                sw.Stop();
                var time = sw.Elapsed;
                Assert.IsNotNull(l4);
                db.CloseConnection();
            }
        }

        [TestMethod]
        public void Comparer()
        {
            DatabaseComparer comp = new DatabaseComparer();
            Database db = new SqlDataBaseConnector();
            if (db.ConnectToServer())
            {
                DataTable dtMaths = new DataTable("Maths");
                dtMaths.Columns.Add("StudID", typeof(int));
                dtMaths.Columns.Add("StudName", typeof(string));
                Func<IDataRecord, DataRow> select = delegate (IDataRecord s)
                {
                    DataRow dr = dtMaths.NewRow();
                    dr.ItemArray = new object[4];
                    for (int i = 0; i < s.FieldCount; i++)
                    {
                        dr.ItemArray[i] = s[i];
                    }

                    return dr;
                };
                var l1 = db.GetDatabasesList();
                var l2 = db.GetTablesList(l1[8]);
                var l3 = comp.CompareFullData();
                db.CloseConnection();
            }
        }

        [TestMethod]
        public void SQLiteConnection()
        {
            try
            {
                var db = new SQLiteDatabaseConnector();
                db.ConnectToFile(
                    @"D:\GitHub\Database_Comaparer\DatabaseComparer-Asp-ASP.NET-Core-2.0-\TestApp\UnitTestProject1\bin\Debug\chinook.db");
                var list = db.GetTablesList();
                var list2 = db.GetTableInfo(list[3]);
                var list3 = db.Read($"Select * from {list[3]}", db.FullStringArraySelector);
                Assert.IsTrue(true);
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                Assert.Fail();
            }

        }
    }
}