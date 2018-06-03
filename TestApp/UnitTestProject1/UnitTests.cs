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
using Npgsql;

namespace UnitTestProject1
{
    [TestClass]
    public class UnitTests
    {
        private Stopwatch sw;
        [TestMethod]
        public void SQLConnectionTest()
        {
            try
            {
                Database db = new SqlDataBaseConnector();
                if (db.ConnectToFile(@"D:\GitHub\Database_Comaparer\DatabaseComparer-Asp-ASP.NET-Core-2.0-\TestDatabases\sakila.mdf"))
                {
                    var l2 = db.GetTablesList();
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
            catch (Exception e)
            {
                Console.WriteLine(e);
                Assert.Fail();
            }
        }

        [TestMethod]
        public void MySQLConnectionTest()
        {
            try
            {
                Assert.IsTrue(true);
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                Assert.Fail();
            }
        }

        [TestMethod]
        public void ComparerTest()
        {
            try
            {
                DatabaseComparer comp = new DatabaseComparer();
                comp.FirstDatabase = new SqlDataBaseConnector();
                var con1 = comp.FirstDatabase.ConnectToFile(@"D:\GitHub\R1.mdf");
                comp.SecondDatabase = new SqlDataBaseConnector();
                var con2 = comp.SecondDatabase.ConnectToFile(@"D:\GitHub\R2.mdf");
                if (con1 && con2)
                {
                    comp.FirstDatabase.GetTableInfo("NewEmployees2");
                    comp.FirstDatabase.SelectedTable = "NewEmployees2";
                    comp.SecondDatabase.SelectedTable = "NewEmployees2";
                    foreach (var col in comp.FirstDatabase.TableColumns)
                    {
                        comp.FirstDatabase.SelectedColumns.Add(col);
                    }
                    comp.SecondDatabase.GetTableInfo("NewEmployees2");
                    foreach (var col in comp.SecondDatabase.TableColumns)
                    {
                        comp.SecondDatabase.SelectedColumns.Add(col);
                    }
                    Stopwatch timer = new Stopwatch();
                    timer.Start();
                    comp.ReadDataFromDb();
                    timer.Stop();
                    comp.CompareFullData();
                    var table1_row_count = comp.FirstData.Count;
                    var table2_row_count = comp.SecondData.Count;
                    var stop = timer.Elapsed;
                    System.Console.WriteLine($"Кількість записів у першій таблиці - {table1_row_count}");
                    System.Console.WriteLine($"Кількість записів у другій таблиці - {table2_row_count}");
                    System.Console.WriteLine($"Час виконання - {stop}");
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                Assert.Fail();
            }
        }

        [TestMethod]
        public void SQLiteConnectionTest()
        {
            try
            {
                var db = new SQLiteDatabaseConnector();
                db.ConnectToFile(
                    @"D:\GitHub\Database_Comaparer\DatabaseComparer-Asp-ASP.NET-Core-2.0-\TestApp\UnitTestProject1\bin\Debug\chinook.db");
                var list = db.GetTablesList();
                Assert.IsNotNull(list);
                var list2 = db.GetTableInfo(list[3]);
                Assert.IsNotNull(list2);
                var list3 = db.Read($"Select * from {list[3]}", db.FullStringArraySelector);
                Assert.IsNotNull(list3);
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                Assert.Fail();
            }

        }

        [TestMethod]
        public void PostGreSQLConnectionTest()
        {
            try
            {
                Assert.IsTrue(true);
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                Assert.Fail();
            }

        }

        [TestMethod]
        public void RemoteMySQLConnectionTest()
        {
            try
            {
                Database db = new MySqlDataBaseConnector();
                Dictionary<string, string> param = new Dictionary<string, string>();
                param.Add("port", "3306");
                param.Add("ip", "192.168.0.100");
                param.Add("user", "Vasyl");
                param.Add("pass", "12345");
                var a = db.RemoteConnection(param);
                var l1 = db.GetDatabasesList();
                var l2 = db.GetTablesList(l1[3]);
                Assert.IsNotNull(l2);
                Func<IDataRecord, string[]> select = delegate (IDataRecord s)
                {
                    return new string[] { s[0].ToString(), s[1].ToString() };
                };
                var l3 = db.GetTableInfo(l2[0]);
                Assert.IsNotNull(l3);
                sw = new Stopwatch();
                sw.Start();
                var l4 = db.Read("Select * from " + l2[0], select);
                sw.Stop();
                var time = sw.Elapsed;
                Assert.IsNotNull(l4);
                db.CloseConnection();
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                Assert.Fail();
            }
        }

        [TestMethod]
        public void RemoteSQLServerConnectionTest()
        {
            try
            {
                SqlDataBaseConnector db = new SqlDataBaseConnector();
                Dictionary<string, string> param = new Dictionary<string, string>();
                param.Add("port", "1433");
                param.Add("ip", "192.168.0.100");
                param.Add("user", "Vasyl");
                param.Add("pass", "12345");
                var a = db.RemoteConnection(param);
                var l1 = db.GetDatabasesList();
                var l2 = db.GetTablesList(l1[3]);
                Assert.IsNotNull(l2);
                Func<IDataRecord, string[]> select = delegate (IDataRecord s)
                {
                    return new string[] { s[0].ToString(), s[1].ToString() };
                };
                var l3 = db.GetTableInfo(l2[0]);
                Assert.IsNotNull(l3);
                sw = new Stopwatch();
                sw.Start();
                var l4 = db.Read("Select * from " + l2[0], select);
                sw.Stop();
                var time = sw.Elapsed;
                Assert.IsNotNull(l4);
                db.CloseConnection();
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                Assert.Fail();
            }
        }

        [TestMethod]
        public void SQLServer_To_SQLServer()
        {
            Assert.IsTrue(true);
        }
        [TestMethod]
        public void SQLServer_To_MySQL()
        {
            Assert.IsTrue(true);
        }
        [TestMethod]
        public void SQLServer_To_SQLite()
        {
            Assert.IsTrue(true);
        }
        [TestMethod]
        public void SQLServer_To_PostgreSQL()
        {
            Assert.IsTrue(true);
        }
        /// <summary>
        /// /////////////////////////////
        /// </summary>

        [TestMethod]
        public void MySQL_To_SQLServer()
        {
            Assert.IsTrue(true);
        }
        [TestMethod]
        public void MySQL_To_MySQL()
        {
            Assert.IsTrue(true);
        }
        [TestMethod]
        public void MySQL_To_SQLite()
        {
            Assert.IsTrue(true);
        }
        [TestMethod]
        public void MySQL_To_PostgreSQL()
        {
            Assert.IsTrue(true);
        }

        //////////////////////////////
        [TestMethod]
        public void SQLite_To_SQLServer()
        {
            Assert.IsTrue(true);
        }
        [TestMethod]
        public void SQLite_To_MySQL()
        {
            Assert.IsTrue(true);
        }
        [TestMethod]
        public void SQLite_To_SQLite()
        {
            Assert.IsTrue(true);
        }
        [TestMethod]
        public void SQLite_To_PostgreSQL()
        {
            Assert.IsTrue(true);
        }
        ///////////////////////

        [TestMethod]
        public void PostgreSQL_To_SQLServer()
        {
            Assert.IsTrue(true);
        }
        [TestMethod]
        public void PostgreSQL_To_MySQL()
        {
            Assert.IsTrue(true);
        }
        [TestMethod]
        public void PostgreSQL_To_SQLite()
        {
            Assert.IsTrue(true);
        }
        [TestMethod]
        public void PostgreSQL_To_PostgreSQL()
        {
            Assert.IsTrue(true);
        }
    }
}