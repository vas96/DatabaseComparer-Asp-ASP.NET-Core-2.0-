using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
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
                Func<IDataRecord, DataRow> select = delegate(IDataRecord s)
                {
                    DataRow dr = dtMaths.NewRow();
                    dr.ItemArray = new object[4];
                    for (int i = 0; i < s.FieldCount; i++)
                    {
                        dr.ItemArray[i] = s[i];
                    }

                    return dr;
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
                Func<IDataRecord, string> select = delegate(IDataRecord s)
                {
                    return String.Format("{0} {1}", s[0], s[1]);
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
                Func<IDataRecord, DataRow> select = delegate(IDataRecord s)
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
                var l3 = comp.FindDifferences();
                db.CloseConnection();
            }
        }

        [TestMethod]
        public void FullSQLTest()
        {
            //Створюєм компарер
            DatabaseComparer comp = new DatabaseComparer();

            //Створюєм дві бази даних в компарері
            comp.FirstDatabase = new SqlDataBaseConnector();
            comp.SecondDatabase = new SqlDataBaseConnector();

            //Підключаємся до баз даних
            var conf1 = comp.FirstDatabase.ConnectToFile(@"D:\GitHub\TeamBuilding.mdf");
            var conf2 = comp.SecondDatabase.ConnectToFile(@"D:\GitHub\TeamBuilding (2).mdf");

            //Обираєм які таблички порівнювати
            comp.FirstDatabase.SelectedTable = comp.SecondDatabase.SelectedTable = "Users";

            //Витягуєм структуру з обраних табличок
            comp.FirstDatabase.GetTableInfo();
            comp.SecondDatabase.GetTableInfo();

            //Заносимо отриману структуру без змін (але можна змінювати, якшо типи співпадають)
            foreach (var item in comp.FirstDatabase.TableColumns)
            {
                comp.FirstDatabase.SelectedColumns.Add(item.Name);
            }

            foreach (var item in comp.SecondDatabase.TableColumns)
            {
                comp.SecondDatabase.SelectedColumns.Add(item.Name);
            }

            Stopwatch sw = new Stopwatch();
            sw.Start();
            //Створюємо запити на вибірку даних
            string query1 = comp.FirstDatabase.BuildSelectQuery();
            string query2 = comp.SecondDatabase.BuildSelectQuery();

            //Ствоюєм контейнер для даних
            comp.FirstData = comp.FirstDatabase.BuildContainer();

            //Виконуєм вибірку даних
            comp.FirstData =
                (comp.FirstDatabase as SqlDataBaseConnector).Read(query1, comp.FirstDatabase.FullRowSelector);

            //То саме, тільки з другов таблицев
            comp.SecondDatabase.BuildContainer();
            comp.SecondData =
                (comp.SecondDatabase as SqlDataBaseConnector).Read(query2, comp.SecondDatabase.FullRowSelector);
            sw.Stop();
            //Собсно, порівняння
            var res1 = comp.CompareColumns(1);
            var res2 = comp.CompareColumns(2);
            var res3 = comp.CompareColumns(3);
            var test = comp.GetFullRows(res1[0], 0, 1);
            System.Console.Write(sw.Elapsed);
        }

        [TestMethod]
        public void TestTwoMySQLInstance()
        {
            try
            {
                //шлях до Mysql
                string FileName = @"C:\Program Files\MariaDB 10.2\bin\mysqld.exe";
                //створюєм 2 сервери, кожен в свой  папці, кожен на своєму порті
                Process.Start(FileName, @"--user=root --password=danyliv --datadir=D:\GitHub\testFolder\F1 --port=1111");
                //пробуєм підключитис до тих серверів
                var con1 = new MySqlConnection("SERVER=localhost;Port=1111;UID='root';PASSWORD='danyliv';");
                con1.Open();
                Process.Start(FileName, @"--user=root --password=danyliv --datadir=D:\GitHub\testFolder\F2 --port=2222");
                var con2 = new MySqlConnection("SERVER=localhost;Port=2222;UID='root';PASSWORD='danyliv';");
                con2.Open();
                //виконуєм різні скрипти на тих серверах
                //1-й
                var DbList1 = GetDatabasesList(con1);
                MySqlScript script1 = new MySqlScript((con1 as MySqlConnection), File.ReadAllText(@"D:\GitHub\testFolder\F1\Site.sql"));
                script1.Delimiter = ";";
                script1.Execute();
                var NewDbName1 = GetDatabasesList(con1).Except(DbList1).First();
                //2-й
                var DbList2 = GetDatabasesList(con2);
                MySqlScript script2 = new MySqlScript((con2 as MySqlConnection), File.ReadAllText(@"D:\GitHub\testFolder\F2\sakila.sql"));
                script2.Delimiter = ";";
                script2.Execute();
                var NewDbName2 = GetDatabasesList(con2).Except(DbList2).First();
                Console.WriteLine("Хопа!");
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }

        }

        public List<string> GetDatabasesList(MySqlConnection connection)
        {
            try
            {
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
    }
}