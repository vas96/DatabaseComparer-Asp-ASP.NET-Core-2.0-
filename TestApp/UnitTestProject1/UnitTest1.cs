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

namespace UnitTestProject1
{
    [TestClass]
    public class UnitTest1
    {
        [TestMethod]
        public void TestMethod1()
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
                var l3 = db.GetTableInfo(l2[3]);
                Assert.IsNotNull(l3);
                var l4 = db.Read("Select * from " + l2[3], select);
                Assert.IsNotNull(l4);
                db.CloseConnection();
            }
        }
        [TestMethod]
        public void TestMethod2()
        {
            Database db = new MySqlDataBaseConnector();
            if (db.ConnectToServer())
            {
                var l1 = db.GetDatabasesList();
                Assert.IsNotNull(l1);
                var l2 = db.GetTablesList(l1[1]);
                Assert.IsNotNull(l2);
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
                var l3 = db.GetTableInfo(l2[3]);
                Assert.IsNotNull(l3);
                var l4 = db.Read("Select * from " + l2[3], select);
                Assert.IsNotNull(l4);
                db.CloseConnection();
            }
        }

        [TestMethod]
        public void TestMethod3()
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
                var l3 = comp.FindDifferences();
                db.CloseConnection();
            }
        }

        [TestMethod]
        public void TestMethod4()
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
            comp.FirstData = (comp.FirstDatabase as SqlDataBaseConnector).Read(query1, comp.FirstDatabase.FullRowSelector);
            
            //То саме, тільки з другов таблицев
            comp.SecondDatabase.BuildContainer();
            comp.SecondData = (comp.SecondDatabase as SqlDataBaseConnector).Read(query2, comp.SecondDatabase.FullRowSelector);
            sw.Stop();
            //Собсно, порівняння
            var res1 = comp.CompareColumns(1);
            var res2 = comp.CompareColumns(2);
            var res3 = comp.CompareColumns(3);
            var test = comp.GetFullRows(res1[0], 0,1);
            System.Console.Write(sw.Elapsed);

        }
    }
}
