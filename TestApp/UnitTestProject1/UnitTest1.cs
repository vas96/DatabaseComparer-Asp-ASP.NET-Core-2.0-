using System;
using System.Data;
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
            Database db=new SqlDataBaseConnector();
            if (db.ConnectToServer())
            {
                var l1 = db.GetDatabasesList();
                Assert.IsNotNull(l1);
                var l2 = db.GetTablesList(l1[8]);
                Assert.IsNotNull(l2);
                Func<IDataRecord, string> select = delegate(IDataRecord s)
                {
                    return String.Format("{0} {1}", s[0], s[1]);
                };
                var l3 = db.GetTableInfo(l2[3]);
                Assert.IsNotNull(l3);
                var l4 = db.Read("Select * from "+l2[3], select);
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
                Func<IDataRecord, string> select = delegate (IDataRecord s)
                {
                    return String.Format("{0} {1}", s[0], s[1]);
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
            DatabaseComparer comp = DatabaseComparer.GetComparer;
            Database db = new SqlDataBaseConnector();
            if (db.ConnectToServer())
            {
                Func<IDataRecord, string> select = delegate (IDataRecord s)
                {
                    return String.Format("{0} {1}", s[0], s[1]);
                };
                var l1 = db.GetDatabasesList();
                var l2 = db.GetTablesList(l1[8]);
                comp.FirstData = db.Read("Select * from " + l2[11], select);
                comp.SecondData = db.Read("Select * from " + l2[12], select);
                var l3 = comp.FindDifferences();
                db.CloseConnection();
            }
        }
    }
}
