using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using DbComparer;
using Microsoft.AspNetCore.Http;

namespace DBTest
{
    public class DatabaseComparer
    {
        public Database FirstDatabase = null;
        public Database SecondDatabase = null;

        public string[] FirstData = null;
        public string[] SecondData = null;

        public string FirstPath = null;
        public string SecondPath = null;

        public DatabaseComparer()
        {
        }

        public string Folder { get; set; }

        public bool LoadFirstData(string query, Func<IDataRecord, string> selector)
        {
            try
            {
                FirstData = FirstDatabase.Read(query, selector);
                return true;
            }
            catch (Exception e)
            {
                return false;
            }
        }

        public bool LoadSecondData(string query, Func<IDataRecord, string> selector)
        {
            try
            {
                SecondData = SecondDatabase.Read(query, selector);
                return true;
            }
            catch (Exception e)
            {
                return false;
            }
        }

        public IEnumerable<string> FindDifferences()
        {
            var list1 = FirstData.Except(SecondData);
            var list2 = SecondData.Except(FirstData);
            var list3 = list1.Union(list2);
            return list3;
        }

        public bool CloseConnection()
        {
            try
            {
                if (FirstDatabase != null
                    && FirstDatabase.connection != null
                    && FirstDatabase.connection.State != ConnectionState.Closed)
                    FirstDatabase.CloseConnection();
                if (SecondDatabase != null
                    && SecondDatabase.connection != null
                    && SecondDatabase.connection.State != ConnectionState.Closed)
                    SecondDatabase.CloseConnection();
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                return false;
            }
            return true;
        }

        public bool DeleteActiveFolder()
        {
            try
            {
                if ((FirstDatabase == null || FirstDatabase.connection == null || FirstDatabase.connection.State==ConnectionState.Closed)
                    && (SecondDatabase == null || SecondDatabase.connection == null || SecondDatabase.connection.State == ConnectionState.Closed))
                {
                    if (Directory.Exists(Folder))
                    {
                        string[] files = Directory.GetFiles(Folder);
                        foreach (string file in files)
                        {
                            File.SetAttributes(file, FileAttributes.Normal);
                            File.Delete(file);
                        }
                        Directory.Delete(Folder, false);
                        Folder = null;
                    }
                    return true;
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                return false;
            }
            return true;
        }
    }
}
