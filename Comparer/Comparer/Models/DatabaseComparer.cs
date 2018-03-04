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

        public DatabaseComparer()
        {
        }

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
    }
}
