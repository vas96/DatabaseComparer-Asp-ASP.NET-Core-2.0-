using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using DbComparer;

namespace DBTest
{
    public class DatabaseComparer
    {
        public Database FirstDatabase;
        public Database SecondDatabase;

        public DataTable FirstData;
        public DataTable SecondData;

        public Statistick[] AdditionalInfo;

        public DatabaseComparer()
        {
            FirstData = null;
            SecondData = null;
            FirstDatabase = null;
            SecondDatabase = null;
            Folder = null;
            AdditionalInfo = new Statistick[2];
            AdditionalInfo[0] = new Statistick();
            AdditionalInfo[1] = new Statistick();
        }

        public string Folder { get; set; }

        public bool LoadFirstData(string query, Func<IDataRecord, DataRow> selector)
        {
            try
            {
                //                FirstData = FirstDatabase.Read(query, selector);
                return true;
            }
            catch (Exception e)
            {
                return false;
            }
        }

        public bool LoadSecondData(string query, Func<IDataRecord, DataRow> selector)
        {
            try
            {
                //                SecondData = SecondDatabase.Read(query, selector);
                return true;
            }
            catch (Exception e)
            {
                return false;
            }
        }

        public IEnumerable<string> FindDifferences()
        {
            //            var list1 = FirstData.Except(SecondData);
            //            var list2 = SecondData.Except(FirstData);
            //            var list3 = list1.Union(list2);
            //            return list3;
            return null;
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
                if ((FirstDatabase == null || FirstDatabase.connection == null || FirstDatabase.connection.State == ConnectionState.Closed)
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

        /// <summary>
        /// Повертає порівняння лвох колонок у вигляді рядків
        /// </summary>
        /// <param name="SelectedColumn">Номер колонки</param>
        /// <returns></returns>
        public List<string>[] CompareColumns(int SelectedColumn)
        {
            List<string>[] Result = new List<string>[2];
            try
            {
                int min = Math.Min(FirstDatabase.TableColumns.Count(item => item.ISKey),
                    SecondDatabase.TableColumns.Count(item => item.ISKey));
                var list1 = FirstDatabase.TableColumns.Where(item => item.ISKey).ToList();
                var list2 = FirstDatabase.TableColumns.Where(item => item.ISKey).ToList();
                string[] FirstKeys = new string[min + 1];
                string[] SecondKeys = new string[min + 1];
                for (int i = 0; i < min; i++)
                {
                    FirstKeys[i] = list1[i].Name;
                    SecondKeys[i] = list2[i].Name;
                }
                FirstKeys[min] = FirstDatabase.TableColumns[SelectedColumn].Name;
                SecondKeys[min] = SecondDatabase.TableColumns[SelectedColumn].Name;
                DataView FirstView = new DataView(FirstData);
                DataTable FirstSelected = FirstView.ToTable("Selected", false, FirstKeys);
                DataView SecondView = new DataView(SecondData);
                DataTable SecondSelected = SecondView.ToTable("Selected", false, SecondKeys);
                IEnumerable<string> ListOne = FirstSelected.ReturnEnumerateRows();
                IEnumerable<string> ListTwo = SecondSelected.ReturnEnumerateRows();
                Result[0] = ListOne.Except(ListTwo).ToList();
                Result[1] = ListTwo.Except(ListOne).ToList();
                AdditionalInfo[0].SameInColumn.Add(SelectedColumn, FirstSelected.Rows.Count - Result[0].Count);
                AdditionalInfo[1].SameInColumn.Add(SelectedColumn, SecondSelected.Rows.Count - Result[1].Count);
                AdditionalInfo[0].DifferentInColumn.Add(SelectedColumn, Result[0].Count);
                AdditionalInfo[1].DifferentInColumn.Add(SelectedColumn, Result[1].Count);
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                return Result;
            }
            return Result;
        }

        /// <summary>
        /// Знаходить цілий рядок за поданими даними
        /// </summary>
        /// <param name="list">Список даних</param>
        /// <param name="dataBase">В якій ДБ шукати</param>
        /// <param name="colName">Номер колонки</param>
        /// <returns></returns>
        public DataTable GetFullRows(List<string> list, int dataBase, int colName)
        {
            int counter = list.Count;
            DataTable dt;
            string[] keys;
            if (dataBase == 0)
            {
                dt = FirstData;
                keys = FirstDatabase.TableColumns.Where(item => item.ISKey).Select(item => item.Name).ToArray();
            }
            else
            {
                dt = SecondData;
                keys = SecondDatabase.TableColumns.Where(item => item.ISKey).Select(item => item.Name).ToArray();
            }
            DataView TableView = new DataView(FirstData);
            DataTable Result = dt.Clone();
            string Sort;
            if (keys.Length < 1)
            {
                Sort = SecondData.Columns[colName].ColumnName;
            }
            else
            {
                Sort = keys[0];
                for (int i = 1; i < keys.Length; i++)
                {
                    Sort += ", " + keys[i];
                }
                Sort += "," + dt.Columns[colName];
            }
            TableView.Sort = Sort;
            if (colName == 0)
                foreach (var item in list)
                {
                    TableView.Find(item);
                }
            else
                foreach (var item in list)
                {
                    Result.Rows.Add(dt.Rows[TableView.Find(item.Split())].ItemArray);
                }
            return Result;
        }
    }

    /// <summary>
    /// Для статистики пошуку
    /// </summary>
    public class Statistick
    {
        public Dictionary<int, int> SameInColumn;
        public Dictionary<int, int> DifferentInColumn;

        public Statistick()
        {
            SameInColumn = new Dictionary<int, int>();
            DifferentInColumn = new Dictionary<int, int>();
        }
    }
    public static class Extension
    {
        /// <summary>
        /// Перетворює DataRow у рядок
        /// </summary>
        /// <param name="table">Таблиця</param>
        /// <returns></returns>
        public static IEnumerable<string> ReturnEnumerateRows(this DataTable table)
        {
            foreach (DataRow row in table.Rows)
            {
                string str = row.ItemArray[0].ToString();
                for (int i = 1; i < row.ItemArray.Length; i++)
                {
                    str += " " + row.ItemArray[i];
                }
                yield return str;
            }
        }

        /// <summary>
        /// Витягує з рядка єдине останнє значення
        /// </summary>
        /// <param name="table"></param>
        /// <returns></returns>
        public static IEnumerable<string> ReturnLastItem(this DataTable table)
        {
            foreach (DataRow row in table.Rows)
            {
                int last = row.ItemArray.Length-1;
                yield return row.ItemArray[last].ToString();
            }
        }
        /*
         На всяк випадок 
         Юзабельний код
            System.Data.DataView view = new System.Data.DataView(dtMaths);
            System.Data.DataTable selected = view.ToTable("Selected", false, dtMaths.Columns[0].ToString());
            IEnumerable<DataRow> l1 = dtMaths.EnumerateRows();
            IEnumerable<DataRow> l2 = dtEnglish.EnumerateRows();

            var dtOnlyMaths = (from cust in l1
                    select cust.ItemArray[1])
                .Except
                (from emp in l2
                    select emp.ItemArray[1]);
         */
    }
}
