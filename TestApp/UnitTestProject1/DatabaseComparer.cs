using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using DbComparer;
using NaturalSort.Extension;

namespace DBTest
{
    public class DatabaseComparer
    {
        /// <summary>
        /// Екземпляри БД
        /// </summary>
        public Database FirstDatabase, SecondDatabase;

        /// <summary>
        /// Результати читання з БД
        /// </summary>
        public List<string[]> FirstData, SecondData;

        /// <summary>
        /// Результати порівняння
        /// </summary>
        public Dictionary<int, List<string[]>> ComparingResult;

        /// <summary>
        /// Статистика про порівняні рядки
        /// </summary>
        public Statistick[] AdditionalInfo;

        /// <summary>
        /// Ініціалізація полів
        /// </summary>
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
            ComparingResult = new Dictionary<int, List<string[]>>();
        }

        /// <summary>
        /// Шлях до папки активного користувача
        /// </summary>
        public string Folder { get; set; }

        /// <summary>
        /// Закриває всі підключення до БД
        /// </summary>
        /// <returns></returns>
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

        /// <summary>
        /// Видаляє папку активного користувача
        /// </summary>
        /// <returns></returns>
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

        public void DeleteDirectory(string path, bool recursive)
        {
            if (path == null) path = Folder;
            if (recursive)
            {
                var subfolders = Directory.GetDirectories(path);
                foreach (var s in subfolders)
                {
                    DeleteDirectory(s, recursive);
                }
            }
            var files = Directory.GetFiles(path);
            foreach (var f in files)
            {
                try
                {
                    var attr = File.GetAttributes(f);
                    if ((attr & FileAttributes.ReadOnly) == FileAttributes.ReadOnly)
                    {
                        File.SetAttributes(f, attr ^ FileAttributes.ReadOnly);
                    }
                    File.Delete(f);
                }
                catch (IOException)
                {
                }
            }

            // At this point, all the files and sub-folders have been deleted.
            // So we delete the empty folder using the OOTB Directory.Delete method.
            Directory.Delete(path);
        }

        /// <summary>
        /// Читає дані з 1-ї і 2-ї БД
        /// Запити генеруються на основі SelectedColumns
        /// </summary>
        /// <returns></returns>
        public bool ReadDataFromDb()
        {
            try
            {
                var FirstTask = new Task(() =>
                {
                    FirstData = FirstDatabase.Read(FirstDatabase.BuildSelectQuery(),
                        FirstDatabase.FullStringArraySelector);
                });
                FirstTask.Start();
                var SecondTask = new Task(() =>
                {
                    SecondData = SecondDatabase.Read(SecondDatabase.BuildSelectQuery(),
                        SecondDatabase.FullStringArraySelector);
                });
                SecondTask.Start();
                Task.WaitAll(FirstTask, SecondTask);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
                return false;
            }
            return true;
        }


        /// <summary>
        /// Повертає словник з 2-х елементів
        /// 1-й FirstDb.Except(SecondDb)
        /// 2-й SecondDb.Except(FirstDb)
        /// </summary>
        /// <returns></returns>
        public Dictionary<int, List<string[]>> CompareFullData()
        {
            Dictionary<int, List<string[]>> Result = new Dictionary<int, List<string[]>>();
            try
            {
                List<string[]> col1 = null, col2 = null;
                var FirstTask = new Task(() =>
                {
                    col1 = FirstData.Except(SecondData, new StringArrayComparer()).ToList();
                    var list1 = FirstDatabase.TableColumns.Where(item => item.ISKey).ToList();
                    int[] FirstKeys = new int[list1.Count];
                    if (list1.Count != 0)
                    {
                        IOrderedQueryable<string[]> temp = col1.AsQueryable().OrderBy(i => i[list1[0].Position], StringComparer.OrdinalIgnoreCase.WithNaturalSort());
                        for (int i = 1; i < list1.Count; i++)
                        {
                            FirstKeys[i] = list1[i].Position;
                            temp = temp.ThenBy(j => j[FirstKeys[i]]);
                        }
                        col1 = temp.ToList();
                    }
                });
                FirstTask.Start();
                var SecondTask = new Task(() =>
                {
                    col2 = SecondData.Except(FirstData, new StringArrayComparer()).ToList();
                    var list2 = SecondDatabase.TableColumns.Where(item => item.ISKey).ToList();
                    int[] SecondKeys = new int[list2.Count];
                    if (list2.Count != 0)
                    {
                        IOrderedQueryable<string[]> temp = col2.AsQueryable().OrderBy(i => i[list2[0].Position], StringComparer.OrdinalIgnoreCase.WithNaturalSort());
                        for (int i = 1; i < list2.Count; i++)
                        {
                            SecondKeys[i] = list2[i].Position;
                            temp = temp.ThenBy(j => j[SecondKeys[i]]);
                        }
                        col2 = temp.ToList();
                    }
                });
                SecondTask.Start();
                Task.WaitAll(FirstTask, SecondTask);
                Result.Add(1, col1);
                Result.Add(2, col2);
                FindUniqueDifferencess(Result);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
                return null;
            }
            return Result;
        }

        /// <summary>
        /// Визначає унікальні рядки в наборі
        /// Первинні ключі визначаються автоматично
        /// </summary>
        /// <param name="items">Набір</param>
        public Dictionary<int, List<string[]>> FindUniqueDifferencess(Dictionary<int, List<string[]>> items)
        {
            var list1 = FirstDatabase.TableColumns.Where(item => item.ISKey).Select(i => i.Position).ToArray();
            var list2 = SecondDatabase.TableColumns.Where(item => item.ISKey).Select(i => i.Position).ToArray();
            if (list1.Length != list2.Length)
            {
                items.Add(3, new List<string[]>());
                items.Add(4, new List<string[]>());
                return items;
            }

            List<string[]> first=new List<string[]>(), second=new List<string[]>();
            var FirstTaskPrepare = new Task(() =>
            {
                first = items[1].KeyPlusDataSelection(list1);
            });
            FirstTaskPrepare.Start();
            var SecondTaskPrepare = new Task(() =>
            {
                second = items[2].KeyPlusDataSelection(list2);
            });

            var FirstTask = new Task(() =>
            {
                List<string[]> temp = new List<string[]>();
                List<string[]> temp2 = new List<string[]>();
                int counter = second.Count;
                for (int i = 0; i < counter; i++)
                {
                    if (!first.Contains(second[i], new StringArrayComparer()))
                    {
                        temp.Add(items[2][i]);
                        continue;
                    }
                    temp2.Add(items[2][i]);
                }
                items[2] = temp2;
                items.Add(3, temp);
            });
            FirstTask.Start();
            var SecondTask = new Task(() =>
            {
                List<string[]> temp = new List<string[]>();
                List<string[]> temp2 = new List<string[]>();
                int counter = first.Count;
                for (int i = 0; i < counter; i++)
                {
                    if (!second.Contains(first[i], new StringArrayComparer()))
                    {
                        temp.Add(items[1][i]);
                        continue;
                    }
                    temp2.Add(items[1][i]);
                }
                items[1] = temp2;
                items.Add(4, temp);
            });
            SecondTask.Start();
            Task.WaitAll(FirstTask, SecondTask);
            return items;
        }

        /// <summary>
        /// Повертає порівняння лвох колонок як масив рядків
        /// </summary>
        /// <param name="SelectedColumn">Номер колонки</param>
        /// <returns></returns>
        public Dictionary<int, List<string[]>> CompareColumnsWithKeys(int SelectedColumn)
        {
            Dictionary<int, List<string[]>> Result = new Dictionary<int, List<string[]>>();
            try
            {
                //дістаєм імена колонок-ключів
                var list1 = FirstDatabase.TableColumns.Where(item => item.ISKey).ToList();
                var list2 = FirstDatabase.TableColumns.Where(item => item.ISKey).ToList();
                //Дістаєм мінімальну к-сть головних ключів з таблиць
                int min = Math.Min(list1.Count, list2.Count);
                int[] FirstKeys = new int[min + 1];
                int[] SecondKeys = new int[min + 1];
                //додаєм назви цих колонок до запитів
                for (int i = 0; i < min; i++)
                {
                    FirstKeys[i] = list1[i].Position;
                    SecondKeys[i] = list2[i].Position;
                }

                List<string[]> col1 = null, col2 = null;
                var task1 = new Task(() =>
                {
                    FirstKeys[min] = FirstDatabase.TableColumns[SelectedColumn].Position;
                    col1 = FirstData.KeyPlusDataSelection(FirstKeys).ToList();
                });
                task1.Start();
                var task2 = new Task(() =>
                    {
                        SecondKeys[min] = SecondDatabase.TableColumns[SelectedColumn].Position;
                        col2 = SecondData.KeyPlusDataSelection(SecondKeys).ToList();
                    });
                task2.Start();
                Task.WaitAll(task1, task2);
                Result.Add(1, col1);
                Result.Add(2, col2);
                AdditionalInfo[0].SameInColumn.Add(SelectedColumn, Result[0].Count() - Result[0].Count());
                AdditionalInfo[0].DifferentInColumn.Add(SelectedColumn, Result[0].Count());
                AdditionalInfo[1].SameInColumn.Add(SelectedColumn, Result[1].Count() - Result[1].Count());
                AdditionalInfo[1].DifferentInColumn.Add(SelectedColumn, Result[1].Count());
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                return null;
            }
            return Result;
        }

        public Dictionary<int, List<string[]>> CompareColumns(int SelectedColumn)
        {
            Dictionary<int, List<string[]>> Result = new Dictionary<int, List<string[]>>();
            try
            {
                List<string[]> col1 = null, col2 = null;
                var task1 = new Task(() =>
                {
                    int[] col = new[] { FirstDatabase.TableColumns[SelectedColumn].Position };
                    col1 = FirstData.KeyPlusDataSelection(col).ToList();
                    AdditionalInfo[0].SameInColumn.Add(SelectedColumn, Result[0].Count() - Result[0].Count());
                    AdditionalInfo[0].DifferentInColumn.Add(SelectedColumn, Result[0].Count());
                });
                task1.Start();
                var task2 = new Task(() =>
                {
                    int[] col = new[] { SecondDatabase.TableColumns[SelectedColumn].Position };
                    col2 = SecondData.KeyPlusDataSelection(col).ToList();
                    AdditionalInfo[1].SameInColumn.Add(SelectedColumn, Result[1].Count() - Result[1].Count());
                    AdditionalInfo[1].DifferentInColumn.Add(SelectedColumn, Result[1].Count());
                });
                task2.Start();
                Task.WaitAll(task1, task2);
                Result.Add(1, col1);
                Result.Add(2, col2);
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                return null;
            }
            return Result;
        }
    }

    /// <summary>
    /// Для статистики пошуку
    /// </summary>
    public class Statistick
    {
        /// <summary>
        /// Одинакових колонок
        /// </summary>
        public Dictionary<int, int> SameInColumn;
        /// <summary>
        /// Відмінних колонок
        /// </summary>
        public Dictionary<int, int> DifferentInColumn;

        public Statistick()
        {
            SameInColumn = new Dictionary<int, int>();
            DifferentInColumn = new Dictionary<int, int>();
        }
    }

    /// <summary>
    /// Клас з розширеннями IEnumerable<string[]>
    /// </summary>
    public static class Extension
    {
        /// <summary>
        /// Я не знаю, нашо тут це розширення. Просто тому шо я можу
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
        /// Ага, тово тоже не потрібне
        /// </summary>
        /// <param name="table"></param>
        /// <returns></returns>
        public static IEnumerable<string> ReturnLastItem(this DataTable table)
        {
            foreach (DataRow row in table.Rows)
            {
                int last = row.ItemArray.Length - 1;
                yield return row.ItemArray[last].ToString();
            }
        }

        /// <summary>
        /// Витягує з нумерованого масиву рядків певні стовпці
        /// </summary>
        /// <param name="arr1">Собсно масив</param>
        /// <param name="arr2">А тут - масив рядків, які тре дістати</param>
        /// <returns></returns>
        public static List<string[]> KeyPlusDataSelection(this List<string[]> arr1, int[] arr2)
        {
            return (from item in arr1 select (from col in arr2 select item.ToArray()[col]).ToArray()).ToList();
        }
        /*
         На всяк випадок 
         Юзабельний код
         Update - виявилося, шо ет не сильно юзабельний код
         Але най ше побуде - мож пригодиться
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

    class StringArrayComparer : IEqualityComparer<string[]>
    {

        public bool Equals(string[] x, string[] y)
        {
            if (y == null || x == null)
            {
                return false;
            }
            return x.SequenceEqual(y);
        }

        public int GetHashCode(string[] obj)
        {
            int hash = 0;
            foreach (var item in obj)
            {
                hash += item.GetHashCode();
            }
            return (hash);
        }
    }
}
