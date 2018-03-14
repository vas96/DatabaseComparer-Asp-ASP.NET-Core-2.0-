using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using DbComparer;

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
        public IQueryable<string[]> FirstData, SecondData;

        /// <summary>
        /// Статистика про порівняні рядки
        /// </summary>
        public Statistick[] AdditionalInfo;


        public (IQueryable<string[]>, IQueryable<string[]>) ComparingResult;

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
        public (IQueryable<string[]>, IQueryable<string[]>) CompareFullData()
        {
            IQueryable<string[]> ResultFirst = null;
            IQueryable<string[]> ResultSecond = null;
            (IQueryable<string[]>, IQueryable<string[]>) ResultAll = (source: ResultFirst, target: ResultSecond);
            try
            {
                var FirstTask = new Task(() =>
                {
                    ResultFirst = FirstData.Except(SecondData, new SomeComparison());
                });
                FirstTask.Start();
                var SecondTask = new Task(() =>
                {
                    ResultSecond = SecondData.Except(FirstData, new SomeComparison());
                });
                SecondTask.Start();
                Task.WaitAll(FirstTask, SecondTask);

#pragma warning disable CS8123 // The tuple element name is ignored because a different name or no name is specified by the assignment target.
                ResultAll = (source: ResultFirst, target: ResultSecond);
#pragma warning restore CS8123 // The tuple element name is ignored because a different name or no name is specified by the assignment target.

            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
               // return null;
            }

            return ResultAll;
        }

        /// <summary>
        /// Повертає порівняння двох колонок як масив рядків
        /// </summary>
        /// <param name="SelectedColumn">Номер колонки</param>
        /// <returns></returns>
        public Dictionary<int, IQueryable<string[]>> CompareColumns(int SelectedColumn)
        {
            Dictionary<int, IQueryable<string[]>> Result = new Dictionary<int, IQueryable<string[]>>();
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
                FirstKeys[min] = FirstDatabase.TableColumns[SelectedColumn].Position;
                SecondKeys[min] = SecondDatabase.TableColumns[SelectedColumn].Position;
                //створєм предавлення таблиць з врахування первинних ключів
                var FirstSelected = FirstData.KeyPlusDataSelection(FirstKeys);
                var SecondSelected = SecondData.KeyPlusDataSelection(SecondKeys);
                //заповнюєм статистику
                AdditionalInfo[0].SameInColumn.Add(SelectedColumn, FirstSelected.Count() - Result[0].Count());
                AdditionalInfo[1].SameInColumn.Add(SelectedColumn, SecondSelected.Count() - Result[1].Count());
                AdditionalInfo[0].DifferentInColumn.Add(SelectedColumn, Result[0].Count());
                AdditionalInfo[1].DifferentInColumn.Add(SelectedColumn, Result[1].Count());
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
        /// Витягує з нумерованого масиву рядків певні рядки
        /// </summary>
        /// <param name="arr1">Собсно масив</param>
        /// <param name="arr2">А тут - масив рядків, які тре дістати</param>
        /// <returns></returns>
        public static IQueryable<string[]> KeyPlusDataSelection(this IQueryable<string[]> arr1, int[] arr2)
        {
            return (from item in arr1 select (from col in arr2 select item.ToArray()[col]).ToArray()).AsQueryable();
        }


        public static int GetMax(int first, int second)
        {
            return first > second ? first : second;
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
}
