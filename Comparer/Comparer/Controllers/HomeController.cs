using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Comparer.Models;
using DbComparer;
using DBTest;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore.Internal;
using Newtonsoft.Json;

namespace Comparer.Controllers
{
    public class HomeController : Controller
    {
        private readonly ICounter _counterService;

        private readonly IHostingEnvironment _hostingEnvironment;
        private int _randomInt;
        private DatabaseComparer db;

        public HomeController(DatabaseComparer context, IHostingEnvironment hostingEnvironment, ICounter counter)
        {
            db = context;
            _hostingEnvironment = hostingEnvironment;
            _counterService = counter;
        }

        #region Error
        /// <summary>
        /// Сторінка для помилок
        /// </summary>
        /// <returns></returns>
        public IActionResult Error()
        {
            return View(new ErrorViewModel {RequestId = Activity.Current?.Id ?? HttpContext.TraceIdentifier});
        }

        #endregion

        #region Pages
        /// <summary>
        /// Стартова сторінка
        /// </summary>
        /// <returns></returns>
        public IActionResult Index()
        {
            db.CloseConnection();
            return View();
        }

        public IActionResult Comparer()
        {
            db.CloseConnection();
            return View(db);
        }
        /// <summary>
        /// Про розробників
        /// </summary>
        /// <returns></returns>
        public IActionResult About()
        {      
            ViewData["Message"] = "Your application description page.";
            return View();
        }

        /// <summary>
        /// Контакти
        /// </summary>
        /// <returns></returns>
        public IActionResult Contact()
        {
            db.CloseConnection();
            ViewData["Message"] = "Your contact page.";
            return View();
        }

        #endregion

        #region Partial
        /// <summary>
        /// Часткове представлення
        /// Сторінка завантаження файлів
        /// </summary>
        /// <returns></returns>
        [HttpPost]
        public IActionResult FilesDownloaded()
        {
            CleanNotUsedData(1);
            return PartialView("_DownloadFiles", db);
        }

        /// <summary>
        /// Часткове представлення
        /// Інформація про таблиці
        /// </summary>
        /// <returns></returns>
        [HttpPost]
        public IActionResult TableInfo()
        {
            try
            {
                if (db.FirstDatabase.connection == null || db.FirstDatabase.connection.State != ConnectionState.Open ||
                    db.SecondDatabase.connection == null || db.SecondDatabase.connection.State != ConnectionState.Open)
                    return PartialView("_Error");
                return PartialView("_TableInfo", db);
            }
            catch (Exception ex)
            {
                return PartialView("_Error", ex);
            }
        }

        /// <summary>
        /// Часткове представлення
        /// Маппінг полів
        /// </summary>
        /// <param name="array">масив, кожен елем. - назва таблиці</param>
        /// <returns></returns>
        [HttpPost]
        public IActionResult ColumnMapping(string[] array = null)
        {
            CleanNotUsedData(3);
            if (array.Length < 1)
            {
                if (db.FirstDatabase.SelectedTable == null || db.SecondDatabase.SelectedTable == null)
                    return PartialView("_Error");
                array = new string[2];
                array[0] = db.FirstDatabase.SelectedTable;
                array[1] = db.SecondDatabase.SelectedTable;
            }
            db.FirstDatabase.SelectedTable = array[0];
            db.SecondDatabase.SelectedTable = array[1];
            if (db.FirstDatabase == null ||
                db.FirstDatabase.connection == null ||
                db.FirstDatabase.connection.State != ConnectionState.Open || db.SecondDatabase == null ||
                db.SecondDatabase.connection == null || db.SecondDatabase.connection.State != ConnectionState.Open)
                return PartialView("_Error");
            if (db.FirstDatabase.SelectedTable == "" || db.SecondDatabase.SelectedTable == "")
                return PartialView("_Error");
            db.FirstDatabase.GetTableInfo();
            db.SecondDatabase.GetTableInfo();
            return PartialView("_ColumnMapping", db);
        }

        /// <summary>
        /// Часткове представлення
        /// Порівняння
        /// </summary>
        /// <param name="array">масив полів які пройшли валідацію</param>
        /// <returns></returns>
        [HttpPost]
        public IActionResult Comparing(string[] array)
        {
            CleanNotUsedData(4);
            if (array.Length == 0)
                return PartialView("_Error");
            db.FirstDatabase.SelectedColumns.Clear();
            db.SecondDatabase.SelectedColumns.Clear();
            var min = Math.Min(db.FirstDatabase.TableColumns.Count, db.SecondDatabase.TableColumns.Count);
            for (var i = 0; i < min; i++)
            {
                db.FirstDatabase.SelectedColumns.Add(db.FirstDatabase.TableColumns[i]);
                foreach (var column in db.SecondDatabase.TableColumns)
                    if (column.Name == array[i] &&
                        AdditionalFunctions.IsTypesComparable(column.Type, db.FirstDatabase.TableColumns[i].Type))
                        db.SecondDatabase.SelectedColumns.Add(column);
            }
            if (db.FirstDatabase.SelectedColumns.Count !=
                db.SecondDatabase.SelectedColumns.Count)
                return PartialView("_Error");
            if (db.ReadDataFromDb())
                db.ComparingResult = db.CompareFullData();
            else return PartialView("_Error");
            return PartialView("_Comparing", db);
        }

        [HttpPost]
        public IActionResult SelectTable()
        {
            return PartialView("_SelectTable", db);
        }

        #endregion

        #region UploadFile
        /// <summary>
        /// Завантаження файлів на сервер
        /// </summary>
        /// <param name="file">файл</param>
        /// <param name="id">source чи target</param>
        /// <param name="type">тип обраної бази даних</param>
        /// <returns></returns>
        [HttpPost]
        public async Task<bool> Upload(IFormFile file, int? id, string type)
        {
            if (file != null)
                try
                {
                    string path;
                    if (db.Folder == null || !Directory.Exists(db.Folder))
                    {
                        while (true)
                        {
                            _randomInt = _counterService.Value;
                            path = _hostingEnvironment.WebRootPath + "\\Uploads\\Folder_" + _randomInt;
                            if (!Directory.Exists(path)) break;
                        }

                        Directory.CreateDirectory(path);
                        db.Folder = path;
                    }
                    else
                    {
                        path = db.Folder;
                    }

                    path += "\\File_" + id + "_" + file.FileName;
                    using (var fileStream = new FileStream(path, FileMode.Append))
                    {
                        var fileWriter = new StreamWriter(fileStream);
                        fileWriter.AutoFlush = true;
                        await file.CopyToAsync(fileStream);
                    }

                    var dbase = Database.InitializeType(type);
                    var a = dbase.ConnectToFile(path);
                    dbase.FileName = Path.GetFileNameWithoutExtension(file.FileName);
                    switch (id)
                    {
                        case 1:
                        {
                            if (db.FirstDatabase != null)
                            {
                                db.FirstDatabase.CloseConnection();
                                System.IO.File.SetAttributes(path, FileAttributes.Normal);
                            }

                            db.FirstDatabase = dbase;
                            break;
                        }
                        case 2:
                        {
                            if (db.SecondDatabase != null)
                            {
                                db.SecondDatabase.CloseConnection();
                                System.IO.File.SetAttributes(path, FileAttributes.Normal);
                            }

                            db.SecondDatabase = dbase;
                            break;
                        }
                        default: return false;
                    }

                    if (dbase.connection.State == ConnectionState.Open)
                        return true;
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                    return false;
                }

            return false;
        }

        /// <summary>
        /// Віддалене підключення
        /// </summary>
        /// <param name="data">набір даних у форматі JSON</param>
        /// <returns></returns>
        [HttpPost]
        public bool RemoteAccess(string data)
        {
            try
            {
                string[] DbWithoutRemote = {"SQLite", "PostgreSQL"};
                var values = JsonConvert.DeserializeObject<Dictionary<string, string>>(data);
                var dbase = Database.InitializeType(values["dbType"]);
                if (DbWithoutRemote.Contains(values["dbType"])) return false;
                var id = values["from"];
                var a = dbase.RemoteConnection(values);
                switch (id)
                {
                    case "source":
                    {
                        if (db.FirstDatabase != null) db.FirstDatabase.CloseConnection();
                        db.FirstDatabase = dbase;
                        break;
                    }
                    case "target":
                    {
                        if (db.SecondDatabase != null) db.SecondDatabase.CloseConnection();
                        db.SecondDatabase = dbase;
                        break;
                    }
                    default: return false;
                }

                if (dbase.connection.State != ConnectionState.Open)
                    return false;
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                return false;
            }

            return true;
        }

        /// <summary>
        /// Очищення даних після того як користувач покидає сторінку
        /// </summary>
        /// <param name="i"></param>
        public void CleanNotUsedData(int i)
        {
            switch (i)
            {
                case 1:
                {
                    db = new DatabaseComparer();
                    PageClosedAction("");
                    break;
                }
                case 2:
                {
                    db.FirstDatabase.SelectedTable = null;
                    db.SecondDatabase.SelectedTable = null;
                    break;
                }
                case 3:
                {
                    db.FirstDatabase.TableColumns.Clear();
                    db.SecondDatabase.TableColumns.Clear();
                    db.FirstDatabase.SelectedColumns.Clear();
                    db.SecondDatabase.SelectedColumns.Clear();
                    break;
                }
                case 4:
                {
                    db.ComparingResult = null;
                    db.FirstData = null;
                    db.SecondData = null;
                    db.AdditionalInfo = null;
                    break;
                }
            }
        }

        /// <summary>
        /// Закриває зєднання та видаляє дані
        /// </summary>
        /// <param name="page"></param>
        [HttpPost]
        public void PageClosedAction(string page)
        {
            if (db.Folder != null)
                try
                {
                    var sleepTimer = 100;
                    var conClosed = db.CloseConnection();
                    for (var i = 0; i < 20 && !conClosed; i++)
                    {
                        Thread.Sleep(sleepTimer + i * 100);
                        conClosed = db.CloseConnection();
                    }

                    var folderDeleted = db.DeleteActiveFolder();
                    for (var i = 0; i < 20 && !folderDeleted; i++)
                    {
                        Thread.Sleep(sleepTimer + i * 100);
                        folderDeleted = db.DeleteActiveFolder();
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                    throw;
                }
        }

        /// <summary>
        /// Створення скриптів на вставку та оновлення даних
        /// </summary>
        /// <param name="id"></param>
        /// <param name="arrayN"></param>
        /// <param name="arrayU"></param>
        /// <returns></returns>
        [HttpPost]
        public JsonResult CreateScript(int id, string[] arrayN = null, string[] arrayU = null)
        {
            string[] Insert = null, Update = null;
            switch (id)
            {
                case 1:
                {
                    Update = db.SecondDatabase.BuildUpdate(db.ComparingResult[2], db.ComparingResult[1], arrayN);
                    Insert = db.SecondDatabase.BuildInsert(db.ComparingResult[3], arrayU);
                    break;
                }
                case 2:
                {
                    Update = db.SecondDatabase.BuildUpdate(db.ComparingResult[2], db.ComparingResult[1], arrayN);
                    Insert = db.FirstDatabase.BuildUpdate(db.ComparingResult[1], db.ComparingResult[2], arrayN);
                    break;
                }
                case 3:
                {
                    Update = db.FirstDatabase.BuildUpdate(db.ComparingResult[1], db.ComparingResult[2], arrayN);
                    Insert = db.FirstDatabase.BuildInsert(db.ComparingResult[4], arrayU);
                    break;
                }
            }
            var ForReturn = new {Insert = Insert.Join("\n"), Update = Update.Join("\n")};
            return Json(ForReturn);
        }
        #endregion
    }
}
