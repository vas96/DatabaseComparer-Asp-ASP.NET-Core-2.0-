using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Comparer.Models;
using DbComparer;
using DBTest;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc.Rendering;
using Microsoft.Net.Http.Headers;

namespace Comparer.Controllers
{
    public class HomeController : Controller
    {
        private DatabaseComparer db;

        private readonly IHostingEnvironment _hostingEnvironment;
        private readonly ICounter _counterService;
        private int _randomInt;

        public HomeController(DatabaseComparer context, IHostingEnvironment hostingEnvironment, ICounter counter)
        {
            db = context;
            _hostingEnvironment = hostingEnvironment;
            _counterService = counter;
        }

        #region Pages

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

        public IActionResult About()
        {
            db.CloseConnection();
            ViewData["Message"] = "Your application description page.";
            var db1 = db.FirstDatabase;
            db1 = new SqlDataBaseConnector();
            db1.ConnectToDatabase("Repair");
            return View(db1);
        }

        public IActionResult Contact()
        {
            db.CloseConnection();
            ViewData["Message"] = "Your contact page.";
            return View();
        }

        #endregion

        #region Error

        public IActionResult Error()
        {
            return View(new ErrorViewModel { RequestId = Activity.Current?.Id ?? HttpContext.TraceIdentifier });
        }

        #endregion

        #region Partial

        public IActionResult FilesDownloaded()
        {
            return PartialView("_DownloadFiles", db);
        }


        public IActionResult TableInfo()
        {
            if ((db.FirstDatabase.connection == null || db.FirstDatabase.connection.State != ConnectionState.Open) ||
                (db.SecondDatabase.connection == null || db.SecondDatabase.connection.State != ConnectionState.Open))
                return PartialView("_Error");
            return PartialView("_TableInfo", db);
        }
        
        public IActionResult ColumnMapping(string[] array = null)
        {
            if (array == null || array.Length <= 2)
                array = new[] { "Projects", "Users" };
            db.FirstDatabase.SelectedTable = array[0];
            db.SecondDatabase.SelectedTable = array[1];
            if ((db.FirstDatabase.connection == null || db.FirstDatabase.connection.State != ConnectionState.Open) ||
                (db.SecondDatabase.connection == null || db.SecondDatabase.connection.State != ConnectionState.Open))
                return PartialView("_Error");
            if (db.FirstDatabase.SelectedTable == "" || db.SecondDatabase.SelectedTable == "")
                return PartialView("_Error");
            db.FirstDatabase.GetTableInfo();
            db.SecondDatabase.GetTableInfo();
            return PartialView("_ColumnMapping", db);

        }
        
        public IActionResult Comparing(string[] array)
        {
            if (array.Length == 0)
                return PartialView("_Error");
            int min = Math.Min(db.FirstDatabase.TableColumns.Count, db.SecondDatabase.TableColumns.Count);
            for (int i = 0; i < min; i++)
            {
                db.FirstDatabase.SelectedColumns.Add(db.FirstDatabase.TableColumns[i].Name);
                foreach (var column in db.SecondDatabase.TableColumns)
                {
                    if (column.Name == array[i] && column.Type== db.FirstDatabase.TableColumns[i].Type)
                        db.SecondDatabase.SelectedColumns.Add(array[i]);
                }
            }
            if (db.FirstDatabase.SelectedColumns.Count !=
                db.SecondDatabase.SelectedColumns.Count)
                return PartialView("_Error");
            return PartialView("_Comparing", db);
        }

        public IActionResult SelectTable()
        {
            return PartialView("_SelectTable", db);
        }
        #endregion

        #region UploadFile

        [HttpPost]
        public async Task<bool> Upload(IFormFile file, int? id)
        {
            if (file != null)
            {
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
                        path = db.Folder;
                    path += "\\File_" + id + "_" + file.FileName;
                    using (var fileStream = new FileStream(path, FileMode.Append))
                    {
                        var fileWriter = new StreamWriter(fileStream);
                        fileWriter.AutoFlush = true;
                        await file.CopyToAsync(fileStream);
                    }
                    Database dbase = Database.InitializeType(file);
                    var a=dbase.ConnectToFile(path);
                    switch (id)
                    {
                        case 1:
                            {
                                if (db.FirstDatabase != null)
                                    db.FirstDatabase.CloseConnection();
                                db.FirstDatabase = dbase;
                                break;
                            }
                        case 2:
                            {
                                if (db.SecondDatabase != null)
                                    db.SecondDatabase.CloseConnection();
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
            }
            return false;
        }

        [HttpPost]
        public void PageClosedAction(string page)
        {
            if (db.Folder!=null)
            {
                try
                {
                    int sleepTimer = 100;
                    bool conClosed = db.CloseConnection();
                    for (int i = 0; i < 20 && !conClosed; i++)
                    {
                        Thread.Sleep(sleepTimer+(i*100));
                        conClosed = db.CloseConnection();
                    }
                    bool folderDeleted = db.DeleteActiveFolder();
                    for (int i = 0; i < 20 && !folderDeleted; i++)
                    {
                        Thread.Sleep(sleepTimer + (i * 100));
                        folderDeleted = db.DeleteActiveFolder();
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                    throw;
                }
            }
        }
        #endregion
    }
}
