﻿using System;
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
using Microsoft.EntityFrameworkCore.Internal;

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

        [HttpPost]
        public IActionResult FilesDownloaded()
        {
            CleanNotUsedData(1);
            return PartialView("_DownloadFiles", db);
        }


        [HttpPost]
        public IActionResult TableInfo()
        {
            try
            {
                if ((db.FirstDatabase.connection == null || db.FirstDatabase.connection.State != ConnectionState.Open) ||
                    (db.SecondDatabase.connection == null || db.SecondDatabase.connection.State != ConnectionState.Open))
                    return PartialView("_Error");
                return PartialView("_TableInfo", db);
            }
            catch (Exception ex)
            {
                return PartialView("_Error", ex);
            }
        }

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
            if ((db.FirstDatabase == null ||
                 db.FirstDatabase.connection == null ||
                 db.FirstDatabase.connection.State != ConnectionState.Open) ||
                (db.SecondDatabase == null ||
                 db.SecondDatabase.connection == null ||
                 db.SecondDatabase.connection.State != ConnectionState.Open))
            {
                return PartialView("_Error");
            }
            if (db.FirstDatabase.SelectedTable == "" || db.SecondDatabase.SelectedTable == "")
                return PartialView("_Error");
            db.FirstDatabase.GetTableInfo();
            db.SecondDatabase.GetTableInfo();
            return PartialView("_ColumnMapping", db);
        }

        [HttpPost]
        public IActionResult Comparing(string[] array)
        {
            CleanNotUsedData(4);
            if (array.Length == 0)
                return PartialView("_Error");
            db.FirstDatabase.SelectedColumns.Clear();
            db.SecondDatabase.SelectedColumns.Clear();
            int min = Math.Min(db.FirstDatabase.TableColumns.Count, db.SecondDatabase.TableColumns.Count);
            for (int i = 0; i < min; i++)
            {
                db.FirstDatabase.SelectedColumns.Add(db.FirstDatabase.TableColumns[i]);
                foreach (var column in db.SecondDatabase.TableColumns)
                {
                    if (column.Name == array[i] && AdditionalFunctions.IsTypesComparable(column.Type, db.FirstDatabase.TableColumns[i].Type))
                        db.SecondDatabase.SelectedColumns.Add(column);
                }
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
            }
            return false;
        }


        [HttpPost]
        public bool RemoteAccess(string[] attr)
        {
            string DbType = attr[0];
            string IP = attr[0];
            string Port = attr[0];
            Database dbase = Database.InitializeType(DbType);
            return false;
        }

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
                        db.ComparingResult.Clear();
                        db.FirstData = null;
                        db.SecondData = null;
                        db.AdditionalInfo = null;
                        break;
                    }
            }
        }

        [HttpPost]
        public void PageClosedAction(string page)
        {
            if (db.Folder != null)
            {
                try
                {
                    int sleepTimer = 100;
                    bool conClosed = db.CloseConnection();
                    for (int i = 0; (i < 20) && !conClosed; i++)
                    {
                        Thread.Sleep(sleepTimer + (i * 100));
                        conClosed = db.CloseConnection();
                    }
                    bool folderDeleted = db.DeleteActiveFolder();
                    for (int i = 0; (i < 20) && !folderDeleted; i++)
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

        [HttpPost]
        public JsonResult CreateScript(int id, string[] arrayN, string[] arrayU = null)
        {
            string[] Insert=null, Update=null;
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


/*
 Код для "дебагу"
 Вставити на будь-яку сторінку
             DatabaseComparer comp = new DatabaseComparer();
            var task1 = new Task(() =>
            {
                comp.FirstDatabase = new SqlDataBaseConnector();
                var c1 = comp.FirstDatabase.ConnectToServer();
                var c2 = comp.FirstDatabase.ConnectToDatabase("Repair");
                comp.FirstDatabase.SelectedTable = "NewEmployees";
                comp.FirstDatabase.GetTableInfo("NewEmployees");
                foreach (var item in comp.FirstDatabase.TableColumns)
                {
                    comp.FirstDatabase.SelectedColumns.Add(item.Name);
                }
            });
            task1.Start();
            var task2 = new Task(() =>
            {
                comp.SecondDatabase = new SqlDataBaseConnector();
                var c12 = comp.SecondDatabase.ConnectToServer();
                var c22 = comp.SecondDatabase.ConnectToDatabase("Repair");
                comp.SecondDatabase.SelectedTable = "NewEmployees2";
                comp.SecondDatabase.GetTableInfo();
                foreach (var item in comp.SecondDatabase.TableColumns)
                {
                    comp.SecondDatabase.SelectedColumns.Add(item.Name);
                }
            });
            task2.Start();
            Task.WaitAll(task1, task2);
            Stopwatch sw1 = new Stopwatch();
            Stopwatch sw2 = new Stopwatch();
            sw1.Start();
            IEnumerable<string[]> table1 = null, table2 = null;
            var dasda = comp.ReadDataFromDb();
            sw1.Stop();
            sw2.Start();
            var res = comp.CompareFullData();
            sw2.Stop();
            System.Console.WriteLine(1);
 */
