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
using Microsoft.Net.Http.Headers;

namespace Comparer.Controllers
{
    public class HomeController : Controller
    {
        private DatabaseComparer db;

        private readonly IHostingEnvironment _hostingEnvironment;
        
        public HomeController(DatabaseComparer context, IHostingEnvironment hostingEnvironment)
        {
            db = context;
            _hostingEnvironment = hostingEnvironment;
        }

        #region Pages

        public IActionResult Index()
        {
            return View();
        }

        public IActionResult Comparer()
        {
            return View(db);
        }

        public IActionResult About()
        {
            ViewData["Message"] = "Your application description page.";
            var db1 = db.FirstDatabase;
            db1 = new SqlDataBaseConnector();
            db1.ConnectToDatabase("Repair");
            return View(db1);
        }

        public IActionResult Contact()
        {
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

        public IActionResult SelectTable()
        {
            return PartialView("_SelectTable", db);
        }

        public IActionResult TableInfo()
        {
            return PartialView("_TableInfo", db);
        }
        public IActionResult ColumnMapping()
        {
            db.FirstDatabase.SelectedTable = "Projects";
            db.SecondDatabase.SelectedTable = "Users";
            return PartialView("_ColumnMapping", db);
        }


        #endregion

        #region UploadFile

        [HttpPost]
        public void Upload(IFormFile file, int? id)
        {
            if (file != null)
            {
                string path = _hostingEnvironment.WebRootPath+"\\Uploads\\File"+id+"_" + file.FileName;
                using (var fileStream = new FileStream(path, FileMode.Append))
                {
                    var fileWriter = new StreamWriter(fileStream);
                    fileWriter.AutoFlush = true;
                    file.CopyTo(fileStream);
                }
                Database dbase = new SqlDataBaseConnector();
                var a=dbase.ConnectToFile(path);
                switch (id)
                {
                    case 1:
                        {
                            db.FirstDatabase = dbase;
                            break;
                        }
                    case 2:
                        {
                            db.SecondDatabase = dbase;
                            break;
                        }
                }
            } 
            return;
        }
        #endregion
    }
}
