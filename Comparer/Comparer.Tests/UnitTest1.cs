using Comparer.Models;
using DBTest;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc;
using Comparer.Controllers;
using Moq;
using Xunit;


namespace Comparer.Tests
{
    public class UnitTest1
    {

        private readonly ICounter _counterService;

        private readonly IHostingEnvironment _hostingEnvironment;
        private int _randomInt;
        private DatabaseComparer db;

        [Fact]
        public void DownladPartialResultNotNull()
        {
            // Arrange
            HomeController controller = new HomeController(db, _hostingEnvironment, _counterService);
            // Act
            PartialViewResult result = controller.FilesDownloaded() as PartialViewResult;
            // Assert
            Assert.NotNull(result);
        }

        [Fact]
        public void TableInfoPartialResultNotNull()
        {
            // Arrange
            HomeController controller = new HomeController(db, _hostingEnvironment, _counterService);
            // Act
            PartialViewResult result = controller.TableInfo() as PartialViewResult;
            // Assert
            Assert.NotNull(result);
        }


        [Fact]
        public void SmallTableInfoPartialResultNotNull()
        {
            // Arrange
            HomeController controller = new HomeController(db, _hostingEnvironment, _counterService);
            // Act
            PartialViewResult result = controller.SmallTableInfo() as PartialViewResult;
            // Assert
            Assert.NotNull(result);
        }

        [Fact]
        public void SelectTablePartialResultNotNull()
        {
            // Arrange
            HomeController controller = new HomeController(db, _hostingEnvironment, _counterService);
            // Act
            PartialViewResult result = controller.SelectTable() as PartialViewResult;
            // Assert
            Assert.NotNull(result);
        }

        [Fact]
        public void AboutViewDataMessage()
        {
            // Arrange
            HomeController controller = new HomeController(db,_hostingEnvironment,_counterService);

            // Act
            ViewResult result = controller.About() as ViewResult;

            // Assert
            Assert.Equal("Your application description page.", result?.ViewData["Message"]);
        }

        [Fact]
        public void AboutViewResultNotNull()
        {
            // Arrange
            HomeController controller = new HomeController(db, _hostingEnvironment, _counterService);
            // Act
            ViewResult result = controller.About() as ViewResult;
            // Assert
            Assert.NotNull(result);
        }

        [Fact]
        public void AboutViewNameEqualIndex()
        {
            // Arrange
            HomeController controller = new HomeController(db, _hostingEnvironment, _counterService);
            // Act
            ViewResult result = controller.About() as ViewResult;
            // Assert
            Assert.Equal("About", result?.ViewName);
        }
   
    }
   
}
