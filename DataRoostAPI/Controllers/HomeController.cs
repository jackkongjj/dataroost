using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using CCS.Fundamentals.DataRoostAPI.CommLogger;


namespace CCS.Fundamentals.DataRoostAPI.Controllers {
    [CommunicationLogger]
    public class HomeController : Controller {
		public ActionResult Index() {
			return View();
		}
	}
}
