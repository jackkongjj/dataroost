using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Reflection;
using System.Web.Http;

using CCS.Fundamentals.DataRoostAPI.Filters;

namespace CCS.Fundamentals.DataRoostAPI.Controllers
{
	[ExceptionHandlerFilter]
	[RoutePrefix("api/v1")]
	public class StatusController : ApiController {

		[AllowAnonymous]
		[Route("status/")]
		[HttpGet]
		public string GetStatus() {
			return "SUCCESS";
		}

		[AllowAnonymous]
		[Route("version/")]
		[HttpGet]
		public string GetVersion() {
			return Assembly.GetExecutingAssembly().GetName().Version.ToString();
		}
	}
}
