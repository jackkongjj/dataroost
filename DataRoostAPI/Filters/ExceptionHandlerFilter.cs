using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Web;
using System.Web.Http.Filters;

using log4net;

namespace CCS.Fundamentals.DataRoostAPI.Filters {

	public class ExceptionHandlerFilter : ExceptionFilterAttribute {

		private static readonly ILog logger = LogManager.GetLogger(typeof(ExceptionHandlerFilter));

		public override void OnException(HttpActionExecutedContext actionExecutedContext) {
			string message = string.Format("URL: {0}", actionExecutedContext.Request.RequestUri);
			logger.Error(message, actionExecutedContext.Exception);
		}
	}
}