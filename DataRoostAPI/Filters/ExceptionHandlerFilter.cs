using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Web;
using System.Web.Http.Filters;

using NLog;

namespace CCS.Fundamentals.DataRoostAPI.Filters {

	public class ExceptionHandlerFilter : ExceptionFilterAttribute {

		private static readonly ILogger Logger = LogManager.GetCurrentClassLogger();

        public override void OnException(HttpActionExecutedContext actionExecutedContext) {
			string message = string.Format("URL: {0}", actionExecutedContext.Request.RequestUri);
            Logger.Error(actionExecutedContext.Exception, message);
		}
	}
}