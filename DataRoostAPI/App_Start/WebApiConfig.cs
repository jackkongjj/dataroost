using System;
using System.Collections.Generic;
using System.Linq;
using System.Web.Http;
using System.Web.Http.Cors;

namespace CCS.Fundamentals.DataRoostAPI {
	public static class WebApiConfig {
		public static void Register(HttpConfiguration config) {

			config.EnableCors(new EnableCorsAttribute("*", "*", "*"));

			config.MapHttpAttributeRoutes();

			config.Routes.MapHttpRoute(
					name: "DefaultApi",
					routeTemplate: "api/{controller}/{id}",
					defaults: new { id = RouteParameter.Optional }
			);

			// Our DTO inheritance really doesn't play well with XML, so we just completely disable it.
			config.Formatters.Remove(config.Formatters.XmlFormatter);

			// Because spacing things out nice makes things easy to read
			config.Formatters.JsonFormatter.Indent = true;

			// Uncomment the following line of code to enable query support for actions with an IQueryable or IQueryable<T> return type.
			// To avoid processing unexpected or malicious queries, use the validation settings on QueryableAttribute to validate incoming queries.
			// For more information, visit http://go.microsoft.com/fwlink/?LinkId=279712.
			//config.EnableQuerySupport();

			// To disable tracing in your application, please comment out or remove the following line of code
			// For more information, refer to: http://www.asp.net/web-api
			config.EnableSystemDiagnosticsTracing();
		}
	}
}
