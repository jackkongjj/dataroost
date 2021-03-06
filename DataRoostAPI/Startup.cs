using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http.Formatting;
using System.Web;
using System.Web.Http;
using System.Web.Http.Cors;

using CCS.Fundamentals.DataRoostAPI.Filters;

using Microsoft.Owin;
using Microsoft.Owin.Cors;

using Newtonsoft.Json.Converters;

using Owin;

[assembly: log4net.Config.XmlConfigurator(Watch = true)]
[assembly: OwinStartup(typeof(DataRoost.Startup))]
namespace DataRoost {

	public class Startup {

		public void Configuration(IAppBuilder app) {

			app.UseCors(CorsOptions.AllowAll);

			HttpConfiguration config = new HttpConfiguration();
			//config.EnableCors(new EnableCorsAttribute("*", "*", "*"));

			config.Formatters.Clear();
			JsonMediaTypeFormatter jsonFormatter = new JsonMediaTypeFormatter();
			StringEnumConverter enumConverter = new StringEnumConverter();
			jsonFormatter.SerializerSettings.Converters.Add(enumConverter);
			jsonFormatter.Indent = true;
			config.Formatters.Add(jsonFormatter);

			config.Filters.Add(new ExceptionHandlerFilter());

			config.MapHttpAttributeRoutes();

			app.UseWebApi(config);
		}
	}
}