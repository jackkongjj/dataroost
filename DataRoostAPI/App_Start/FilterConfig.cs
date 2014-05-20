using System.Web;
using System.Web.Mvc;

namespace DataRoostAPI {
	public class FilterConfig {
		public static void RegisterGlobalFilters(GlobalFilterCollection filters) {
			filters.Add(new HandleErrorAttribute());
		}
	}
}