using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using DataRoostAPI.Common.Interfaces;

namespace DataRoostAPI.Common.Access {
	public static class DataRoostAccessFactory {

		public static IStandardizedDataAccess GetVoyagerDataAccess(string connectionString) {
			return new StandardizedDataAccess(connectionString, "voyager");
		}

		public static IStandardizedDataAccess GetSuperFastDataAccess(string connectionString) {
			return new StandardizedDataAccess(connectionString, "superfast");
		}

		public static ISfVoyDataAccess GetSfVoyDataAccess(string connectionString) {
			return new SfVoyDataAccess(connectionString, "sfvoy_join");
		}

		public static IAsReportedDataAccess GetAsReportedDataAccess(string connectionString) {
			return new AsReportedDataAccess(connectionString);
		}

		public static ICompanyDataAccess GetCompanyDataAccess(string connectionString) {
			return new CompanyDataAccess(connectionString);
		}

		public static IExportedItemsDataAccess GetExportedItemsDataAccess(string connectionString) {
			return new ExportedItemsDataAccess(connectionString);
		}
	}
}
