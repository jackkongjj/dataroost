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

		public static IAsReportedDataAccess GetAsReportedDataAccess(string connectionString) {
			return new AsReportedDataAccess(connectionString);
		}

		public static ICompanyDataAccess GetCompanyDataAccess(string connectionString) {
			return new CompanyDataAccess(connectionString);
		}
	}
}
