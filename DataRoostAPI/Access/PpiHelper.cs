using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Data;
using System.Data.SqlClient;
using Oracle.ManagedDataAccess.Client;
using FactSet.Data.SqlClient;

using Fundamentals.Helpers.DataAccess;

namespace CCS.Fundamentals.DataRoostAPI.Access.Voyager {
	public class PpiHelper : SqlHelper {

		private readonly string _connectionString;

		public PpiHelper(string connectionString) {
			_connectionString = connectionString;
		}

		public string GetPPIByIconum(int iconum) {
			string query = "SELECT PPI FROM FdsTriPpiMap WHERE iconum = @iconum";

			return ExecuteQuery<string>(query, new List<SqlParameter> { new SqlParameter("@iconum", iconum) }, reader => {
				                                                                                    return reader.GetString(0);
			                                                                                    }).FirstOrDefault();
		}

		public string GetPPIBase(string ppi) {
			return ppi.Substring(0, ppi.Length - 1) + "%";
		}

		protected override SqlConnection GetDatabaseConnection() {
			return new SqlConnection(_connectionString);
		}
	}
}