using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Data;
using System.Data.SqlClient;

using DataRoostAPI.Common.Exceptions;

using Oracle.ManagedDataAccess.Client;
using FactSet.Data.SqlClient;

using Fundamentals.Helpers.DataAccess;

namespace CCS.Fundamentals.DataRoostAPI.Access.Voyager {
	public class PpiHelper : SqlHelper {

		private readonly string _connectionString;

		public PpiHelper(string connectionString) {
			_connectionString = connectionString;
		}

		public Dictionary<string, int> GetIconumPpiDictionary(List<int> iconums) {
			Dictionary<string, int> iconumPpiDictionary = new Dictionary<string, int>();
			DataTable table = new DataTable();
			table.Columns.Add("iconum", typeof(int));
			foreach (int iconum in iconums) {
				table.Rows.Add(iconum);
			}

			const string createTableQuery = @"CREATE TABLE #CompanyIds ( iconum INT NOT NULL )";

			const string query = @"SELECT i.iconum, fds.PPI FROM FdsTriPpiMap fds JOIN #CompanyIds i ON i.iconum = fds.iconum WHERE fds.Cusip IS NOT NULL";

			using (SqlConnection connection = new SqlConnection(_connectionString)) {
				connection.Open();
				using (SqlCommand cmd = new SqlCommand(createTableQuery, connection)) {
					cmd.ExecuteNonQuery();
				}

				// Upload all iconums to Temp table
				using (SqlBulkCopy bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, null)) {
					bulkCopy.BatchSize = table.Rows.Count;
					bulkCopy.DestinationTableName = "#CompanyIds";
					bulkCopy.WriteToServer(table);
				}

				using (SqlCommand cmd = new SqlCommand(query, connection)) {
					using (SqlDataReader reader = cmd.ExecuteReader()) {
						while (reader.Read()) {
							int iconum = reader.GetInt32(0);
							string ppi = reader.GetStringSafe(1);
							if (!iconumPpiDictionary.ContainsKey(ppi)) {
								iconumPpiDictionary.Add(ppi, iconum);
							}
						}
					}
				}
			}

			return iconumPpiDictionary;
		}

		public string GetPPIByIconum(int iconum) {
			string query = "SELECT PPI FROM FdsTriPpiMap WHERE iconum = @iconum ORDER BY IsAdr, cusip DESC, PPI";

			string ppi = ExecuteQuery<string>(query, new List<SqlParameter> { new SqlParameter("@iconum", iconum) }, reader => {
				                                                                                    return reader.GetString(0);
			                                                                                    }).FirstOrDefault();
			if (ppi == null) {
				throw new SymbologyMappingException(string.Format("Can't find PPI for Iconum {0}", iconum));
			}
			return ppi;
		}

		public string GetPPIBase(string ppi) {
			return ppi.Substring(0, ppi.Length - 1) + "%";
		}

		protected override SqlConnection GetDatabaseConnection() {
			return new SqlConnection(_connectionString);
		}
	}
}