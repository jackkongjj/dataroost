using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Web;
using DataRoostAPI.Common.Models.AsReported;
using FactSet.Data.SqlClient;
using CCS.Fundamentals.DataRoostAPI.CommLogger;

namespace CCS.Fundamentals.DataRoostAPI.Access.AsReported {
	public class CompanyFinancialTermsHelper {

		private readonly string _sfConnectionString;

		public CompanyFinancialTermsHelper(string sfConnectionString) {
			_sfConnectionString = sfConnectionString;
		}

		public CompanyFinancialTerm[] GetCompanyFinancialTerms(int iconum) {

            string starttime = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff");
            string query =
				@"SELECT DISTINCT cft.ID, cft.Description
																			FROM DocumentSeries ds
																					JOIN CompanyFinancialTerm cft ON cft.DocumentSeriesId = ds.Id
																			WHERE ds.CompanyID = @iconum";

			List<CompanyFinancialTerm> companyFinancialTerms = new List<CompanyFinancialTerm>();
			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {
				using (SqlCommand cmd = new SqlCommand(query, conn)) {
					conn.Open();
					cmd.Parameters.AddWithValue("@iconum", iconum);
					using (SqlDataReader reader = cmd.ExecuteReader()) {
						while (reader.Read()) {
							CompanyFinancialTerm document = new CompanyFinancialTerm
							{
								Id = reader.GetInt32(0),
								Description = reader.GetStringSafe(1),
							};
							companyFinancialTerms.Add(document);
						}
					}
				}
			}
            CommunicationLogger.LogEvent("GetCompanyFinancialTerms", "DataRoost", starttime, DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff"));
            return companyFinancialTerms.ToArray();
		}
	}
}