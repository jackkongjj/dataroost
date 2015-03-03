using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Data.SqlClient;
using FactSet.Data.SqlClient;
using CCS.Fundamentals.DataRoostAPI.Models;
using CCS.Fundamentals.DataRoostAPI.Models.AsReported;

namespace CCS.Fundamentals.DataRoostAPI.Access.AsReported {
	
	public class DocumentHelper {

		private string _sfConnectionString;

		public DocumentHelper(string sfConnectionString) {
			_sfConnectionString = sfConnectionString;
		}

		//public void GetDocument(int iconum, string documentId) {
		//	string query = @"";

		//	using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {
		//		using (SqlCommand cmd = new SqlCommand(query, conn)) {
		//			conn.Open();
		//			cmd.Parameters.AddWithValue("@iconum", iconum);

		//			using (SqlDataReader sdr = cmd.ExecuteReader()) {
		//				if (sdr.Read()) {
		//					AsReportedDocument document = new AsReportedDocument
		//					{

		//					};

		//					company.Name = sdr.GetStringSafe(0);
		//					company.CompanyType = sdr.GetStringSafe(1);
		//					company.CountryId = sdr.GetStringSafe(4);
		//					company.Country = new CountryDTO()
		//					{
		//						LongName = sdr.GetStringSafe(2),
		//						ShortName = sdr.GetStringSafe(3),
		//						Id = sdr.GetStringSafe(4),
		//						Iso3 = sdr.GetStringSafe(4),
		//					};
		//				}
		//			}
		//		}
		//	}
		//}

		//public AsReportedTable[] GetDocumentTables() {

		//}
	}
}