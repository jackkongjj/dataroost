using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Web;

using DataRoostAPI.Common.Models;
using DataRoostAPI.Common.Models.AsReported;

using FactSet.Data.SqlClient;

namespace CCS.Fundamentals.DataRoostAPI.Access.SuperFast {
	public class DocumentHelper {

		private readonly string _sfConnectionString;

		public DocumentHelper(string sfConnectionString) {
			_sfConnectionString = sfConnectionString;
		}

		public Document[] GetDocuments(int iconum, DateTime startDate, DateTime endDate, string reportType) {
			const string queryWithReportType =
				@"SELECT d.DocumentDate, d.PublicationDateTime, d.ReportTypeID, d.FormTypeID, d.DAMDocumentId, d.Id
																			FROM DocumentSeries s
																					JOIN Document d ON d.DocumentSeriesID = s.Id
																			WHERE s.CompanyID = @iconum
																				AND (d.ExportFlag = 1 OR d.ArdExportFlag = 1)
																				AND d.ReportTypeID = @reportType
																				AND d.DocumentDate >= @startDate
																				AND d.DocumentDate <= @endDate
																			ORDER BY d.DocumentDate DESC";
			const string queryWithoutReportType =
				@"SELECT d.DocumentDate, d.PublicationDateTime, d.ReportTypeID, d.FormTypeID, d.DAMDocumentId, d.Id
																			FROM DocumentSeries s
																					JOIN Document d ON d.DocumentSeriesID = s.Id
																			WHERE s.CompanyID = @iconum
																				AND (d.ExportFlag = 1 OR d.ArdExportFlag = 1)
																				AND d.DocumentDate >= @startDate
																				AND d.DocumentDate <= @endDate
																			ORDER BY d.DocumentDate DESC";
			string query = null;
			if (reportType == null) {
				query = queryWithoutReportType;
			} else {
				query = queryWithReportType;
			}
			List<Document> documents = new List<Document>();
			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {
				using (SqlCommand cmd = new SqlCommand(query, conn)) {
					conn.Open();
					cmd.Parameters.AddWithValue("@iconum", iconum);
					cmd.Parameters.AddWithValue("@startDate", startDate);
					cmd.Parameters.AddWithValue("@endDate", endDate);
					if (reportType != null) {
						cmd.Parameters.AddWithValue("@reportType", reportType);
					}
					using (SqlDataReader reader = cmd.ExecuteReader()) {
						while (reader.Read()) {
							AsReportedDocument document = new AsReportedDocument
							{
								ReportDate = reader.GetDateTime(0),
								PublicationDate = reader.GetDateTime(1),
								ReportType = reader.GetStringSafe(2),
								FormType = reader.GetStringSafe(3),
								Id = reader.GetGuid(4).ToString(),
								SuperFastDocumentId = reader.GetGuid(5).ToString(),
							};
							documents.Add(document);
						}
					}
				}
			}
			return documents.ToArray();
		}

	}
}