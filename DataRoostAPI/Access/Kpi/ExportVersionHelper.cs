using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Web;
using DataRoostAPI.Common.Models;

namespace CCS.Fundamentals.DataRoostAPI.Access.Kpi {
	public class ExportVersionHelper {
		private readonly string _kpiConnectionString;

		public ExportVersionHelper(string kpiConnectionString) {
			_kpiConnectionString = kpiConnectionString;
		}

		public ExportedVersionInfo[] GetExportedVersion(string SecPermId) {
			if (string.IsNullOrEmpty(SecPermId))
				return null;

			List<ExportedVersionInfo> list = new List<ExportedVersionInfo>();
			const string query = @"select distinct edl.VersionID , el.ExportDateUtc from ExportLog el 
															join ExportDocumentLog edl on edl.ExportID = el.ExportID
															join TimeSlice ts on ts.ExportID = edl.ExportID
															where ts.permID = @PermId";
			using (SqlConnection conn = new SqlConnection(_kpiConnectionString)) {
				conn.Open();
				using (SqlCommand cmd = new SqlCommand(query, conn)) {
					cmd.Parameters.AddWithValue("@PermId", SecPermId);
					using (SqlDataReader reader = cmd.ExecuteReader()) {
						list.AddRange(
										reader.Cast<IDataRecord>().Select(r => new ExportedVersionInfo()
										{
											Id=reader.GetInt32(0).ToString(),
											ExportDateUtc=reader.GetDateTime(1)
										}));
					}
				}
			}
			return list.ToArray();
		}
	}
}