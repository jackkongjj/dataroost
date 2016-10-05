using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Web;
using DataRoostAPI.Common.Models;

namespace CCS.Fundamentals.DataRoostAPI.Access.Segment {
	public class ExportVersionHelper {
		private readonly string _segmentConnectionString;

		public ExportVersionHelper(string segmentConnectionString) {
			_segmentConnectionString = segmentConnectionString;
		}

		public ExportedVersionInfo[] GetExportedVersion(string SecPermId) {
			if (string.IsNullOrEmpty(SecPermId))
				return null;

			List<ExportedVersionInfo> list = new List<ExportedVersionInfo>();
			const string query = @"SELECT ID, ExportDateUTC from SegEx.Versions where PermSecId = @PermId";
			using (SqlConnection conn = new SqlConnection(_segmentConnectionString)) {
				conn.Open();
				using (SqlCommand cmd = new SqlCommand(query, conn)) {
					cmd.Parameters.AddWithValue("@PermId", SecPermId);
					using (SqlDataReader reader = cmd.ExecuteReader()) {
						list.AddRange(
										reader.Cast<IDataRecord>().Select(r => new ExportedVersionInfo()
										{
											Id = reader.GetGuid(0).ToString(),
											ExportDateUtc = reader.GetDateTime(1)
										}));
					}
				}
			}
			return list.ToArray();
		}
	}
}