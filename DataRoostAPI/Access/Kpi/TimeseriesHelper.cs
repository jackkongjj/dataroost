using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Data;
using System.Data.SqlClient;
using System.Linq;

using DataRoostAPI.Common.Models;
using DataRoostAPI.Common.Models.KPI;
using DataRoostAPI.Common.Models.TimeseriesValues;

using FactSet.Data.SqlClient;

namespace CCS.Fundamentals.DataRoostAPI.Access.Kpi {
	public class TimeseriesHelper {
		private readonly string connectionString;

		public TimeseriesHelper(string connectionString) {
			this.connectionString = connectionString;
		}

		public Dictionary<int, Dictionary<Guid, KpiTimeSeriesDTO>> GetExportedTimeSeries(string versionId, string secPermId) {
			const string query = @"select PeriodEndDate,
																Duration,
																PeriodTypeID,
																FiscalYear,
																AcquisitionStatusID,
																AccountingStandardID,
																ConsolidatedTypeID,
																IsRecap,
																IsProforma,
																IsRestated,
																ID
															 from TimeSlice where VersionId = @VersionId and permID = @PermId ";

			Dictionary<int, Dictionary<Guid, KpiTimeSeriesDTO>> result = new Dictionary<int, Dictionary<Guid, KpiTimeSeriesDTO>>();
			List<KpiTimeSeriesDTO> list = new List<KpiTimeSeriesDTO>();
 
			using (SqlConnection sqlConn = new SqlConnection(connectionString))
			using (SqlCommand cmd = new SqlCommand(query, sqlConn)) {
				cmd.CommandType = CommandType.Text;
				cmd.Parameters.AddWithValue("@VersionId", versionId);
				cmd.Parameters.AddWithValue("@PermId", secPermId);
		
				sqlConn.Open();
				using (SqlDataReader sdr = cmd.ExecuteReader()) {
					while (sdr.Read()) {
						list.Add(new KpiTimeSeriesDTO
						{
							PeriodEndDate = sdr.GetDateTime(0),
							Duration = sdr.GetInt16(1),
							PeriodType = sdr.GetStringSafe(2),
							CompanyFiscalYear = sdr.GetInt32(3),
							AcquisitionStatus = sdr.GetStringSafe(4),
							AccountingStandard = sdr.GetStringSafe(5),
							ConsolidatedType = sdr.GetStringSafe(6),
							IsRecap = sdr.GetBoolean(7),
							IsProforma = sdr.GetBoolean(8),
							IsRestated = sdr.GetBoolean(9),
							Id = sdr.GetGuid(10),

						});
					}
				}
			}

			foreach (var year in list.GroupBy(o => o.CompanyFiscalYear)) {
				Dictionary<Guid, KpiTimeSeriesDTO> vts = new Dictionary<Guid, KpiTimeSeriesDTO>();
				foreach (var ts in year) {
					string Restated = ts.IsRestated ? " - Restated" : "";
					string Recap = ts.IsRecap ? " - Recap" : "";
					ts.AAADisplay = ts.PeriodEndDate.ToString("MMM-dd-yyyy") + " - " + ts.PeriodType + Recap + Restated;
					vts.Add(ts.Id, ts);
				}
				result.Add(year.Key, vts);
			}

			return result;
		}

		public Dictionary<string, string> GetTimeseriesSTDValues(string timeSliceId, string versionId) {
			Dictionary<string, string> toRet = new System.Collections.Generic.Dictionary<string, string>();
			const string query = @" SELECT  ts.ID , std.VersionID , std.STDCode  , si.ItemName , std.NumericValue, ua.UnitDescription  from TimeSlice ts 
																join STDValue std on std.TimeSliceID = ts.ID and std.VersionID = ts.VersionId
																join STDItem si on si.STDCode = std.STDCode
																left join UnitAlias ua on ua.ID = std.UnitAliasID
															 where std.TimeSliceID = @TimeSliceId and std.VersionID = @VersionId";
			using (SqlConnection sqlConn = new SqlConnection(connectionString))
			using (SqlCommand cmd = new SqlCommand(query, sqlConn)) {
				cmd.Parameters.AddWithValue("@TimeSliceId", timeSliceId);
				cmd.Parameters.AddWithValue("@VersionId", versionId);
				sqlConn.Open();
				using (SqlDataReader reader = cmd.ExecuteReader()) {
					while (reader.Read()) {
						toRet.Add(reader.GetStringSafe(2) + " - " + reader.GetStringSafe(3), string.Format("{0:###,###,###,###,###,##0.#####}", reader.GetDecimal(4)) + "  " + reader.GetStringSafe(5));
					}
				}
			}
			return toRet;
		}
	}
}