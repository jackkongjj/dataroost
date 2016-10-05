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

		public Dictionary<int, Dictionary<Guid, TimeseriesDTO>> GetExportedTimeSeries(string versionId, string secPermId, string timeSeriesId) {
			const string query = @"select ID             ,
															PeriodEndDate		  ,
															Duration			  ,
															PeriodTypeID		  ,
															FiscalYear			  ,
															AcquisitionStatusID	  ,
															AccountingStandardID  ,
															ConsolidatedTypeID	  ,
															IsRecap				  ,
															IsProforma			  ,
															IsRestated			  ,
															VersionId
															 from TimeSlice where VersionId = @VersionId and permID = @PermId and id = isnull(@timeSeriesId, id)";
			
			Dictionary<int, Dictionary<Guid, TimeseriesDTO>> result = new Dictionary<int, Dictionary<Guid, TimeseriesDTO>>();
			List<KpiTimeSeriesDTO> list = new List<KpiTimeSeriesDTO>();
 
			using (SqlConnection sqlConn = new SqlConnection(connectionString))
			using (SqlCommand cmd = new SqlCommand(query, sqlConn)) {
				cmd.CommandType = CommandType.Text;
				cmd.Parameters.AddWithValue("@VersionId", versionId);
				cmd.Parameters.AddWithValue("@PermId", secPermId);
				if(timeSeriesId == null)
					cmd.Parameters.AddWithValue("@timeSeriesId", DBNull.Value);
				else
					cmd.Parameters.AddWithValue("@timeSeriesId", Guid.Parse(timeSeriesId));
				sqlConn.Open();
				using (SqlDataReader sdr = cmd.ExecuteReader()) {
					while (sdr.Read()) {
						list.Add(new KpiTimeSeriesDTO
						{
							Id = sdr.GetGuid(0).ToString(),
							PeriodEndDate = sdr.GetDateTime(1),
							Duration = sdr.IsDBNull(2) ? 0 : sdr.GetInt16(2),
							PeriodType = sdr.GetStringSafe(3).Trim(),
							CompanyFiscalYear = sdr.GetInt32(4),
							AcquisitionStatus = sdr.GetStringSafe(5),
							AccountType = sdr.GetStringSafe(6),
							ConsolidatedType = sdr.GetStringSafe(7),
							IsRecap = sdr.GetBoolean(8),
							IsProforma = sdr.IsDBNull(9) ? false : sdr.GetBoolean(9),
							IsRestated = sdr.IsDBNull(10) ? false : sdr.GetBoolean(10),
							VersionId = sdr.GetInt32(11).ToString()					
						});
					}
				}
			}
			foreach (var v in list.GroupBy(o => o.CompanyFiscalYear)) {
				Dictionary<Guid, TimeseriesDTO> vts = new Dictionary<Guid, TimeseriesDTO>();
				foreach (var a in v) {
					if (timeSeriesId != null)
						a.Values = _GetTimeseriesSTDValues(a.Id, a.VersionId);
					vts.Add(Guid.Parse(a.Id), a);
				}
				result.Add(v.Key, vts);
			}

			return result;
		}

		private Dictionary<string, TimeseriesValueDTO> _GetTimeseriesSTDValues(string timeSliceId, string versionId) {
			Dictionary<string, TimeseriesValueDTO> toRet = new Dictionary<string, TimeseriesValueDTO>();
			const string query = @" select si.STDCode, si.ItemName, sv.NumericValue from STDValue sv 
															 join STDItem si on si.STDCode = sv.STDCode
															 join UnitAlias ua on ua.ID = sv.UnitAliasID
															 where sv.TimeSliceID = @TimeSliceId and sv.VersionID = @VersionId";
			using (SqlConnection sqlConn = new SqlConnection(connectionString))
			using (SqlCommand cmd = new SqlCommand(query, sqlConn)) {
				cmd.Parameters.AddWithValue("@TimeSliceId", timeSliceId);
				cmd.Parameters.AddWithValue("@VersionId", versionId);
				sqlConn.Open();
				using (SqlDataReader reader = cmd.ExecuteReader(CommandBehavior.SingleResult | CommandBehavior.SequentialAccess)) {
					while (reader.Read()) {
						TimeseriesValueDTO val = new TimeseriesValueDTO();
						int c = 0;
						string stdItem = reader.GetStringSafe(c++);
						string description = reader.GetStringSafe(c++);
						decimal value = reader.GetDecimal(c++);
						val.Contents = value.ToString();

						val.ValueDetails = new LookupTimeseriesValueDetailKpiDTO() { LookupName = description, ItemCode = stdItem, NumericValue = value, Value = value.ToString() };
						toRet.Add(stdItem, val);
					}
				}
			}
			return toRet;
		}
	}
}