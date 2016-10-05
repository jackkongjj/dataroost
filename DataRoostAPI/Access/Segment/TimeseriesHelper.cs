using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Data;
using System.Data.SqlClient;
using System.Linq;

using DataRoostAPI.Common.Models;
using DataRoostAPI.Common.Models.Segment;
using DataRoostAPI.Common.Models.TimeseriesValues;

using FactSet.Data.SqlClient;

namespace CCS.Fundamentals.DataRoostAPI.Access.Segment {
	public class TimeseriesHelper {
		private readonly string connectionString;

		public TimeseriesHelper(string connectionString) {
			this.connectionString = connectionString;
		}

		public Dictionary<int, Dictionary<int, TimeseriesDTO>> GetExportedTimeSeries(string versionId, string secPermId, string timeSeriesId) {
			const string query = @"select Id,
															PeriodEndDate,
															PeriodType,
															FiscalYear,
															IsRestated,
															Duration,
															Currency,
															ContentSource,
															PrimaryDocumentId,
															IsFish from SegEx.TimeSeries WHERE VersionId = @VersionId and Id = isnull(@timeSeriesId, Id)";

			Dictionary<int, Dictionary<int, TimeseriesDTO>> result = new Dictionary<int, Dictionary<int, TimeseriesDTO>>();
			List<SegmentTimeSeriesDTO> list = new List<SegmentTimeSeriesDTO>();

			using (SqlConnection sqlConn = new SqlConnection(connectionString))
			using (SqlCommand cmd = new SqlCommand(query, sqlConn)) {
				cmd.CommandType = CommandType.Text;
				cmd.Parameters.AddWithValue("@VersionId", versionId);

				if (timeSeriesId == null)
					cmd.Parameters.AddWithValue("@timeSeriesId", DBNull.Value);
				else
					cmd.Parameters.AddWithValue("@timeSeriesId", int.Parse(timeSeriesId));
				sqlConn.Open();
				using (SqlDataReader sdr = cmd.ExecuteReader()) {
					while (sdr.Read()) {
						list.Add(new SegmentTimeSeriesDTO
						{
							Id = sdr.GetInt32(0).ToString(),
							PeriodEndDate = sdr.GetDateTime(1),							
							PeriodType = sdr.GetStringSafe(2),
							CompanyFiscalYear = sdr.GetInt32(3),
							IsRestated = sdr.GetBoolean(4),
							Duration = sdr.GetInt32(5),
							IsoCurrency = sdr.GetStringSafe(6),
							ContentSource = sdr.GetStringSafe(7),
							IsFish = sdr.GetBoolean(9),
							VersionId = versionId						
						});
					}
				}
			}
			foreach (var v in list.GroupBy(o => o.CompanyFiscalYear)) {
				Dictionary<int, TimeseriesDTO> vts = new Dictionary<int, TimeseriesDTO>();
				foreach (var a in v) {
					if (timeSeriesId != null)
						a.Values = _GetTimeseriesSTDValues(a.Id, a.VersionId, secPermId);
					vts.Add(int.Parse(a.Id), a);
				}
				result.Add(v.Key, vts);
			}

			return result;
		}

		private Dictionary<string, TimeseriesValueDTO> _GetTimeseriesSTDValues(string timeSliceId, string versionId, string secPermId) {
			Dictionary<string, TimeseriesValueDTO> toRet = new Dictionary<string, TimeseriesValueDTO>();
			const string query_g = @"select null as ConceptName, gai.Name as AccountTitle , null as SegmentTitle, gvv.AsReportedLabel ,null as AsRepStdCode,null as STDCode, null as SICCode, null as NAICCode,null as SICStdCode, null as NAICStdCode, gvv.Value , gvv.MathML,  null as IsCorpElim ,null as IsExceptionalCharges, null as IsDiscontinued , 'G' as Type
															from [SegEx].[GeoRevValues] gvv
																		 join SegEx.TimeSeries t on gvv.tsid = t.id and gvv.VersionId= t.VersionId
																		 left join segex.Versions v
																						on gvv.versionid = v.id
																		 left join [SegEx].[GeorevStructuredFootNotes] gsn
																		 on v.ID = gsn.VersionID
																		 and gvv.tsid = gsn.timesliceid 
																		 and gsn.GeoRevValueID = gvv.id
																		 left join  [SegEx].[GeoRevDesc] gai
																		 on gai.areaid = gsn.areaid
																	 where  gvv.VersionID = @Versionid and gsn.TimeSliceID = @TimeSeriesId
															union
															Select sct.ConceptName , a.Title as AccountTitle , s.Title as SegmentTitle ,null as AsReportedLabel, s.AsRepStdCode , s.StandardizedStdCode as STDCode, s.SIC as SICCode , s.NAIC as NAICCode , s.SICStdCode , s.NAICStdCode , v.AsReportedValue as Value , v.Mathml , s.IsCorpElim , s.IsExceptionalCharges , s.IsDiscontinued, 'O' as Type
																from SegEx.[Values] v 
																left join SegEx.Versions c with (nolock) on v.VersionId = c.id 
																left join segEx.Segments s with (nolock) on s.versionid = c.id and s.VersionId = v.VersionId and v.SegmentId = s.id and v.PeriodId = s.tsid
																left join segex.segmentconcepttypes sct with (nolock) on sct.id = s.SegmentConceptType
																left join segEx.TimeSeries t with (nolock) on t.VersionId = c.id and t.VersionId = v.VersionId and t.Id =v.PeriodId
																left join SegEx.Account a  with (nolock) on a.ID  = v.AccountId
															where v.VersionId = @Versionid and t.Id = @TimeSeriesId
															union 
															Select sc.ConceptName,A.Title as AccountTitle, null as SegmentTitle,null as AsReportedLabel,null as AsRepStdCode,t.STDCode,t.SICCode,t.NAICCode,t.SICStdCode,t.NAICStdCode ,t.Total as Value , null as Mathml , null as IsCorpElim ,null as IsExceptionalCharges, null as IsDiscontinued, 'T' as Type from SegEx.Totals T JOIN SegEx.Versions V ON T.VersionId = v.ID
																														left join SegEx.TimeSeries ts on t.VersionId = ts.VersionId and t.tsid = ts.id  
																																								left join SegEx.Account a on a.id = t.AccountId
																													 join SegEx.SegmentConceptTypes sc on sc.id = t.SegConceptTypeId 
															 WHERE V.PermSecId = @PermId AND  v.id = @Versionid and ts.Id = @TimeSeriesId";

			var list = new[] { new { conceptName = "", AccountName = "", SegNode = new SegmentNode(), type = "" } }.ToList();
			list.Clear();
			using (SqlConnection sqlConn = new SqlConnection(connectionString))
			using (SqlCommand cmd = new SqlCommand(query_g, sqlConn)) {
				cmd.Parameters.AddWithValue("@TimeSeriesId", timeSliceId);
				cmd.Parameters.AddWithValue("@VersionId", versionId);
				cmd.Parameters.AddWithValue("@PermId", secPermId);
				sqlConn.Open();
				using (SqlDataReader reader = cmd.ExecuteReader(CommandBehavior.SingleResult)) {
					while (reader.Read()) {						
						list.Add(new
						{
							conceptName = reader.GetStringSafe(0),
							AccountName = reader.GetStringSafe(1),
							SegNode = new SegmentNode()
							{
								SegmentTitle = reader.GetStringSafe(2),
								AsReportedLabel = reader.GetStringSafe(3),
								AsReportedSTDCode = reader.GetNullable<int>(4),
								STDCode = reader.GetNullable<int>(5),
								SICCode = reader.GetStringSafe(6),
								NAICCode = reader.GetStringSafe(7),
								SICStdCode = reader.GetNullable<int>(8),
								NAICStdCode = reader.GetNullable<int>(9),
								Value = reader.GetDecimal(10),
								MathMl = reader.GetStringSafe(11),
								IsCorpElim = reader.GetNullable<bool>(12),
								IsExceptionalCharges = reader.GetNullable<bool>(13),
								IsDiscontinued = reader.GetNullable<bool>(14),
								Type = reader.GetStringSafe(15),
								VersionId = Guid.Parse(versionId)
							},
							type = reader.GetStringSafe(15)
						});
					}
				}
			}

			
			foreach (var vTypes in list.GroupBy(o => o.type)) {
				if (vTypes.Key == "O") {
					Dictionary<string, Dictionary<string, Dictionary<string, SegmentNode>>> conceptTypes = new Dictionary<string, Dictionary<string, Dictionary<string, SegmentNode>>>();
					foreach (var conceptType in vTypes.GroupBy(o => o.conceptName)) {
						Dictionary<string, Dictionary<string, SegmentNode>> AccountTypes = new Dictionary<string, Dictionary<string, SegmentNode>>();
						foreach (var accountType in conceptType.GroupBy(o => o.AccountName)) {
							Dictionary<string, SegmentNode> SegmentTypes = new System.Collections.Generic.Dictionary<string, SegmentNode>();
							foreach (var seg in accountType) {
								SegmentTypes.Add(seg.SegNode.SegmentTitle, seg.SegNode);
							}
							AccountTypes.Add(accountType.Key, SegmentTypes);
						}
						conceptTypes.Add(conceptType.Key, AccountTypes);
					}
					TimeseriesValueDTO tItems = new TimeseriesValueDTO()
					{
						Contents = "O",
						ValueDetails = new SegmentTimeseriesValueODetailDTO() { Detail = conceptTypes }
					};
					toRet.Add("O", tItems);
				} else if (vTypes.Key == "G") {
					Dictionary<string, SegmentNode> gDetailItems = new Dictionary<string, SegmentNode>(); 
					foreach (var geo in vTypes) {
						gDetailItems.Add(geo.AccountName + " - " + geo.SegNode.AsReportedLabel, geo.SegNode);
					}
					TimeseriesValueDTO oItems = new TimeseriesValueDTO() { Contents = "G", ValueDetails = new SegmentTimeseriesValueGDetailDTO() { Detail = gDetailItems } };
					toRet.Add("G", oItems);
				} else if (vTypes.Key == "T") {
					Dictionary<string, Dictionary<string, SegmentNode>> conceptTypes = new Dictionary<string, Dictionary<string, SegmentNode>>();
					foreach (var conceptType in vTypes.GroupBy(o => o.conceptName)) {
						Dictionary<string, SegmentNode> AccountTypes = new System.Collections.Generic.Dictionary<string, SegmentNode>();
						foreach (var accountType in conceptType) {
							AccountTypes.Add(accountType.AccountName + " - " + accountType.SegNode.STDCode, accountType.SegNode);
						}
						conceptTypes.Add(conceptType.Key, AccountTypes);
					}
					TimeseriesValueDTO tItems = new TimeseriesValueDTO() { Contents = "T", 
						ValueDetails = new SegmentTimeseriesValueTDetailDTO() { Detail = conceptTypes } };
					toRet.Add("T", tItems);
				}
			}
			
			return toRet;
		}
	}
}