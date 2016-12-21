using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.ComponentModel;
using System.Data;
using System.Data.SqlClient;
using System.Dynamic;
using System.Linq;
using DataRoostAPI.Common.Models;
using DataRoostAPI.Common.Models.Segment;
using DataRoostAPI.Common.Models.TimeseriesValues;
using FactSet.Data.SqlClient;
using Newtonsoft.Json;

namespace CCS.Fundamentals.DataRoostAPI.Access.Segment {
	public class TimeseriesHelper {
		private readonly string connectionString;

		public TimeseriesHelper(string connectionString) {
			this.connectionString = connectionString;
		}

		public List<Guid> GetDocumentId(int companyId, string timeSliceId) {
			List<Guid> Documents = new List<Guid>();
			const string query = @"select DISTINCT d.DocumentId from SegEx.TimeSeries  ts
															join SegEx.Documents d on d.VersionId = ts.VersionId
															where ts.id = @TimeSeriesId and ts.Iconum = @Iconum";

			using (SqlConnection sqlConn = new SqlConnection(connectionString))
			using (SqlCommand cmd = new SqlCommand(query, sqlConn)) {
				cmd.CommandType = CommandType.Text;
				cmd.Parameters.AddWithValue("@TimeSeriesId", timeSliceId);
				cmd.Parameters.AddWithValue("@Iconum", companyId);

				sqlConn.Open();
				using (SqlDataReader sdr = cmd.ExecuteReader()) {
					if (sdr.Read()) {
						Documents.Add(sdr.GetGuid(0));
					}
				}
			}
			return Documents;
		}


		public Dictionary<int, Dictionary<int, SegmentsTimeSeriesDTO>> GetExportedTimeSeries(string versionId) {
			const string query = @"select  ts.Id , ts.PeriodEndDate , ts.Duration , ts.PeriodType , ts.FiscalYear , ts.IsRestated , 
ts.Currency , ts.ContentSource , ts.IsFish from  SegEx.TimeSeries ts WHERE ts.VersionId = @VersionId ";

			Dictionary<int, Dictionary<int, SegmentsTimeSeriesDTO>> result = new Dictionary<int, Dictionary<int, SegmentsTimeSeriesDTO>>();
			List<SegmentsTimeSeriesDTO> list = new List<SegmentsTimeSeriesDTO>();

			using (SqlConnection sqlConn = new SqlConnection(connectionString))
			using (SqlCommand cmd = new SqlCommand(query, sqlConn)) {
				cmd.CommandType = CommandType.Text;
				cmd.Parameters.AddWithValue("@VersionId", versionId);

				sqlConn.Open();
				using (SqlDataReader sdr = cmd.ExecuteReader()) {
					while (sdr.Read()) {
						list.Add(new SegmentsTimeSeriesDTO
						{
							Id = sdr.GetInt32(0),
							PeriodEndDate = sdr.GetDateTime(1),
							Duration = sdr.GetInt32(2),
							PeriodType = sdr.GetStringSafe(3),
							CompanyFiscalYear = sdr.GetInt32(4),
							IsRestated = sdr.GetBoolean(5),
							Currency = sdr.GetStringSafe(6),
							ContentSource = sdr.GetStringSafe(7),
							IsFish = sdr.GetBoolean(8),
						});
					}
				}
			}

			foreach (var year in list.GroupBy(o => o.CompanyFiscalYear)) {
				Dictionary<int, SegmentsTimeSeriesDTO> vts = new Dictionary<int, SegmentsTimeSeriesDTO>();
				foreach (var ts in year) {
					string Restated = ts.IsRestated ? " - Restated" : "";
					ts.AAADisplay = ts.PeriodEndDate.ToString("MMM-dd-yyyy") + " - " + ts.ContentSource + Restated;
					vts.Add(ts.Id, ts);
				}
				result.Add(year.Key, vts);
			}

			return result;
		}

		public Dictionary<string, object> GetTimeseriesSTDValues(string timeSliceId, string versionId, string secPermId) {
			Dictionary<string, object> toRet = new System.Collections.Generic.Dictionary<string, object>();
			const string query_g = @"select null as ConceptName, gai.Name as AccountTitle , null as SegmentTitle, gvv.AsReportedLabel ,null as AsRepStdCode,null as STDCode, null as SICCode, null as NAICCode, gvv.Value , gvv.MathML,  null as IsCorpElim ,null as IsExceptionalCharges, null as IsDiscontinued , 'GeoRev' as Type, null

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
	   where  gvv.VersionID =@VersionId and gsn.TimeSliceID = @TimeSeriesId
union
select a.ConceptName , a.AccountTitle , b.SegmentTitle ,b.AsReportedLabel, b.AsRepStdCode ,b.STDCode,b.SICCode , b.NAICCode , b.Value , b.Mathml , b.IsCorpElim , b.IsExceptionalCharges , b.IsDiscontinued, isnull(b.Type,'Segments'), b.SegmentId
 from (
select ac.ID as AccountId , ac.Title as AccountTitle , ct.ID as ConceptId , ct.ConceptName as ConceptName  from 
SegEx.SegmentConceptTypes ct 
cross join SegEx.Account ac
) a
left join 
(
select s.SegmentConceptType , v.AccountId ,  s.Title as SegmentTitle ,null as AsReportedLabel, s.AsRepStdCode , s.StandardizedStdCode as STDCode,
s.SIC as SICCode , s.NAIC as NAICCode ,  v.AsReportedValue as Value , v.Mathml , s.IsCorpElim , s.IsExceptionalCharges , s.IsDiscontinued, 'Segments' as Type, s.Id as SegmentId
from  SegEx.Segments s 
left join SegEx.[Values] v on  v.PeriodId = s.TSID and s.VersionId = v.VersionId  and v.SegmentId = s.Id
left JOIN SegEx.Versions c on c.ID = v.VersionId
left join SegEx.TimeSeries t on t.VersionId = s.VersionId and t.Id = v.PeriodId
where s.VersionId =@VersionId  and t.Id = @TimeSeriesId )b on b.AccountId = a.AccountId and a.ConceptId = b.SegmentConceptType 
where  (b.STDCode is null or b.STDCode < 65999)
union 
Select 'Total',  sc.ConceptName as AccountTitle,a.Title as SegmentTitle,null as AsReportedLabel,null as AsRepStdCode,t.STDCode,t.SICCode,t.NAICCode ,t.Total as Value , null as Mathml , null as IsCorpElim ,null as IsExceptionalCharges, null as IsDiscontinued, 'Segments' as Type, null from SegEx.Totals T JOIN SegEx.Versions V ON T.VersionId = v.ID
                              left join SegEx.TimeSeries ts on t.VersionId = ts.VersionId and t.tsid = ts.id  
                                                  left join SegEx.Account a on a.id = t.AccountId
                             join SegEx.SegmentConceptTypes sc on sc.id = t.SegConceptTypeId 
 WHERE V.PermSecId = @PermId AND  v.id = @VersionId and ts.Id = @TimeSeriesId

union 


select a.ConceptName , a.AccountTitle , b.SegmentTitle ,b.AsReportedLabel, b.AsRepStdCode ,b.STDCode,b.SICCode , b.NAICCode , b.Value , b.Mathml , b.IsCorpElim , b.IsExceptionalCharges , b.IsDiscontinued, b.Type, b.SegmentId
 from (
select ac.ID as AccountId , ac.Title as AccountTitle , ct.ID as ConceptId , ct.ConceptName as ConceptName  from 
SegEx.SegmentConceptTypes ct 
cross join SegEx.Account ac
) a
left join 
(
select s.SegmentConceptType , v.AccountId ,  s.Title as SegmentTitle ,null as AsReportedLabel, s.AsRepStdCode , s.StandardizedStdCode as STDCode,
s.SIC as SICCode , s.NAIC as NAICCode ,  v.AsReportedValue as Value , v.Mathml , s.IsCorpElim , s.IsExceptionalCharges , s.IsDiscontinued, 'Segments BreakOut' as Type, s.Id as SegmentId
from  SegEx.Segments s 
left join SegEx.[Values] v on  v.PeriodId = s.TSID and s.VersionId = v.VersionId  and v.SegmentId = s.Id
left JOIN SegEx.Versions c on c.ID = v.VersionId
left join SegEx.TimeSeries t on t.VersionId = s.VersionId and t.Id = v.PeriodId
where s.VersionId =@VersionId  and t.Id = @TimeSeriesId )b on b.AccountId = a.AccountId and a.ConceptId = b.SegmentConceptType 
where b.STDCode > 65999


";

			List<SegmentNode> list = new List<SegmentNode>();
			using (SqlConnection sqlConn = new SqlConnection(connectionString))
			using (SqlCommand cmd = new SqlCommand(query_g, sqlConn)) {
				cmd.Parameters.AddWithValue("@TimeSeriesId", timeSliceId);
				cmd.Parameters.AddWithValue("@VersionId", versionId);
				cmd.Parameters.AddWithValue("@PermId", secPermId);
				sqlConn.Open();
				using (SqlDataReader sdr = cmd.ExecuteReader()) {
					while (sdr.Read()) {
						list.Add(new SegmentNode()
						{
							ConceptName = sdr.GetStringSafe(0),
							AccountName = sdr.GetStringSafe(1),
							SegmentTitle = sdr.GetStringSafe(2),
							AsReportedLabel = sdr.GetStringSafe(3),
							AsReportedSTDCode = sdr.GetNullable<int>(4),
							STDCode = sdr.GetNullable<int>(5),
							SICCode = sdr.GetStringSafe(6),
							NAICCode = sdr.GetStringSafe(7),
							AAAValue = sdr.GetNullable<decimal>(8) == null ? "" : string.Format("{0:###,###,###,###,###,##0.#####}", sdr.GetNullable<decimal>(8)),
							MathMl = sdr.GetStringSafe(9),
							IsCorpElim = sdr.GetNullable<bool>(10),
							IsExceptionalCharges = sdr.GetNullable<bool>(11),
							IsDiscontinued = sdr.GetNullable<bool>(12),
							Type = sdr.GetStringSafe(13),
							SegmentId = sdr.GetNullable<int>(14)
						});
					}
				}
			}

			List<FootNotes> footNotes = GetFootNotes(new Guid(versionId));

			foreach (var vTypes in list.GroupBy(o => o.Type)) {
				if (vTypes.Key == "Segments" || vTypes.Key == "Segments BreakOut") {
					Dictionary<string, object> conceptTypes = new System.Collections.Generic.Dictionary<string, object>();
					foreach (var conceptType in vTypes.GroupBy(o => o.ConceptName)) {
						Dictionary<string, object> AccountTypes = new System.Collections.Generic.Dictionary<string, object>();
						foreach (var accountType in conceptType.GroupBy(o => o.AccountName)) {
							Dictionary<string, object> SegmentTypes = new System.Collections.Generic.Dictionary<string, object>();
							foreach (var seg in accountType) {
								if (!string.IsNullOrWhiteSpace(seg.SegmentTitle)) {
									string segmentCode = conceptType.Key == "Total" ? seg.SegmentTitle + " - " + seg.STDCode : seg.SegmentTitle;

									if (conceptType.Key == "Total") {
										SegmentTypes.Add(segmentCode, seg.AAAValue);
									} else if (conceptType.Key == "Geo1OperationsSegments" || conceptType.Key == "Geo2CustomerLocationSegments") {
										IDictionary<string, object> segObject = seg.ToDynamic();
										int i = 0;
										segObject.Remove("NAICCode");
										segObject.Remove("SICCode");
										foreach (var footnote in footNotes.Where(o => o.SegmentId == seg.SegmentId)) {
											i++;
											segObject[footnote.GeoRevType + i] = footnote.Area;
										}
										SegmentTypes.Add(segmentCode, segObject);
									} else {
										SegmentTypes.Add(segmentCode, seg);
									}


								}
							}
							AccountTypes.Add(accountType.Key, SegmentTypes);
						}
						conceptTypes.Add(conceptType.Key, AccountTypes);
					}
					toRet.Add(vTypes.Key, conceptTypes);
				} else if (vTypes.Key == "GeoRev") {
					Dictionary<string, object> sg = new System.Collections.Generic.Dictionary<string, object>();
					foreach (var geo in vTypes) {
						sg.Add(geo.AccountName + " - " + geo.AsReportedLabel, new { geo.AAAValue, geo.MathMl });
					}
					toRet.Add(vTypes.Key, sg);
				}
			}

			return toRet;
		}


		private List<FootNotes> GetFootNotes(Guid versionId) {
			List<FootNotes> items = new List<FootNotes>();
			string query = @"
											select gfn.SegmentID , grd.Name , grt.Type from [SegEx].[GeoStructuredFootNotes] gfn 
											join SegEx.GeoRevTypes grt on gfn.GeoRevType = grt.ID
											JOIN SegEx.GeoRevDesc grd on grd.AreaId = gfn.AreaId
											where gfn.VersionId = @VersionId";
			using (SqlConnection sqlConn = new SqlConnection(connectionString))
			using (SqlCommand cmd = new SqlCommand(query, sqlConn)) {
				cmd.CommandType = CommandType.Text;
				cmd.Parameters.AddWithValue("@VersionId", versionId);
				sqlConn.Open();
				using (SqlDataReader sdr = cmd.ExecuteReader()) {
					while (sdr.Read()) {
						items.Add(new FootNotes
						{
							SegmentId = sdr.GetInt32(0),
							Area = sdr.GetStringSafe(1),
							GeoRevType = sdr.GetStringSafe(2)
						});
					}
				}
			}
			return items;
		}

	}

	public static class DynamicExtensions {
		public static IDictionary<string, object> ToDynamic(this object value) {
			IDictionary<string, object> expando = new ExpandoObject();

			foreach (PropertyDescriptor property in TypeDescriptor.GetProperties(value.GetType())) {
				if (!property.Attributes.OfType<JsonIgnoreAttribute>().Any()) {
					expando.Add(property.Name, property.GetValue(value));
				}
			}
			return expando;
		}
	}

}