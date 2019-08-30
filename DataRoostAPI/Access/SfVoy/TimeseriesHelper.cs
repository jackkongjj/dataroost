using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using sf = CCS.Fundamentals.DataRoostAPI.Access.SuperFast;
using voy = CCS.Fundamentals.DataRoostAPI.Access.Voyager;
using DataRoostAPI.Common.Models;
using DataRoostAPI.Common.Models.SuperFast;
using DataRoostAPI.Common.Models.SfVoy;
using DataRoostAPI.Common.Models.TimeseriesValues;
using FactSet.Data.SqlClient;
using Oracle.ManagedDataAccess.Client;
using DataRoostAPI.Common.Models.Voyager;

namespace CCS.Fundamentals.DataRoostAPI.Access.SfVoy {
	public class TimeseriesHelper {
		private readonly string _sfConnectionString;		
		private voy.PpiHelper _ppiHelper;

		public TimeseriesHelper(string SfConnectionString) {
			this._sfConnectionString = SfConnectionString;			
			_ppiHelper = new voy.PpiHelper(_sfConnectionString);
		}

		public SfVoyTimeSeries[] QueryTimeseries(int iconum, sf.TemplateIdentifier templateId, TimeseriesIdentifier timeseriesId,
            StandardizationType dataType, string statementType,  NameValueCollection queryFilter = null) {
			sf.TimeseriesHelper tsh = new sf.TimeseriesHelper(_sfConnectionString);
			sf.TimeseriesIdentifier sfId = null;
			List<SfVoyTimeSeries> results = new List<SfVoyTimeSeries>();

			//get specific timeserie
			if (timeseriesId != null) {
				TimeseriesDTO sf = null;
				VoyagerTimeseriesDTO voy = null;
				var scalingFactorLookup = GetScalingFactor();

				if (timeseriesId.HasSf) {
					sfId = new sf.TimeseriesIdentifier(new SFTimeseriesDTO
					{
						SFDocumentId = timeseriesId.SFDocumentId,
						InterimType = timeseriesId.InterimType,
						IsAutoCalc = timeseriesId.IsAutoCalc,
						PeriodEndDate = timeseriesId.PeriodEndDate,
						CompanyFiscalYear = timeseriesId.CompanyFiscalYear,
						AccountType = timeseriesId.AccountType
					});
					sf = tsh.QuerySDBTimeseries(iconum, templateId, sfId, dataType,statementType ,queryFilter)[0];
				}
				if (timeseriesId.HasVoy) {
					//get voy data
					var voyTS = (from x in GetVoyagerTimeseries(iconum, timeseriesId, dataType,statementType, queryFilter)
											 where x.CompanyFiscalYear == timeseriesId.CompanyFiscalYear
									 orderby x.DamDocumentId descending, x.DCN descending
									 select x).ToList();

					foreach (var ts in voyTS) {
						var tsId = new voy.TimeseriesIdentifier(ts.Id);						
					}

					voy = new VoyagerTimeseriesDTO()
					{
						InterimType = timeseriesId.InterimType,
						AccountType = timeseriesId.AccountType,
						PeriodEndDate = timeseriesId.PeriodEndDate,
						ReportType = timeseriesId.ReportType,
						Values = voyTS.First().Values,
						DamDocumentId = voyTS.First().DamDocumentId,
						DCN = voyTS.First().DCN,
						CompanyFiscalYear = voyTS.First().CompanyFiscalYear,
						DocumentDate = voyTS.First().DocumentDate,
						PublicationDate = voyTS.First().PublicationDate,
						PeriodLength = voyTS.First().PeriodLength,
						PeriodType = voyTS.First().PeriodType,
						PerShareValues = voyTS.First().PerShareValues,
						IsoCurrency = voyTS.First().IsoCurrency,
						VoyagerFormType = voyTS.First().VoyagerFormType,
						IsRecap = voyTS.First().IsRecap,
						IsAutoCalc = voyTS.First().IsAutoCalc,
						Id = voyTS.First().Id,
						ScalingFactor = "A"			//convert all to Actual						
					};
				}
				results.Add(new SfVoyTimeSeries { SfTimeSerie = (SFTimeseriesDTO)sf, VoyTimeSerie = voy, Id = timeseriesId.GetToken() });
			} else {
				TimeseriesDTO[] sf = tsh.QuerySDBTimeseries(iconum, templateId, sfId, dataType, statementType , queryFilter);				
				var superfastTS = sf.ToList<TimeseriesDTO>();
				var voyTS = GetVoyagerTimeseries(iconum, dataType,statementType ,queryFilter);
				foreach (var ts in superfastTS) {
					TimeseriesIdentifier id;

					var match = from x in voyTS
											where dataType == StandardizationType.SDB ? x.InterimType == ts.InterimType && ts.PeriodEndDate == x.PeriodEndDate && ts.ReportType == x.ReportType && x.AccountType == ts.AccountType && x.CompanyFiscalYear == ts.CompanyFiscalYear	: x.StdTimeSeriesCode == ts.StdTimeSeriesCode && x.PeriodEndDate == ts.PeriodEndDate && x.CompanyFiscalYear == ts.CompanyFiscalYear
											orderby x.DamDocumentId descending, x.DCN descending	//order by one with document
											select x;
                    Guid? sfDocumentId = ((SFTimeseriesDTO)ts).SFDocumentId;
                    var temp = match.ToList();
					if (temp.Count() > 0) {
						var voy = temp.First();
						voy.ScalingFactor = "A";	//convert all to Actual
                       
                        id = new TimeseriesIdentifier(sfDocumentId.HasValue ? sfDocumentId.Value : Guid.Empty , ts.CompanyFiscalYear, ts.IsAutoCalc, ts.PeriodEndDate, ts.InterimType, ts.ReportType, voy.StdTimeSeriesCode, ts.AccountType, true, true);
						results.Add(new SfVoyTimeSeries { SfTimeSerie = (SFTimeseriesDTO)ts, VoyTimeSerie = voy, Id = id.GetToken() });
						if (dataType == StandardizationType.SDB)
							voyTS.RemoveAll(x => x.InterimType == ts.InterimType && ts.PeriodEndDate == x.PeriodEndDate && ts.ReportType == x.ReportType && x.AccountType == ts.AccountType && x.CompanyFiscalYear == ts.CompanyFiscalYear);
						else
							voyTS.RemoveAll(x => x.StdTimeSeriesCode == ts.StdTimeSeriesCode && x.PeriodEndDate == ts.PeriodEndDate && x.CompanyFiscalYear == ts.CompanyFiscalYear);
					} else {
						id = new TimeseriesIdentifier(sfDocumentId.HasValue ? sfDocumentId.Value : Guid.Empty, ts.CompanyFiscalYear, ts.IsAutoCalc, ts.PeriodEndDate, ts.InterimType, ts.ReportType, ts.StdTimeSeriesCode, ts.AccountType, true, false);
						results.Add(new SfVoyTimeSeries { SfTimeSerie = (SFTimeseriesDTO)ts, VoyTimeSerie = null, Id = id.GetToken() });
					}
				}
				//add the rest of voyagerTS, group the SDB timeseries
				if (dataType == StandardizationType.SDB) {
					//add all the voyager TS that are not mapped to superfast TS, group all Voyager TS by report date, interimType, reporttype and AccountType (since CF,IS,PS,BS their own timerseries)
					var groupVoyTS = (from x in voyTS
														group x by new { x.AccountType, x.InterimType, x.PeriodEndDate, x.ReportType, x.CompanyFiscalYear }
															into grp
															select new
															{
																grp.Key.AccountType,
																grp.Key.InterimType,
																grp.Key.PeriodEndDate,
																grp.Key.ReportType,
																grp.Key.CompanyFiscalYear
															}).ToList();

					foreach (var ts in groupVoyTS) {
						var match = from x in voyTS
												where x.InterimType == ts.InterimType && ts.PeriodEndDate == x.PeriodEndDate && ts.ReportType == x.ReportType && x.AccountType == ts.AccountType && x.CompanyFiscalYear == ts.CompanyFiscalYear
												orderby x.DamDocumentId descending, x.DCN descending	//order by one with document
												select x;
						var temp = match.ToList();
						if (temp.Count() > 0) {
							var voy = temp.First();
							voy.ScalingFactor = "A";	//convert all to Actual
							TimeseriesIdentifier id = new TimeseriesIdentifier(Guid.Empty, voy.CompanyFiscalYear, false, ts.PeriodEndDate, ts.InterimType, ts.ReportType, voy.StdTimeSeriesCode, ts.AccountType, false, true);
							results.Add(new SfVoyTimeSeries { SfTimeSerie = null, VoyTimeSerie = voy, Id = id.GetToken() });
							voyTS.RemoveAll(x => x.InterimType == ts.InterimType && ts.PeriodEndDate == x.PeriodEndDate && ts.ReportType == x.ReportType && x.AccountType == ts.AccountType && x.CompanyFiscalYear == ts.CompanyFiscalYear);
						}
					}
				}
				//add any remaing for SDB???, OR add the rest of STD timeseries
				foreach (var ts in voyTS) {
					TimeseriesIdentifier id = new TimeseriesIdentifier(Guid.Empty, ts.CompanyFiscalYear, false, ts.PeriodEndDate, ts.InterimType, ts.ReportType, ts.StdTimeSeriesCode, ts.AccountType, false, true);
					ts.ScalingFactor = "A"; //convert all to Actual
					results.Add(new SfVoyTimeSeries { SfTimeSerie = null, VoyTimeSerie = ts, Id = id.GetToken() });
				}
			}
			return results.ToArray();
		}

		private List<VoyagerTimeseriesDTO> GetVoyagerTimeseries(int iconum, StandardizationType dataType, string statementType, NameValueCollection queryFilter = null) {
			return GetVoyagerTimeseries(iconum, null, dataType,statementType, queryFilter);
		}

		private List<VoyagerTimeseriesDTO> GetVoyagerTimeseries(int iconum, TimeseriesIdentifier tsId, StandardizationType dataType, string statementType, NameValueCollection queryFilter = null) {
			if (dataType == StandardizationType.SDB) {
				if(tsId != null)
					return GetVoyagerSDBTimeseries(iconum, tsId.PeriodEndDate, tsId.InterimType, tsId.ReportType, tsId.AccountType,statementType, queryFilter);
				return GetVoyagerSDBTimeseries(iconum, null, null, null, null, statementType,queryFilter);
			}

			if(tsId != null)
				return GetVoyagerSTDTimeseries(iconum, tsId.PeriodEndDate, tsId.TimeSeriesCode, queryFilter);
			return GetVoyagerSTDTimeseries(iconum, null, null, queryFilter);
		}

		private List<VoyagerTimeseriesDTO> GetVoyagerSTDTimeseries(int iconum, DateTime? periodEndDate, string timeSeriesCode, NameValueCollection queryFilter = null) {		
			List<VoyagerTimeseriesDTO> timeSeriesList = new List<VoyagerTimeseriesDTO>();			
			return timeSeriesList;
		}

		private List<VoyagerTimeseriesDTO> GetVoyagerSDBTimeseries(int iconum, DateTime? periodEndDate, string interimType, string reportType, string accountType,string statementType , NameValueCollection queryFilter = null) {			
			List<VoyagerTimeseriesDTO> timeSeriesList = new List<VoyagerTimeseriesDTO>();			
			return timeSeriesList;
		}

		private Dictionary<string, TimeseriesValueDTO> PopulateMapSDBItem(List<VoyagerTimeseriesDTO> voyTS, int iconum, sf.TemplateIdentifier templateId) {
			//get all the mapping rules
			const string exp_query = @"WITH TemplateMasterID AS
(
	select distinct sdm.Id
	from CompanyIndustry ci 
	join SDBtemplateDetail std on ci.IndustryDetailID = std.IndustryDetailId
	join DocumentSeries ds on ci.Iconum = ds.CompanyId
	join Document d on ds.Id = d.DocumentSeriesId
	join FDSTriPPIMap f on d.PPI = f.PPI
	join SDBCountryGroupCountries sc on sc.CountriesIsoCountry = f.IsoCountry
	join SDBCountryGroup sg on sc.SDBCountryGroupID = sg.Id
		and std.SDBCountryGroupID = sg.ID
	join SDBTemplateMaster sdm on std.SDBTemplateMasterId = sdm.Id
	where ci.Iconum = @iconum
		and std.ReportTypeID = @reportTypeId
		and std.UpdateTypeID = @updateTypeId
		and std.TemplateTypeId = @templateTypeId
)
select tid.id, tm.sdbItem_id, re.expressionFlat, re.[order]
 from TemplateMasterID tid
join [sfdv].[TemplatesMap] tm on tm.[sdbTemplateMaster_id] = tid.id
join [sfdv].RuleExpression re on re.mappingRule_id = tm.mappingRule_id
where tm.isCompleted = 1 and re.iscompleted = 1
order by tm.sdbItem_id, re.[order]";

			var result = new[] { new { tempId = 0, sdbId = 0, expressionFlat = "", ordering = 1 } };
			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {
				conn.Open();
				using (SqlCommand cmd = new SqlCommand(exp_query, conn)) {
					cmd.Parameters.Add(new SqlParameter("@iconum", SqlDbType.Int) { Value = iconum });
					cmd.Parameters.Add(new SqlParameter("@reportTypeId", SqlDbType.NVarChar, 64) { Value = templateId.ReportType });
					cmd.Parameters.Add(new SqlParameter("@updateTypeId", SqlDbType.NVarChar, 64) { Value = templateId.UpdateType });
					cmd.Parameters.Add(new SqlParameter("@templateTypeId", SqlDbType.Int) { Value = templateId.TemplateType });
					using (SqlDataReader reader = cmd.ExecuteReader()) {
						result = reader.Cast<IDataRecord>().Select(r => new
						{
							tempId = reader.GetInt32(0),
							sdbId = reader.GetInt32(1),
							expressionFlat = reader.GetString(2),
							ordering = reader.GetInt32(3)
						}).ToArray();
					}
				}
			}
			Dictionary<string, TimeseriesValueDTO> toRet = new Dictionary<string, TimeseriesValueDTO>();
			var cellValues = new Dictionary<string, TimeseriesValueDTO>();
			foreach(var ts in voyTS){
				foreach (var v in ts.Values) {
					var cellKey = ((ExpressionTimeseriesValueDetailVoySDBDTO)v.Value.ValueDetails).isStar ? v.Key + "*" : v.Key;
					if (!cellValues.ContainsKey(cellKey))
						cellValues.Add(cellKey, v.Value);					
				}
			}
			
			//calculate the expression
			foreach (var item in result) {				
				if (toRet.ContainsKey(item.sdbId.ToString())) {
					if (toRet[item.sdbId.ToString()].Contents != "0")
						continue;
				}
				string[] exp = item.expressionFlat.Split(new string[] { "+", "-" }, StringSplitOptions.RemoveEmptyEntries);
				int foundNum = 0;
				string overrideScalingFactor = null;
				string expFlat = item.expressionFlat;
				foreach (var e in exp) {
					string val = e.Replace("(", "").Replace(")", "");

					if (cellValues.ContainsKey(val) || (!val.Contains("*") && cellValues.ContainsKey(val + "*"))) {
						foundNum++;
						var k = cellValues.ContainsKey(val) ? val : val + "*";
						string tempval = string.IsNullOrEmpty(cellValues[k].Contents) ? "0" : cellValues[k].Contents + "*1.0";
						expFlat = ReplaceFirst(expFlat, e, tempval);
						var temp = (ExpressionTimeseriesValueDetailVoySDBDTO)cellValues[k].ValueDetails;
						if (!String.IsNullOrEmpty(temp.OverrideScalingFactor))
							overrideScalingFactor = temp.OverrideScalingFactor;
					} else {
						expFlat = ReplaceFirst(expFlat, e, "0");
					}
				}
				if (foundNum > 0) {
					//eval it
					DataTable dt = new DataTable();
					var value = dt.Compute(expFlat, "");
					TimeseriesValueDTO tsValue = new TimeseriesValueDTO();
					tsValue.Contents = Double.Parse(value.ToString()).ToString("F5");
					tsValue.ValueDetails = new ExpressionTimeseriesValueDetailVoySDBDTO();
					((ExpressionTimeseriesValueDetailVoySDBDTO)tsValue.ValueDetails).OverrideScalingFactor = overrideScalingFactor;
					if (toRet.ContainsKey(item.sdbId.ToString())) {
						toRet[item.sdbId.ToString()] = tsValue;
					} else {
						toRet.Add(item.sdbId.ToString(), tsValue);
					}
				}
			}
			return toRet;
		}

		private string ReplaceFirst(string text, string search, string replace) {
			int pos = text.IndexOf(search);
			if (pos < 0) {
				return text;
			}
			return text.Substring(0, pos) + replace + text.Substring(pos + search.Length);
		}

		private Dictionary<string, TimeseriesValueDTO> PopulateMapSTDItem(List<VoyagerTimeseriesDTO> voyTS, int iconum, sf.TemplateIdentifier templateId) {
			const string stdItem_query = @"WITH TemplateMasterID AS
(
	select distinct sdm.Code
	from CompanyIndustry ci 
	join STDTemplateDetail std on ci.IndustryDetailID = std.IndustryDetailId
	join DocumentSeries ds on ci.Iconum = ds.CompanyId
	join Document d on ds.Id = d.DocumentSeriesId
	join FDSTriPPIMap f on d.PPI = f.PPI
	join STDCountryGroupCountries sc on sc.CountriesIsoCountry = f.IsoCountry
	join STDCountryGroup sg on sc.STDCountryGroupID = sg.Id
		and std.STDCountryGroupID = sg.ID
	join STDTemplateMaster sdm on std.STDTemplateMasterCode = sdm.Code
	where ci.Iconum = @iconum
		and std.ReportTypeID = @reportTypeId
		and std.UpdateTypeID = @updateTypeId
		and std.TemplateTypeId = @templateTypeId
)
select tmi.code, stdi.id, stdcode
from STDTemplateItem stdti
join TemplateMasterID tmi on tmi.Code = stdti.STDTemplateMasterCode
join STDItem stdi on stdi.id = stdti.STDItemID";
			var result = new[] { new { tempId = "", stdId = 0, expressionFlat = "" } };
			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {
				conn.Open();
				using (SqlCommand cmd = new SqlCommand(stdItem_query, conn)) {
					cmd.Parameters.Add(new SqlParameter("@iconum", SqlDbType.Int) { Value = iconum });
					cmd.Parameters.Add(new SqlParameter("@reportTypeId", SqlDbType.NVarChar, 64) { Value = templateId.ReportType });
					cmd.Parameters.Add(new SqlParameter("@updateTypeId", SqlDbType.NVarChar, 64) { Value = templateId.UpdateType });
					cmd.Parameters.Add(new SqlParameter("@templateTypeId", SqlDbType.Int) { Value = templateId.TemplateType });
					using (SqlDataReader reader = cmd.ExecuteReader()) {
						result = reader.Cast<IDataRecord>().Select(r => new
						{
							tempId = reader.GetString(0),
							stdId = reader.GetInt32(1),
							expressionFlat = reader.GetString(2)
						}).ToArray();
					}
				}
			}
			Dictionary<string, TimeseriesValueDTO> toRet = new Dictionary<string, TimeseriesValueDTO>();
			
			foreach (var ts in voyTS) {
				var matched = result.Where(k => ts.Values.ContainsKey(k.expressionFlat)).Select(k => new { key = k.stdId, value = ts.Values[k.expressionFlat] });
				//use for manual check with query
				//var nomatch = from n in ts.Values
				//							let item = from x in result select x.expressionFlat
				//							where !item.Contains(n.Key)
				//							select n;

				//add to the result
				foreach (var item in matched) {
					if(!toRet.ContainsKey(item.key.ToString()))
						toRet.Add(item.key.ToString(), item.value);
				}
			}
			return toRet;
		}

		private Dictionary<string, decimal> GetScalingFactor() {
			const string query = @"SELECT id, [Value] FROM ScalingFactor";
			Dictionary<string, decimal> ret = new Dictionary<string, decimal>();
			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {
				conn.Open();
				using (SqlCommand cmd = new SqlCommand(query, conn)) {
					using (SqlDataReader reader = cmd.ExecuteReader()) {
						while (reader.Read()) {
							ret.Add(reader.GetString(0), (decimal)reader.GetDouble(1));
						}
					}
				}
			}
			return ret;
		}
	}
}