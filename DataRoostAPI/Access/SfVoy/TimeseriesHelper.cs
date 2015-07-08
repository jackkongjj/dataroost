﻿using System;
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
		private readonly string _voyConnectionString;
		private voy.PpiHelper _ppiHelper;

		public TimeseriesHelper(string SfConnectionString, string VoyConnectionString) {
			this._sfConnectionString = SfConnectionString;
			this._voyConnectionString = VoyConnectionString;
			_ppiHelper = new voy.PpiHelper(_sfConnectionString);
		}

		public SfVoyTimeSeries[] QueryTimeseries(int iconum, sf.TemplateIdentifier templateId, TimeseriesIdentifier timeseriesId, StandardizationType dataType, NameValueCollection queryFilter = null) {
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
						CompanyFiscalYear = timeseriesId.CompanyFiscalYear
					});
					sf = tsh.QuerySDBTimeseries(iconum, templateId, sfId, dataType, queryFilter)[0];
				}
				if (timeseriesId.HasVoy) {
					//get voy data
					var voyTS = GetVoyagerTimeseries(iconum, timeseriesId.PeriodEndDate, timeseriesId.InterimType, timeseriesId.ReportType, timeseriesId.AccountType, queryFilter);
					foreach (var ts in voyTS) {
						var tsId = new voy.TimeseriesIdentifier(ts.Id);
						ts.Values = Voyager.TimeseriesHelper.PopulateSDBCells(tsId.MasterId, scalingFactorLookup[ts.ScalingFactor]);
					}
					
					voy = new VoyagerTimeseriesDTO()
					{
						InterimType = timeseriesId.InterimType,
						AccountType = timeseriesId.AccountType,
						PeriodEndDate = timeseriesId.PeriodEndDate,
						ReportType = timeseriesId.ReportType,
						Values = PopulateMapSDBItem(voyTS, iconum, templateId),
						ScalingFactor = "A"			//convert all to Actual						
					};
				}
				results.Add(new SfVoyTimeSeries { SfTimeSerie = (SFTimeseriesDTO)sf, VoyTimeSerie = voy, Id = timeseriesId.GetToken() });
			} else {
				TimeseriesDTO[] sf = tsh.QuerySDBTimeseries(iconum, templateId, sfId, dataType, queryFilter);				
				var superfastTS = sf.ToList<TimeseriesDTO>();
				var voyTS = GetVoyagerTimeseries(iconum);
				foreach (var ts in superfastTS) {
					TimeseriesIdentifier id;

					var match = from x in voyTS
											where x.InterimType == ts.InterimType && ts.PeriodEndDate == x.PeriodEndDate && ts.ReportType == x.ReportType && x.AccountType == "S"		//superfast only has "S"
											orderby x.DamDocumentId descending, x.DCN descending	//order by one with document
											select x;

					var temp = match.ToList();
					if (temp.Count() > 0) {
						var voy = temp.First();
						voy.ScalingFactor = "A";	//convert all to Actual
						id = new TimeseriesIdentifier(((SFTimeseriesDTO)ts).SFDocumentId, ts.CompanyFiscalYear, ts.IsAutoCalc, ts.PeriodEndDate, ts.InterimType, ts.ReportType, "S", true, true);
						results.Add(new SfVoyTimeSeries { SfTimeSerie = (SFTimeseriesDTO)ts, VoyTimeSerie = voy, Id = id.GetToken() });
						voyTS.RemoveAll(x => x.InterimType == ts.InterimType && ts.PeriodEndDate == x.PeriodEndDate && ts.ReportType == x.ReportType && x.AccountType == "S");
					} else {
						id = new TimeseriesIdentifier(((SFTimeseriesDTO)ts).SFDocumentId, ts.CompanyFiscalYear, ts.IsAutoCalc, ts.PeriodEndDate, ts.InterimType, ts.ReportType, "S", true, false);
						results.Add(new SfVoyTimeSeries { SfTimeSerie = (SFTimeseriesDTO)ts, VoyTimeSerie = null, Id = id.GetToken() });
					}
				}
				//add all the voyager TS that are not mapped to superfast TS, group all Voyager TS by report date, interimType, reporttype and AccountType (since CF,IS,PS,BS their own timerseries)
				var groupVoyTS = (from x in voyTS
													group x by new { x.AccountType, x.InterimType, x.PeriodEndDate, x.ReportType }
														into grp
														select new
														{
															grp.Key.AccountType,
															grp.Key.InterimType,
															grp.Key.PeriodEndDate,
															grp.Key.ReportType
														}).ToList();

				foreach (var ts in groupVoyTS) {
					TimeseriesIdentifier id = new TimeseriesIdentifier(Guid.Empty, 0, false, ts.PeriodEndDate, ts.InterimType, ts.ReportType, ts.AccountType, false, true);
					var match = from x in voyTS
											where x.InterimType == ts.InterimType && ts.PeriodEndDate == x.PeriodEndDate && ts.ReportType == x.ReportType && x.AccountType == ts.AccountType
											orderby x.DamDocumentId descending, x.DCN descending	//order by one with document
											select x;
					var temp = match.ToList();
					if (temp.Count() > 0) {
						var voy = temp.First();
						voy.ScalingFactor = "A";	//convert all to Actual						
						results.Add(new SfVoyTimeSeries { SfTimeSerie = null, VoyTimeSerie = voy, Id = id.GetToken() });
						voyTS.RemoveAll(x => x.InterimType == ts.InterimType && ts.PeriodEndDate == x.PeriodEndDate && ts.ReportType == x.ReportType && x.AccountType == ts.AccountType);
					}
				}
				//add any remaing???
				foreach (var ts in voyTS) {
					TimeseriesIdentifier id = new TimeseriesIdentifier(Guid.Empty, 0, false, ts.PeriodEndDate, ts.InterimType, ts.ReportType, ts.AccountType, false, true);
					ts.ScalingFactor = "A"; //convert all to Actual
					results.Add(new SfVoyTimeSeries { SfTimeSerie = null, VoyTimeSerie = ts, Id = id.GetToken() });
				}
			}
			return results.ToArray();
		}

		private List<VoyagerTimeseriesDTO> GetVoyagerTimeseries(int iconum, NameValueCollection queryFilter = null) {
			return GetVoyagerTimeseries(iconum, null, null, null, null, queryFilter);
		}

		private List<VoyagerTimeseriesDTO> GetVoyagerTimeseries(int iconum, DateTime? periodEndDate, string interimType, string reportType, string accountType, NameValueCollection queryFilter = null) {
			string startYear = "1900";
			string endYear = "2100";
			if (queryFilter != null) {
				if (!string.IsNullOrEmpty(queryFilter["startyear"]) && !string.IsNullOrEmpty(queryFilter["endyear"])) {
					startYear = queryFilter["startyear"];
					endYear = queryFilter["endyear"];
				}
			}
			const string query = @"SELECT RM.master_id,
  RM.data_year,
  RM.timeseries,
  RM.InterimType,
  RM.Report_Duration,
  RM.Duration_IND,
  RM.reptype,
 -- RM.TableType,
  RM.iso_CCY_CODE,
  RM.SCLG_FCTR,
  f.document_id,
  f.file_type,
  x.doc_id,
  coalesce(ac.publication_date, aca.publication_date) publicationdate,
  coalesce(coalesce(ac.company_document_type, aca.company_document_type),m.doc_type) FormType,
	coalesce(f.dcn, m.dcn) dcn,
  rm.time_series_code,
	RM.account_type,
	f.DATE_ADDED
FROM ar_details d
JOIN ar_master m ON m.master_id = d.master_id
LEFT JOIN dam_doc_feed f ON f.dcn = m.dcn
LEFT JOIN dcn_xref x ON x.dcn = f.dcn
LEFT JOIN doc_admin_document ad ON ad.doc_id = x.doc_id
LEFT JOIN doc_admin_company ac ON ac.doc_id = x.doc_id
LEFT JOIN doc_admin_company_section acs ON acs.doc_id = x.doc_id
LEFT JOIN doc_admin_company_archive aca ON aca.doc_id = x.doc_id
LEFT JOIN doc_admin_comp_section_archive acsa ON acsa.doc_id = x.doc_id
RIGHT JOIN (
select distinct rm.report_duration, rm.duration_ind, case when rm.interim_type is null then 'XX' else rm.interim_type end interimtype,
rm.report_date timeseries,
rm.master_id, 
rm.data_year,
--CASE when GNRC_CODE = 66 then 'CF' WHEN GNRC_CODE = 70 then 'BS' WHEN GNRC_CODE = 34 then 'PS' WHEN GNRC_CODE = 46 then 'IS' END tabletype,
rm.iso_ccy_code, rm.SCLG_FCTR, ct.co_temp_item_id, case when rm.rep_type = 'AR' then 'A' else rm.rep_type end reptype, 
rm.account_type, rm.interim_type, mts.time_series_code,
(select SUBSTR(replace(replace(replace(replace(mathml_expression, '<mo>',''),'</mo>',''),'<mi>',''),'</mi>',''),0,12) from ar_sdb_map e where e.master_id = rm.master_id and rownum=1) mathml
from report_master rm
JOIN MAP_SDB_TIME_SERIES mts on mts.rep_type = rm.rep_type AND mts.account_type = rm.account_type AND mts.interim_type = COALESCE(rm.interim_type, ' ')
join company_template ct on ct.co_temp_item_id = rm.co_temp_item_id and ct.TID_GNRC_CODE in (34,46,66,70,72) 
join company_category cc on cc.co_cat_id  = ct.co_cat_id
join Company_Template_Items rd on RD.co_temp_item_id = ct.co_temp_item_id
JOIN TEMPLATE_ITEM TI on RD.GNRC_CODE = TI.ITEM_GNRC_CODE and RD.GROUP_CODE = TI.GROUP_CODE and RD.SUB_GROUP_CODE = TI.SUB_GROUP_CODE and RD.ITEM_CODE = TI.ITEM_CODE
join generic gnrc on GNRC.GNRC_CODE = CT.TID_GNRC_CODE
where CC.PPI LIKE :ppiBase
  AND rm.data_year >= :startYear
  AND rm.data_year <= :endYear
	AND CASE WHEN rm.rep_type = 'AR' THEN 'A' ELSE rm.rep_type END = COALESCE(:repType, rm.rep_type)
	AND rm.report_date = COALESCE(:reportDate, CAST(rm.report_date as varchar(9)))
	AND COALESCE(rm.INTERIM_TYPE,'XX') = COALESCE(:interimType, COALESCE(rm.INTERIM_TYPE, 'XX'))
  AND rm.account_type = COALESCE(:accountType, rm.account_type)
) RM on RM.mathml = ar_item_id 
order by RM.timeseries desc, RM.co_temp_item_id, RM.reptype, RM.account_type, RM.interim_type";


			string ppi = _ppiHelper.GetPPIByIconum(iconum);
			string ppiBase = _ppiHelper.GetPPIBase(ppi);

			List<VoyagerTimeseriesDTO> timeSeriesList = new List<VoyagerTimeseriesDTO>();

			using (OracleConnection connection = new OracleConnection(_voyConnectionString)) {
				connection.Open();
				using (OracleCommand command = new OracleCommand(query, connection)) {
					command.Parameters.Add(new OracleParameter() { OracleDbType = OracleDbType.Varchar2, Direction = ParameterDirection.Input, ParameterName = "ppiBase", Value = ppiBase });
					command.Parameters.Add(new OracleParameter() { OracleDbType = OracleDbType.Int32, Direction = ParameterDirection.Input, ParameterName = "startYear", Value = startYear });
					command.Parameters.Add(new OracleParameter() { OracleDbType = OracleDbType.Int32, Direction = ParameterDirection.Input, ParameterName = "endYear", Value = endYear });
					command.Parameters.Add(new OracleParameter() { OracleDbType = OracleDbType.Varchar2, Direction = ParameterDirection.Input, ParameterName = "repType", Value = reportType });
					command.Parameters.Add(new OracleParameter() { OracleDbType = OracleDbType.Varchar2, Direction = ParameterDirection.Input, ParameterName = "reportDate", Value = periodEndDate == null? null : ((DateTime)periodEndDate).ToString("dd-MMM-yy") });
					command.Parameters.Add(new OracleParameter() { OracleDbType = OracleDbType.Varchar2, Direction = ParameterDirection.Input, ParameterName = "interimType", Value = interimType });
					command.Parameters.Add(new OracleParameter() { OracleDbType = OracleDbType.Varchar2, Direction = ParameterDirection.Input, ParameterName = "accountType", Value = accountType });
					using (OracleDataReader sdr = command.ExecuteReader(CommandBehavior.SequentialAccess | CommandBehavior.SingleResult)) {
						while (sdr.Read()) {
							voy.TimeseriesIdentifier id = new voy.TimeseriesIdentifier(sdr.GetStringSafe(0), int.Parse(sdr.GetStringSafe(1)), sdr.GetDateTime(2), sdr.GetStringSafe(15));
							timeSeriesList.Add(new VoyagerTimeseriesDTO()
							{
								Id = id.GetToken(),
								PeriodLength = int.Parse(sdr.GetString(4)),
								PeriodType = sdr.GetString(5),
								ReportType = sdr.GetString(6),
								IsoCurrency = sdr.GetString(7),
								ScalingFactor = sdr.GetString(8),
								DamDocumentId = sdr.GetStringSafe(9) == null ? Guid.Empty : Guid.Parse(sdr.GetStringSafe(9)),
								PublicationDate = sdr.GetDateTimeSafe(12) == null ? new DateTime() : (DateTime)sdr.GetDateTimeSafe(12),
								VoyagerFormType = sdr.GetStringSafe(13),
								DCN = sdr.GetStringSafe(14),
								InterimType = sdr.GetStringSafe(3),
								PeriodEndDate = sdr.GetDateTime(2),
								AccountType = sdr.GetStringSafe(16),
								CompanyFiscalYear = int.Parse(sdr.GetString(1)),
								DocumentDate = sdr.GetDateTimeSafe(17) == null ? new DateTime() : (DateTime)sdr.GetDateTimeSafe(17),
							});
						}
					}
				}
				connection.Close();
			}

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
				foreach (var v in ts.Values)
					
					cellValues.Add(v.Key, v.Value);
			}
			
			//calculate the expression
			foreach (var item in result) {				
				if (toRet.ContainsKey(item.sdbId.ToString())) {
					if (toRet[item.sdbId.ToString()].Contents != "0")
						continue;
				}
				string[] exp = item.expressionFlat.Split(new string[] { "+", "-" }, StringSplitOptions.RemoveEmptyEntries);
				int foundNum = 0;
				string expFlat = item.expressionFlat;
				foreach (var e in exp) {
					string val = e.Replace("(", "").Replace(")", "").Replace("*", "");
					if (cellValues.ContainsKey(e)) {
						foundNum++;
						expFlat = expFlat.Replace(e, cellValues[e].Contents);
					} else {
						expFlat = expFlat.Replace(e, "0");
					}					
				}
				if (foundNum > 0) {
					//eval it
					DataTable dt = new DataTable();
					var value = dt.Compute(expFlat, "");
					TimeseriesValueDTO tsValue = new TimeseriesValueDTO();
					tsValue.Contents = value.ToString();
					if (toRet.ContainsKey(item.sdbId.ToString())) {
						toRet[item.sdbId.ToString()] = tsValue;
					} else {
						toRet.Add(item.sdbId.ToString(), tsValue);
					}
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