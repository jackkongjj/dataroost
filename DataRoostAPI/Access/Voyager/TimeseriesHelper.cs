using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Web;
using Oracle.ManagedDataAccess.Client;
using CCS.Fundamentals.DataRoostAPI.Models;
using CCS.Fundamentals.DataRoostAPI.Models.Voyager;
using CCS.Fundamentals.DataRoostAPI.Models.TimeseriesValues;
using FactSet.Data.SqlClient;

namespace CCS.Fundamentals.DataRoostAPI.Access.Voyager {
	public class TimeseriesHelper {

		private readonly string _connectionString;
		private VoyagerHelper _voyagerHelper;

		public TimeseriesHelper(string connectionString) {
			_connectionString = connectionString;
			_voyagerHelper = new VoyagerHelper(_connectionString);
		}

		public VoyagerTimeseriesDTO[] QuerySTDTimeseries(int iconum, TemplateIdentifier templateId, TimeseriesIdentifier timeseriesId) {
			var stdtimeseries = QuerySTDTimeseries(iconum, templateId, 1900, 2100, timeseriesId.MasterId).Where(x => x.ReportType == templateId.ReportType).ToArray<VoyagerTimeseriesDTO>();
			if (stdtimeseries.Length == 1) {
				stdtimeseries[0].Values = PopulateSTDCells(timeseriesId.MasterId);
				return stdtimeseries;
			}

			return new VoyagerTimeseriesDTO[] { };
		}

		public VoyagerTimeseriesDTO[] QuerySTDTimeseries(int iconum, TemplateIdentifier templateId) {
			return QuerySTDTimeseries(iconum, templateId, 1900, 2100);
		}

		public VoyagerTimeseriesDTO[] QuerySTDTimeseries(int iconum, TemplateIdentifier templateId, int startYear, int endYear) {
			return QuerySTDTimeseries(iconum, templateId, startYear, endYear, null);
		}

		private VoyagerTimeseriesDTO[] QuerySTDTimeseries(int iconum, TemplateIdentifier templateId, int startYear, int endYear, string master_id) {
			string ppi = _voyagerHelper.GetPPIByIconum(iconum);
			string ppiBase = _voyagerHelper.GetPPIBase(ppi);
			string query = @"select sm.master_id, sm.data_year, sm.report_date, sm.time_series_code, sm.rep_type, sm.interim_type, sm.ISO_CCY_CODE, sm.SCLG_FACTOR,
										 f.document_id,
										f.file_type,
										x.doc_id,
										coalesce(f.dcn, m.dcn) dcn,
										coalesce(ac.publication_date, aca.publication_date),
										--coalesce(ac.report_date, aca.report_date),
										coalesce(coalesce(ac.company_document_type, aca.company_document_type),m.doc_type) FormType
										FROM ar_details d
										RIGHT JOIN (
											SELECT distinct sm.master_id, sm.data_year, report_date, sm.time_series_code, mts.rep_type, mts.interim_type, sm.ISO_CCY_CODE, sm.SCLG_FACTOR,
											CAST((select SUBSTR(replace(replace(replace(replace(mathml_expression, '<mo>',''),'</mo>',''),'<mi>',''),'</mi>',''),0,12) from ar_std_map e where e.master_id = sm.master_id and rownum=1) as varchar(12)) mathml
													FROM STD_MASTER sm  
													JOIN MAP_SDB_TIME_SERIES mts on mts.time_series_code = sm.time_series_code
													WHERE SM.PPI LIKE :ppiBase
															AND SM.data_year >= :startYear
															AND SM.data_year <= :endYear
															AND SM.master_id  = COALESCE(:master_id, SM.master_id)
										)SM on SM.mathml  = ar_item_id
										JOIN ar_master m ON m.master_id = d.master_id
										LEFT JOIN dam_doc_feed f ON f.dcn = m.dcn
										LEFT JOIN dcn_xref x ON x.dcn = f.dcn
										LEFT JOIN doc_admin_document ad ON ad.doc_id = x.doc_id
										LEFT JOIN doc_admin_company ac ON ac.doc_id = x.doc_id
										LEFT JOIN doc_admin_company_section acs ON acs.doc_id = x.doc_id
										LEFT JOIN doc_admin_company_archive aca ON aca.doc_id = x.doc_id
										LEFT JOIN doc_admin_comp_section_archive acsa ON acsa.doc_id = x.doc_id
										ORDER BY sm.REPORT_DATE DESC";

			List<VoyagerTimeseriesDTO> timeSeriesList = new List<VoyagerTimeseriesDTO>();

			using (OracleConnection connection = new OracleConnection(_connectionString)) {
				connection.Open();
				using (OracleCommand command = new OracleCommand(query, connection)) {
					command.Parameters.Add(new OracleParameter() { OracleDbType = OracleDbType.Varchar2, Direction = ParameterDirection.Input, ParameterName = "ppiBase", Value = ppiBase });
					command.Parameters.Add(new OracleParameter() { OracleDbType = OracleDbType.Int32, Direction = ParameterDirection.Input, ParameterName = "startYear", Value = startYear });
					command.Parameters.Add(new OracleParameter() { OracleDbType = OracleDbType.Int32, Direction = ParameterDirection.Input, ParameterName = "endYear", Value = endYear });
					if (master_id != null) {
						command.Parameters.Add(new OracleParameter() { OracleDbType = OracleDbType.Varchar2, Direction = ParameterDirection.Input, ParameterName = "master_id", Value = master_id });
					} else {
						command.Parameters.Add(new OracleParameter() { OracleDbType = OracleDbType.Varchar2, Direction = ParameterDirection.Input, ParameterName = "master_id", Value = null });
					}
					using (OracleDataReader sdr = command.ExecuteReader(CommandBehavior.SequentialAccess | CommandBehavior.SingleResult)) {
						while (sdr.Read()) {
							TimeseriesIdentifier id = new TimeseriesIdentifier(sdr.GetStringSafe(0), sdr.GetInt16(1), sdr.GetDateTime(2), sdr.GetStringSafe(3));
							timeSeriesList.Add(new VoyagerTimeseriesDTO()
							{
								Id = id.GetToken(),								
								ReportType = sdr.GetString(4),
								InterimType = sdr.GetStringSafe(5),
								IsoCurrency = sdr.GetString(6),
								ScalingFactor = sdr.GetString(7),
								DamDocumentId = sdr.GetStringSafe(8) == null ? Guid.Empty : Guid.Parse(sdr.GetStringSafe(8)),
								PublicationDate = sdr.GetDateTimeSafe(12) == null ? new DateTime() : (DateTime)sdr.GetDateTimeSafe(12),
								VoyagerFormType = sdr.GetStringSafe(13),
								DCN = sdr.GetStringSafe(11)								
							});
						}
					}
				}
				connection.Close();
			}

			return timeSeriesList.ToArray();
		}

		private static Dictionary<string, TimeseriesValueDTO> PopulateSTDCells(string masterId) {
			string query = @"SELECT d.data_type, d.item_code, d.text_value, d.numeric_value, m.mathml_expression, d.scaling_factor, ar.bookmark
                                    FROM 
                                           (SELECT reported_text text_value, null numeric_value, i.item_code item_code, master_id master_id, 'text' data_type, null scaling_factor
                                                FROM std_text_details d
                                                  JOIN item_std i ON i.item_code = d.item_code
                                                WHERE  i.data_type_flag = 'A' AND char_type_flag = 'A' AND master_id = :masterId
                                            UNION
                                            SELECT reported_text text_value, null numeric_value, i.item_code item_code, master_id master_id, 'date' data_type, null scaling_factor
                                                FROM std_text_details d
                                                    JOIN item_std i ON i.item_code = d.item_code
                                                WHERE i.data_type_flag = 'A' AND char_type_flag = 'D' AND master_id = :masterId
                                            UNION
                                            SELECT null text_value, reported_value numeric_value, i.item_code item_code, master_id master_id, 'number' data_type, over_sclg_factor scaling_factor
                                                FROM std_details d
                                                    JOIN item_std i ON i.item_code = d.item_code
                                                WHERE i.data_type_flag = 'N' AND char_type_flag = 'N' AND master_id = :masterId) d
                                        LEFT JOIN ar_std_map m ON d.item_code = m.item_code AND d.master_id = m.master_id
																				LEFT JOIN ar_details ar ON ar.ar_item_id = TO_NUMBER(SUBSTR(m.mathml_expression, INSTR(m.mathml_expression, '<mi>', 1, 1) + 4, INSTR(m.mathml_expression, '</mi>', 1, 1) - INSTR(m.mathml_expression, '<mi>', 1, 1) - 4))";

			Dictionary<string, TimeseriesValueDTO> cells = new Dictionary<string, TimeseriesValueDTO>();
			string connectionString = ConfigurationManager.ConnectionStrings["Voyager"].ToString();
			using (OracleConnection connection = new OracleConnection(connectionString)) {
				using (OracleCommand command = new OracleCommand(query, connection)) {
					connection.Open();
					command.BindByName = true;
					command.Parameters.Add(new OracleParameter() { OracleDbType = OracleDbType.Varchar2, Direction = ParameterDirection.Input, ParameterName = "masterId", Value = masterId });
					using (OracleDataReader sdr = command.ExecuteReader(CommandBehavior.SequentialAccess | CommandBehavior.SingleResult)) {
						while (sdr.Read()) {
							string dataType = sdr.GetStringSafe(0);
							string itemCode = sdr.GetStringSafe(1);
							decimal? numericValue = sdr.GetNullable<decimal>(3);
							string textValue = sdr.GetStringSafe(2);
							string mathMlString = sdr.GetStringSafe(4);
							//int scalingFactor = sdr.GetInt32(5);
							string offsetString = sdr.GetStringSafe(6);							

							TimeseriesValueDTO valueDTO = new TimeseriesValueDTO();
							if (dataType == "text") {
								TextTimeseriesValueDetailDTO valueDetailsDTO = new TextTimeseriesValueDetailDTO();
								valueDetailsDTO.Text = textValue;
								valueDTO.Contents = textValue;
								valueDTO.ValueDetails = valueDetailsDTO;
							}

							if (dataType == "date") {
								DateTimeseriesValueDetailDTO valueDetailsDTO = new DateTimeseriesValueDetailDTO();
								valueDetailsDTO.Date = DateTime.ParseExact(textValue, "ddMMyyyy", null);
								valueDTO.Contents = valueDetailsDTO.Date.ToShortDateString();
								valueDTO.ValueDetails = valueDetailsDTO;
							}

							if (dataType == "number") {
								ExpressionTimeseriesValueDetailDTO valueDetailsDTO = new ExpressionTimeseriesValueDetailDTO();
								valueDetailsDTO.Operation = "=";
								CellExpressionNode node = new CellExpressionNode();
								node.NumericValue = (decimal)numericValue;
								//node.ScalingBase10 = scalingFactor;
								if (!string.IsNullOrWhiteSpace(offsetString)) {
									node.Offset = FLYTOffset.Parse(offsetString);
								}
								valueDetailsDTO.LeftNode = node;
								valueDTO.Contents = numericValue.ToString();
								valueDTO.ValueDetails = valueDetailsDTO;
							}

							cells.Add(itemCode, valueDTO);
						}
					}
				}
			}

			return cells;
		}

		public VoyagerTimeseriesDTO[] QuerySDBTimeseries(int iconum, TemplateIdentifier templateId) {
			return QuerySDBTimeseries(iconum, templateId, 1900, 2100);
		}

		public VoyagerTimeseriesDTO[] QuerySDBTimeseries(int iconum, TemplateIdentifier templateId, TimeseriesIdentifier timeseriesId) {
			var sdbtimeseries = QuerySDBTimeseries(iconum, templateId, 1900, 2100, timeseriesId.MasterId);
			if (sdbtimeseries.Length == 1) {
				sdbtimeseries[0].Values = PopulateSDBCells(timeseriesId.MasterId);
				return sdbtimeseries;
			}
			return new VoyagerTimeseriesDTO[] { };
		}

		public VoyagerTimeseriesDTO[] QuerySDBTimeseries(int iconum, TemplateIdentifier templateId, int startYear, int endYear) {
			return QuerySDBTimeseries(iconum, templateId, 1900, 2100, null);
		}

		private VoyagerTimeseriesDTO[] QuerySDBTimeseries(int iconum, TemplateIdentifier templateId, int startYear, int endYear, string master_id) {
			string ppi = _voyagerHelper.GetPPIByIconum(iconum);
			string ppiBase = _voyagerHelper.GetPPIBase(ppi);

			string query = @"SELECT RM.master_id,
  RM.data_year,
  RM.timeseries,
  RM.InterimType,
  RM.Report_Duration,
  RM.Duration_IND,
  RM.rep_type,
 -- RM.TableType,
  RM.iso_CCY_CODE,
  RM.SCLG_FCTR,
  f.document_id,
  f.file_type,
  x.doc_id,
  coalesce(ac.publication_date, aca.publication_date) publicationdate,
  coalesce(coalesce(ac.company_document_type, aca.company_document_type),m.doc_type) FormType,
	coalesce(f.dcn, m.dcn) dcn,
  rm.time_series_code
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
rm.iso_ccy_code, rm.SCLG_FCTR, ct.co_temp_item_id, rm.rep_type, rm.account_type, rm.interim_type, mts.time_series_code,
(select SUBSTR(replace(replace(replace(replace(mathml_expression, '<mo>',''),'</mo>',''),'<mi>',''),'</mi>',''),0,12) from ar_sdb_map e where e.master_id = rm.master_id and rownum=1) mathml
from report_master rm
JOIN MAP_SDB_TIME_SERIES mts on mts.rep_type = rm.rep_type AND mts.account_type = rm.account_type AND mts.interim_type = COALESCE(rm.interim_type, ' ')
join company_template ct on ct.co_temp_item_id = rm.co_temp_item_id 
join company_category cc on cc.co_cat_id  = ct.co_cat_id
join Company_Template_Items rd on RD.co_temp_item_id = ct.co_temp_item_id
JOIN TEMPLATE_ITEM TI on RD.GNRC_CODE = TI.ITEM_GNRC_CODE and RD.GROUP_CODE = TI.GROUP_CODE and RD.SUB_GROUP_CODE = TI.SUB_GROUP_CODE and RD.ITEM_CODE = TI.ITEM_CODE
join generic gnrc on GNRC.GNRC_CODE = CT.TID_GNRC_CODE
where CC.PPI LIKE :ppiBase
  AND rm.data_year >= :startYear
  AND rm.data_year <= :endYear
--and gnrc.GNRC_CODE in (34,46,66,70)
	AND (ti.tid_cat_code || ti.tid_gnrc_code || ti.tid_type_code || ti.tid_seq_no)  = :templateCode
  AND ct.rep_type = :repType
	AND rm.master_id  = COALESCE(:master_id, rm.master_id)
) RM on RM.mathml = ar_item_id 
order by RM.timeseries desc, RM.co_temp_item_id, RM.rep_type, RM.account_type, RM.interim_type";
			List<VoyagerTimeseriesDTO> timeSeriesList = new List<VoyagerTimeseriesDTO>();

			using (OracleConnection connection = new OracleConnection(_connectionString)) {
				connection.Open();
				using (OracleCommand command = new OracleCommand(query, connection)) {
					command.Parameters.Add(new OracleParameter() { OracleDbType = OracleDbType.Varchar2, Direction = ParameterDirection.Input, ParameterName = "ppiBase", Value = ppiBase });
					command.Parameters.Add(new OracleParameter() { OracleDbType = OracleDbType.Int32, Direction = ParameterDirection.Input, ParameterName = "startYear", Value = startYear });
					command.Parameters.Add(new OracleParameter() { OracleDbType = OracleDbType.Int32, Direction = ParameterDirection.Input, ParameterName = "endYear", Value = endYear });
					command.Parameters.Add(new OracleParameter() { OracleDbType = OracleDbType.Varchar2, Direction = ParameterDirection.Input, ParameterName = "templateCode", Value = templateId.TemplateCode });
					command.Parameters.Add(new OracleParameter() { OracleDbType = OracleDbType.Varchar2, Direction = ParameterDirection.Input, ParameterName = "repType", Value = templateId.ReportType });
					if (master_id != null) {
						command.Parameters.Add(new OracleParameter() { OracleDbType = OracleDbType.Varchar2, Direction = ParameterDirection.Input, ParameterName = "master_id", Value = master_id });
					} else {
						command.Parameters.Add(new OracleParameter() { OracleDbType = OracleDbType.Varchar2, Direction = ParameterDirection.Input, ParameterName = "master_id", Value = null });
					}
					using (OracleDataReader sdr = command.ExecuteReader(CommandBehavior.SequentialAccess | CommandBehavior.SingleResult)) {
						while (sdr.Read()) {
							TimeseriesIdentifier id = new TimeseriesIdentifier(sdr.GetStringSafe(0), int.Parse(sdr.GetStringSafe(1)), sdr.GetDateTime(2), sdr.GetStringSafe(15));
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
								InterimType = sdr.GetStringSafe(3)
							});
						}
					}
				}
				connection.Close();
			}

			return timeSeriesList.ToArray();
		}

		private static Dictionary<string, TimeseriesValueDTO> PopulateSDBCells(string masterId) {
			string query = @"select rd.GNRC_CODE||rd.GROUP_CODE||rd.SUB_GROUP_CODE||rd.ITEM_CODE item_code, rd.reported_value, rd.indicator, m.mathml_expression, ar.bookmark
											from report_details rd 											
											LEFT JOIN ar_sdb_map m on 
												m.GNRC_CODE = rd.GNRC_CODE
												AND m.GROUP_CODE = rd.GROUP_CODE
												AND m.SUB_GROUP_CODE = rd.SUB_GROUP_CODE
												AND m.ITEM_CODE = rd.ITEM_CODE
												AND m.master_id = rd.master_id
											LEFT JOIN ar_details ar ON ar.ar_item_id = TO_NUMBER(SUBSTR(m.mathml_expression, INSTR(m.mathml_expression, '<mi>', 1, 1) + 4, INSTR(m.mathml_expression, '</mi>', 1, 1) - INSTR(m.mathml_expression, '<mi>', 1, 1) - 4))
											where rd.master_id = :masterId";

			Dictionary<string, TimeseriesValueDTO> cells = new Dictionary<string, TimeseriesValueDTO>();
			string connectionString = ConfigurationManager.ConnectionStrings["Voyager"].ToString();
			using (OracleConnection connection = new OracleConnection(connectionString)) {
				using (OracleCommand command = new OracleCommand(query, connection)) {
					connection.Open();
					command.BindByName = true;
					command.Parameters.Add(new OracleParameter() { OracleDbType = OracleDbType.Varchar2, Direction = ParameterDirection.Input, ParameterName = "masterId", Value = masterId });
					using (OracleDataReader sdr = command.ExecuteReader(CommandBehavior.SequentialAccess | CommandBehavior.SingleResult)) {
						while (sdr.Read()) {
							string itemCode = sdr.GetStringSafe(0);
							decimal? numericValue = sdr.GetNullable<decimal>(1);
							string starIndicator = sdr.GetStringSafe(2);
							string mathMlString = sdr.GetStringSafe(3);							
							string offsetString = sdr.GetStringSafe(4);

							TimeseriesValueDTO valueDTO = new TimeseriesValueDTO();
							
							ExpressionTimeseriesValueDetailVoySDBDTO valueDetailsDTO = new ExpressionTimeseriesValueDetailVoySDBDTO();
							valueDetailsDTO.Operation = "=";
							valueDetailsDTO.isStar = (starIndicator == "*");
							CellExpressionNode node = new CellExpressionNode();
							node.NumericValue = (decimal)numericValue;
							//node.ScalingBase10 = scalingFactor;
							if (!string.IsNullOrWhiteSpace(offsetString)) {
								node.Offset = FLYTOffset.Parse(offsetString);
							}
							valueDetailsDTO.LeftNode = node;
							valueDTO.Contents = numericValue.ToString();
							valueDTO.ValueDetails = valueDetailsDTO;							

							cells.Add(itemCode, valueDTO);
						}
					}
				}
			}
			return cells;
		}
	}
}