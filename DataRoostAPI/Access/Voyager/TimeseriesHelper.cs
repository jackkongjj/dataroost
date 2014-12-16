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

		public TimeseriesHelper(string connectionString) {
			_connectionString = connectionString;
		}

		public VoyagerTimeseriesDTO[] QuerySTDTimeseries(int iconum, TemplateIdentifier templateId, TimeseriesIdentifier timeseriesId) {
			string ppi = GetPPIByIconum(iconum);

			TemplatesHelper th = new TemplatesHelper(_connectionString, iconum, StandardizationType.STD);
			//int templMaster = th.GetTemplateMasterId(templateId);
			VoyagerTimeseriesDTO timeseriesDTO = new VoyagerTimeseriesDTO();
			timeseriesDTO.Id = timeseriesId.GetToken();
			string mathML = null;
			timeseriesDTO.Values = PopulateSTDCells(timeseriesId.MasterId, out mathML);
			PopulateDocumentTimeSeriesData(timeseriesDTO, mathML);
			return new VoyagerTimeseriesDTO[] { timeseriesDTO };
		}

		public VoyagerTimeseriesDTO[] QuerySTDTimeseries(int iconum, TemplateIdentifier templateId) {
			return QuerySTDTimeseries(iconum, templateId, 1900, 2100);
		}

		public VoyagerTimeseriesDTO[] QuerySTDTimeseries(int iconum, TemplateIdentifier templateId, int startYear, int endYear) {
			string ppi = GetPPIByIconum(iconum);

			TemplatesHelper th = new TemplatesHelper(_connectionString, iconum, StandardizationType.STD);
			//int templMaster = th.GetTemplateMasterId(templateId);
			IEnumerable<TimeseriesIdentifier> timeseries = GetAllTimeSeriesIdsByPPI(ppi, startYear, endYear);
			List<VoyagerTimeseriesDTO> timeSeriesList = new List<VoyagerTimeseriesDTO>();
			foreach (TimeseriesIdentifier timeSeriesId in timeseries) {
				VoyagerTimeseriesDTO timeseriesDTO = new VoyagerTimeseriesDTO();
				timeseriesDTO.Id = timeSeriesId.GetToken();
				string mathML = null;
				PopulateDocumentTimeSeriesData(timeseriesDTO, mathML);
				timeSeriesList.Add(timeseriesDTO);
			}

			return timeSeriesList.ToArray();
		}

		private static string GetPPIBase(string ppi) {
			return ppi.Substring(0, ppi.Length - 1) + "%";
		}

		private static IEnumerable<TimeseriesIdentifier> GetAllTimeSeriesIdsByPPI(string ppi, int startYear, int endYear) {
			string ppiBase = GetPPIBase(ppi);
			string query = @"SELECT sm.master_id, sm.data_year, report_date, time_series_code
                                FROM STD_MASTER sm
                                WHERE SM.PPI LIKE :ppiBase
                                    AND SM.data_year >= :startYear
                                    AND SM.data_year <= :endYear
                                ORDER BY sm.REPORT_DATE DESC";
			List<TimeseriesIdentifier> idList = new List<TimeseriesIdentifier>();
			string connectionString = ConfigurationManager.ConnectionStrings["Voyager"].ToString();
			using (OracleConnection connection = new OracleConnection(connectionString)) {
				connection.Open();
				using (OracleCommand command = new OracleCommand(query, connection)) {
					command.Parameters.Add(new OracleParameter() { OracleDbType = OracleDbType.Varchar2, Direction = ParameterDirection.Input, ParameterName = "ppiBase", Value = ppiBase });
					command.Parameters.Add(new OracleParameter() { OracleDbType = OracleDbType.Int32, Direction = ParameterDirection.Input, ParameterName = "startYear", Value = startYear });
					command.Parameters.Add(new OracleParameter() { OracleDbType = OracleDbType.Int32, Direction = ParameterDirection.Input, ParameterName = "endYear", Value = endYear });
					using (OracleDataReader sdr = command.ExecuteReader(CommandBehavior.SequentialAccess | CommandBehavior.SingleResult)) {
						while (sdr.Read()) {
							TimeseriesIdentifier id = new TimeseriesIdentifier(sdr.GetStringSafe(0), sdr.GetInt16(1), sdr.GetDateTime(2), sdr.GetStringSafe(3));
							idList.Add(id);
						}
					}
				}
				connection.Close();
			}

			return idList;
		}

		private static Dictionary<int, TimeseriesValueDTO> PopulateSTDCells(string masterId, out string mathML) {
			string query = @"  SELECT d.data_type, d.item_code, d.text_value, d.numeric_value, m.mathml_expression, d.scaling_factor, ar.bookmark
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

			mathML = null;
			Dictionary<int, TimeseriesValueDTO> cells = new Dictionary<int, TimeseriesValueDTO>();
			string connectionString = ConfigurationManager.ConnectionStrings["Voyager"].ToString();
			using (OracleConnection connection = new OracleConnection(connectionString)) {
				using (OracleCommand command = new OracleCommand(query, connection)) {
					connection.Open();
					command.BindByName = true;
					command.Parameters.Add(new OracleParameter() { OracleDbType = OracleDbType.Varchar2, Direction = ParameterDirection.Input, ParameterName = "masterId", Value = masterId });
					using (OracleDataReader sdr = command.ExecuteReader(CommandBehavior.SequentialAccess | CommandBehavior.SingleResult)) {
						while (sdr.Read()) {
							string dataType = sdr.GetStringSafe(0);
							int itemCode = int.Parse(sdr.GetStringSafe(1));
							decimal? numericValue = sdr.GetNullable<decimal>(3);
							string textValue = sdr.GetStringSafe(2);
							string mathMlString = sdr.GetStringSafe(4);
							int scalingFactor = sdr.GetInt32(5);
							string offsetString = sdr.GetStringSafe(6);

							if (!string.IsNullOrEmpty(mathMlString)) {
								mathML = mathMlString;
							}

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
								node.ScalingBase10 = scalingFactor;
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

		private void PopulateDocumentTimeSeriesData(VoyagerTimeseriesDTO timeSeriesGroup, string firstMathML) {
			if (string.IsNullOrEmpty(firstMathML)) {
				return;
			}

			string arItemString = firstMathML.Substring(firstMathML.IndexOf("<mi>") + 4, firstMathML.IndexOf("</mi>") - 4);
			string query = @"SELECT f.document_id,
                                    f.file_type,
                                    x.doc_id,
                                    coalesce(ac.publication_date, aca.publication_date),
                                    coalesce(ac.report_date, aca.report_date),
                                    coalesce(ac.company_document_type, aca.company_document_type)
                                FROM ar_details d
                                    JOIN ar_master m ON m.master_id = d.master_id
                                    JOIN dam_doc_feed f ON f.dcn = m.dcn
                                    JOIN dcn_xref x ON x.dcn = f.dcn
                                    JOIN doc_admin_document ad ON ad.doc_id = x.doc_id
                                    LEFT JOIN doc_admin_company ac ON ac.doc_id = x.doc_id
                                    LEFT JOIN doc_admin_company_section acs ON acs.doc_id = x.doc_id
                                    LEFT JOIN doc_admin_company_archive aca ON aca.doc_id = x.doc_id
                                    LEFT JOIN doc_admin_comp_section_archive acsa ON acsa.doc_id = x.doc_id
                                WHERE ar_item_id = :arItemId";

			decimal arItemId;
			if (decimal.TryParse(arItemString, out arItemId)) {
				using (OracleConnection connection = new OracleConnection(_connectionString)) {
					connection.Open();
					using (OracleCommand command = new OracleCommand(query, connection)) {
						command.Parameters.Add(new OracleParameter() { OracleDbType = OracleDbType.Decimal, Direction = ParameterDirection.Input, ParameterName = "arItemId", Value = arItemId });
						using (OracleDataReader sdr = command.ExecuteReader(CommandBehavior.SequentialAccess | CommandBehavior.SingleResult)) {
							if (sdr.Read()) {
								Guid damDocumentId = Guid.Parse(sdr.GetStringSafe(0));
								string fileType = sdr.GetStringSafe(1);
								string documentId = sdr.GetStringSafe(2);
								DateTime publicationDate = sdr.GetDateTime(3);
								DateTime reportDate = sdr.GetDateTime(4);
								string formType = sdr.GetStringSafe(5);

								timeSeriesGroup.PublicationDate = publicationDate;
								timeSeriesGroup.ReportType = formType;
								timeSeriesGroup.DamDocumentId = damDocumentId;
							}
						}
					}
					connection.Close();
				}
			}
		}

		private string GetPPIByIconum(int iconum) {
			string query = @"SELECT PPI_OPER FROM FDS_TRI_PPI_MAP WHERE ICO_OPER = :iconum";

				using (OracleConnection connection = new OracleConnection(_connectionString)) {
					connection.Open();
					using (OracleCommand command = new OracleCommand(query, connection)) {
						command.Parameters.Add(new OracleParameter() { OracleDbType = OracleDbType.Int32, Direction = ParameterDirection.Input, ParameterName = "iconum", Value = iconum });
						using (OracleDataReader sdr = command.ExecuteReader(CommandBehavior.SequentialAccess | CommandBehavior.SingleResult)) {
							if (sdr.Read()) {
								return sdr.GetString(0);
							}
						}
					}
					connection.Close();
				}

				return null;
		}
	}
}