using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

using DataRoostAPI.Common.Models;

using FactSet.Data.SqlClient;

using Oracle.ManagedDataAccess.Client;

namespace CCS.Fundamentals.DataRoostAPI.Access.Voyager {

	public class VoyagerSharesHelper {

		// SQL to create Voyager tmp table
		// CREATE GLOBAL TEMPORARY TABLE TMP_PPIS ( PPI VARCHAR2(10) NOT NULL ) ON COMMIT PRESERVE ROWS

		private readonly string _connectionString;
		private readonly PpiHelper _ppiHelper;

		public VoyagerSharesHelper(string voyagerConnectionString, string sfConnectionString) {
			_connectionString = voyagerConnectionString;
			_ppiHelper = new PpiHelper(sfConnectionString);
		}

		public Dictionary<int, Dictionary<string, List<ShareClassDataItem>>> GetLatestCompanyFPEShareData(List<int> iconums,
		                                                                                                  DateTime?
			                                                                                                  reportSearchDate,
		                                                                                                  DateTime? since) {

			const string insertSql = "INSERT INTO TMP_PPIS (PPI) VALUES (:ppis)";

			const string queryWithStartDate =
				@"SELECT d.ppi, d.report_date, d.data_year, d.time_series_code, d.std_item_code, d.value_num, d.value_text, d.value_date, d.done_date_time, i.item_name, d.doc_publication_date, d.dcn
							FROM
									(SELECT d.ppi, d.report_date, d.data_year, d.time_series_code, d.std_item_code, d.value_num, d.value_text, d.value_date, d.done_date_time, d.doc_publication_date, d.dcn,
												RANK() OVER (PARTITION BY d.std_item_code, d.PPI ORDER BY d.report_date DESC, t.display_order ASC) RANK
											FROM CSO_PSIT_DETAILS d
												JOIN TMP_PPIS tmp ON tmp.PPI = d.PPI
												JOIN time_series t ON t.time_series_code = d.time_series_code AND INSTR(t.time_series_desc, 'CUM') = 0
											WHERE report_date BETWEEN :since_date AND :report_date) d
								JOIN item_std i ON d.std_item_code = i.item_code
							WHERE rank = 1";

			const string queryIfStartDateIsNull =
				@"SELECT d.ppi, d.report_date, d.data_year, d.time_series_code, d.std_item_code, d.value_num, d.value_text, d.value_date, d.done_date_time, i.item_name, d.doc_publication_date, d.dcn
							FROM
									(SELECT d.ppi, d.report_date, d.data_year, d.time_series_code, d.std_item_code, d.value_num, d.value_text, d.value_date, d.done_date_time, d.doc_publication_date, d.dcn,
												RANK() OVER (PARTITION BY d.std_item_code, d.PPI ORDER BY d.report_date DESC, t.display_order ASC) RANK
											FROM CSO_PSIT_DETAILS d
												JOIN TMP_PPIS tmp ON tmp.PPI = d.PPI
												JOIN time_series t ON t.time_series_code = d.time_series_code AND INSTR(t.time_series_desc, 'CUM') = 0
											WHERE report_date <= :report_date) d
								JOIN item_std i ON d.std_item_code = i.item_code
							WHERE rank = 1";

			string query = queryIfStartDateIsNull;
			if (since != null) {
				query = queryWithStartDate;
			}

			DateTime searchDate = DateTime.Now;
			if (reportSearchDate != null) {
				searchDate = (DateTime) reportSearchDate;
			}
			Dictionary<string, List<ShareClassDataItem>> dataByPpi = new Dictionary<string, List<ShareClassDataItem>>();
			Dictionary<string, int> ppiDictionary = _ppiHelper.GetIconumPpiDictionary(iconums);

			if (ppiDictionary == null || ppiDictionary.Count < 1) {
				return new Dictionary<int, Dictionary<string, List<ShareClassDataItem>>>();
			}

			using (OracleConnection connection = new OracleConnection(_connectionString)) {
				connection.Open();

				string[] ppiArray = ppiDictionary.Keys.ToArray();
				using (OracleCommand insertCmd = new OracleCommand(insertSql, connection)) {
					insertCmd.BindByName = true;
					insertCmd.ArrayBindCount = ppiArray.Length;
					insertCmd.Parameters.Add(":ppis", OracleDbType.Varchar2, ppiArray, ParameterDirection.Input);

					insertCmd.ExecuteNonQuery();
				}

				using (OracleCommand command = new OracleCommand(query, connection)) {
					command.BindByName = true;
					command.Parameters.Add(new OracleParameter
					                       {
						                       OracleDbType = OracleDbType.Date,
						                       Direction = ParameterDirection.Input,
						                       ParameterName = ":report_date",
						                       Value = searchDate
					                       });
					if (since != null) {
						command.Parameters.Add(new OracleParameter
						                       {
							                       OracleDbType = OracleDbType.Date,
							                       Direction = ParameterDirection.Input,
							                       ParameterName = ":since_date",
							                       Value = (DateTime) since
						                       });
					}

					using (OracleDataReader sdr = command.ExecuteReader(CommandBehavior.SequentialAccess)) {
						while (sdr.Read()) {
							string ppi = sdr.GetString(0);
							DateTime reportDate = sdr.GetDateTime(1);
							int dataYear = sdr.GetInt32(2);
							string timeSeriesCode = sdr.GetString(3);
							string itemCode = sdr.GetStringSafe(4);
							decimal? numericValue = sdr.GetNullable<decimal>(5);
							string textValue = sdr.GetStringSafe(6);
							string dateValue = sdr.GetStringSafe(7);
							DateTime updatedDate = sdr.GetDateTime(8);
							string itemName = sdr.GetString(9).Trim();

							ShareClassDataItem item = null;
							if (dateValue != null) {
								item = new ShareClassDateItem
								       {
									       ItemId = itemCode,
									       Name = itemName,
									       ReportDate = reportDate,
									       TimeSeriesCode = timeSeriesCode,
									       UpdatedDate = updatedDate,
												 Value = DateTime.ParseExact(dateValue, "ddMMyyyy", null)
								       };
							}
							else if (numericValue != null) {
								item = new ShareClassNumericItem
								       {
									       ItemId = itemCode,
									       Name = itemName,
									       ReportDate = reportDate,
									       TimeSeriesCode = timeSeriesCode,
									       UpdatedDate = updatedDate,
									       Value = (decimal) numericValue
								       };
							}
							else if (textValue != null) {
								item = new ShareClassTextItem
								       {
									       ItemId = itemCode,
									       Name = itemName,
									       ReportDate = reportDate,
									       TimeSeriesCode = timeSeriesCode,
									       UpdatedDate = updatedDate,
									       Value = textValue
								       };
							}

							if (!dataByPpi.ContainsKey(ppi)) {
								dataByPpi.Add(ppi, new List<ShareClassDataItem>());
							}

							dataByPpi[ppi].Add(item);
						}
					}
				}

				connection.Close();
			}

			Dictionary<int, Dictionary<string, List<ShareClassDataItem>>> companyShareData =
				new Dictionary<int, Dictionary<string, List<ShareClassDataItem>>>();
			foreach (KeyValuePair<string, List<ShareClassDataItem>> ppiData in dataByPpi) {
				if (ppiDictionary.ContainsKey(ppiData.Key)) {
					int iconum = ppiDictionary[ppiData.Key];
					if (!companyShareData.ContainsKey(iconum)) {
						companyShareData.Add(iconum, new Dictionary<string, List<ShareClassDataItem>>());
					}
					companyShareData[iconum].Add(ppiData.Key, ppiData.Value);
				}
			}

			return companyShareData;
		}

		public void PopulateTypeOfShare(Dictionary<int, List<ShareClassDataDTO>> companyShareClasses) {

			Dictionary<string, List<ShareClassDTO>> ppiDictionary = new Dictionary<string, List<ShareClassDTO>>();
			foreach (KeyValuePair<int, List<ShareClassDataDTO>> keyValue in companyShareClasses) {
				foreach (ShareClassDTO shareClass in keyValue.Value) {
					if (!ppiDictionary.ContainsKey(shareClass.PPI)) {
						ppiDictionary.Add(shareClass.PPI, new List<ShareClassDTO>());
					}
					ppiDictionary[shareClass.PPI].Add(shareClass);
				}
			}

			if (!ppiDictionary.Any()) {
				return;
			}

			const string insertSql = "INSERT INTO TMP_PPIS (PPI) VALUES (:ppis)";

			const string query = @"SELECT d.ppi, d.update_date, d.reported_text
																FROM current_details d
																	JOIN TMP_PPIS t ON t.ppi = d.ppi
																WHERE d.GNRC_CODE = '15' AND d.GROUP_CODE = '40' AND d.SUB_GROUP_CODE = '100' AND d.ITEM_CODE = '080'";

			using (OracleConnection connection = new OracleConnection(_connectionString)) {
				connection.Open();

				string[] ppiArray = ppiDictionary.Keys.ToArray();
				using (OracleCommand insertCmd = new OracleCommand(insertSql, connection)) {
					insertCmd.BindByName = true;
					insertCmd.ArrayBindCount = ppiArray.Length;
					insertCmd.Parameters.Add(":ppis", OracleDbType.Varchar2, ppiArray, ParameterDirection.Input);

					insertCmd.ExecuteNonQuery();
				}

				using (OracleCommand command = new OracleCommand(query, connection)) {
					using (OracleDataReader sdr = command.ExecuteReader(CommandBehavior.SequentialAccess)) {
						while (sdr.Read()) {
							string ppi = sdr.GetString(0);
							DateTime reportDate = sdr.GetDateTime(1);
							object obj = sdr.GetValue(2);
							string typeOfShare = null;
							if (obj != null) {
								typeOfShare = obj.ToString();
							}
							if (ppiDictionary.ContainsKey(ppi)) {
								foreach (ShareClassDTO shareClass in ppiDictionary[ppi]) {
									shareClass.TypeOfShare = typeOfShare;
								}
							}
						}
					}
				}
			}
		}

		public Dictionary<string, List<ShareClassDataItem>> GetCurrentShareDataItems(int iconum) {
			string query =
				@"SELECT i.item_code, c.reported_value, c.reported_date, c.reported_text, o.item_type, c.ppi, i.item_name
                                    FROM current_details c
                                        JOIN map_file2 m ON m.item_gnrc_code = c.gnrc_code AND m.group_code = c.group_code AND m.sub_group_code = c.sub_group_code AND m.item_code = c.item_code
                                        JOIN official_item o ON o.gnrc_code = c.gnrc_code AND o.group_code = c.group_code AND o.sub_group_code = c.sub_group_code AND o.item_code = c.item_code
                                        JOIN item_std i ON i.item_code = m.std_item_code
                                        JOIN STD_TEMPLATE_ITEM stdti ON stdti.item_code = i.item_code
                                        JOIN STD_TEMPLATE_MASTER stdtm ON stdtm.template_code = stdti.template_code
                                        JOIN FDS_TRI_PPI_MAP map ON map.ppi_oper = c.ppi
                                    WHERE stdtm.template_code = 'SHRC'
                                        AND c.ppi LIKE :ppiBase
                                    ORDER BY stdti.item_position";

			Dictionary<string, List<ShareClassDataItem>> perShareData = new Dictionary<string, List<ShareClassDataItem>>();
			string rootPPI = _ppiHelper.GetPPIByIconum(iconum);
			string basePPI = _ppiHelper.GetPPIBase(rootPPI);

			using (OracleConnection connection = new OracleConnection(_connectionString)) {
				connection.Open();

				using (OracleCommand command = new OracleCommand(query, connection)) {
					command.BindByName = true;
					command.Parameters.Add(new OracleParameter
					                       {
						                       OracleDbType = OracleDbType.Varchar2,
						                       Direction = ParameterDirection.Input,
						                       ParameterName = "ppiBase",
						                       Value = basePPI
					                       });

					using (
						OracleDataReader reader = command.ExecuteReader(CommandBehavior.SequentialAccess | CommandBehavior.SingleResult)) {
						while (reader.Read()) {
							string ppi = reader.GetStringSafe(5);
							if (!perShareData.ContainsKey(ppi)) {
								perShareData.Add(ppi, new List<ShareClassDataItem>());
							}
							List<ShareClassDataItem> items = perShareData[ppi];

							string itemCode = reader.GetStringSafe(0);
							decimal? reportedValue = reader.GetNullable<decimal>(1);
							DateTime? reportedDate = reader.GetNullable<DateTime>(2);
							string reportedText = reader.GetStringSafe(3);
							string itemType = reader.GetStringSafe(4);
							string itemName = reader.GetStringSafe(6);

							ShareClassDataItem item = null;
							if (itemType == "D") {
								item = new ShareClassDateItem { ItemId = itemCode, Name = itemName, Value = (DateTime) reportedDate };
							}
							else if (itemType == "F" || itemType == "N") {
								item = new ShareClassNumericItem { ItemId = itemCode, Name = itemName, Value = (decimal) reportedValue };
							}
						}
					}
				}
			}

			return perShareData;
		}

	}

}
