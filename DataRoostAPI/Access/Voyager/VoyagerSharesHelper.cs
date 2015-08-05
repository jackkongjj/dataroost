using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Data;
using System.Data.SqlClient;

using DataRoostAPI.Common.Models;

using Oracle.ManagedDataAccess.Client;
using FactSet.Data.SqlClient;

namespace CCS.Fundamentals.DataRoostAPI.Access.Voyager {
	public class VoyagerSharesHelper {

		private readonly string _connectionString;
		private PpiHelper _ppiHelper;

		public VoyagerSharesHelper(string voyagerConnectionString, string sfConnectionString) {
			_connectionString = voyagerConnectionString;
			_ppiHelper = new PpiHelper(sfConnectionString);
		}

		public Dictionary<string, List<ShareClassDataItem>> GetLatestFPEShareData(int iconum, DateTime? reportDateToFind) {
//			string query = @"SELECT PPI, REPORTED_VALUE, REPORT_DATE FROM
//                                    (SELECT ts.SEDOL, m.PPI, d.REPORTED_VALUE, m.REPORT_DATE, RANK() OVER (PARTITION BY m.ppi ORDER BY m.report_date DESC) RANK
//																			FROM item_std i
//                                        JOIN std_details d ON d.item_code = i.item_code AND d.REPORTED_VALUE > 0
//                                        JOIN std_master m ON m.master_id = d.master_id AND m.template_code = 'SHRC' AND EXTRACT(YEAR FROM m.REPORT_DATE) >= (Extract(YEAR FROM SYSDATE) - 1) 
//																			WHERE m.PPI LIKE :ppiBase
//                                    ) where RANK = 1 ORDER BY 1, 2";

//			string query = @"SELECT j.PPI, j.data_type, j.item_code, j.text_value, j.numeric_value, j.item_name, j.REPORT_DATE FROM
//																					(SELECT reported_text text_value, null numeric_value, i.item_code item_code, item_name, REPORT_DATE, PPI, 'date' data_type, RANK() OVER (PARTITION BY i.item_code ORDER BY m.report_date DESC) RANK
//                                                FROM std_text_details d
//                                                    JOIN item_std i ON i.item_code = d.item_code
//																										JOIN std_master m ON m.master_id = d.master_id AND EXTRACT(YEAR FROM m.REPORT_DATE) >= (Extract(YEAR FROM SYSDATE) - 1) 
//																										JOIN std_template_item t ON t.item_code = i.item_code AND t.template_code = 'SHRC'
//                                                WHERE i.data_type_flag = 'A' AND char_type_flag = 'D' AND m.PPI LIKE :ppiBase
//                                            UNION
//                                            SELECT null text_value, reported_value numeric_value, i.item_code item_code, item_name, REPORT_DATE, PPI, 'number' data_type, RANK() OVER (PARTITION BY i.item_code ORDER BY m.report_date DESC) RANK
//                                                FROM std_details d
//                                                    JOIN item_std i ON i.item_code = d.item_code
//																										JOIN std_master m ON m.master_id = d.master_id AND EXTRACT(YEAR FROM m.REPORT_DATE) >= (Extract(YEAR FROM SYSDATE) - 1) 
//																										JOIN std_template_item t ON t.item_code = i.item_code AND t.template_code = 'SHRC'
//                                                WHERE i.data_type_flag = 'N' AND char_type_flag = 'N' AND m.PPI LIKE :ppiBase) j
//																					where j.RANK = 1 ORDER BY 1, 2";

			string query = @"SELECT ppi, data_type, itemCode, text_value, numeric_value, itemName, report_date FROM
                                          (SELECT reported_text text_value, null numeric_value, item_code itemCode, i.item_name itemName, 'date' data_type, d.UPDATE_DATE udate, m.report_date report_date, m.PPI ppi, t.template_code, RANK() OVER (PARTITION BY i.item_code ORDER BY m.report_date DESC) RANK
                                                FROM std_text_details d
                                                    JOIN item_std i ON i.item_code = d.item_code
																										JOIN std_master m ON m.master_id = d.master_id
																										JOIN std_template_item t ON t.item_code = i.item_code AND t.template_code = 'PSIT'
                                                WHERE i.data_type_flag = 'A' AND char_type_flag = 'D' AND m.PPI LIKE :ppiBase AND m.report_date <= :reportDate
                                            UNION
                                            SELECT null text_value, reported_value numeric_value, i.item_code itemCode, i.item_name itemName, 'numeric' data_type, d.UPDATE_DATE udate, m.report_date report_date, m.PPI ppi, t.template_code, RANK() OVER (PARTITION BY i.item_code ORDER BY m.report_date DESC) RANK
                                                FROM std_details d
                                                    JOIN item_std i ON i.item_code = d.item_code
																										JOIN std_master m ON m.master_id = d.master_id
																										JOIN std_template_item t ON t.item_code = i.item_code AND t.template_code = 'PSIT'
                                                WHERE i.data_type_flag = 'N' AND char_type_flag = 'N' AND t.template_code = 'PSIT' AND m.PPI LIKE :ppiBase AND m.report_date <= :reportDate) tmp
                                            WHERE rank = 1 ORDER BY 1, 2";

			DateTime searchDate = DateTime.Now;
			if (reportDateToFind != null) {
				searchDate = (DateTime)reportDateToFind;
			}
			Dictionary<string, List<ShareClassDataItem>> dataByShareClass = new Dictionary<string, List<ShareClassDataItem>>();
			string rootPPI = _ppiHelper.GetPPIByIconum(iconum);
			string basePPI = _ppiHelper.GetPPIBase(rootPPI);

			using (OracleConnection connection = new OracleConnection(_connectionString)) {
				connection.Open();

				using (OracleCommand command = new OracleCommand(query, connection)) {
					command.BindByName = true;
					command.Parameters.Add(new OracleParameter() { OracleDbType = OracleDbType.Varchar2, Direction = ParameterDirection.Input, ParameterName = "ppiBase", Value = basePPI });
					command.Parameters.Add(new OracleParameter() { OracleDbType = OracleDbType.Date, Direction = ParameterDirection.Input, ParameterName = "reportDate", Value = searchDate });

					using (OracleDataReader sdr = command.ExecuteReader(CommandBehavior.SequentialAccess | CommandBehavior.SingleResult)) {
						while (sdr.Read()) {
							string ppi = sdr.GetString(0);
							string dataType = sdr.GetStringSafe(1);
							string itemCode = sdr.GetStringSafe(2);
							string textValue = sdr.GetStringSafe(3);
							decimal? numericValue = sdr.GetNullable<decimal>(4);
							string itemName = sdr.GetStringSafe(5);
							DateTime reportDate = sdr.GetDateTime(6);
							List<ShareClassDataItem> itemValues = null;

							if (dataByShareClass.ContainsKey(ppi)) {
								itemValues = dataByShareClass[ppi];
							} else {
								itemValues = new List<ShareClassDataItem>();
								dataByShareClass.Add(ppi, itemValues);
							}

							ShareClassDataItem item = null;
							if (dataType == "date") {
								item = new ShareClassDateItem
								{
									ItemId = itemCode,
									Name = itemName,
									ReportDate = reportDate,
									Value = DateTime.ParseExact(textValue, "ddMMyyyy", null),
								};
							} else if (dataType == "numeric") {
								item = new ShareClassNumericItem
								{
									ItemId = itemCode,
									Name = itemName,
									ReportDate = reportDate,
									Value = (decimal)numericValue,
								};
							}
							itemValues.Add(item);
						}
					}
				}
			}

			return dataByShareClass;
		}

		public Dictionary<string, List<ShareClassDataItem>> GetCurrentShareDataItems(int iconum) {
			string query = @"SELECT i.item_code, c.reported_value, c.reported_date, c.reported_text, o.item_type, c.ppi, i.item_name
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
					command.Parameters.Add(new OracleParameter() { OracleDbType = OracleDbType.Varchar2, Direction = ParameterDirection.Input, ParameterName = "ppiBase", Value = basePPI });

					using (OracleDataReader reader = command.ExecuteReader(CommandBehavior.SequentialAccess | CommandBehavior.SingleResult)) {
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
								item = new ShareClassDateItem
								{
									ItemId = itemCode,
									Name = itemName,
									Value = (DateTime)reportedDate,
								};
							} else if (itemType == "F" || itemType == "N") {
								item = new ShareClassNumericItem
								{
									ItemId = itemCode,
									Name = itemName,
									Value = (decimal)reportedValue,
								};
							}
						}
					}
				}
			}

			return perShareData;
		}
	}
}