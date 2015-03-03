using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Data;
using System.Data.SqlClient;
using Oracle.ManagedDataAccess.Client;
using CCS.Fundamentals.DataRoostAPI.Models;
using CCS.Fundamentals.DataRoostAPI.Models.Voyager;
using CCS.Fundamentals.DataRoostAPI.Models.TimeseriesValues;
using FactSet.Data.SqlClient;

namespace CCS.Fundamentals.DataRoostAPI.Access.Voyager {
	public class VoyagerSharesHelper {

		private readonly string _connectionString;
		private VoyagerHelper _voyagerHelper;

		public VoyagerSharesHelper(string connectionString) {
			_connectionString = connectionString;
			_voyagerHelper = new VoyagerHelper(_connectionString);
		}

		public Dictionary<string, List<ShareClassDataItem>> GetLatestFPEShareData(int iconum) {
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
																										JOIN std_template_item t ON t.item_code = i.item_code AND t.template_code = 'SHRC'
                                                WHERE i.data_type_flag = 'A' AND char_type_flag = 'D' AND m.PPI LIKE :ppiBase
                                            UNION
                                            SELECT null text_value, reported_value numeric_value, i.item_code itemCode, i.item_name itemName, 'numeric' data_type, d.UPDATE_DATE udate, m.report_date report_date, m.PPI ppi, t.template_code, RANK() OVER (PARTITION BY i.item_code ORDER BY m.report_date DESC) RANK
                                                FROM std_details d
                                                    JOIN item_std i ON i.item_code = d.item_code
																										JOIN std_master m ON m.master_id = d.master_id
																										JOIN std_template_item t ON t.item_code = i.item_code AND t.template_code = 'SHRC'
                                                WHERE i.data_type_flag = 'N' AND char_type_flag = 'N' AND t.template_code = 'SHRC' AND m.PPI LIKE :ppiBase) tmp
                                            WHERE rank = 1 ORDER BY 1, 2";

			Dictionary<string, List<ShareClassDataItem>> dataByShareClass = new Dictionary<string, List<ShareClassDataItem>>();
			string rootPPI = _voyagerHelper.GetPPIByIconum(iconum);
			string basePPI = _voyagerHelper.GetPPIBase(rootPPI);

			using (OracleConnection connection = new OracleConnection(_connectionString)) {
				connection.Open();

				using (OracleCommand command = new OracleCommand(query, connection)) {
					command.BindByName = true;
					command.Parameters.Add(new OracleParameter() { OracleDbType = OracleDbType.Varchar2, Direction = ParameterDirection.Input, ParameterName = "ppiBase", Value = basePPI });

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
							} else if (dataType == "number") {
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
	}
}