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

		public Dictionary<int, Dictionary<string, List<ShareClassDataItem>>> GetLatestCompanyFPEShareData(List<int> iconums,
		                                                                                           DateTime? reportSearchDate,
			DateTime? since) {
			//const string createSql = "CREATE GLOBAL TEMPORARY TABLE TMP_PPIS ( PPI VARCHAR2(10) NOT NULL ) ON COMMIT PRESERVE ROWS";

			const string insertSql = "INSERT INTO TMP_PPIS (PPI) VALUES (:ppis)";

//			const string query = @"SELECT ppi, data_type, itemCode, text_value, numeric_value, itemName, report_date FROM
//                                          (SELECT reported_text text_value, null numeric_value, item_code itemCode, i.item_name itemName, 'date' data_type, d.UPDATE_DATE udate, m.report_date report_date, m.PPI ppi, t.template_code, RANK() OVER (PARTITION BY i.item_code ORDER BY m.report_date DESC) RANK
//                                                FROM std_text_details d
//                                                    JOIN item_std i ON i.item_code = d.item_code
//																										JOIN std_master m ON m.master_id = d.master_id
//																										JOIN std_template_item t ON t.item_code = i.item_code AND t.template_code = 'PSIT'
//																										JOIN TMP_PPIS i ON i.PPI = m.PPI
//                                                WHERE i.data_type_flag = 'A' AND char_type_flag = 'D' AND m.report_date <= :reportDate
//																						UNION
//                                            SELECT reported_text text_value, null numeric_value, i.item_code itemCode, i.item_name itemName, 'text' data_type, d.UPDATE_DATE udate, m.report_date report_date, m.PPI ppi, t.template_code, RANK() OVER (PARTITION BY i.item_code ORDER BY m.report_date DESC) RANK
//                                                FROM std_text_details d
//                                                    JOIN item_std i ON i.item_code = d.item_code
//																										JOIN std_master m ON m.master_id = d.master_id
//																										JOIN std_template_item t ON t.item_code = i.item_code AND t.template_code = 'PSIT'
//																										JOIN TMP_PPIS i ON i.PPI = m.PPI
//                                                WHERE i.data_type_flag = 'A' AND char_type_flag = 'A' AND t.template_code = 'PSIT' AND m.report_date <= :reportDate
//                                            UNION
//                                            SELECT null text_value, reported_value numeric_value, i.item_code itemCode, i.item_name itemName, 'numeric' data_type, d.UPDATE_DATE udate, m.report_date report_date, m.PPI ppi, t.template_code, RANK() OVER (PARTITION BY i.item_code ORDER BY m.report_date DESC) RANK
//                                                FROM std_details d
//                                                    JOIN item_std i ON i.item_code = d.item_code
//																										JOIN std_master m ON m.master_id = d.master_id
//																										JOIN std_template_item t ON t.item_code = i.item_code AND t.template_code = 'PSIT'
//																										JOIN TMP_PPIS i ON i.PPI = m.PPI
//                                                WHERE i.data_type_flag = 'N' AND char_type_flag = 'N' AND t.template_code = 'PSIT' AND m.report_date <= :reportDate) tmp
//                                            WHERE rank = 1 ORDER BY 1, 2";

//			const string query = @"SELECT ppi, data_type, itemCode, text_value, numeric_value, itemName, report_date FROM
//                                          (SELECT reported_text text_value, null numeric_value, item_code itemCode, i.item_name itemName, 'date' data_type, d.UPDATE_DATE udate, m.report_date report_date, m.PPI ppi, t.template_code, RANK() OVER (PARTITION BY i.item_code, m.PPI ORDER BY m.report_date DESC) RANK
//                                                FROM std_text_details d
//                                                    JOIN item_std i ON i.item_code = d.item_code
//																										JOIN std_master m ON m.master_id = d.master_id
//																										JOIN std_template_item t ON t.item_code = i.item_code AND t.template_code = 'PSIT'
//																										JOIN TEMP_PPIS tmp ON tmp.PPI = m.PPI
//                                                WHERE i.data_type_flag = 'A' AND char_type_flag = 'D' AND m.report_date <= :reportDate
//																						UNION
//                                            SELECT reported_text text_value, null numeric_value, i.item_code itemCode, i.item_name itemName, 'text' data_type, d.UPDATE_DATE udate, m.report_date report_date, m.PPI ppi, t.template_code, RANK() OVER (PARTITION BY i.item_code, m.PPI ORDER BY m.report_date DESC) RANK
//                                                FROM std_text_details d
//                                                    JOIN item_std i ON i.item_code = d.item_code
//																										JOIN std_master m ON m.master_id = d.master_id
//																										JOIN std_template_item t ON t.item_code = i.item_code AND t.template_code = 'PSIT'
//																										JOIN TEMP_PPIS tmp ON tmp.PPI = m.PPI
//                                                WHERE i.data_type_flag = 'A' AND char_type_flag = 'A' AND t.template_code = 'PSIT' AND m.report_date <= :reportDate
//                                            UNION
//                                            SELECT null text_value, reported_value numeric_value, i.item_code itemCode, i.item_name itemName, 'numeric' data_type, d.UPDATE_DATE udate, m.report_date report_date, m.PPI ppi, t.template_code, RANK() OVER (PARTITION BY i.item_code, m.PPI ORDER BY m.report_date DESC) RANK
//                                                FROM std_details d
//                                                    JOIN item_std i ON i.item_code = d.item_code
//																										JOIN std_master m ON m.master_id = d.master_id
//																										JOIN std_template_item t ON t.item_code = i.item_code AND t.template_code = 'PSIT'
//																										JOIN TEMP_PPIS tmp ON tmp.PPI = m.PPI
//                                                WHERE i.data_type_flag = 'N' AND char_type_flag = 'N' AND t.template_code = 'PSIT' AND m.report_date <= :reportDate) tmp
//                                            WHERE rank = 1 ORDER BY 1, 2";

//			const string query =
//			@"
//SELECT ppi, data_type, itemCode, text_value, numeric_value, itemName, report_date  FROM (
//SELECT * FROM (
//SELECT null text_value, reported_value numeric_value, d.item_code itemCode, i.item_name itemName, 'numeric' data_type, d.UPDATE_DATE udate, m.report_date report_date, m.PPI ppi, t.template_code, RANK() OVER (PARTITION BY i.item_code, m.PPI ORDER BY m.report_date DESC) RANK
//    FROM std_details d
//        JOIN std_master m ON m.master_id = d.master_id
//        JOIN std_template_item t ON t.item_code = d.item_code AND t.template_code = 'PSIT'
//        JOIN TMP_PPIS tmp ON tmp.PPI = m.PPI
//        JOIN (SELECT i.item_code, i.item_name FROM item_std i JOIN std_template_item t ON t.item_code = i.item_code AND t.template_code = 'PSIT' WHERE i.data_type_flag = 'A' AND i.char_type_flag = 'A') i ON i.item_code = t.item_code
//) WHERE rank = 1
//UNION
//SELECT * FROM (
//SELECT null text_value, reported_value numeric_value, d.item_code itemCode, i.item_name itemName, 'numeric' data_type, d.UPDATE_DATE udate, m.report_date report_date, m.PPI ppi, t.template_code, RANK() OVER (PARTITION BY i.item_code, m.PPI ORDER BY m.report_date DESC) RANK
//    FROM std_details d
//        JOIN std_master m ON m.master_id = d.master_id
//        JOIN std_template_item t ON t.item_code = d.item_code AND t.template_code = 'PSIT'
//        JOIN TMP_PPIS tmp ON tmp.PPI = m.PPI
//        JOIN (SELECT i.item_code, i.item_name FROM item_std i JOIN std_template_item t ON t.item_code = i.item_code AND t.template_code = 'PSIT' WHERE i.data_type_flag = 'A' AND i.char_type_flag = 'D') i ON i.item_code = t.item_code
//) WHERE rank = 1
//UNION
//SELECT * FROM (
//SELECT null text_value, reported_value numeric_value, d.item_code itemCode, i.item_name itemName, 'numeric' data_type, d.UPDATE_DATE udate, m.report_date report_date, m.PPI ppi, t.template_code, RANK() OVER (PARTITION BY i.item_code, m.PPI ORDER BY m.report_date DESC) RANK
//    FROM std_details d
//        JOIN std_master m ON m.master_id = d.master_id
//        JOIN std_template_item t ON t.item_code = d.item_code AND t.template_code = 'PSIT'
//        JOIN TMP_PPIS tmp ON tmp.PPI = m.PPI
//        JOIN (SELECT i.item_code, i.item_name FROM item_std i JOIN std_template_item t ON t.item_code = i.item_code AND t.template_code = 'PSIT' WHERE i.data_type_flag = 'N' AND i.char_type_flag = 'N') i ON i.item_code = t.item_code
//) WHERE rank = 1
//)";

//			const string queryWithStartDate = @"SELECT ppi, data_type, item_code, text_value, numeric_value, item_name, report_date  FROM (
//SELECT
///*+ FULL(d) PARALLEL(d, 35) */
//reported_text text_value, null numeric_value, d.item_code item_code, i.item_name item_name, i.data_type data_type, d.UPDATE_DATE udate, m.report_date report_date, m.PPI ppi, t.template_code, RANK() OVER (PARTITION BY i.item_code, m.PPI ORDER BY m.report_date DESC) RANK
//    FROM std_text_details d
//        JOIN std_master m ON m.master_id = d.master_id
//        JOIN std_template_item t ON t.item_code = d.item_code AND t.template_code = 'PSIT'
//        JOIN TMP_PPIS tmp ON tmp.PPI = m.PPI
//        JOIN (
//        SELECT i.item_code, i.item_name, 'text' data_type FROM item_std i JOIN std_template_item t ON t.item_code = i.item_code AND t.template_code = 'PSIT' WHERE i.data_type_flag = 'A' AND i.char_type_flag = 'A'
//        UNION
//        SELECT i.item_code, i.item_name, 'date' date_type FROM item_std i JOIN std_template_item t ON t.item_code = i.item_code AND t.template_code = 'PSIT' WHERE i.data_type_flag = 'A' AND i.char_type_flag = 'D'
//        ) i ON i.item_code = t.item_code
//		WHERE m.report_date <= :reportDate AND m.report_date >= :since
//UNION
//SELECT
///*+ FULL(d) PARALLEL(d, 35) */
//null text_value, reported_value numeric_value, d.item_code item_code, i.item_name item_name, 'numeric' data_type, d.UPDATE_DATE udate, m.report_date report_date, m.PPI ppi, t.template_code, RANK() OVER (PARTITION BY i.item_code, m.PPI ORDER BY m.report_date DESC) RANK
//    FROM std_details d
//        JOIN std_master m ON m.master_id = d.master_id
//        JOIN std_template_item t ON t.item_code = d.item_code AND t.template_code = 'PSIT'
//        JOIN TMP_PPIS tmp ON tmp.PPI = m.PPI
//        JOIN (
//        SELECT i.item_code, i.item_name FROM item_std i JOIN std_template_item t ON t.item_code = i.item_code AND t.template_code = 'PSIT' WHERE i.data_type_flag = 'N' AND i.char_type_flag = 'N'
//        ) i ON i.item_code = t.item_code
//		WHERE m.report_date <= :reportDate AND m.report_date >= :since
//) WHERE rank = 1";

//			const string queryWithStartDate = @"
//SELECT t.ppi, t.data_type, t.item_code, t.item_name, null text_value, d.reported_value numeric_value, t.report_date 
///*+ FULL(d) PARALLEL(d, 35) */
//FROM std_details d
//JOIN
//(SELECT master_id, item_code, item_name, 'numeric' data_type, report_date, ppi
// FROM
//(SELECT m.master_id, i.item_code item_code, i.item_name item_name, 'numeric' data_type, m.report_date report_date, m.PPI ppi, t.template_code, RANK() OVER (PARTITION BY i.item_code, m.PPI ORDER BY m.report_date DESC) RANK
//  FROM std_master m
//    JOIN std_template_item t ON t.template_code = 'PSIT'
//    JOIN TMP_PPIS tmp ON tmp.PPI = m.PPI
//    JOIN (
//    SELECT i.item_code, i.item_name FROM item_std i JOIN std_template_item t ON t.item_code = i.item_code AND t.template_code = 'PSIT' WHERE i.data_type_flag = 'N' AND i.char_type_flag = 'N'
//    ) i ON i.item_code = t.item_code
//		WHERE m.report_date <= :reportDate AND m.report_date >= :since
//  ) WHERE rank = 1) t ON t.master_id = d.master_id AND t.item_code = d.item_code";

			const string queryWithStartDate = @"
SELECT ppi, data_type, item_code, text_value, numeric_value, item_name, report_date FROM
(
SELECT t.ppi, t.data_type, t.item_code, t.item_name, null text_value, d.reported_value numeric_value, t.report_date
/*+ FULL(d) PARALLEL(d, 35) */
FROM std_details d
JOIN
(SELECT master_id, item_code, item_name, 'numeric' data_type, report_date, ppi
 FROM
(SELECT m.master_id, i.item_code item_code, i.item_name item_name, m.report_date report_date, m.PPI ppi, t.template_code, RANK() OVER (PARTITION BY i.item_code, m.PPI ORDER BY m.report_date DESC) RANK
  FROM std_master m
    JOIN std_template_item t ON t.template_code = 'PSIT'
    JOIN TMP_PPIS tmp ON tmp.PPI = m.PPI
    JOIN (
    SELECT i.item_code, i.item_name FROM item_std i JOIN std_template_item t ON t.item_code = i.item_code AND t.template_code = 'PSIT' WHERE i.data_type_flag = 'N' AND i.char_type_flag = 'N'
    ) i ON i.item_code = t.item_code
		WHERE m.report_date <= :reportDate AND m.report_date >= :since
  ) WHERE rank = 1) t ON t.master_id = d.master_id AND t.item_code = d.item_code
UNION
SELECT t.ppi, t.data_type, t.item_code, t.item_name, d.reported_text text_value, null numeric_value, t.report_date
/*+ FULL(d) PARALLEL(d, 35) */
FROM std_text_details d
JOIN
(SELECT master_id, item_code, item_name, report_date, ppi, data_type
 FROM
(SELECT m.master_id, i.item_code item_code, i.item_name item_name, i.data_type, m.report_date report_date, m.PPI ppi, t.template_code, RANK() OVER (PARTITION BY i.item_code, m.PPI ORDER BY m.report_date DESC) RANK
  FROM std_master m
    JOIN std_template_item t ON t.template_code = 'PSIT'
    JOIN TMP_PPIS tmp ON tmp.PPI = m.PPI
    JOIN (
        SELECT i.item_code, i.item_name, 'text' data_type FROM item_std i JOIN std_template_item t ON t.item_code = i.item_code AND t.template_code = 'PSIT' WHERE i.data_type_flag = 'A' AND i.char_type_flag = 'A'
        UNION
        SELECT i.item_code, i.item_name, 'date' date_type FROM item_std i JOIN std_template_item t ON t.item_code = i.item_code AND t.template_code = 'PSIT' WHERE i.data_type_flag = 'A' AND i.char_type_flag = 'D'
        ) i ON i.item_code = t.item_code
		WHERE m.report_date <= :reportDate AND m.report_date >= :since
  ) WHERE rank = 1) t ON t.master_id = d.master_id AND t.item_code = d.item_code
)";

			const string queryIfStartDateIsNull = @"
SELECT ppi, data_type, item_code, text_value, numeric_value, item_name, report_date FROM
(
SELECT t.ppi, t.data_type, t.item_code, t.item_name, null text_value, d.reported_value numeric_value, t.report_date
FROM std_details d
JOIN
(SELECT master_id, item_code, item_name, 'numeric' data_type, report_date, ppi
 FROM
(SELECT m.master_id, i.item_code item_code, i.item_name item_name, m.report_date report_date, m.PPI ppi, t.template_code, RANK() OVER (PARTITION BY i.item_code, m.PPI ORDER BY m.report_date DESC) RANK
  FROM std_master m
    JOIN std_template_item t ON t.template_code = 'PSIT'
    JOIN TMP_PPIS tmp ON tmp.PPI = m.PPI
    JOIN (
    SELECT i.item_code, i.item_name FROM item_std i JOIN std_template_item t ON t.item_code = i.item_code AND t.template_code = 'PSIT' WHERE i.data_type_flag = 'N' AND i.char_type_flag = 'N'
    ) i ON i.item_code = t.item_code
		WHERE m.report_date <= :reportDate
  ) WHERE rank = 1) t ON t.master_id = d.master_id AND t.item_code = d.item_code
UNION
SELECT t.ppi, t.data_type, t.item_code, t.item_name, d.reported_text text_value, null numeric_value, t.report_date
FROM std_text_details d
JOIN
(SELECT master_id, item_code, item_name, report_date, ppi, data_type
 FROM
(SELECT m.master_id, i.item_code item_code, i.item_name item_name, i.data_type, m.report_date report_date, m.PPI ppi, t.template_code, RANK() OVER (PARTITION BY i.item_code, m.PPI ORDER BY m.report_date DESC) RANK
  FROM std_master m
    JOIN std_template_item t ON t.template_code = 'PSIT'
    JOIN TMP_PPIS tmp ON tmp.PPI = m.PPI
    JOIN (
        SELECT i.item_code, i.item_name, 'text' data_type FROM item_std i JOIN std_template_item t ON t.item_code = i.item_code AND t.template_code = 'PSIT' WHERE i.data_type_flag = 'A' AND i.char_type_flag = 'A'
        UNION
        SELECT i.item_code, i.item_name, 'date' date_type FROM item_std i JOIN std_template_item t ON t.item_code = i.item_code AND t.template_code = 'PSIT' WHERE i.data_type_flag = 'A' AND i.char_type_flag = 'D'
        ) i ON i.item_code = t.item_code
		WHERE m.report_date <= :reportDate
  ) WHERE rank = 1) t ON t.master_id = d.master_id AND t.item_code = d.item_code
)";

//			string dropSql = @"BEGIN
//                                    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_PPIS';
//                                    EXECUTE IMMEDIATE 'DROP TABLE TMP_PPIS';
//                                EXCEPTION
//                                    WHEN OTHERS THEN
//                                        IF SQLCODE != -942 THEN
//                                            RAISE;
//                                        END IF;
//                                END;";

//			string clearTable = @"BEGIN
//                                    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_PPIS';
//                                EXCEPTION
//                                    WHEN OTHERS THEN
//                                        IF SQLCODE != -942 THEN
//                                            RAISE;
//                                        END IF;
//                                END;";

			string query = queryIfStartDateIsNull;
			if (since != null) {
				query = queryWithStartDate;
			}

			DateTime searchDate = DateTime.Now;
			if (reportSearchDate != null) {
				searchDate = (DateTime)reportSearchDate;
			}
			Dictionary<string, List<ShareClassDataItem>> dataByPpi = new Dictionary<string, List<ShareClassDataItem>>();
			Dictionary<string, int> ppiDictionary = _ppiHelper.GetIconumPpiDictionary(iconums);

			if (ppiDictionary == null || ppiDictionary.Count < 1) {
				return new Dictionary<int, Dictionary<string, List<ShareClassDataItem>>>();
			}

			using (OracleConnection connection = new OracleConnection(_connectionString)) {
				connection.Open();

				string[] ppiArray = ppiDictionary.Keys.ToArray();
				using (var insertCmd = new OracleCommand(insertSql, connection)) {
					insertCmd.BindByName = true;
					insertCmd.ArrayBindCount = ppiArray.Length;
					insertCmd.Parameters.Add(":ppis", OracleDbType.Varchar2, ppiArray, ParameterDirection.Input);

					insertCmd.ExecuteNonQuery();
				}

				using (OracleCommand command = new OracleCommand(query, connection)) {
					command.BindByName = true;
					command.Parameters.Add(new OracleParameter()
																 {
																	 OracleDbType = OracleDbType.Date,
																	 Direction = ParameterDirection.Input,
																	 ParameterName = ":reportDate",
																	 Value = searchDate
																 });
					if (since != null) {
						command.Parameters.Add(new OracleParameter()
						                       {
							                       OracleDbType = OracleDbType.Date,
							                       Direction = ParameterDirection.Input,
							                       ParameterName = ":since",
							                       Value = (DateTime) since
						                       });
					}

					using (
						OracleDataReader sdr = command.ExecuteReader(CommandBehavior.SequentialAccess)) {
						while (sdr.Read()) {
							string ppi = sdr.GetString(0);
							string dataType = sdr.GetStringSafe(1);
							string itemCode = sdr.GetStringSafe(2);
							string textValue = sdr.GetStringSafe(3);
							decimal? numericValue = sdr.GetNullable<decimal>(4);
							string itemName = sdr.GetStringSafe(5);
							DateTime reportDate = sdr.GetDateTime(6);

							ShareClassDataItem item = null;
							if (dataType == "date") {
								item = new ShareClassDateItem
											 {
												 ItemId = itemCode,
												 Name = itemName,
												 ReportDate = reportDate,
												 Value = DateTime.ParseExact(textValue, "ddMMyyyy", null),
											 };
							} else if (dataType == "numeric" && numericValue != null) {
								item = new ShareClassNumericItem
											 {
												 ItemId = itemCode,
												 Name = itemName,
												 ReportDate = reportDate,
												 Value = (decimal)numericValue,
											 };
							} else if (dataType == "text") {
								item = new ShareClassTextItem
											 {
												 ItemId = itemCode,
												 Name = itemName,
												 ReportDate = reportDate,
												 Value = textValue,
											 };
							}

							if (!dataByPpi.ContainsKey(ppi)) {
								dataByPpi.Add(ppi, new List<ShareClassDataItem>());
							}

							dataByPpi[ppi].Add(item);
						}
					}
				}
			}

			Dictionary<int, Dictionary<string, List<ShareClassDataItem>>> companyShareData = new Dictionary<int, Dictionary<string, List<ShareClassDataItem>>>();
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

		public Dictionary<string, List<ShareClassDataItem>> GetLatestFPEShareData(int iconum, DateTime? reportDateToFind, DateTime? since) {
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

			const string query = @"SELECT ppi, data_type, itemCode, text_value, numeric_value, itemName, report_date FROM
                                          (SELECT reported_text text_value, null numeric_value, item_code itemCode, i.item_name itemName, 'date' data_type, d.UPDATE_DATE udate, m.report_date report_date, m.PPI ppi, t.template_code, RANK() OVER (PARTITION BY i.item_code, m.PPI ORDER BY m.report_date DESC) RANK
                                                FROM std_text_details d
                                                    JOIN item_std i ON i.item_code = d.item_code
																										JOIN std_master m ON m.master_id = d.master_id
																										JOIN std_template_item t ON t.item_code = i.item_code AND t.template_code = 'PSIT'
                                                WHERE i.data_type_flag = 'A' AND char_type_flag = 'D' AND m.PPI LIKE :ppiBase AND m.report_date <= :reportDate AND (:since IS NULL OR m.report_date >= :since)
																						UNION
                                            SELECT reported_text text_value, null numeric_value, i.item_code itemCode, i.item_name itemName, 'text' data_type, d.UPDATE_DATE udate, m.report_date report_date, m.PPI ppi, t.template_code, RANK() OVER (PARTITION BY i.item_code, m.PPI ORDER BY m.report_date DESC) RANK
                                                FROM std_text_details d
                                                    JOIN item_std i ON i.item_code = d.item_code
																										JOIN std_master m ON m.master_id = d.master_id
																										JOIN std_template_item t ON t.item_code = i.item_code AND t.template_code = 'PSIT'
                                                WHERE i.data_type_flag = 'A' AND char_type_flag = 'A' AND t.template_code = 'PSIT' AND m.PPI LIKE :ppiBase AND m.report_date <= :reportDate AND (:since IS NULL OR m.report_date >= :since)
                                            UNION
                                            SELECT null text_value, reported_value numeric_value, i.item_code itemCode, i.item_name itemName, 'numeric' data_type, d.UPDATE_DATE udate, m.report_date report_date, m.PPI ppi, t.template_code, RANK() OVER (PARTITION BY i.item_code, m.PPI ORDER BY m.report_date DESC) RANK
                                                FROM std_details d
                                                    JOIN item_std i ON i.item_code = d.item_code
																										JOIN std_master m ON m.master_id = d.master_id
																										JOIN std_template_item t ON t.item_code = i.item_code AND t.template_code = 'PSIT'
                                                WHERE i.data_type_flag = 'N' AND char_type_flag = 'N' AND t.template_code = 'PSIT' AND m.PPI LIKE :ppiBase AND m.report_date <= :reportDate AND (:since IS NULL OR m.report_date >= :since)) tmp
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
					if (since == null) {
						command.Parameters.Add(new OracleParameter()
						                       {
							                       OracleDbType = OracleDbType.Date,
							                       Direction = ParameterDirection.Input,
																		 ParameterName = "since",
							                       Value = DBNull.Value
						                       });
					}
					else {
						command.Parameters.Add(new OracleParameter()
						                       {
							                       OracleDbType = OracleDbType.Date,
							                       Direction = ParameterDirection.Input,
																		 ParameterName = "since",
							                       Value = since
						                       });
					}

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
							} else if (dataType == "text") {
								item = new ShareClassTextItem
								{
									ItemId = itemCode,
									Name = itemName,
									ReportDate = reportDate,
									Value = textValue,
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