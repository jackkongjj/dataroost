using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Data;
using System.Data.SqlClient;

using DataRoostAPI.Common.Models;
using DataRoostAPI.Common.Models.Voyager;

using Oracle.ManagedDataAccess.Client;
using FactSet.Data.SqlClient;

namespace CCS.Fundamentals.DataRoostAPI.Access.Voyager {
	public class TemplatesHelper {
		private readonly string _connectionString;
		private readonly int _iconum;
		private readonly StandardizationType _dataType;

		public TemplatesHelper(string connectionString, int iconum, StandardizationType dataType) {
			_dataType = dataType;
			_iconum = iconum;
			_connectionString = connectionString;
		}

		public TemplateDTO[] GetTemplates(string templateId) {
			string ppi = GetPPIByIconum(_iconum);

			string stdQuery = @"SELECT DISTINCT m.TEMPLATE_NAME,
																 d.REP_TYPE,
																 d.DOC_UPDATE_TYPE,
																 m.TEMPLATE_CODE
															FROM STD_TEMPLATE_MASTER m
																JOIN STD_TEMPLATE_DETAILS d ON d.TEMPLATE_CODE = m.TEMPLATE_CODE
																JOIN STD_COMPANY_INDUSTRY c ON c.STD_INDUSTRY_CODE = d.STD_INDUSTRY_CODE
																JOIN COMPANY_COUNTRY cc ON cc.PPI = c.PPI
															WHERE c.PPI = :ppi
																AND (US_FLAG = 'B' OR US_FLAG = (CASE WHEN cc.ISO_CTRY_CODE = 'USA' THEN 'U' ELSE 'N' END))
																AND m.TEMPLATE_CODE = COALESCE(:templateCode, m.TEMPLATE_CODE)
                                AND d.REP_TYPE = COALESCE(:repType, d.REP_TYPE)
                                AND d.DOC_UPDATE_TYPE  = COALESCE(:updateType, d.DOC_UPDATE_TYPE)";

			string sdbQuery = @"select  distinct ti.TEMPLATE_NAME, ct.rep_type, trt.doc_update_type,
													CAST(ti.tid_cat_code || ti.tid_gnrc_code || ti.tid_type_code || ti.tid_seq_no as varchar(10)) as TEMPLATE_CODE
													FROM company_category cc
													JOIN company_template ct ON cc.co_cat_id = ct.co_cat_id
													JOIN template_id ti ON cc.cat_code = ti.tid_cat_code 
															and ct.tid_gnrc_code = ti.tid_gnrc_code 
															and ct.tid_type_code = ti.tid_type_code 
															and ct.tid_seq_no = ti.tid_seq_no
													JOIN template_report_type trt ON trt.tid_seq_no = ti.tid_seq_no
														and trt.tid_gnrc_code = ti.tid_gnrc_code 
														and trt.tid_type_code = ti.tid_type_code 
														and trt.tid_cat_code = ti.tid_cat_code 
													where cc.ppi = :ppi
													AND (ti.tid_cat_code || ti.tid_gnrc_code || ti.tid_type_code || ti.tid_seq_no) = COALESCE(:templateCode, (ti.tid_cat_code || ti.tid_gnrc_code || ti.tid_type_code || ti.tid_seq_no))
													AND ct.rep_type = COALESCE(:repType, ct.rep_type)
													AND trt.doc_update_type  = COALESCE(:updateType, trt.doc_update_type)";

			TemplateIdentifier templateIdentifier = null;

			List<VoyagerTemplateDTO> templates = new List<VoyagerTemplateDTO>();
			bool requestedSpecificTemplate = (templateId != null);
			if (requestedSpecificTemplate) {
				templateIdentifier = TemplateIdentifier.GetTemplateIdentifier(templateId);
			}

			using (OracleConnection conn = new OracleConnection(_connectionString)) {
				conn.Open();
				using (OracleCommand cmd = new OracleCommand(_dataType == StandardizationType.SDB ? sdbQuery : stdQuery, conn)) {
					cmd.Parameters.Add(new OracleParameter("ppi", OracleDbType.Varchar2) { Value = ppi });
					if (requestedSpecificTemplate) {
						cmd.Parameters.Add(new OracleParameter("templateCode", OracleDbType.Varchar2) { Value = templateIdentifier.TemplateCode });
						cmd.Parameters.Add(new OracleParameter("repType", OracleDbType.Varchar2) { Value = templateIdentifier.ReportType });
						cmd.Parameters.Add(new OracleParameter("updateType", OracleDbType.Varchar2) { Value = templateIdentifier.UpdateType });
					} else {
						cmd.Parameters.Add(new OracleParameter("templateCode", OracleDbType.Varchar2) { Value = null });
						cmd.Parameters.Add(new OracleParameter("repType", OracleDbType.Varchar2) { Value = null });
						cmd.Parameters.Add(new OracleParameter("updateType", OracleDbType.Varchar2) { Value = null });
					}

					using (OracleDataReader reader = cmd.ExecuteReader()) {
						templates.AddRange(
										reader.Cast<IDataRecord>().Select(r => new VoyagerTemplateDTO()
										{
											Id = new TemplateIdentifier()
											{
												UpdateType = reader.GetString(2),
												ReportType = reader.GetString(1),
												TemplateCode = reader.GetString(3),
											}.GetToken(),
											Name = reader.GetString(0),
											ReportType = reader.GetString(1),
											UpdateType = reader.GetString(2),
											TemplateCode = reader.GetString(3),
										}));
					}
				}
			}

			if (requestedSpecificTemplate) {
				foreach (VoyagerTemplateDTO templateDTO in templates) {
					templateDTO.Items = PopulateTemplateItem(TemplateIdentifier.GetTemplateIdentifier(templateDTO.Id), ppi);
				}
			}

			return templates.ToArray();
		}


		private List<TemplateItemDTO> PopulateTemplateItem(TemplateIdentifier templateId, string ppi) {
			string SQL_SDB_Items = @"select distinct CAST(rd.GNRC_CODE||rd.GROUP_CODE||rd.SUB_GROUP_CODE||rd.ITEM_CODE as varchar(12)) sdb_code, OI.ITEM_OFFICIAL_NAME, oi.no_decimals, OI.Item_type
															FROM company_category cc
																JOIN company_template ct ON cc.co_cat_id = ct.co_cat_id 
																join generic gnrc on GNRC.GNRC_CODE = CT.TID_GNRC_CODE
																join Company_Template_Items rd on RD.co_temp_item_id = ct.co_temp_item_id
																join official_item oi on RD.GNRC_CODE = OI.GNRC_CODE and RD.GROUP_CODE = OI.GROUP_CODE and RD.SUB_GROUP_CODE = OI.SUB_GROUP_CODE and RD.ITEM_CODE = OI.ITEM_CODE
																JOIN TEMPLATE_ITEM TI on RD.GNRC_CODE = TI.ITEM_GNRC_CODE and RD.GROUP_CODE = TI.GROUP_CODE and RD.SUB_GROUP_CODE = TI.SUB_GROUP_CODE and RD.ITEM_CODE = TI.ITEM_CODE
																JOIN template_id tid ON cc.cat_code = tid.tid_cat_code and ct.tid_gnrc_code = tid.tid_gnrc_code and ct.tid_type_code = tid.tid_type_code and ct.tid_seq_no = tid.tid_seq_no
															where (ti.tid_cat_code || ti.tid_gnrc_code || ti.tid_type_code || ti.tid_seq_no) = :templateCode
															AND cc.ppi = :ppi AND ct.rep_type = :repType";

			string SQL_STD_Items = @"SELECT iSTD.item_code,
																			iSTD.item_short_name,
																			iSTD.no_decimals,
																			(CASE WHEN (iSTD.DATA_TYPE_FLAG = 'A' AND iSTD.CHAR_TYPE_FLAG = 'A') THEN 'D'
																						WHEN (iSTD.DATA_TYPE_FLAG = 'A' AND iSTD.CHAR_TYPE_FLAG = 'D') THEN 'T'
																						WHEN (iSTD.DATA_TYPE_FLAG = 'N' AND iSTD.CHAR_TYPE_FLAG = 'N') THEN 'E'
																						ELSE '' END) valueType,
																			item_position itemsequence,
																			0 sdbItemLevel,
																			iSTD.ITEM_NAME sdbDescription,
																			rownum SDBCode,
																			data_type_flag ItemUsageTypeDescription
																	FROM ITEM_STD iSTD
																			INNER JOIN STD_TEMPLATE_ITEM stdti ON stdti.item_code = iSTD.item_code
																			INNER JOIN STD_TEMPLATE_MASTER stdtm ON stdtm.template_code = stdti.template_code
																	WHERE stdtm.template_code = :templateCode
																	ORDER BY stdti.item_position";

			using (OracleConnection conn = new OracleConnection(_connectionString)) {
				conn.Open();
				using (OracleCommand cmd = new OracleCommand(_dataType == StandardizationType.SDB ? SQL_SDB_Items : SQL_STD_Items, conn)) {
					cmd.Parameters.Add(new OracleParameter("templateCode", OracleDbType.Varchar2) { Value = templateId.TemplateCode });
					if (_dataType == StandardizationType.SDB) {
						cmd.Parameters.Add(new OracleParameter("ppi", OracleDbType.Varchar2) { Value = ppi });
						cmd.Parameters.Add(new OracleParameter("repType", OracleDbType.Varchar2) { Value = templateId.ReportType });
					}
					using (OracleDataReader reader = cmd.ExecuteReader()) {
						return reader.Cast<IDataRecord>().Select(r => new TemplateItemDTO()
						{
							Id = reader.GetString(0),
							Code = reader.GetString(0),
							Description = reader.GetString(1),
							Precision = reader.GetByte(2),
							ValueType = reader.GetString(3),
							//StatementTypeId = reader.GetString(3),
							//UsageType = reader.GetString(4),
							//IndentLevel = reader.GetInt32(5),
							//IsSecurity = reader.GetBoolean(7),
							//IsPIT = reader.GetBoolean(8),
						}).ToList<TemplateItemDTO>();
					}
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