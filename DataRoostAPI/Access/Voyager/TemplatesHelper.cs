using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Data;
using System.Data.SqlClient;
using Oracle.ManagedDataAccess.Client;
using CCS.Fundamentals.DataRoostAPI.Models;
using CCS.Fundamentals.DataRoostAPI.Models.Voyager;
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

			string stdQuery = @"SELECT m.TEMPLATE_NAME,
																 d.REP_TYPE,
																 d.DOC_UPDATE_TYPE,
																 m.TEMPLATE_CODE
															FROM STD_TEMPLATE_MASTER m
																JOIN STD_TEMPLATE_DETAILS d ON d.TEMPLATE_CODE = m.TEMPLATE_CODE
																JOIN STD_COMPANY_INDUSTRY c ON c.STD_INDUSTRY_CODE = d.STD_INDUSTRY_CODE
																JOIN COMPANY_COUNTRY cc ON cc.PPI = c.PPI
															WHERE c.PPI = :ppi
																AND (US_FLAG = 'B' OR US_FLAG = (CASE WHEN cc.ISO_CTRY_CODE = 'USA' THEN 'U' ELSE 'N' END))
																AND m.TEMPLATE_CODE = COALESCE(:templateCode, m.TEMPLATE_CODE)";

			string sdbQuery = @"SELECT m";

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
					} else {
						cmd.Parameters.Add(new OracleParameter("templateCode", OracleDbType.Varchar2) { Value = null });
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
					templateDTO.Items = PopulateTemplateItem(TemplateIdentifier.GetTemplateIdentifier(templateDTO.Id));
				}
			}

			return templates.ToArray();
		}


		private List<TemplateItemDTO> PopulateTemplateItem(TemplateIdentifier templateId) {
			string SQL_SDB_Items = @"";

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
					using (OracleDataReader reader = cmd.ExecuteReader()) {
						return reader.Cast<IDataRecord>().Select(r => new TemplateItemDTO()
						{
							Id = int.Parse(reader.GetString(0)),
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