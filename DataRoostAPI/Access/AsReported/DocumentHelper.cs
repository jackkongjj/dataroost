using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Data.SqlClient;
using DataRoostAPI.Common.Models.AsReported;
using FactSet.Data.SqlClient;
using DataRoostAPI.Common.Models.TINT;
using System.Text;
using System.Text.RegularExpressions;
using FactSet.Fundamentals.Sourcelinks;
using Nest;
using System.Configuration;
using DataRoostAPI.Common.Models.SuperFast;
using System.Web.Mvc;
using System.Globalization;
using ExpressionStore.Components;
using CCS.Fundamentals.DataRoostAPI.CommLogger;
namespace CCS.Fundamentals.DataRoostAPI.Access.AsReported {

	public class DocumentHelper {
		private readonly Regex rxHtmlBookmark = new Regex(@"o(-?\d+)\|l(\d+)", RegexOptions.Compiled);
		private readonly string _sfConnectionString;
		private readonly string _damConnectionString;

		private DirectCollectionHelper dcHelper;

		public DocumentHelper(string sfConnectionString, string damConnectionString) {
			_sfConnectionString = sfConnectionString;
			_damConnectionString = damConnectionString;
			dcHelper = new DirectCollectionHelper(_sfConnectionString);
		}

		public AsReportedDocument GetDocument(int iconum, string documentId) {
            string starttime = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff");


            string query = @"SELECT d.DocumentDate, d.PublicationDateTime, d.ReportTypeID, d.FormTypeID, d.DAMDocumentId, d.Id
																			FROM DocumentSeries s
																					JOIN Document d ON d.DocumentSeriesID = s.Id
																			WHERE s.CompanyID = @iconum
																				AND d.DAMDocumentId = @documentId";

			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {
				using (SqlCommand cmd = new SqlCommand(query, conn)) {
					conn.Open();
					cmd.Parameters.AddWithValue("@iconum", iconum);
					cmd.Parameters.AddWithValue("@documentId", documentId);

					using (SqlDataReader reader = cmd.ExecuteReader()) {
						if (reader.Read()) {
							AsReportedDocument document = new AsReportedDocument
							{
								ReportDate = reader.GetDateTime(0),
								PublicationDate = reader.GetDateTime(1),
								ReportType = reader.GetStringSafe(2),
								FormType = reader.GetStringSafe(3),
								Id = reader.GetGuid(4).ToString(),
								SuperFastDocumentId = reader.GetGuid(5).ToString(),
							};
							document.Tables = GetDocumentTables(document.SuperFastDocumentId);
							return document;
						}
					}
				}
			}
            CommunicationLogger.LogEvent("GetDocument", "DataRoost", starttime, DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff"));
            return null;
        }

		public AsReportedDocument[] GetDocuments(int iconum, string documentId) {

			const string query = @"declare @DocumentYear int 
select @DocumentYear = year(d.documentdate) from document d
join dbo.DocumentSeries ds on ds.ID = d.documentseriesid 
where d.damdocumentid = @DamDocumentId

declare @Years table(Yr int, Diff int)

insert into @Years
SELECT DISTINCT year(d.DocumentDate) , year(d.DocumentDate) - @DocumentYear
FROM DocumentSeries s
JOIN Document d ON d.DocumentSeriesID = s.Id
WHERE s.CompanyID = @iconum
AND (d.ExportFlag = 1 OR d.ArdExportFlag = 1 OR d.IsDocSetUpCompleted = 1)

SELECT d.DocumentDate, d.PublicationDateTime, d.ReportTypeID, d.FormTypeID, d.DAMDocumentId, d.Id, d.hasXBRL
FROM DocumentSeries s
JOIN Document d ON d.DocumentSeriesID = s.Id
WHERE s.CompanyID = @iconum
AND (d.ExportFlag = 1 OR d.ArdExportFlag = 1 OR d.IsDocSetUpCompleted = 1)
and YEAR(d.DocumentDate) in (select top 4 Yr from @Years where Diff in (0,1,2,3,-1,-2,-3) order by Yr  desc)";


            string starttime = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff");
            List<AsReportedDocument> documents = new List<AsReportedDocument>();

			using (SqlConnection connection = new SqlConnection(_sfConnectionString)) {
				connection.Open();

				using (SqlCommand cmd = new SqlCommand(query, connection)) {
					cmd.Parameters.AddWithValue("@iconum", iconum);
					cmd.Parameters.AddWithValue("@DamDocumentId", documentId);

					using (SqlDataReader reader = cmd.ExecuteReader()) {
						while (reader.Read()) {
							AsReportedDocument document = new AsReportedDocument
							{
								ReportDate = reader.GetDateTime(0),
								PublicationDate = reader.GetDateTime(1),
								ReportType = reader.GetStringSafe(2),
								FormType = reader.GetStringSafe(3),
								Id = reader.GetGuid(4).ToString(),
								SuperFastDocumentId = reader.GetGuid(5).ToString(),
							};
							document.Tables = GetDocumentTables(document.SuperFastDocumentId);
							documents.Add(document);
						}
					}
				}
			}
            CommunicationLogger.LogEvent("GetDocuments", "DataRoost", starttime, DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff"));

            return documents.ToArray();
		}

		public AsReportedDocument[] GetDocuments(int iconum, DateTime startDate, DateTime endDate, string reportType) {

			const string queryWithReportType =
								@"SELECT d.DocumentDate, d.PublicationDateTime, d.ReportTypeID, d.FormTypeID, d.DAMDocumentId, d.Id, d.hasXBRL
																			FROM DocumentSeries s
																					JOIN Document d ON d.DocumentSeriesID = s.Id
																			WHERE s.CompanyID = @iconum
																				AND ( d.ArdExportFlag = 1 OR d.IsDocSetUpCompleted = 1)
																				AND d.ReportTypeID = @reportType
																				AND d.DocumentDate >= @startDate
																				AND d.DocumentDate <= @endDate
																			ORDER BY d.DocumentDate DESC";
			const string queryWithoutReportType =
								@"SELECT d.DocumentDate, d.PublicationDateTime, d.ReportTypeID, d.FormTypeID, d.DAMDocumentId, d.Id, d.hasXBRL
																			FROM DocumentSeries s
																					JOIN Document d ON d.DocumentSeriesID = s.Id
																			WHERE s.CompanyID = @iconum
																				AND ( d.ArdExportFlag = 1 OR d.IsDocSetUpCompleted = 1)
																				AND d.DocumentDate >= @startDate
																				AND d.DocumentDate <= @endDate
																			ORDER BY d.DocumentDate DESC";
			string query = null;
			if (reportType == null) {
				query = queryWithoutReportType;
			} else {
				query = queryWithReportType;
			}
			List<AsReportedDocument> documents = new List<AsReportedDocument>();
            string starttime = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff");

            using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {
				using (SqlCommand cmd = new SqlCommand(query, conn)) {
					conn.Open();
					cmd.Parameters.AddWithValue("@iconum", iconum);
					cmd.Parameters.AddWithValue("@startDate", startDate);
					cmd.Parameters.AddWithValue("@endDate", endDate);
					if (reportType != null) {
						cmd.Parameters.AddWithValue("@reportType", reportType);
					}
					using (SqlDataReader reader = cmd.ExecuteReader()) {
						while (reader.Read()) {
							AsReportedDocument document = new AsReportedDocument
							{
								ReportDate = reader.GetDateTime(0),
								PublicationDate = reader.GetDateTime(1),
								ReportType = reader.GetStringSafe(2),
								FormType = reader.GetStringSafe(3),
								Id = reader.GetGuid(4).ToString(),
								SuperFastDocumentId = reader.GetGuid(5).ToString(),
								HasXbrl = reader.GetBoolean(6),
							};
							document.Tables = GetDocumentTables(document.SuperFastDocumentId);
							documents.Add(document);
						}
					}
				}
			}
            CommunicationLogger.LogEvent("GetDocuments", "DataRoost", starttime, DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff"));

            return documents.ToArray();
		}

		private AsReportedTable[] GetDocumentTables(string documentId) {
			string query = @"SELECT t.ID, tt.Description, d.ID, d.Label, d.AdjustedOrder, dt.Description
												FROM DocumentTable t
													JOIN TableType tt ON tt.ID = t.TableTypeID
													JOIN TableDimension d ON d.DocumentTableID = t.ID
													JOIN DimensionType dt ON dt.ID = d.DimensionTypeID
												WHERE t.DocumentID = @documentId";
            string starttime = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff");

            Dictionary<int, AsReportedTable> tables = new Dictionary<int, AsReportedTable>();
			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {
				using (SqlCommand cmd = new SqlCommand(query, conn)) {
					conn.Open();
					cmd.Parameters.AddWithValue("@documentId", documentId);
					using (SqlDataReader reader = cmd.ExecuteReader()) {
						while (reader.Read()) {
							int tableId = reader.GetInt32(0);
							AsReportedTable table;
							if (!tables.ContainsKey(tableId)) {
								table = new AsReportedTable
								{
									Id = tableId,
									TableType = reader.GetStringSafe(1),
									Cells = new List<Cell>(),
									Rows = new List<Row>(),
									Columns = new List<Column>()
								};
								tables.Add(tableId, table);
							}
							table = tables[tableId];

							int dimensionId = reader.GetInt32(2);
							string dimensionLabel = reader.GetStringSafe(3);
							int dimensionOrder = reader.GetInt32(4);
							string dimensionTypeDescription = reader.GetStringSafe(5);
							if (dimensionTypeDescription == "Column") {
								Column column = new Column { Id = dimensionId, Label = dimensionLabel, };
								table.Columns.Add(column);
							} else if (dimensionTypeDescription == "Free Text Label" || dimensionTypeDescription == "Row") {
								Row row = new Row { Id = dimensionId, Label = dimensionLabel, Order = dimensionOrder, };
								table.Rows.Add(row);
							}
						}
					}
				}
			}
            CommunicationLogger.LogEvent("GetDocumentTables", "DataRoost", starttime, DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff"));

            foreach (AsReportedTable table in tables.Values) {
				table.Cells = GetTableCells(table);
			}
			return tables.Values.ToArray();
		}

		private List<Cell> GetTableCells(AsReportedTable table) {
			string query = @"SELECT d.ID, c.ID, c.CompanyFinancialTermID, c.CellDate, c.Value, c.ValueNumeric, 
                              c.PeriodLength, c.PeriodTypeID, c.Offset, c.ScalingFactorID, c.CurrencyCode,
                              dt.Description, cft.Description, c.XBRLTag , 	nlsl.Label
													FROM TableDimension d with (nolock)
														JOIN DimensionType dt with (nolock) ON dt.ID = d.DimensionTypeID
														JOIN DimensionToCell dtc with (nolock) ON dtc.TableDimensionID = d.ID
														JOIN TableCell c with (nolock) ON c.ID = dtc.TableCellID
														JOIN CompanyFinancialTerm cft with (nolock) ON cft.ID = c.CompanyFinancialTermID
	                          left join [tint].[TableCellsToNativeLabel] tcnl with (nolock) on tcnl.TableCellId = c.id
											    	left join [tint].[NativeLabelSourcelinks] nlsl with (nolock) on nlsl.Id = tcnl.RowLabelId														
													WHERE d.DocumentTableID = @tableId";

			Dictionary<int, Cell> cells = new Dictionary<int, Cell>();
            string starttime = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff");
            using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {
				using (SqlCommand cmd = new SqlCommand(query, conn)) {
					conn.Open();
					cmd.Parameters.AddWithValue("@tableId", table.Id);
					using (SqlDataReader reader = cmd.ExecuteReader()) {
						while (reader.Read()) {
							int dimensionId = reader.GetInt32(0);
							string dimensionType = reader.GetStringSafe(11);
							int cellId = reader.GetInt32(1);
							Cell cell;
							if (!cells.ContainsKey(cellId)) {
								int? cftId = reader.GetNullable<int>(2);
								DateTime? cellDate = reader.GetNullable<DateTime>(3);
								string value = reader.GetStringSafe(4);
								decimal? numericValue = reader.GetNullable<decimal>(5);
								int? periodLengthInt = reader.GetNullable<int>(6);
								string periodLength = null;
								if (periodLengthInt != null) {
									periodLength = periodLengthInt.ToString();
								}
								string periodType = reader.GetStringSafe(7);
								string offset = reader.GetStringSafe(8);
								string scalingFactor = reader.GetStringSafe(9);
								string currencyCode = reader.GetStringSafe(10);
								string companyFinancialTermDescription = reader.GetStringSafe(12);
								string xbrlTag = reader.GetStringSafe(13);
								cell = new Cell
								{
									Id = cellId,
									CftId = cftId,
									Currency = currencyCode,
									Date = cellDate,
									Value = value,
									NumericValue = numericValue,
									PeriodLength = periodLength,
									PeriodType = periodType,
									Offset = offset,
									ScalingFactor = scalingFactor,
									CompanyFinancialTermDescription = companyFinancialTermDescription,
									XbrlTag = xbrlTag,
									NativeLabel = reader.GetStringSafe(14)
								};
								cells.Add(cellId, cell);
							}

							cell = cells[cellId];
							if (dimensionType == "Column") {
								cell.ColumnId = dimensionId;
							} else if (dimensionType == "Free Text Label" || dimensionType == "Row") {
								cell.RowId = dimensionId;
							}
						}
					}
				}
			}
            CommunicationLogger.LogEvent("GetTableCells", "DataRoost", starttime, DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff"));

            return cells.Values.ToList();
		}

		#region Direct Collection

		public AsReportedDocument GetDCDocument(int iconum, string documentId) {
			string query = @"SELECT d.DocumentDate, d.PublicationDateTime, d.ReportTypeID, d.FormTypeID, d.DAMDocumentId, d.Id
																			FROM DocumentSeries s
																					JOIN Document d ON d.DocumentSeriesID = s.Id
																			WHERE s.CompanyID = @iconum
																				AND d.DAMDocumentId = @documentId";
            string starttime = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff");
            using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {
				using (SqlCommand cmd = new SqlCommand(query, conn)) {
					conn.Open();
					cmd.Parameters.AddWithValue("@iconum", iconum);
					cmd.Parameters.AddWithValue("@documentId", documentId);

					using (SqlDataReader reader = cmd.ExecuteReader()) {
						if (reader.Read()) {
							AsReportedDocument document = new AsReportedDocument
							{
								ReportDate = reader.GetDateTime(0),
								PublicationDate = reader.GetDateTime(1),
								ReportType = reader.GetStringSafe(2),
								FormType = reader.GetStringSafe(3),
								Id = reader.GetGuid(4).ToString(),
								SuperFastDocumentId = reader.GetGuid(5).ToString(),
							};

							document.Cells = GetTableCells(document.SuperFastDocumentId);

							document.Cells = document.Cells.Where(o => o.CompanyFinancialTermDescription != null).ToList();
							return document;
						}
					}
				}
			}
            CommunicationLogger.LogEvent("GetDCDocument", "DataRoost", starttime, DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff"));

            return null;
		}

		public AsReportedDocument[] GetDCDocuments(int iconum, List<string> documentIds) {
			const string createTableQuery = @"CREATE TABLE #documentIds ( documentId VARCHAR(50) NOT NULL )";

			const string query = @"SELECT d.DocumentDate, d.PublicationDateTime, d.ReportTypeID, d.FormTypeID, d.DAMDocumentId, d.Id
																			FROM DocumentSeries s
																					JOIN Document d ON d.DocumentSeriesID = s.Id
																					JOIN #documentIds i ON i.documentId = d.DAMDocumentId
																			WHERE s.CompanyID = @iconum AND (d.ExportFlag = 1 OR d.ArdExportFlag = 1 OR d.IsDocSetUpCompleted = 1)";

			DataTable table = new DataTable();
			table.Columns.Add("documentId", typeof(string));
			foreach (string documentId in documentIds) {
				table.Rows.Add(documentId);
			}

			List<AsReportedDocument> documents = new List<AsReportedDocument>();
            string starttime = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff");
            using (SqlConnection connection = new SqlConnection(_sfConnectionString)) {
				connection.Open();

				using (SqlCommand cmd = new SqlCommand(createTableQuery, connection)) {
					cmd.ExecuteNonQuery();
				}

				// Upload all iconums to Temp table
				using (SqlBulkCopy bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, null)) {
					bulkCopy.BatchSize = table.Rows.Count;
					bulkCopy.DestinationTableName = "#documentIds";
					bulkCopy.WriteToServer(table);
				}

				using (SqlCommand cmd = new SqlCommand(query, connection)) {
					cmd.Parameters.AddWithValue("@iconum", iconum);

					using (SqlDataReader reader = cmd.ExecuteReader()) {
						while (reader.Read()) {
							AsReportedDocument document = new AsReportedDocument
							{
								ReportDate = reader.GetDateTime(0),
								PublicationDate = reader.GetDateTime(1),
								ReportType = reader.GetStringSafe(2),
								FormType = reader.GetStringSafe(3),
								Id = reader.GetGuid(4).ToString(),
								SuperFastDocumentId = reader.GetGuid(5).ToString(),
							};
							if (dcHelper.IsIconumDC(iconum)) {
								document.Cells = GetTableCells(GetDamDocumentID(document.SuperFastDocumentId).ToString(), iconum);
								var tableCells = GetTableCells(document.SuperFastDocumentId);
								foreach (var cell in tableCells) {
									Cell existingCell = document.Cells.FirstOrDefault(o => o.Offset == cell.Offset);
									if (existingCell != null) {
										existingCell.RowOrder = cell.RowOrder;
										existingCell.TableName = cell.TableName;
									} else {
										document.Cells.Add(cell);
									}
								}
							} else {
								document.Cells = GetTableCells(document.SuperFastDocumentId);
							}
							document.Cells = document.Cells.Where(o => o.CompanyFinancialTermDescription != null).ToList();
							documents.Add(document);
						}
					}
				}
			}
            CommunicationLogger.LogEvent("GetDCDocuments", "DataRoost", starttime, DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff"));

            return documents.ToArray();
		}

		public AsReportedDocument[] GetDCDocuments(int iconum, DateTime startDate, DateTime endDate, string reportType) {
			const string queryWithReportType =
								@"SELECT d.DocumentDate, d.PublicationDateTime, d.ReportTypeID, d.FormTypeID, d.DAMDocumentId, d.Id, d.hasXBRL
																			FROM DocumentSeries s
																					JOIN Document d ON d.DocumentSeriesID = s.Id
																			WHERE s.CompanyID = @iconum
																				AND ( d.ArdExportFlag = 1 OR d.IsDocSetUpCompleted = 1)
																				AND d.ReportTypeID = @reportType
																				AND d.DocumentDate >= @startDate
																				AND d.DocumentDate <= @endDate
																			ORDER BY d.DocumentDate DESC";
			const string queryWithoutReportType =
								@"SELECT d.DocumentDate, d.PublicationDateTime, d.ReportTypeID, d.FormTypeID, d.DAMDocumentId, d.Id, d.hasXBRL
																			FROM DocumentSeries s
																					JOIN Document d ON d.DocumentSeriesID = s.Id
																			WHERE s.CompanyID = @iconum
																				AND (d.ArdExportFlag = 1 OR d.IsDocSetUpCompleted = 1)
																				AND d.DocumentDate >= @startDate
																				AND d.DocumentDate <= @endDate
																			ORDER BY d.DocumentDate DESC";
			string query = null;
			if (reportType == null) {
				query = queryWithoutReportType;
			} else {
				query = queryWithReportType;
			}
			List<AsReportedDocument> documents = new List<AsReportedDocument>();
            string starttime = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff");
            using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {
				using (SqlCommand cmd = new SqlCommand(query, conn)) {
					conn.Open();
					cmd.Parameters.AddWithValue("@iconum", iconum);
					cmd.Parameters.AddWithValue("@startDate", startDate);
					cmd.Parameters.AddWithValue("@endDate", endDate);
					if (reportType != null) {
						cmd.Parameters.AddWithValue("@reportType", reportType);
					}
					using (SqlDataReader reader = cmd.ExecuteReader()) {
						while (reader.Read()) {
							AsReportedDocument document = new AsReportedDocument
							{
								ReportDate = reader.GetDateTime(0),
								PublicationDate = reader.GetDateTime(1),
								ReportType = reader.GetStringSafe(2),
								FormType = reader.GetStringSafe(3),
								Id = reader.GetGuid(4).ToString(),
								SuperFastDocumentId = reader.GetGuid(5).ToString(),
								HasXbrl = reader.GetBoolean(6),
							};
							if (dcHelper.IsIconumDC(iconum)) {
								document.Cells = GetTableCells(GetDamDocumentID(document.SuperFastDocumentId).ToString(), iconum);
								var tableCells = GetTableCells(document.SuperFastDocumentId);
								foreach (var cell in tableCells) {
									Cell existingCell = document.Cells.FirstOrDefault(o => o.Offset == cell.Offset);
									if (existingCell != null) {
										existingCell.RowOrder = cell.RowOrder;
										existingCell.TableName = cell.TableName;
									} else {
										document.Cells.Add(cell);
									}
								}
							} else {
								document.Cells = GetTableCells(document.SuperFastDocumentId);
							}
							document.Cells = document.Cells.Where(o => o.CompanyFinancialTermDescription != null).ToList();
							documents.Add(document);
						}
					}
				}
			}
            CommunicationLogger.LogEvent("GetDCDocuments", "DataRoost", starttime, DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff"));

            return documents.ToArray();
		}
		//	AND d.ReportTypeID = @reportType
		public AsReportedDocument[] GetHistory(int iconum, string documentId, string reportType) {
			const string queryWithReportType =
								@"declare @DocDate datetime
select  @DocDate = d.documentdate  from document d
where d.damdocumentid = @DamDocumentId

SELECT d.DocumentDate, d.PublicationDateTime, d.ReportTypeID, d.FormTypeID, d.DAMDocumentId, d.Id, d.hasXBRL
FROM DocumentSeries s
JOIN Document d ON d.DocumentSeriesID = s.Id
WHERE s.CompanyID = @iconum
AND d.ReportTypeID = @reportType
AND (d.ArdExportFlag = 1 OR d.IsDocSetUpCompleted = 1)
and d.DocumentDate between dateadd(Year, -1.1, @DocDate) and dateadd(Year, 1.1, @DocDate) ";
			const string queryWithoutReportType =
								@"declare @DocDate datetime
select  @DocDate = d.documentdate  from document d
where d.damdocumentid = @DamDocumentId

SELECT d.DocumentDate, d.PublicationDateTime, d.ReportTypeID, d.FormTypeID, d.DAMDocumentId, d.Id, d.hasXBRL
FROM DocumentSeries s
JOIN Document d ON d.DocumentSeriesID = s.Id
WHERE s.CompanyID = @iconum
AND (d.ArdExportFlag = 1 OR d.IsDocSetUpCompleted = 1)
and d.DocumentDate between dateadd(Year, -1.1, @DocDate) and dateadd(Year, 1.1, @DocDate) ";
			string query = null;
			if (string.IsNullOrEmpty(reportType)) {
				query = queryWithoutReportType;
			} else {
				query = queryWithReportType;
			}
            string starttime = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff");
            List<AsReportedDocument> documents = new List<AsReportedDocument>();
			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {
				using (SqlCommand cmd = new SqlCommand(query, conn)) {
					conn.Open();
					cmd.Parameters.AddWithValue("@iconum", iconum);
					cmd.Parameters.AddWithValue("@DamDocumentId", documentId);
					if (reportType != null) {
						cmd.Parameters.AddWithValue("@reportType", reportType);
					}
					using (SqlDataReader reader = cmd.ExecuteReader()) {
						while (reader.Read()) {
							AsReportedDocument document = new AsReportedDocument
							{
								ReportDate = reader.GetDateTime(0),
								PublicationDate = reader.GetDateTime(1),
								ReportType = reader.GetStringSafe(2),
								FormType = reader.GetStringSafe(3),
								Id = reader.GetGuid(4).ToString(),
								SuperFastDocumentId = reader.GetGuid(5).ToString(),
								HasXbrl = reader.GetBoolean(6),
							};
							if (dcHelper.IsIconumDC(iconum)) {
								document.Cells = GetTableCells(GetDamDocumentID(document.SuperFastDocumentId).ToString(), iconum);
								var tableCells = GetTableCells(document.SuperFastDocumentId);
								foreach (var cell in tableCells) {
									Cell existingCell = document.Cells.FirstOrDefault(o => o.Offset == cell.Offset);
									if (existingCell != null) {
										existingCell.RowOrder = cell.RowOrder;
										existingCell.TableName = cell.TableName;
										existingCell.CftId = cell.CftId;
										existingCell.CompanyFinancialTermDescription = cell.CompanyFinancialTermDescription;
										existingCell.Id = cell.Id;
										existingCell.NativeLabel = cell.NativeLabel;
									} else {
										if (cell.TableName == "IS" || cell.TableName == "BS" || cell.TableName == "CF") {
											document.Cells.Add(cell);
										}
									}
								}
							} else {
								document.Cells = GetTableCells(document.SuperFastDocumentId);
							}
							document.Cells = document.Cells.Where(o => o.CompanyFinancialTermDescription != null).ToList();
							documents.Add(document);
						}
					}
				}
			}
            CommunicationLogger.LogEvent("GetHistory", "DataRoost", starttime, DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff"));

            return documents.ToArray();
		}

		public AsReportedDocument[] GetHistory(int iconum, string documentId, double years) {

			string query = @"declare @DocDate datetime
select  @DocDate = d.documentdate  from document d
where d.damdocumentid = @DamDocumentId

SELECT d.DocumentDate, d.PublicationDateTime, d.ReportTypeID, d.FormTypeID, d.DAMDocumentId, d.Id, d.hasXBRL
FROM DocumentSeries s
JOIN Document d ON d.DocumentSeriesID = s.Id
WHERE s.CompanyID = @iconum
AND (OR d.ArdExportFlag = 1 OR d.IsDocSetUpCompleted = 1) 
and d.DocumentDate  between
";
			if (years > 0) {
				query += " @DocDate and  dateadd(Year, @Years, @DocDate)";
			} else {
				query += " dateadd(Year, @Years, @DocDate) and @DocDate";
			}

			List<AsReportedDocument> documents = new List<AsReportedDocument>();
            string starttime = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff");
            using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {
				using (SqlCommand cmd = new SqlCommand(query, conn)) {
					conn.Open();
					cmd.Parameters.AddWithValue("@iconum", iconum);
					cmd.Parameters.AddWithValue("@DamDocumentId", documentId);
					cmd.Parameters.AddWithValue("@Years", years);
					using (SqlDataReader reader = cmd.ExecuteReader()) {
						while (reader.Read()) {
							AsReportedDocument document = new AsReportedDocument
							{
								ReportDate = reader.GetDateTime(0),
								PublicationDate = reader.GetDateTime(1),
								ReportType = reader.GetStringSafe(2),
								FormType = reader.GetStringSafe(3),
								Id = reader.GetGuid(4).ToString(),
								SuperFastDocumentId = reader.GetGuid(5).ToString(),
								HasXbrl = reader.GetBoolean(6),
							};
							if (dcHelper.IsIconumDC(iconum)) {
								document.Cells = GetTableCells(GetDamDocumentID(document.SuperFastDocumentId).ToString(), iconum);
								var tableCells = GetTableCells(document.SuperFastDocumentId);
								foreach (var cell in tableCells) {
									Cell existingCell = document.Cells.FirstOrDefault(o => o.Offset == cell.Offset);
									if (existingCell != null) {
										existingCell.RowOrder = cell.RowOrder;
										existingCell.TableName = cell.TableName;
										existingCell.CftId = cell.CftId;
										existingCell.CompanyFinancialTermDescription = cell.CompanyFinancialTermDescription;
										existingCell.Id = cell.Id;
										existingCell.NativeLabel = cell.NativeLabel;
									} else {
										if (cell.TableName == "IS" || cell.TableName == "BS" || cell.TableName == "CF") {
											document.Cells.Add(cell);
										}
									}
								}
							} else {
								document.Cells = GetTableCells(document.SuperFastDocumentId);
							}
							document.Cells = document.Cells.Where(o => o.CompanyFinancialTermDescription != null).ToList();
							documents.Add(document);
						}
					}
				}
			}
            CommunicationLogger.LogEvent("GetHistory", "DataRoost", starttime, DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff"));

            return documents.ToArray();
		}

		public string DownloadFile(int iconum, string documentId) {
            string starttime = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff");
            AsReportedDocument document = GetDCDocument(iconum, documentId);
			//export Data as CSV
			StringBuilder sb = new StringBuilder();
			foreach (var tc in document.Cells.OrderBy(o => o.CftId).GroupBy(o => o.CftId)) {
				sb.Append(tc.First().Label.Replace(",", "") + ",");
				foreach (var exportCell in tc) {
					sb.Append(exportCell.Date + "," + exportCell.Value.Replace(",", "") + " " + exportCell.ScalingFactor + ",");
				}
				sb.AppendLine();
			}
			string contents = sb.ToString();
            CommunicationLogger.LogEvent("DownloadFile", "DataRoost", starttime, DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff"));

            return contents;

		}

		public void InsertTINTOffsets(string documentId, int CompanyId) {
            string starttime = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff");
            TINT.DocumentHelper helper = new TINT.DocumentHelper(_sfConnectionString, _damConnectionString);
			Dictionary<byte, Tint> tintFiles = helper.GetTintFiles(documentId);
			Guid SFDocumentId = GetSuperFastDocumentID(documentId, CompanyId).Value;
			int documentSeries = GetDocumentSeriesID(SFDocumentId.ToString());
			List<Cell> tableCells = GetTableCells(GetSuperFastDocumentID(documentId, CompanyId).Value.ToString()).ToList();

			foreach (var root in tintFiles.Keys) {
				foreach (var tint in tintFiles[root]) {
					int currentTerm = 0;
					string Label = string.Empty;
					TableCell tc = new TableCell(_sfConnectionString);
					foreach (TintCell cell in tint) {
						string offset = string.IsNullOrEmpty(cell.OriginalOffset) ? "" : cell.OriginalOffset;

						int length = cell.Value.Length;
						if (!string.IsNullOrEmpty(cell.OffSet) && cell.OffSet.ToLower()[0] != 'p') {
							try {
								string[] offsets = cell.OffSet.Split('|');
								length = int.Parse(offsets[1].Replace('l', ' '));
							} catch { }
						}


						string CellOffsetValue = string.IsNullOrEmpty(cell.OriginalOffset) ? "" : cell.HasBoundingBox ?
							new Bookmark(cell.OriginalOffset, root).ToString() : new Bookmark(int.Parse(offset), length, root).ToString();

						if (tableCells.Count(o => o.Offset == CellOffsetValue) == 0) {
							try {
								if (cell.ColumnType == "Description") {
									Label = cell.Value;
									if (cell.Value.EndsWith("]")) {
										int start = cell.Value.LastIndexOf("[");
										String pre = cell.Value.Substring(0, start);

										String value = cell.Value.Substring(start + 1, cell.Value.Length - start - 2);
										cell.Value = pre + value;
									}
									currentTerm = tc.CreateNewTerm(documentSeries, cell.Value);

								} else {
									Cell cc = new Cell();
									cc.Id = tc.Create(cell.Value, CellOffsetValue, cell.HasBoundingBox, cell.PeriodType, cell.PeriodLength,
																											 cell.ColumnDay, cell.ColumnMonth, cell.ColumnYear, currentTerm, tint.Unit, tint.Type, root, tint.Currency, cell.XbrlTag, SFDocumentId, Label, cell.OffSet);


								}

							} catch (Exception e) {
								System.Diagnostics.Debug.WriteLine(e.ToString());
								continue;
							}
						}
					}
				}
			}
            CommunicationLogger.LogEvent("InsertTINTOffsets", "DataRoost", starttime, DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff"));

        }

        public List<Cell> GetDocumentTableCells(string documentId, int CompanyId) {
			return GetTableCells(GetSuperFastDocumentID(documentId, CompanyId).Value.ToString());
		}

		private List<Cell> GetTableCells(string documentId) {
            string starttime = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff");
            string query = @"select  c.ID, c.CompanyFinancialTermID, c.CellDate, c.Value, c.ValueNumeric, c.PeriodLength, c.PeriodTypeID, c.Offset,
												c.ScalingFactorID, c.CurrencyCode, cft.Description, c.XBRLTag, isnull(td.label,c.Label) ,isnull(tt.Description,''), td.AdjustedOrder , c.ScarUpdated, 
												nlsl.Label
												from dbo.TableCell c with (NOLOCK)												
												join dbo.CompanyFinancialTerm cft  with (NOLOCK) on cft.ID = c.CompanyFinancialTermID
												left join dbo.DimensionToCell dtc  with (NOLOCK) on dtc.TableCellID = c.ID
												left JOIN dbo.TableDimension td  with (NOLOCK) on td.ID = dtc.TableDimensionID
												left join dbo.DocumentTable dt  with (NOLOCK) on dt.ID = td.DocumentTableID
												left join dbo.TableType tt  with (NOLOCK) on tt.ID = dt.TableTypeID
												left join [tint].[TableCellsToNativeLabel] tcnl with (nolock) on tcnl.TableCellId = c.id
												left join [tint].[NativeLabelSourcelinks] nlsl with (nolock) on nlsl.Id = tcnl.RowLabelId
												WHERE c.DocumentId = @documentId and (td.DimensionTypeID is null or TD.DimensionTypeID in (1,3))  and (tt.id is null or tt.description != 'ReferencedCells') ";

			List<Cell> cells = new List<Cell>();
			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {
				using (SqlCommand cmd = new SqlCommand(query, conn)) {
					conn.Open();
					cmd.Parameters.AddWithValue("@documentId", documentId);
					cmd.CommandTimeout = 1000;
					using (SqlDataReader reader = cmd.ExecuteReader()) {
						while (reader.Read()) {
							int? periodLengthInt = reader.GetNullable<int>(5);
							string periodLength = null;
							if (periodLengthInt != null) {
								periodLength = periodLengthInt.ToString();
							}

							cells.Add(new Cell
							{
								Id = reader.GetInt32(0),
								CftId = reader.GetNullable<int>(1),
								Currency = reader.GetStringSafe(9),
								Date = reader.GetNullable<DateTime>(2),
								Value = reader.GetStringSafe(3),
								NumericValue = reader.GetNullable<decimal>(4),
								PeriodLength = periodLength,
								PeriodType = reader.GetStringSafe(6),
								Offset = reader.GetStringSafe(7),
								ScalingFactor = reader.GetStringSafe(8),
								CompanyFinancialTermDescription = reader.GetStringSafe(10),
								XbrlTag = reader.GetStringSafe(11),
								Label = reader.GetStringSafe(12),
								TableName = reader.GetStringSafe(13),
								RowOrder = reader.GetNullable<int>(14),
								SCARUpdated = reader.GetBoolean(15),
								NativeLabel = reader.GetStringSafe(16)
							});

						}
					}
				}
			}
            CommunicationLogger.LogEvent("GetTableCells", "DataRoost", starttime, DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff"));

            return cells;
		}

		private Dictionary<string, string> GetSDBItems() {
            string starttime = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff");
            Dictionary<string, string> items = new Dictionary<string, string>();
			string query = "select distinct SDBCode , Description from SDBItem";
			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {
				using (SqlCommand cmd = new SqlCommand(query, conn)) {
					conn.Open();
					cmd.CommandTimeout = 1000;
					using (SqlDataReader reader = cmd.ExecuteReader()) {
						while (reader.Read()) {
							items.Add(reader.GetStringSafe(0), reader.GetStringSafe(1));
						}
					}
				}
			}
            CommunicationLogger.LogEvent("GetSDBItems", "DataRoost", starttime, DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff"));

            return items;
		}

		private List<Cell> GetTableCells(string documentId, int Iconum) {
			string connString = ConfigurationManager.AppSettings["ElasticEndpointA"];
            string starttime = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff");
            var terms = connString.Split(';');
			string uri = "http://" + terms[0].Substring(terms[0].IndexOf("NodeUri=") + 8);
			string Username = terms[1].Substring(terms[0].IndexOf("Username=") + 10);
			string Password = terms[2].Substring(terms[0].IndexOf("Password=") + 10);

			var server = new Uri(uri);
			var settings = new ConnectionSettings(server);
			var settingAuthentication = settings.BasicAuthentication(Username, Password);
			var elastic = new ElasticClient(settings);

			Dictionary<string, string> items = GetSDBItems();
			List<SFTimeseriesDTO> timeSlices = GetTimeSliceForDocument(documentId, Iconum);

			List<Cell> cells = new List<Cell>();
			ISearchResponse<ElasticObjectTree> request = elastic.Search<ElasticObjectTree>(s => s
				.Index("ffcore")
				.AllTypes()
				.From(0)
				.Size(10000)
				.Query(q => q.Term(p => p.DocumentId, documentId) && q.Term(p => p.Iconum, Iconum) && (q.Term(p => p.Name, "mi") || q.Term(p => p.Name, "mn")))
				);

			List<ElasticObjectTree> ebObjects = new List<ElasticObjectTree>(request.Documents);
			foreach (var eboTS in ebObjects.Where(o => o.AutoClacFlag == 0).GroupBy(o => new { o.InterimTypeID, o.ReportTypeID, o.AccountTypeID, o.EncoreFlag })) {
				SFTimeseriesDTO ts = timeSlices.FirstOrDefault(o => o.InterimType == eboTS.Key.InterimTypeID && o.ReportType == eboTS.Key.ReportTypeID && o.AccountType == eboTS.Key.AccountTypeID
					&& o.IsRecap == eboTS.Key.EncoreFlag);
				if (ts == null) continue;
				foreach (var ebo in eboTS.GroupBy(o => o.Offset)) {
					var eb = ebo.FirstOrDefault();
					string ItemDescription = null;
					items.TryGetValue(eb.ItemCode, out ItemDescription);
					cells.Add(new Cell
					{
						CompanyFinancialTermDescription = eb.CompanyFinancialTerm,
						CftId = eb.CompanyFinancialTermId,
						Currency = eb.CurrencyCode,
						Value = eb.Value,
						NumericValue = string.IsNullOrEmpty(eb.ValueNumeric) ? 0 : decimal.Parse(eb.ValueNumeric, NumberStyles.Any),
						Offset = eb.Offset,
						ScalingFactor = eb.ScalingFactor,
						XbrlTag = eb.XbrlTag,
						Label = eb.OffsetLabelWithHierarchy,
						PeriodLength = ts.PeriodLength.ToString(),
						PeriodType = ts.PeriodType,
						Date = ts.PeriodEndDate,
						ItemDescription = ItemDescription
					});
				}

			}
            CommunicationLogger.LogEvent("GetTableCells", "DataRoost", starttime, DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff"));

            return cells;
		}

		private Guid? GetSuperFastDocumentID(string DamDocId, int iconum) {
			const string sqltxt = @"prcGet_FFDocHist_GetSuperFastDocumentID";
            string starttime = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff");
            try {
				using (SqlConnection sqlConn = new SqlConnection(_sfConnectionString))
				using (SqlCommand cmd = new SqlCommand(sqltxt, sqlConn)) {
					cmd.CommandType = CommandType.StoredProcedure;
					cmd.Parameters.AddWithValue("@docId", DamDocId);
					cmd.Parameters.AddWithValue("@iconum", iconum);
					sqlConn.Open();
					using (SqlDataReader sdr = cmd.ExecuteReader()) {
                        CommunicationLogger.LogEvent("GetSuperFastDocumentID", "DataRoost", starttime, DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff"));
                        if (sdr.Read())
							return sdr.GetGuid(0);
						else
							return null;

					}

                }
            } catch (Exception e) {
				return null;
			}
		}

		private Guid GetDamDocumentID(string SFDocumentId) {
            string starttime = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff");
            const string SQLTEXT = @"select DamDocumentID from document (nolock) where id = @sfdocID";
			Guid DamDocID;
			using (SqlConnection conn = new SqlConnection(_sfConnectionString))
			using (SqlCommand sql = new SqlCommand(SQLTEXT, conn)) {
				sql.Parameters.AddWithValue("@sfdocID", SFDocumentId);
				conn.Open();
				using (SqlDataReader sdr = sql.ExecuteReader()) {
					sdr.Read();
					DamDocID = sdr.GetGuid(0);
				}
                CommunicationLogger.LogEvent("GetDamDocumentID", "DataRoost", starttime, DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff"));

            }

            return DamDocID;
		}

		public Guid? GetSuperFastDocumentID(Guid DamDocId, int iconum) {
            string starttime = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff");
            const string sqltxt = @"prcGet_FFDocHist_GetSuperFastDocumentID";

			try {
				using (SqlConnection sqlConn = new SqlConnection(_sfConnectionString))
				using (SqlCommand cmd = new SqlCommand(sqltxt, sqlConn)) {
					cmd.CommandType = CommandType.StoredProcedure;
					cmd.Parameters.AddWithValue("@docId", DamDocId);
					cmd.Parameters.AddWithValue("@iconum", iconum);
					sqlConn.Open();
					using (SqlDataReader sdr = cmd.ExecuteReader()) {
                        CommunicationLogger.LogEvent("GetSuperFastDocumentID", "DataRoost", starttime, DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff"));
                        if (sdr.Read())
							return sdr.GetGuid(0);
						else
							return null;
                    }

                }
			} catch (Exception e) {
				return null;
			}
		}

		private int GetDocumentSeriesID(string DocumentId) {

			const string sqltxt = @"select DocumentSeriesID from document with (nolock) where ID = @docId";
            string starttime = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff");

            using (SqlConnection sqlConn = new SqlConnection(_sfConnectionString))
			using (SqlCommand cmd = new SqlCommand(sqltxt, sqlConn)) {
				cmd.Parameters.AddWithValue("@docId", DocumentId);
				sqlConn.Open();
				using (SqlDataReader sdr = cmd.ExecuteReader()) {
                    CommunicationLogger.LogEvent("GetDocument", "DataRoost", starttime, DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff"));

                    if (sdr.Read())
						return sdr.GetInt32(0);
					else
						return -1;
				}
			}

        }

        private List<SFTimeseriesDTO> GetTimeSliceForDocument(string documentId, int Iconum) {
            string starttime = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff");
            List<SFTimeseriesDTO> timeSlices = new List<SFTimeseriesDTO>();
			const string sqltxt = @" select ts.TimeSliceDate, ts.AccountTypeID, ts.ReportTypeID, ts.InterimTypeID, ts.EncoreFlag, ts.AutoCalcFlag,ts.PeriodLength, ts.PeriodTypeID from dbo.TimeSlice ts 
 join document d on d.id = ts.DocumentID
 JOIN dbo.DocumentSeries ds on ds.id = d.documentseriesid
 where d.DAMDocumentId = @docId and ds.CompanyID = @iconum";

			using (SqlConnection sqlConn = new SqlConnection(_sfConnectionString))
			using (SqlCommand cmd = new SqlCommand(sqltxt, sqlConn)) {
				cmd.Parameters.AddWithValue("@docId", documentId);
				cmd.Parameters.AddWithValue("@iconum", Iconum);
				sqlConn.Open();
				using (SqlDataReader sdr = cmd.ExecuteReader()) {
					while (sdr.Read()) {
						timeSlices.Add(new SFTimeseriesDTO
						{
							PeriodEndDate = sdr.GetDateTime(0),
							AccountType = sdr.GetStringSafe(1),
							ReportType = sdr.GetStringSafe(2),
							InterimType = sdr.GetStringSafe(3),
							IsRecap = sdr.GetBoolean(4),
							IsAutoCalc = sdr.GetInt32(5) == 0 ? false : true,
							PeriodLength = sdr.GetInt32(6),
							PeriodType = sdr.GetStringSafe(7)
						});
					}
				}
			}
            CommunicationLogger.LogEvent("GetTimeSliceForDocument", "DataRoost", starttime, DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff"));

            return timeSlices;
		}

		public String UpdateSplitAdjustmentDate(string SFDocumentId, String date, int Iconum) {
			try {
                string starttime = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff");
                const string sqltxt = @"UPDATE Document 
																	set 
                                      SplitAdjustmentDate=@SplitAdjustmentDate 
																WHERE ID=@id";

				using (SqlConnection conn = new SqlConnection(_sfConnectionString))
				using (SqlCommand cmd = new SqlCommand(sqltxt, conn)) {
					conn.Open();
					cmd.Parameters.AddWithValue("@id", SFDocumentId);
					cmd.Parameters.AddWithValue("@SplitAdjustmentDate", date);
					cmd.ExecuteNonQuery();
				}
                CommunicationLogger.LogEvent("UpdateSplitAdjustmentDate", "DataRoost", starttime, DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff"));

            }
            catch (Exception ex) {
				return ex.Message;
			}
			return "";
		}

		#endregion

	}
}