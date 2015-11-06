using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Data.SqlClient;

using DataRoostAPI.Common.Models.AsReported;

using FactSet.Data.SqlClient;

namespace CCS.Fundamentals.DataRoostAPI.Access.AsReported {

	public class DocumentHelper {

		private string _sfConnectionString;

		public DocumentHelper(string sfConnectionString) {
			_sfConnectionString = sfConnectionString;
		}

		public AsReportedDocument GetDocument(int iconum, string documentId) {
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
							return document;
						}
					}
				}
			}
			return null;
		}

		public AsReportedDocument[] GetDocuments(int iconum, DateTime startDate, DateTime endDate, string reportType) {
			string queryWithReportType =
				@"SELECT d.DocumentDate, d.PublicationDateTime, d.ReportTypeID, d.FormTypeID, d.DAMDocumentId, d.Id
																			FROM DocumentSeries s
																					JOIN Document d ON d.DocumentSeriesID = s.Id
																			WHERE s.CompanyID = @iconum
																				AND d.ExportFlag = 1
																				AND d.ReportTypeID = @reportType
																				AND d.DocumentDate >= @startDate
																				AND d.DocumentDate <= @endDate
																			ORDER BY d.DocumentDate DESC";
			string queryWithoutReportType =
				@"SELECT d.DocumentDate, d.PublicationDateTime, d.ReportTypeID, d.FormTypeID, d.DAMDocumentId, d.Id
																			FROM DocumentSeries s
																					JOIN Document d ON d.DocumentSeriesID = s.Id
																			WHERE s.CompanyID = @iconum
																				AND d.ExportFlag = 1
																				AND d.DocumentDate >= @startDate
																				AND d.DocumentDate <= @endDate
																			ORDER BY d.DocumentDate DESC";
			string query = null;
			if (reportType == null) {
				query = queryWithoutReportType;
			}
			else {
				query = queryWithoutReportType;
			}
			List<AsReportedDocument> documents = new List<AsReportedDocument>();
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
							                              };
							document.Tables = GetDocumentTables(document.SuperFastDocumentId);
							documents.Add(document);
						}
					}
				}
			}
			return documents.ToArray();
		}

		private AsReportedTable[] GetDocumentTables(string documentId) {
			string query = @"SELECT t.ID, tt.Description, d.ID, d.Label, d.AdjustedOrder, dt.Description
												FROM DocumentTable t
													JOIN TableType tt ON tt.ID = t.TableTypeID
													JOIN TableDimension d ON d.DocumentTableID = t.ID
													JOIN DimensionType dt ON dt.ID = d.DimensionTypeID
												WHERE t.DocumentID = @documentId";

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
													Columns = new List<Column>(),
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

			foreach (AsReportedTable table in tables.Values) {
				table.Cells = GetTableCells(table);
			}
			return tables.Values.ToArray();
		}

		private List<Cell> GetTableCells(AsReportedTable table) {
			string query = @"SELECT d.ID, c.ID, c.CompanyFinancialTermID, c.CellDate, c.Value, c.ValueNumeric, c.PeriodLength, c.PeriodTypeID, c.Offset, c.ScalingFactorID, c.CurrencyCode, dt.Description, cft.Description, c.XBRLTag
													FROM TableDimension d 
														JOIN DimensionType dt ON dt.ID = d.DimensionTypeID
														JOIN DimensionToCell dtc ON dtc.TableDimensionID = d.ID
														JOIN TableCell c ON c.ID = dtc.TableCellID
														JOIN CompanyFinancialTerm cft ON cft.ID = c.CompanyFinancialTermID
													WHERE d.DocumentTableID = @tableId";

			Dictionary<int, Cell> cells = new Dictionary<int, Cell>();
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
			return cells.Values.ToList();
		}
	}
}