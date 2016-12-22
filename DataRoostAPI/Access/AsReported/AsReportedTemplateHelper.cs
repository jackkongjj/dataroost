using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Web;
using DataRoostAPI.Common.Models.AsReported;
using FactSet.Data.SqlClient;

namespace CCS.Fundamentals.DataRoostAPI.Access.AsReported {
	public class AsReportedTemplateHelper {

		private readonly string _sfConnectionString;

		public AsReportedTemplateHelper(string sfConnectionString) {
			this._sfConnectionString = sfConnectionString;
		}
		private string SQL_GetCellQuery =
                                @"
SELECT DISTINCT tc.ID, tc.Offset, tc.CellPeriodType, tc.PeriodTypeID, tc.CellPeriodCount, tc.PeriodLength, tc.CellDay, 
				tc.CellMonth, tc.CellYear, tc.CellDate, tc.Value, tc.CompanyFinancialTermID, tc.ValueNumeric, tc.NormalizedNegativeIndicator, 
				tc.ScalingFactorID, tc.AsReportedScalingFactor, tc.Currency, tc.CurrencyCode, tc.Cusip, tc.ScarUpdated, tc.IsIncomePositive, 
				tc.XBRLTag, null, tc.DocumentId, tc.Label, tc.ScalingFactorValue,
				(select aetc.ARDErrorTypeId from ARDErrorTypeTableCell aetc (nolock) where tc.Id = aetc.TableCellId),
				(select metc.MTMWErrorTypeId from MTMWErrorTypeTableCell metc (nolock) where tc.Id = metc.TableCellId), 
				sh.AdjustedOrder, dts.Duration, dts.TimeSlicePeriodEndDate, dts.ReportingPeriodEndDate, d.PublicationDateTime
FROM DocumentSeries ds
JOIN CompanyFinancialTerm cft ON cft.DocumentSeriesId = ds.Id
JOIN StaticHierarchy sh on cft.ID = sh.CompanyFinancialTermID
JOIN TableType tt on sh.TableTypeID = tt.ID
JOIN(
	SELECT distinct dts.ID
	FROM DocumentSeries ds
	JOIN DocumentTimeSlice dts on ds.ID = Dts.DocumentSeriesId
	JOIN Document d on dts.DocumentId = d.ID
	JOIN DocumentTimeSliceTableCell dtstc on dts.ID = dtstc.DocumentTimeSliceID
	JOIN TableCell tc on dtstc.TableCellID = tc.ID
	JOIN DimensionToCell dtc on tc.ID = dtc.TableCellID -- check that is in a table
	JOIN StaticHierarchy sh on tc.CompanyFinancialTermID = sh.CompanyFinancialTermID
	JOIN TableType tt on tt.ID = sh.TableTypeID
	WHERE tc.ID = @cellId
	AND (d.ArdExportFlag = 1 OR d.ExportFlag = 1 OR d.IsDocSetupCompleted = 1)
) as ts on 1=1
JOIN DocumentTimeSlice dts on dts.ID = ts.ID
JOIN(
	SELECT tc.*, dtstc.DocumentTimeSliceID, sf.Value as ScalingFactorValue
	FROM DocumentSeries ds
	JOIN CompanyFinancialTerm cft ON cft.DocumentSeriesId = ds.Id
	JOIN StaticHierarchy sh on cft.ID = sh.CompanyFinancialTermID
	JOIN TableType tt on sh.TableTypeID = tt.ID
	JOIN TableCell tc on tc.CompanyFinancialTermID = cft.ID
	JOIN DocumentTimeSliceTableCell dtstc on dtstc.TableCellID = tc.ID
	JOIN ScalingFactor sf on sf.ID = tc.ScalingFactorID
	WHERE tc.ID = @cellId
) as tc ON tc.DocumentTimeSliceID = ts.ID AND tc.CompanyFinancialTermID = cft.ID
JOIN Document d on dts.documentid = d.ID
WHERE 1=1
ORDER BY sh.AdjustedOrder asc, dts.TimeSlicePeriodEndDate desc, dts.Duration desc, dts.ReportingPeriodEndDate desc, d.PublicationDateTime desc

";//I hate this query, it is so bad


		public AsReportedTemplate GetTemplate(int iconum, string TemplateName, Guid DocumentId) {

			string query =
				@"
SELECT DISTINCT sh.*
FROM DocumentSeries ds
	JOIN CompanyFinancialTerm cft ON cft.DocumentSeriesId = ds.Id
	JOIN StaticHierarchy sh on cft.ID = sh.CompanyFinancialTermID
	JOIN TableType tt on sh.TableTypeID = tt.ID
WHERE ds.CompanyID = @iconum
AND tt.Description = @templateName
ORDER BY sh.AdjustedOrder asc";

			string CellsQuery =
				@"
SELECT DISTINCT tc.ID, tc.Offset, tc.CellPeriodType, tc.PeriodTypeID, tc.CellPeriodCount, tc.PeriodLength, tc.CellDay, 
				tc.CellMonth, tc.CellYear, tc.CellDate, tc.Value, tc.CompanyFinancialTermID, tc.ValueNumeric, tc.NormalizedNegativeIndicator, 
				tc.ScalingFactorID, tc.AsReportedScalingFactor, tc.Currency, tc.CurrencyCode, tc.Cusip, tc.ScarUpdated, tc.IsIncomePositive, 
tc.XBRLTag, 
--tc.UpdateStampUTC
null
, tc.DocumentId, tc.Label, tc.ScalingFactorValue,
				(select aetc.ARDErrorTypeId from ARDErrorTypeTableCell aetc (nolock) where tc.Id = aetc.TableCellId),
				(select metc.MTMWErrorTypeId from MTMWErrorTypeTableCell metc (nolock) where tc.Id = metc.TableCellId), 
				sh.AdjustedOrder, dts.Duration, dts.TimeSlicePeriodEndDate, dts.ReportingPeriodEndDate, d.PublicationDateTime
FROM DocumentSeries ds
JOIN CompanyFinancialTerm cft ON cft.DocumentSeriesId = ds.Id
JOIN StaticHierarchy sh on cft.ID = sh.CompanyFinancialTermID
JOIN TableType tt on sh.TableTypeID = tt.ID
JOIN(
	SELECT distinct dts.ID
	FROM DocumentSeries ds
	JOIN DocumentTimeSlice dts on ds.ID = Dts.DocumentSeriesId
	JOIN Document d on dts.DocumentId = d.ID
	JOIN DocumentTimeSliceTableCell dtstc on dts.ID = dtstc.DocumentTimeSliceID
	JOIN TableCell tc on dtstc.TableCellID = tc.ID
	JOIN DimensionToCell dtc on tc.ID = dtc.TableCellID -- check that is in a table
	JOIN StaticHierarchy sh on tc.CompanyFinancialTermID = sh.CompanyFinancialTermID
	JOIN TableType tt on tt.ID = sh.TableTypeID
	WHERE ds.CompanyID = @iconum
	AND tt.Description = @templateName
	AND (d.ID = @DocumentID OR d.ArdExportFlag = 1 OR d.ExportFlag = 1 OR d.IsDocSetupCompleted = 1)
) as ts on 1=1
JOIN DocumentTimeSlice dts on dts.ID = ts.ID
LEFT JOIN(
	SELECT tc.*, dtstc.DocumentTimeSliceID, sf.Value as ScalingFactorValue
	FROM DocumentSeries ds
	JOIN CompanyFinancialTerm cft ON cft.DocumentSeriesId = ds.Id
	JOIN StaticHierarchy sh on cft.ID = sh.CompanyFinancialTermID
	JOIN TableType tt on sh.TableTypeID = tt.ID
	JOIN TableCell tc on tc.CompanyFinancialTermID = cft.ID
	JOIN DocumentTimeSliceTableCell dtstc on dtstc.TableCellID = tc.ID
	JOIN ScalingFactor sf on sf.ID = tc.ScalingFactorID
	WHERE ds.CompanyID = @iconum
	AND tt.Description = @templateName
) as tc ON tc.DocumentTimeSliceID = ts.ID AND tc.CompanyFinancialTermID = cft.ID
JOIN Document d on dts.documentid = d.ID
WHERE ds.CompanyID = @iconum
AND tt.Description = @templateName
ORDER BY sh.AdjustedOrder asc, dts.TimeSlicePeriodEndDate desc, dts.Duration desc, dts.ReportingPeriodEndDate desc, d.PublicationDateTime desc

";//I hate this query, it is so bad

			string TimeSliceQuery =
				@"SELECT DISTINCT dts.*, d.PublicationDateTime
FROM DocumentSeries ds
	JOIN CompanyFinancialTerm cft ON cft.DocumentSeriesId = ds.Id
	JOIN StaticHierarchy sh on cft.ID = sh.CompanyFinancialTermID
	JOIN TableType tt on sh.TableTypeID = tt.ID
	JOIN TableCell tc on tc.CompanyFinancialTermID = cft.ID
	JOIN DimensionToCell dtc on tc.ID = dtc.TableCellID -- check that is in a table
	JOIN DocumentTimeSliceTableCell dtstc on tc.ID = dtstc.TableCellID
	JOIN DocumentTimeSlice dts on dtstc.DocumentTimeSliceID = dts.ID
	JOIN Document d on dts.DocumentId = d.ID
WHERE ds.CompanyID = @iconum
AND tt.Description = @templateName
AND (d.ID = @DocumentID OR d.ArdExportFlag = 1 OR d.ExportFlag = 1 OR d.IsDocSetupCompleted = 1)
ORDER BY dts.TimeSlicePeriodEndDate desc, dts.Duration desc, dts.ReportingPeriodEndDate desc, d.PublicationDateTime desc";


			Dictionary<Tuple<StaticHierarchy, TimeSlice>, TableCell> CellMap = new Dictionary<Tuple<StaticHierarchy, TimeSlice>, TableCell>();
			Dictionary<Tuple<DateTime, string>, List<int>> TimeSliceMap = new Dictionary<Tuple<DateTime, string>, List<int>>();//int is index into timeslices for fast lookup


			AsReportedTemplate temp = new AsReportedTemplate();

			temp.StaticHierarchies = new List<StaticHierarchy>();
			Dictionary<TableCell, Tuple<StaticHierarchy, int>> BlankCells = new Dictionary<TableCell, Tuple<StaticHierarchy, int>>();
			Dictionary<TableCell, Tuple<StaticHierarchy, int>> CellLookup = new Dictionary<TableCell, Tuple<StaticHierarchy, int>>();
			Dictionary<int, StaticHierarchy> SHLookup = new Dictionary<int, StaticHierarchy>();
			Dictionary<int, List<StaticHierarchy>> SHChildLookup = new Dictionary<int, List<StaticHierarchy>>();
			List<StaticHierarchy> StaticHierarchies = temp.StaticHierarchies;


			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {
				using (SqlCommand cmd = new SqlCommand(query, conn)) {
					conn.Open();
					cmd.Parameters.AddWithValue("@iconum", iconum);
					cmd.Parameters.AddWithValue("@templateName", TemplateName);
					using (SqlDataReader reader = cmd.ExecuteReader()) {
						while (reader.Read()) {
							StaticHierarchy document = new StaticHierarchy
							{
								Id = reader.GetInt32(0),
								CompanyFinancialTermId = reader.GetInt32(1),
								AdjustedOrder = reader.GetInt32(2),
								TableTypeId = reader.GetInt32(3),
								Description = reader.GetStringSafe(4),
								HierarchyTypeId = reader.GetStringSafe(5)[0],
								SeparatorFlag = reader.GetBoolean(6),
								StaticHierarchyMetaId = reader.GetInt32(7),
								UnitTypeId = reader.GetInt32(8),
								IsIncomePositive = reader.GetBoolean(9),
								ChildrenExpandDown = reader.GetBoolean(10),
								ParentID = reader.GetNullable<int>(11),
								Cells = new List<TableCell>()
							};
							StaticHierarchies.Add(document);
							SHLookup.Add(document.Id, document);
							SHChildLookup.Add(document.Id, new List<StaticHierarchy>());
							if (document.ParentID != null) {
								SHChildLookup[document.ParentID.Value].Add(document);
							}
						}
					}
				}

				using (SqlCommand cmd = new SqlCommand(CellsQuery, conn)) {
					cmd.Parameters.AddWithValue("@iconum", iconum);
					cmd.Parameters.AddWithValue("@templateName", TemplateName);
					cmd.Parameters.AddWithValue("@DocumentID", DocumentId);

					using (SqlDataReader reader = cmd.ExecuteReader()) {

						int shix = 0;

						int adjustedOrder = 0;
						while (reader.Read()) {
							TableCell cell;
							if (reader.GetNullable<int>(0).HasValue) {
								cell = new TableCell
								{
									ID = reader.GetInt32(0),
									Offset = reader.GetStringSafe(1),
									CellPeriodType = reader.GetStringSafe(2),
									PeriodTypeID = reader.GetStringSafe(3),
									CellPeriodCount = reader.GetStringSafe(4),
									PeriodLength = reader.GetNullable<int>(5),
									CellDay = reader.GetStringSafe(6),
									CellMonth = reader.GetStringSafe(7),
									CellYear = reader.GetStringSafe(8),
									CellDate = reader.GetNullable<DateTime>(9),
									Value = reader.GetStringSafe(10),
									CompanyFinancialTermID = reader.GetNullable<int>(11),
									ValueNumeric = reader.GetNullable<decimal>(12),
									NormalizedNegativeIndicator = reader.GetBoolean(13),
									ScalingFactorID = reader.GetStringSafe(14),
									AsReportedScalingFactor = reader.GetStringSafe(15),
									Currency = reader.GetStringSafe(16),
									CurrencyCode = reader.GetStringSafe(17),
									Cusip = reader.GetStringSafe(18),
									ScarUpdated = reader.GetBoolean(19),
									IsIncomePositive = reader.GetBoolean(20),
									XBRLTag = reader.GetStringSafe(21),
									UpdateStampUTC = reader.GetNullable<DateTime>(22),
									DocumentID = reader.GetGuid(23),
									Label = reader.GetStringSafe(24),
									ScalingFactorValue = reader.GetDouble(25),
									ARDErrorTypeId = reader.GetNullable<int>(26),
									MTMWErrorTypeId = reader.GetNullable<int>(27)
								};

								adjustedOrder = reader.GetInt32(28);
							} else {
								cell = new TableCell();
								adjustedOrder = reader.GetInt32(28);
							}

							while (adjustedOrder != StaticHierarchies[shix].AdjustedOrder) {
								shix++;
							}

							if (cell.ID == 0) {
								BlankCells.Add(cell, new Tuple<StaticHierarchy, int>(StaticHierarchies[shix], StaticHierarchies[shix].Cells.Count));
							}
							CellLookup.Add(cell, new Tuple<StaticHierarchy, int>(StaticHierarchies[shix], StaticHierarchies[shix].Cells.Count));

							if (cell.ID == 0 || cell.CompanyFinancialTermID == StaticHierarchies[shix].CompanyFinancialTermId) {
								StaticHierarchies[shix].Cells.Add(cell);
							} else {
								throw new Exception();
							}

						}
					}
				}


				temp.TimeSlices = new List<TimeSlice>();
				List<TimeSlice> TimeSlices = temp.TimeSlices;
				using (SqlCommand cmd = new SqlCommand(TimeSliceQuery, conn)) {
					cmd.Parameters.AddWithValue("@iconum", iconum);
					cmd.Parameters.AddWithValue("@templateName", TemplateName);
					cmd.Parameters.AddWithValue("@DocumentID", DocumentId);

					using (SqlDataReader reader = cmd.ExecuteReader()) {

						while (reader.Read()) {
							TimeSlice slice = new TimeSlice
							{
								Id = reader.GetInt32(0),
								DocumentId = reader.GetGuid(1),
								DocumentSeriesId = reader.GetInt32(2),
								TimeSlicePeriodEndDate = reader.GetDateTime(3),
								ReportingPeriodEndDate = reader.GetDateTime(4),
								FiscalDistance = reader.GetInt32(5),
								Duration = reader.GetInt32(6),
								PeriodType = reader.GetStringSafe(7),
								AcquisitionFlag = reader.GetStringSafe(8),
								AccountingStandard = reader.GetStringSafe(9),
								ConsolidatedFlag = reader.GetStringSafe(10),
								IsProForma = reader.GetBoolean(11),
								IsRecap = reader.GetBoolean(12),
								CompanyFiscalYear = reader.GetDecimal(13),
								ReportType = reader.GetStringSafe(14),
								IsAmended = reader.GetBoolean(15),
								IsRestated = reader.GetBoolean(16),
								IsAutoCalc = reader.GetBoolean(17),
								ManualOrgSet = reader.GetBoolean(18),
								PublicationDate = reader.GetDateTime(19)
							};

							TimeSlices.Add(slice);

							Tuple<DateTime, string> tup = new Tuple<DateTime, string>(slice.TimeSlicePeriodEndDate, slice.PeriodType);//TODO: Is this sufficient for Like Period?
							if (!TimeSliceMap.ContainsKey(tup)) {
								TimeSliceMap.Add(tup, new List<int>());
							}

							TimeSliceMap[tup].Add(TimeSlices.Count - 1);

							foreach (StaticHierarchy sh in temp.StaticHierarchies) {
								CellMap.Add(new Tuple<StaticHierarchy, TimeSlice>(sh, slice), sh.Cells[TimeSlices.Count - 1]);
							}

						}
					}
				}

			}

			//foreach (Tuple<StaticHierarchy, int, TableCell> tup in BlankCells) {
			//	StaticHierarchy sh = tup.Item1;
			//	int cellIndex = tup.Item2;
			//	TableCell tc = tup.Item3;


			//}

			foreach (StaticHierarchy sh in StaticHierarchies) {//Finds likeperiod validation failures. Currently failing with virtual cells

				if (!sh.ParentID.HasValue) {
					sh.Level = 0;
				}
				foreach (StaticHierarchy ch in SHChildLookup[sh.Id]) {
					ch.Level = sh.Level + 1;
				}

				for (int i = 0; i < sh.Cells.Count; i++) {
					TimeSlice ts = temp.TimeSlices[i];

					TableCell tc = sh.Cells[i];
					List<int> matches = TimeSliceMap[new Tuple<DateTime, string>(ts.TimeSlicePeriodEndDate, ts.PeriodType)];
					foreach (int j in matches) {
						if (sh.Cells[j] == tc)
							continue;

						bool whatever = false;

						decimal matchValue = CalculateCellValue(sh.Cells[j], BlankCells, SHChildLookup, ref whatever);
						decimal cellValue = CalculateCellValue(tc, BlankCells, SHChildLookup, ref whatever);
						bool anyValidationPasses = matches.Any(t => sh.Cells[t].ARDErrorTypeId.HasValue);

						if (matchValue != cellValue &&//TODO: remove double checks
							!((ts.PublicationDate > temp.TimeSlices[j].PublicationDate && cellValue == 0) || (temp.TimeSlices[j].PublicationDate > ts.PublicationDate && matchValue == 0)) &&
							!anyValidationPasses &&
							tc.ValueNumeric.HasValue
							) {
							tc.LikePeriodValidationFlag = true;
						}
					}

					bool hasChildren = false;
					bool whatever2 = false;

					tc.MTMWValidationFlag = SHChildLookup[sh.Id].Count > 0 && (CalculateCellValue(tc, BlankCells, SHChildLookup, ref whatever2) != CalculateChildSum(tc, CellLookup, SHChildLookup, ref hasChildren)) && !tc.MTMWErrorTypeId.HasValue && hasChildren;
				}
			}

			return temp;
		}

		private decimal CalculateCellValue(TableCell cell, Dictionary<TableCell, Tuple<StaticHierarchy, int>> BlankCells, Dictionary<int, List<StaticHierarchy>> SHChildLookup, ref bool hasChildren) {
			if (cell.ValueNumeric.HasValue) {
				hasChildren = true;
				return cell.ValueNumeric.Value * (cell.IsIncomePositive ? 1 : -1) * (decimal)cell.ScalingFactorValue;
			} else if (cell.VirtualValueNumeric.HasValue) {
				hasChildren = true;
				return cell.VirtualValueNumeric.Value;
			} else {
				if (BlankCells.ContainsKey(cell)) {
					decimal sum = 0;
					StaticHierarchy sh = BlankCells[cell].Item1;
					int timesliceIndex = BlankCells[cell].Item2;

					foreach (StaticHierarchy child in SHChildLookup[sh.Id]) {
						sum += CalculateCellValue(child.Cells[timesliceIndex], BlankCells, SHChildLookup, ref hasChildren);
					}
					if (SHChildLookup[sh.Id].Count > 0) {
						if (!cell.ValueNumeric.HasValue)
							cell.VirtualValueNumeric = sum;

						return sum;
					}
				}
			}
			return 0;
		}

		private decimal CalculateChildSum(TableCell cell, Dictionary<TableCell, Tuple<StaticHierarchy, int>> CellLookup, Dictionary<int, List<StaticHierarchy>> SHChildLookup, ref bool hasChildren) {
			if (CellLookup.ContainsKey(cell)) {
				decimal sum = 0;
				StaticHierarchy sh = CellLookup[cell].Item1;
				int timesliceIndex = CellLookup[cell].Item2;

				foreach (StaticHierarchy child in SHChildLookup[sh.Id]) {
					sum += CalculateCellValue(child.Cells[timesliceIndex], CellLookup, SHChildLookup, ref hasChildren);
				}
				if (SHChildLookup[sh.Id].Count > 0) {
					if (!cell.ValueNumeric.HasValue)
						cell.VirtualValueNumeric = sum;

					return sum;
				}
			}
			return 0;
		}

		public AsReportedTemplateSkeleton GetTemplateSkeleton(int iconum, string TemplateName) {

			string query =
				@"
SELECT DISTINCT sh.ID, sh.AdjustedOrder
FROM DocumentSeries ds
	JOIN CompanyFinancialTerm cft ON cft.DocumentSeriesId = ds.Id
	JOIN StaticHierarchy sh on cft.ID = sh.CompanyFinancialTermID
	JOIN TableType tt on sh.TableTypeID = tt.ID
WHERE ds.CompanyID = @iconum
AND tt.Description = @templateName
ORDER BY sh.AdjustedOrder asc";

			string TimeSliceQuery =
				@"SELECT DISTINCT dts.ID, sh.AdjustedOrder, dts.Duration, dts.TimeSlicePeriodEndDate, dts.ReportingPeriodEndDate
FROM DocumentSeries ds
	JOIN CompanyFinancialTerm cft ON cft.DocumentSeriesId = ds.Id
	JOIN StaticHierarchy sh on cft.ID = sh.CompanyFinancialTermID
	JOIN TableType tt on sh.TableTypeID = tt.ID
	JOIN TableCell tc on tc.CompanyFinancialTermID = cft.ID
	JOIN DocumentTimeSliceTableCell dtstc on tc.ID = dtstc.TableCellID
	JOIN DocumentTimeSlice dts on dtstc.DocumentTimeSliceID = dts.ID
WHERE ds.CompanyID = @iconum
AND tt.Description = @templateName
ORDER BY sh.AdjustedOrder asc, dts.Duration asc, dts.TimeSlicePeriodEndDate desc, dts.ReportingPeriodEndDate desc";


			AsReportedTemplateSkeleton temp = new AsReportedTemplateSkeleton();

			temp.StaticHierarchies = new List<int>();
			List<int> StaticHierarchies = temp.StaticHierarchies;
			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {
				using (SqlCommand cmd = new SqlCommand(query, conn)) {
					conn.Open();
					cmd.Parameters.AddWithValue("@iconum", iconum);
					cmd.Parameters.AddWithValue("@templateName", TemplateName);
					using (SqlDataReader reader = cmd.ExecuteReader()) {
						while (reader.Read()) {
							StaticHierarchies.Add(reader.GetInt32(0));
						}
					}
				}


				temp.TimeSlices = new List<int>();
				List<int> TimeSlices = temp.TimeSlices;
				using (SqlCommand cmd = new SqlCommand(TimeSliceQuery, conn)) {
					cmd.Parameters.AddWithValue("@iconum", iconum);
					cmd.Parameters.AddWithValue("@templateName", TemplateName);

					using (SqlDataReader reader = cmd.ExecuteReader()) {

						while (reader.Read()) {
							TimeSlices.Add(reader.GetInt32(0));
						}
					}
				}

			}
			return temp;
		}

		public StaticHierarchy GetStaticHierarchy(int id) {

			string query = @"SELECT * FROM StaticHierarchy WHERE ID = @id";

			string CellsQuery =
	@"SELECT DISTINCT tc.*,
		(select aetc.ARDErrorTypeId from ARDErrorTypeTableCell aetc (nolock) where tc.Id = aetc.TableCellId),
		(select metc.MTMWErrorTypeId from MTMWErrorTypeTableCell metc (nolock) where tc.Id = metc.TableCellId), 
sh.AdjustedOrder, dts.Duration, dts.TimeSlicePeriodEndDate, dts.ReportingPeriodEndDate
FROM DocumentSeries ds
	JOIN CompanyFinancialTerm cft ON cft.DocumentSeriesId = ds.Id
	JOIN StaticHierarchy sh on cft.ID = sh.CompanyFinancialTermID
	JOIN TableType tt on sh.TableTypeID = tt.ID
	JOIN TableCell tc on tc.CompanyFinancialTermID = cft.ID
	JOIN DocumentTimeSliceTableCell dtstc on tc.ID = dtstc.TableCellID
	JOIN DocumentTimeSlice dts on dtstc.DocumentTimeSliceID = dts.ID

WHERE sh.id = @id
ORDER BY sh.AdjustedOrder asc, dts.Duration asc, dts.TimeSlicePeriodEndDate desc, dts.ReportingPeriodEndDate desc";

			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {
				StaticHierarchy sh;

				using (SqlCommand cmd = new SqlCommand(query, conn)) {
					conn.Open();
					cmd.Parameters.AddWithValue("@id", id);
					using (SqlDataReader reader = cmd.ExecuteReader()) {
						reader.Read();
						sh = new StaticHierarchy
						{
							Id = reader.GetInt32(0),
							CompanyFinancialTermId = reader.GetInt32(1),
							AdjustedOrder = reader.GetInt32(2),
							TableTypeId = reader.GetInt32(3),
							Description = reader.GetStringSafe(4),
							HierarchyTypeId = reader.GetStringSafe(5)[0],
							SeparatorFlag = reader.GetBoolean(6),
							StaticHierarchyMetaId = reader.GetInt32(7),
							UnitTypeId = reader.GetInt32(8),
							IsIncomePositive = reader.GetBoolean(9),
							ChildrenExpandDown = reader.GetBoolean(10),
							Cells = new List<TableCell>()
						};
					}
				}

				using (SqlCommand cmd = new SqlCommand(CellsQuery, conn)) {
					cmd.Parameters.AddWithValue("@id", id);

					using (SqlDataReader reader = cmd.ExecuteReader()) {

						while (reader.Read()) {
							TableCell cell = new TableCell
							{
								ID = reader.GetInt32(0),
								Offset = reader.GetStringSafe(1),
								CellPeriodType = reader.GetStringSafe(2),
								PeriodTypeID = reader.GetStringSafe(3),
								CellPeriodCount = reader.GetStringSafe(4),
								PeriodLength = reader.GetNullable<int>(5),
								CellDay = reader.GetStringSafe(6),
								CellMonth = reader.GetStringSafe(7),
								CellYear = reader.GetStringSafe(8),
								CellDate = reader.GetNullable<DateTime>(9),
								Value = reader.GetStringSafe(10),
								CompanyFinancialTermID = reader.GetNullable<int>(11),
								ValueNumeric = reader.GetNullable<decimal>(12),
								NormalizedNegativeIndicator = reader.GetBoolean(13),
								ScalingFactorID = reader.GetStringSafe(14),
								AsReportedScalingFactor = reader.GetStringSafe(15),
								Currency = reader.GetStringSafe(16),
								CurrencyCode = reader.GetStringSafe(17),
								Cusip = reader.GetStringSafe(18),
								ScarUpdated = reader.GetBoolean(19),
								IsIncomePositive = reader.GetBoolean(20),
								XBRLTag = reader.GetStringSafe(21),
								UpdateStampUTC = reader.GetNullable<DateTime>(22),
								DocumentID = reader.GetGuid(23),
								Label = reader.GetStringSafe(24),
								ARDErrorTypeId = reader.GetNullable<int>(25),
								MTMWErrorTypeId = reader.GetNullable<int>(26)
							};

							sh.Cells.Add(cell);

						}
					}
				}

				return sh;
			}
		}

		public TimeSlice GetTimeSlice(int id) {

			string query = @"SELECT * FROM DocumentTimeSlice WHERE ID = @id";

			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {
				StaticHierarchy sh;

				using (SqlCommand cmd = new SqlCommand(query, conn)) {
					conn.Open();
					cmd.Parameters.AddWithValue("@id", id);
					using (SqlDataReader reader = cmd.ExecuteReader()) {
						reader.Read();
						TimeSlice slice = new TimeSlice
						{
							Id = reader.GetInt32(0),
							DocumentId = reader.GetGuid(1),
							DocumentSeriesId = reader.GetInt32(2),
							TimeSlicePeriodEndDate = reader.GetDateTime(3),
							ReportingPeriodEndDate = reader.GetDateTime(4),
							FiscalDistance = reader.GetInt32(5),
							Duration = reader.GetInt32(6),
							PeriodType = reader.GetStringSafe(7),
							AcquisitionFlag = reader.GetStringSafe(8),
							AccountingStandard = reader.GetStringSafe(9),
							ConsolidatedFlag = reader.GetStringSafe(10),
							IsProForma = reader.GetBoolean(11),
							IsRecap = reader.GetBoolean(12),
							CompanyFiscalYear = reader.GetDecimal(13),
							ReportType = reader.GetStringSafe(14),
							IsAmended = reader.GetBoolean(15),
							IsRestated = reader.GetBoolean(16),
							IsAutoCalc = reader.GetBoolean(17),
							ManualOrgSet = reader.GetBoolean(18)
						};
						return slice;
					}
				}
			}
		}

		private bool setIsIncomePositive(string CellId, bool isIncomePositive) {
			const string SQL_SetIncomePositive = @"UPDATE TableCell 
																set IsIncomePositive = @newValue
																WHERE ID = @id";

			using (SqlConnection sqlConn = new SqlConnection(_sfConnectionString))
			using (SqlCommand cmd = new SqlCommand(SQL_SetIncomePositive, sqlConn)) {
				cmd.Parameters.AddWithValue("@id", CellId);
				cmd.Parameters.AddWithValue("@newValue", isIncomePositive);
				sqlConn.Open();
				cmd.ExecuteNonQuery();
			}

			return true;
		}
		private TableCell[] getSibilingsCells(string CellId, Guid DocumentId) {
            		string SQL_GetSibilingCellsQuery =
                                @"
SELECT *
FROM vw_SCARDocumentTimeSliceTableCell tc
JOIN DocumentTimeSlice dts ON tc.DocumentTimeSliceID = dts.ID
LEFT JOIN DocumentTimeSlice dtsSib ON dts.TimeSlicePeriodEndDate = dtsSib.TimeSlicePeriodEndDate 
                                                                     AND dts.PeriodType = dtsSib.PeriodType 
                                                                     AND dts.DocumentSeriesId = dtsSib.DocumentSeriesId
                                                                     AND dts.ID <> dtsSib.ID
LEFT JOIN Document d on dtsSib.DocumentID = d.ID
LEFT JOIN vw_SCARDocumentTimeSliceTableCell tcSib ON tcSib.DocumentTimeSliceID = dtsSib.ID AND tc.CompanyFinancialTermID = tcSib.CompanyFinancialTermID
WHERE tc.TableCellID = @TCID
AND (d.ID = @DocumentID OR d.ArdExportFlag = 1 OR d.ExportFlag = 1 OR d.IsDocSetupCompleted = 1)
AND not tcSib.TableCellID is null

"; 

            System.Collections.ArrayList tableCells = new System.Collections.ArrayList();

            using (SqlConnection conn = new SqlConnection(_sfConnectionString))
            {
                using (SqlCommand cmd = new SqlCommand(SQL_GetSibilingCellsQuery, conn))
                {
                    conn.Open();
                    //cmd.Parameters.AddWithValue("@DocumentID ", new Guid(@"E6059509-1F34-DE11-9566-0019BB2A8F9C"));
                    cmd.Parameters.AddWithValue("@DocumentID ", DocumentId);
                    cmd.Parameters.AddWithValue("@TCID", CellId);

                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {

                        int shix = 0;

                        int adjustedOrder = 0;
                        while (reader.Read())
                        {
                            TableCell cell;
                            if (reader.GetNullable<int>(0).HasValue)
                            {
                                cell = new TableCell
                                {
                                    ID = reader.GetInt32(1),
                                    Offset = reader.GetStringSafe(2),
                                    CellPeriodType = reader.GetStringSafe(3),
                                    PeriodTypeID = reader.GetStringSafe(4),
                                    CellPeriodCount = reader.GetStringSafe(5),
                                    PeriodLength = reader.GetNullable<int>(6),
                                    CellDay = reader.GetStringSafe(7),
                                    CellMonth = reader.GetStringSafe(8),
                                    CellYear = reader.GetStringSafe(9),
                                    CellDate = reader.GetNullable<DateTime>(10),
                                    Value = reader.GetStringSafe(11),
                                    CompanyFinancialTermID = reader.GetNullable<int>(12),
                                    ValueNumeric = reader.GetNullable<decimal>(13),
                                    NormalizedNegativeIndicator = reader.GetBoolean(14),
                                    ScalingFactorID = reader.GetStringSafe(15),
                                    AsReportedScalingFactor = reader.GetStringSafe(16),
                                    Currency = reader.GetStringSafe(17),
                                    CurrencyCode = reader.GetStringSafe(18),
                                    Cusip = reader.GetStringSafe(19)
                                    //ScarUpdated = reader.GetBoolean(20),
                                    //IsIncomePositive = reader.GetBoolean(21),
                                    //XBRLTag = reader.GetStringSafe(22),
                                    //UpdateStampUTC = reader.GetNullable<DateTime>(23),
                                    //DocumentID = reader.IsDBNull(24) ? Guid.Empty : reader.GetGuid(24),
                                    //Label = reader.GetStringSafe(25),
                                    //ScalingFactorValue = reader.GetDouble(26),
                                    //ARDErrorTypeId = reader.GetNullable<int>(1),
                                    //MTMWErrorTypeId = reader.GetNullable<int>(1)
                                };
                                cell.ScarUpdated = reader.GetBoolean(20);
                                cell.IsIncomePositive = reader.GetBoolean(21);
                                cell.XBRLTag = reader.GetStringSafe(22);
                                //cell.UpdateStampUTC = reader.GetNullable<DateTime>(23);
                                cell.DocumentID = reader.IsDBNull(23) ? Guid.Empty : reader.GetGuid(23);
                                cell.Label = reader.GetStringSafe(24);
                                //cell.ScalingFactorValue = reader.GetDouble(25);
                                //cell.ARDErrorTypeId = reader.GetNullable<int>(1);
                                //cell.MTMWErrorTypeId = reader.GetNullable<int>(1);
                                adjustedOrder++;// = reader.GetInt32(1);
                            }
                            else
                            {
                                cell = new TableCell();
                                adjustedOrder = reader.GetInt32(28);
                            }
                            tableCells.Add(cell);
                        }
                    }
                }
            }
            return (TableCell[])tableCells.ToArray(typeof(TableCell));
		}
		public TableCell FlipSign(string CellId, Guid DocumentId) {
			TableCell currCell = GetCell(CellId);
			if (setIsIncomePositive(CellId, !currCell.IsIncomePositive)) {
				currCell.IsIncomePositive = !currCell.IsIncomePositive;
                TableCell[] sibilings = getSibilingsCells(CellId, DocumentId);
				return currCell;
			} else {
				return new TableCell();
			}
		}
		public TableCell GetCell(string CellId) {
			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {
				using (SqlCommand cmd = new SqlCommand(SQL_GetCellQuery, conn)) {
                    conn.Open();
					cmd.Parameters.AddWithValue("@cellId", CellId);

					using (SqlDataReader reader = cmd.ExecuteReader()) {

						int shix = 0;

						int adjustedOrder = 0;
						while (reader.Read()) {
							TableCell cell;
							if (reader.GetNullable<int>(0).HasValue) {
								cell = new TableCell
								{
									ID = reader.GetInt32(0),
									Offset = reader.GetStringSafe(1),
									CellPeriodType = reader.GetStringSafe(2),
									PeriodTypeID = reader.GetStringSafe(3),
									CellPeriodCount = reader.GetStringSafe(4),
									PeriodLength = reader.GetNullable<int>(5),
									CellDay = reader.GetStringSafe(6),
									CellMonth = reader.GetStringSafe(7),
									CellYear = reader.GetStringSafe(8),
									CellDate = reader.GetNullable<DateTime>(9),
									Value = reader.GetStringSafe(10),
									CompanyFinancialTermID = reader.GetNullable<int>(11),
									ValueNumeric = reader.GetNullable<decimal>(12),
									NormalizedNegativeIndicator = reader.GetBoolean(13),
									ScalingFactorID = reader.GetStringSafe(14),
									AsReportedScalingFactor = reader.GetStringSafe(15),
									Currency = reader.GetStringSafe(16),
									CurrencyCode = reader.GetStringSafe(17),
									Cusip = reader.GetStringSafe(18),
									ScarUpdated = reader.GetBoolean(19),
									IsIncomePositive = reader.GetBoolean(20),
									XBRLTag = reader.GetStringSafe(21),
									UpdateStampUTC = reader.GetNullable<DateTime>(22),
									DocumentID = reader.GetGuid(23),
									Label = reader.GetStringSafe(24),
									ScalingFactorValue = reader.GetDouble(25),
									ARDErrorTypeId = reader.GetNullable<int>(26),
									MTMWErrorTypeId = reader.GetNullable<int>(27)
								};

								adjustedOrder = reader.GetInt32(28);
							} else {
								cell = new TableCell();
								adjustedOrder = reader.GetInt32(28);
							}
							return cell;
						}
					}
				}
			}
			return new TableCell();
		}

		public StitchResult StitchStaticHierarchies(int TargetStaticHierarchyID, Guid DocumentID, List<int> StitchingStaticHierarchyIDs, int iconum) {
			string query = @"SCARStitchRows";

			DataTable dt = new DataTable();
			dt.Columns.Add("StaticHierarchyID", typeof(Int32));
			foreach (int i in StitchingStaticHierarchyIDs) {
				dt.Rows.Add(i);
			}

			StitchResult res = new StitchResult()
			{
				CellToDTS = new Dictionary<TableCell, int>(),
				StaticHierarchyAdjustedOrders = new List<StaticHierarchyAdjustedOrder>(),
				DTSToMTMWComponent = new Dictionary<int, List<CellMTMWComponent>>()
			};

			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {
				conn.Open();
				using (SqlCommand cmd = new SqlCommand(query, conn)) {
					cmd.CommandType = System.Data.CommandType.StoredProcedure;
					cmd.Parameters.AddWithValue("@TargetSH", TargetStaticHierarchyID);
					cmd.Parameters.AddWithValue("@DocumentID", DocumentID);
					cmd.Parameters.AddWithValue("@StaticHierarchyList", dt);
					cmd.Parameters.AddWithValue("@Iconum", iconum);

					using (SqlDataReader sdr = cmd.ExecuteReader()) {
						res.StaticHierarchyAdjustedOrders = sdr.Cast<IDataRecord>().Select(r => new StaticHierarchyAdjustedOrder() { StaticHierarchyID = r.GetInt32(0), NewAdjustedOrder = r.GetInt32(1) }).ToList();
						sdr.NextResult();

						res.ParentCellChangeComponents = sdr.Cast<IDataRecord>().Select(r => new CellMTMWComponent()
						{
							StaticHierarchyID = r.GetInt32(0),
							DocumentTimeSliceID = r.GetInt32(1),
							TableCellID = r.GetInt32(2),
							ValueNumeric = r.GetDecimal(3),
							IsIncomePositive = r.GetBoolean(4),
							ScalingFactorValue = r.GetDouble(5),
							RootStaticHierarchyID = r.GetInt32(6),
							RootDocumentTimeSliceID = r.GetInt32(7)
						}
						).ToList();

						sdr.NextResult();
						List<CellMTMWComponent> comps = sdr.Cast<IDataRecord>().Select(r => new CellMTMWComponent()
						{
							StaticHierarchyID = r.GetInt32(0),
							DocumentTimeSliceID = r.GetInt32(1),
							TableCellID = r.GetInt32(2),
							ValueNumeric = r.GetDecimal(3),
							IsIncomePositive = r.GetBoolean(4),
							ScalingFactorValue = r.GetDouble(5),
							RootStaticHierarchyID = r.GetInt32(6),
							RootDocumentTimeSliceID = r.GetInt32(7)
						}
							).ToList();
						foreach (CellMTMWComponent comp in comps) {
							if (!res.DTSToMTMWComponent.ContainsKey(comp.DocumentTimeSliceID))
								res.DTSToMTMWComponent.Add(comp.DocumentTimeSliceID, new List<CellMTMWComponent>());
							res.DTSToMTMWComponent[comp.DocumentTimeSliceID].Add(comp);
						}
						sdr.NextResult();
						sdr.Read();
						int level = sdr.GetInt32(0);
						sdr.NextResult();
						sdr.Read();
						StaticHierarchy document = new StaticHierarchy
						{
							Id = sdr.GetInt32(0),
							CompanyFinancialTermId = sdr.GetInt32(1),
							AdjustedOrder = sdr.GetInt32(2),
							TableTypeId = sdr.GetInt32(3),
							Description = sdr.GetStringSafe(4),
							HierarchyTypeId = sdr.GetStringSafe(5)[0],
							SeparatorFlag = sdr.GetBoolean(6),
							StaticHierarchyMetaId = sdr.GetInt32(7),
							UnitTypeId = sdr.GetInt32(8),
							IsIncomePositive = sdr.GetBoolean(9),
							ChildrenExpandDown = sdr.GetBoolean(10),
							ParentID = sdr.GetNullable<int>(11),
							Cells = new List<TableCell>(),
							Level = level
						};
						res.StaticHierarchy = document;
						sdr.NextResult();
						while (sdr.Read()) {
							TableCell cell;
							if (sdr.GetNullable<int>(0).HasValue) {
								cell = new TableCell
								{
									ID = sdr.GetInt32(0),
									Offset = sdr.GetStringSafe(1),
									CellPeriodType = sdr.GetStringSafe(2),
									PeriodTypeID = sdr.GetStringSafe(3),
									CellPeriodCount = sdr.GetStringSafe(4),
									PeriodLength = sdr.GetNullable<int>(5),
									CellDay = sdr.GetStringSafe(6),
									CellMonth = sdr.GetStringSafe(7),
									CellYear = sdr.GetStringSafe(8),
									CellDate = sdr.GetNullable<DateTime>(9),
									Value = sdr.GetStringSafe(10),
									CompanyFinancialTermID = sdr.GetNullable<int>(11),
									ValueNumeric = sdr.GetNullable<decimal>(12),
									NormalizedNegativeIndicator = sdr.GetBoolean(13),
									ScalingFactorID = sdr.GetStringSafe(14),
									AsReportedScalingFactor = sdr.GetStringSafe(15),
									Currency = sdr.GetStringSafe(16),
									CurrencyCode = sdr.GetStringSafe(17),
									Cusip = sdr.GetStringSafe(18),
									ScarUpdated = sdr.GetBoolean(19),
									IsIncomePositive = sdr.GetBoolean(20),
									XBRLTag = sdr.GetStringSafe(21),
									UpdateStampUTC = sdr.GetNullable<DateTime>(22),
									DocumentID = sdr.GetGuid(23),
									Label = sdr.GetStringSafe(24),
									ScalingFactorValue = sdr.GetDouble(25),
									ARDErrorTypeId = sdr.GetNullable<int>(26),
									MTMWErrorTypeId = sdr.GetNullable<int>(27),
									LikePeriodValidationFlag = sdr.GetBoolean(28)
								};
							} else {
								cell = new TableCell();
							}
							document.Cells.Add(cell);

							res.CellToDTS.Add(cell, sdr.GetInt32(29));
						}
					}
				}
			}

			foreach (TableCell cell in res.StaticHierarchy.Cells) {
				decimal value = cell.ValueNumeric.Value * (cell.IsIncomePositive ? 1 : -1) * (decimal)cell.ScalingFactorValue;
				decimal sum = 0;
				bool any = false;
				foreach (CellMTMWComponent c in res.DTSToMTMWComponent[res.CellToDTS[cell]]) {
					if (c.StaticHierarchyID != res.StaticHierarchy.Id) {
						any = true;
						sum += c.ValueNumeric * ((decimal)(c.IsIncomePositive ? 1 : -1)) * ((decimal)c.ScalingFactorValue);
					}
				}
				cell.MTMWValidationFlag = value != sum && any;
			}

			//TODO: Optimize
			Dictionary<int, Dictionary<int, bool>> ParentMTMW = new Dictionary<int, Dictionary<int, bool>>();
			foreach (CellMTMWComponent comp in res.ParentCellChangeComponents.Where(c => c.RootDocumentTimeSliceID == c.DocumentTimeSliceID && c.RootStaticHierarchyID == c.StaticHierarchyID)) {
				if (!ParentMTMW.ContainsKey(comp.StaticHierarchyID)) {
					ParentMTMW.Add(comp.StaticHierarchyID, new Dictionary<int, bool>());
				}

				decimal val = comp.ValueNumeric * (comp.IsIncomePositive ? 1 : -1) * (decimal)comp.ScalingFactorValue;
				bool any = false;
				decimal sum = 0;
				foreach (CellMTMWComponent subComp in res.ParentCellChangeComponents.Where(sc => sc.RootStaticHierarchyID == comp.RootStaticHierarchyID && sc.RootDocumentTimeSliceID == comp.RootDocumentTimeSliceID && !sc.Equals(comp))) {
					if (!any) any = true;

					sum += subComp.ValueNumeric * (subComp.IsIncomePositive ? 1 : -1) * (decimal)subComp.ScalingFactorValue;
				}

				ParentMTMW[comp.StaticHierarchyID].Add(comp.DocumentTimeSliceID, any && val != sum);
			}
			res.ParentMTMWChanges = ParentMTMW;

			return res;
		}

		public UnStitchResult UnstitchStaticHierarchy(int StaticHierarchyID, Guid DocumentID, int Iconum) {

			UnStitchResult res = new UnStitchResult()
			{
				StaticHierarchyAdjustedOrders = new List<StaticHierarchyAdjustedOrder>(),
				StaticHierarchies = new List<StaticHierarchy>()
			};

			Dictionary<Tuple<int, int>, TableCell> CellMap = new Dictionary<Tuple<int, int>, TableCell>();
			List<CellMTMWComponent> CellChangeComponents;
			Dictionary<int, int> SHLevels;


			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {
				conn.Open();
				using (SqlCommand cmd = new SqlCommand("SCARUnStitchRows", conn)) {
					cmd.CommandType = System.Data.CommandType.StoredProcedure;
					cmd.Parameters.AddWithValue("@TargetSH", StaticHierarchyID);
					cmd.Parameters.AddWithValue("@DocumentID", DocumentID);
					cmd.Parameters.AddWithValue("@Iconum", Iconum);


					using (SqlDataReader sdr = cmd.ExecuteReader()) {
						res.StaticHierarchyAdjustedOrders = sdr.Cast<IDataRecord>().Select(r => new StaticHierarchyAdjustedOrder() { StaticHierarchyID = r.GetInt32(0), NewAdjustedOrder = r.GetInt32(1) }).ToList();
						sdr.NextResult();

						SHLevels = sdr.Cast<IDataRecord>().Select(r => new Tuple<int, int>(r.GetInt32(0), r.GetInt32(1))).ToDictionary(k => k.Item1, v => v.Item2);
						
						sdr.NextResult();

						while (sdr.Read()) {
							StaticHierarchy document = new StaticHierarchy
							{
								Id = sdr.GetInt32(0),
								CompanyFinancialTermId = sdr.GetInt32(1),
								AdjustedOrder = sdr.GetInt32(2),
								TableTypeId = sdr.GetInt32(3),
								Description = sdr.GetStringSafe(4),
								HierarchyTypeId = sdr.GetStringSafe(5)[0],
								SeparatorFlag = sdr.GetBoolean(6),
								StaticHierarchyMetaId = sdr.GetInt32(7),
								UnitTypeId = sdr.GetInt32(8),
								IsIncomePositive = sdr.GetBoolean(9),
								ChildrenExpandDown = sdr.GetBoolean(10),
								ParentID = sdr.GetNullable<int>(11),
								Cells = new List<TableCell>()
							};
							res.StaticHierarchies.Add(document);
						}

						sdr.NextResult();

						int shix = 0;
						int adjustedOrder = 0;

						while (sdr.Read()) {
							TableCell cell;
							if (sdr.GetNullable<int>(0).HasValue) {
								cell = new TableCell
								{
									ID = sdr.GetInt32(0),
									Offset = sdr.GetStringSafe(1),
									CellPeriodType = sdr.GetStringSafe(2),
									PeriodTypeID = sdr.GetStringSafe(3),
									CellPeriodCount = sdr.GetStringSafe(4),
									PeriodLength = sdr.GetNullable<int>(5),
									CellDay = sdr.GetStringSafe(6),
									CellMonth = sdr.GetStringSafe(7),
									CellYear = sdr.GetStringSafe(8),
									CellDate = sdr.GetNullable<DateTime>(9),
									Value = sdr.GetStringSafe(10),
									CompanyFinancialTermID = sdr.GetNullable<int>(11),
									ValueNumeric = sdr.GetNullable<decimal>(12),
									NormalizedNegativeIndicator = sdr.GetBoolean(13),
									ScalingFactorID = sdr.GetStringSafe(14),
									AsReportedScalingFactor = sdr.GetStringSafe(15),
									Currency = sdr.GetStringSafe(16),
									CurrencyCode = sdr.GetStringSafe(17),
									Cusip = sdr.GetStringSafe(18),
									ScarUpdated = sdr.GetBoolean(19),
									IsIncomePositive = sdr.GetBoolean(20),
									XBRLTag = sdr.GetStringSafe(21),
									UpdateStampUTC = sdr.GetNullable<DateTime>(22),
									DocumentID = sdr.GetGuid(23),
									Label = sdr.GetStringSafe(24),
									ScalingFactorValue = sdr.GetDouble(25),
									ARDErrorTypeId = sdr.GetNullable<int>(26),
									MTMWErrorTypeId = sdr.GetNullable<int>(27),
									LikePeriodValidationFlag = sdr.GetBoolean(28)
								};

								adjustedOrder = sdr.GetInt32(29);
							} else {
								cell = new TableCell();
								adjustedOrder = sdr.GetInt32(29);
							}

							while (adjustedOrder != res.StaticHierarchies[shix].AdjustedOrder) {
								shix++;
							}

							CellMap.Add(new Tuple<int, int>(res.StaticHierarchies[shix].Id, sdr.GetInt32(30)), cell);

							if (cell.ID == 0 || cell.CompanyFinancialTermID == res.StaticHierarchies[shix].CompanyFinancialTermId) {
								res.StaticHierarchies[shix].Cells.Add(cell);
							} else {
								throw new Exception();
							}
						}

						sdr.NextResult();

						CellChangeComponents = sdr.Cast<IDataRecord>().Select(r => new CellMTMWComponent()
						{
							StaticHierarchyID = r.GetInt32(0),
							DocumentTimeSliceID = r.GetInt32(3),
							TableCellID = r.GetInt32(4),
							ValueNumeric = r.GetDecimal(8),
							IsIncomePositive = r.GetBoolean(9),
							ScalingFactorValue = r.GetDouble(10),
							RootStaticHierarchyID = r.GetInt32(6),
							RootDocumentTimeSliceID = r.GetInt32(7)
						}
						).ToList();
					}
				}
			}

			
			Dictionary<Tuple<int, int>, decimal> CellValueMap = new Dictionary<Tuple<int, int>, decimal>();

			foreach (CellMTMWComponent comp in CellChangeComponents) {
				Tuple<int,int> tup = new Tuple<int,int>(comp.RootStaticHierarchyID, comp.RootDocumentTimeSliceID);
				if(!CellValueMap.ContainsKey(tup))
					CellValueMap.Add(tup, 0);

				CellValueMap[tup] += comp.ValueNumeric * ((decimal)comp.ScalingFactorValue) * (comp.IsIncomePositive ? 1 : -1);
			}

			foreach(Tuple<int, int> key in CellMap.Keys){
				TableCell cell = CellMap[key];
				if(CellValueMap.ContainsKey(key))
				cell.MTMWValidationFlag = (cell.ValueNumeric * (decimal)cell.ScalingFactorValue * (cell.IsIncomePositive ? 1 : -1)) != CellValueMap[key];
			}

			foreach (StaticHierarchy sh in res.StaticHierarchies) {
				sh.Level = SHLevels[sh.Id];
			}

			return res;
		}
	}
}