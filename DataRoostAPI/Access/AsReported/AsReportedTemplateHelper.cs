using System;
using System.Collections.Generic;
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
        private string SQL_CellsQuery =
                    @"
SELECT DISTINCT tc.ID, tc.Offset, tc.CellPeriodType, tc.PeriodTypeID, tc.CellPeriodCount, tc.PeriodLength, tc.CellDay, 
				tc.CellMonth, tc.CellYear, tc.CellDate, tc.Value, tc.CompanyFinancialTermID, tc.ValueNumeric, tc.NormalizedNegativeIndicator, 
				tc.ScalingFactorID, tc.AsReportedScalingFactor, tc.Currency, tc.CurrencyCode, tc.Cusip, tc.ScarUpdated, tc.IsIncomePositive, 
				tc.XBRLTag, tc.UpdateStampUTC, tc.DocumentId, tc.Label, tc.ScalingFactorValue,
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
				tc.XBRLTag, tc.UpdateStampUTC, tc.DocumentId, tc.Label, tc.ScalingFactorValue,
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
						}else{
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
							}else{
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

							TimeSliceMap[tup].Add(TimeSlices.Count -1);

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

				for(int i = 0; i< sh.Cells.Count; i++){
					TimeSlice ts = temp.TimeSlices[i];

					TableCell tc = sh.Cells[i];
					List<int> matches = TimeSliceMap[new Tuple<DateTime, string>(ts.TimeSlicePeriodEndDate, ts.PeriodType)];
					foreach (int j in matches) {
						if (sh.Cells[j] == tc)
							continue;

						decimal matchValue = CalculateCellValue(sh.Cells[j], BlankCells, SHChildLookup);
						decimal cellValue = CalculateCellValue(tc, BlankCells, SHChildLookup);
						bool anyValidationPasses = matches.Any(t => sh.Cells[t].ARDErrorTypeId.HasValue);

						if (matchValue != cellValue &&//TODO: remove double checks
							!((ts.PublicationDate > temp.TimeSlices[j].PublicationDate && cellValue == 0) || (temp.TimeSlices[j].PublicationDate > ts.PublicationDate && matchValue == 0)) &&
							!anyValidationPasses &&
							tc.ValueNumeric.HasValue
							) {
								tc.LikePeriodValidationFlag = true;
						}
					}

					tc.MTMWValidationFlag = SHChildLookup[sh.Id].Count > 0 && (CalculateCellValue(tc, BlankCells, SHChildLookup) != CalculateChildSum(tc, CellLookup, SHChildLookup)) && !tc.MTMWErrorTypeId.HasValue;
				}
			}

			return temp;
		}

		private decimal CalculateCellValue(TableCell cell, Dictionary<TableCell, Tuple<StaticHierarchy, int>> BlankCells, Dictionary<int, List<StaticHierarchy>> SHChildLookup) {
			if (cell.ValueNumeric.HasValue) {
				return cell.ValueNumeric.Value * (cell.IsIncomePositive ? 1 : -1) * (decimal)cell.ScalingFactorValue;
			} else if (cell.VirtualValueNumeric.HasValue) {
				return cell.VirtualValueNumeric.Value;
			} else {
				if (BlankCells.ContainsKey(cell)) {
					decimal sum = 0;
					StaticHierarchy sh = BlankCells[cell].Item1;
					int timesliceIndex = BlankCells[cell].Item2;

					foreach (StaticHierarchy child in SHChildLookup[sh.Id]) {
						sum += CalculateCellValue(child.Cells[timesliceIndex], BlankCells, SHChildLookup);
					}
					if (SHChildLookup[sh.Id].Count > 0) {
						if(!cell.ValueNumeric.HasValue)
							cell.VirtualValueNumeric = sum;

						return sum;
					}
				}
			}
			return 0;
		}

		private decimal CalculateChildSum(TableCell cell, Dictionary<TableCell, Tuple<StaticHierarchy, int>> CellLookup, Dictionary<int, List<StaticHierarchy>> SHChildLookup) {
			if (CellLookup.ContainsKey(cell)) {
				decimal sum = 0;
				StaticHierarchy sh = CellLookup[cell].Item1;
				int timesliceIndex = CellLookup[cell].Item2;

				foreach (StaticHierarchy child in SHChildLookup[sh.Id]) {
					sum += CalculateCellValue(child.Cells[timesliceIndex], CellLookup, SHChildLookup);
				}
				if (SHChildLookup[sh.Id].Count > 0) {
					if(!cell.ValueNumeric.HasValue)
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


        public TableCell GetCell(string CellId)
        {
            using (SqlConnection conn = new SqlConnection(_sfConnectionString))
            {
                using (SqlCommand cmd = new SqlCommand(SQL_CellsQuery, conn))
                {
                    cmd.Parameters.AddWithValue("@cellId", CellId);

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
                            }
                            else
                            {
                                cell = new TableCell();
                                adjustedOrder = reader.GetInt32(28);
                            }
                            return cell;

                            //while (adjustedOrder != StaticHierarchies[shix].AdjustedOrder)
                            //{
                            //    shix++;
                            //}

                            //if (cell.ID == 0)
                            //{
                            //    BlankCells.Add(cell, new Tuple<StaticHierarchy, int>(StaticHierarchies[shix], StaticHierarchies[shix].Cells.Count));
                            //}
                            //CellLookup.Add(cell, new Tuple<StaticHierarchy, int>(StaticHierarchies[shix], StaticHierarchies[shix].Cells.Count));

                            //if (cell.ID == 0 || cell.CompanyFinancialTermID == StaticHierarchies[shix].CompanyFinancialTermId)
                            //{
                            //    StaticHierarchies[shix].Cells.Add(cell);
                            //}
                            //else
                            //{
                            //    throw new Exception();
                            //}

                        }
                    }
                }
            }
            return new TableCell();
        }
	}
}