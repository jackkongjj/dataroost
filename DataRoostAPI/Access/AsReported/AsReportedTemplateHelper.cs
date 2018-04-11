using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Net;
using System.Web;
using DataRoostAPI.Common.Models.AsReported;
using FactSet.Data.SqlClient;
using Newtonsoft.Json.Linq;
using System.Text.RegularExpressions;

namespace CCS.Fundamentals.DataRoostAPI.Access.AsReported {
	public class AsReportedTemplateHelper {

		private readonly string _sfConnectionString;

		public AsReportedTemplateHelper(string sfConnectionString) {
			this._sfConnectionString = sfConnectionString;
		}
		#region SQL
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
JOIN DocumentTimeSlice dts on dts.ID = ts.ID and dts.DocumentSeriesId = ds.ID 
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

		#endregion

		public ScarResult GetTemplateInScarResult(int iconum, string TemplateName, Guid DocumentId) {
			ScarResult newFormat = new ScarResult();
			AsReportedTemplate oldFormat = GetTemplate(iconum, TemplateName, DocumentId);
			newFormat.StaticHierarchies = oldFormat.StaticHierarchies;
			newFormat.TimeSlices = oldFormat.TimeSlices;
			return newFormat;
		}
		public AsReportedTemplate GetTemplate(int iconum, string TemplateName, Guid DocumentId) {
			var sw = System.Diagnostics.Stopwatch.StartNew();
			string query =
								@"

BEGIN TRY
DROP TABLE #StaticHierarchy
END TRY
BEGIN CATCH
END CATCH 


SELECT DISTINCT sh.*, shm.Code, tt.Description as 'TableTypeDescription'
INTO #StaticHierarchy
FROM DocumentSeries ds
	JOIN CompanyFinancialTerm cft ON cft.DocumentSeriesId = ds.Id
	JOIN StaticHierarchy sh on cft.ID = sh.CompanyFinancialTermID
	JOIN TableType tt on sh.TableTypeID = tt.ID
    JOIN HierarchyMetaTypes shm on sh.StaticHierarchyMetaId = shm.id
WHERE ds.CompanyID = @iconum
AND tt.Description = @templateName
ORDER BY sh.AdjustedOrder asc

select * from #StaticHierarchy
";

			string CellsQuery =
				@"
--SELECT *
--FROM (
SELECT tc.ID, tc.Offset, tc.CellPeriodType, tc.PeriodTypeID, tc.CellPeriodCount, tc.PeriodLength, tc.CellDay, 
				tc.CellMonth, tc.CellYear, tc.CellDate, tc.Value, tc.CompanyFinancialTermID, tc.ValueNumeric, tc.NormalizedNegativeIndicator, 
				tc.ScalingFactorID, tc.AsReportedScalingFactor, tc.Currency, tc.CurrencyCode, tc.Cusip, tc.ScarUpdated, tc.IsIncomePositive, 
tc.XBRLTag, 
--tc.UpdateStampUTC
null as nul
, tc.DocumentId, tc.Label, tc.ScalingFactorValue,
				(select aetc.ARDErrorTypeId from ARDErrorTypeTableCell aetc (nolock) where tc.Id = aetc.TableCellId) as arderr,
				(select metc.MTMWErrorTypeId from MTMWErrorTypeTableCell metc (nolock) where tc.Id = metc.TableCellId) as mtmwerr, 
				sh.AdjustedOrder, ROW_NUMBER() OVER (PARTITION BY sh.ID, ts.ID ORDER BY tc.ID asc) as rwnm, dts.Duration, dts.TimeSlicePeriodEndDate, dts.ReportingPeriodEndDate, d.PublicationDateTime, sh.CompanyFinancialTermID
				
FROM DocumentSeries ds WITH (NOLOCK) 
JOIN CompanyFinancialTerm cft WITH (NOLOCK)  ON cft.DocumentSeriesId = ds.Id
JOIN #StaticHierarchy sh WITH (NOLOCK)  on cft.ID = sh.CompanyFinancialTermID
JOIN TableType tt WITH (NOLOCK)  on sh.TableTypeID = tt.ID
JOIN(
	SELECT distinct dts.ID
	FROM DocumentSeries ds WITH (NOLOCK) 
	JOIN DocumentTimeSlice dts  WITH (NOLOCK) on ds.ID = Dts.DocumentSeriesId
	JOIN Document d WITH (NOLOCK)  on dts.DocumentId = d.ID
	JOIN DocumentTimeSliceTableCell dtstc WITH (NOLOCK)  on dts.ID = dtstc.DocumentTimeSliceID
	JOIN TableCell tc  WITH (NOLOCK) on dtstc.TableCellID = tc.ID
	JOIN DimensionToCell dtc  WITH (NOLOCK) on tc.ID = dtc.TableCellID -- check that is in a table
	JOIN #StaticHierarchy sh WITH (NOLOCK)  on tc.CompanyFinancialTermID = sh.CompanyFinancialTermID
	JOIN TableType tt  WITH (NOLOCK) on tt.ID = sh.TableTypeID
	WHERE ds.CompanyID = @iconum
	AND tt.Description = @templateName
	AND (d.ID = @DocumentID OR d.ArdExportFlag = 1 OR d.ExportFlag = 1 OR d.IsDocSetupCompleted = 1)
) as ts on 1=1
--JOIN (SELECT DISTINCT dts.*, d.PublicationDateTime
--		FROM DocumentSeries ds
--			JOIN CompanyFinancialTerm cft ON cft.DocumentSeriesId = ds.Id
--			JOIN #StaticHierarchy sh on cft.ID = sh.CompanyFinancialTermID
--			JOIN TableType tt on sh.TableTypeID = tt.ID
--			JOIN TableCell tc on tc.CompanyFinancialTermID = cft.ID
--			JOIN DimensionToCell dtc on tc.ID = dtc.TableCellID -- check that is in a table
--			JOIN DocumentTimeSliceTableCell dtstc on tc.ID = dtstc.TableCellID
--			JOIN DocumentTimeSlice dts on dtstc.DocumentTimeSliceID = dts.ID
--			JOIN Document d on dts.DocumentId = d.ID
--		WHERE ds.CompanyID = @iconum
--		AND tt.Description = @templateName
--		AND (d.ID = @DocumentID OR d.ArdExportFlag = 1 OR d.ExportFlag = 1 OR d.IsDocSetupCompleted = 1) 
--	)dts
join DocumentTimeSlice dts WITH (NOLOCK) 
	on dts.ID = ts.ID and dts.DocumentSeriesId = ds.ID 
LEFT JOIN(
	SELECT tc.*, dtstc.DocumentTimeSliceID, sf.Value as ScalingFactorValue
	FROM DocumentSeries ds WITH (NOLOCK) 
	JOIN CompanyFinancialTerm cft WITH (NOLOCK)  ON cft.DocumentSeriesId = ds.Id
	JOIN #StaticHierarchy sh  WITH (NOLOCK) on cft.ID = sh.CompanyFinancialTermID
	JOIN TableType tt  WITH (NOLOCK) on sh.TableTypeID = tt.ID
	JOIN TableCell tc  WITH (NOLOCK) on tc.CompanyFinancialTermID = cft.ID
	JOIN DocumentTimeSliceTableCell dtstc  WITH (NOLOCK) on dtstc.TableCellID = tc.ID
	JOIN ScalingFactor sf  WITH (NOLOCK) on sf.ID = tc.ScalingFactorID
	WHERE ds.CompanyID = @iconum
	AND tt.Description = @templateName
) as tc ON tc.DocumentTimeSliceID = ts.ID AND tc.CompanyFinancialTermID = cft.ID
JOIN Document d  WITH (NOLOCK) on dts.documentid = d.ID
WHERE ds.CompanyID = @iconum
AND tt.Description = @templateName
ORDER BY sh.AdjustedOrder asc, dts.TimeSlicePeriodEndDate desc, CHARINDEX(dts.PeriodType, '""XX"", ""AR"", ""IF"", ""T3"", ""Q4"", ""Q3"", ""T2"", ""I1"", ""Q2"", ""T1"", ""Q1"", ""Q9"", ""Q8"", ""Q6""') asc, dts.Duration desc, d.PublicationDateTime desc, dts.ReportingPeriodEndDate desc
--) a WHERE rwnm = 1
--ORDER BY AdjustedOrder asc, TimeSlicePeriodEndDate desc, Duration desc, ReportingPeriodEndDate desc, PublicationDateTime desc

";//I hate this query, it is so bad

			string TimeSliceQuery =
				@"SELECT DISTINCT dts.*, d.PublicationDateTime, d.damdocumentid, CHARINDEX(dts.PeriodType, '""XX"", ""AR"", ""IF"", ""T3"", ""Q4"", ""Q3"", ""T2"", ""I1"", ""Q2"", ""T1"", ""Q1"", ""Q9"", ""Q8"", ""Q6""')
FROM DocumentSeries ds WITH (NOLOCK) 
	JOIN CompanyFinancialTerm cft WITH (NOLOCK)  ON cft.DocumentSeriesId = ds.Id
	JOIN #StaticHierarchy sh  WITH (NOLOCK) on cft.ID = sh.CompanyFinancialTermID
	JOIN TableType tt  WITH (NOLOCK) on sh.TableTypeID = tt.ID
	JOIN TableCell tc  WITH (NOLOCK) on tc.CompanyFinancialTermID = cft.ID
	JOIN DimensionToCell dtc WITH (NOLOCK)  on tc.ID = dtc.TableCellID -- check that is in a table
	JOIN DocumentTimeSliceTableCell dtstc  WITH (NOLOCK) on tc.ID = dtstc.TableCellID
	JOIN DocumentTimeSlice dts  WITH (NOLOCK) on dtstc.DocumentTimeSliceID = dts.ID  and dts.DocumentSeriesId = ds.ID 
	JOIN Document d  WITH (NOLOCK) on dts.DocumentId = d.ID
WHERE ds.CompanyID = @iconum
AND tt.Description = @templateName
AND (d.ID = @DocumentID OR d.ArdExportFlag = 1 OR d.ExportFlag = 1 OR d.IsDocSetupCompleted = 1)
--ORDER BY dts.TimeSlicePeriodEndDate desc, dts.Duration desc, dts.ReportingPeriodEndDate desc, d.PublicationDateTime desc
ORDER BY dts.TimeSlicePeriodEndDate desc, CHARINDEX(dts.PeriodType, '""XX"", ""AR"", ""IF"", ""T3"", ""Q4"", ""Q3"", ""T2"", ""I1"", ""Q2"", ""T1"", ""Q1"", ""Q9"", ""Q8"", ""Q6""') asc, dts.Duration desc, d.PublicationDateTime desc, dts.ReportingPeriodEndDate desc
";

			string TimeSliceIsSummaryQuery = @"

select distinct DocumentTimeSliceID, TableType
from DocumentSeries ds 
JOIN Document d on ds.ID = d.DocumentSeriesID
JOIN DocumentTimeSlice dts on dts.DocumentId = d.ID and dts.DocumentSeriesId = ds.ID 
join DocumentTimeSliceTableTypeIsSummary dtsis on dts.id = dtsis.DocumentTimeSliceID
WHERE  CompanyID = @Iconum";


			Dictionary<Tuple<StaticHierarchy, TimeSlice>, SCARAPITableCell> CellMap = new Dictionary<Tuple<StaticHierarchy, TimeSlice>, SCARAPITableCell>();
			Dictionary<Tuple<DateTime, string>, List<int>> TimeSliceMap = new Dictionary<Tuple<DateTime, string>, List<int>>();//int is index into timeslices for fast lookup


			AsReportedTemplate temp = new AsReportedTemplate();
			string query_sproc = @"SCARGetTemplate";
			temp.StaticHierarchies = new List<StaticHierarchy>();
			Dictionary<SCARAPITableCell, Tuple<StaticHierarchy, int>> BlankCells = new Dictionary<SCARAPITableCell, Tuple<StaticHierarchy, int>>();
			Dictionary<SCARAPITableCell, Tuple<StaticHierarchy, int>> CellLookup = new Dictionary<SCARAPITableCell, Tuple<StaticHierarchy, int>>();
			Dictionary<int, StaticHierarchy> SHLookup = new Dictionary<int, StaticHierarchy>();
			Dictionary<int, List<StaticHierarchy>> SHChildLookup = new Dictionary<int, List<StaticHierarchy>>();
			List<StaticHierarchy> StaticHierarchies = temp.StaticHierarchies;
			Dictionary<int, List<string>> IsSummaryLookup = new Dictionary<int, List<string>>();
			query += CellsQuery + TimeSliceQuery + TimeSliceIsSummaryQuery;
			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {
				using (SqlCommand cmd = new SqlCommand(query_sproc, conn)) {
					conn.Open();
					cmd.CommandType = System.Data.CommandType.StoredProcedure;
					cmd.Parameters.AddWithValue("@iconum", iconum);
					cmd.Parameters.AddWithValue("@templateName", TemplateName);
					cmd.Parameters.AddWithValue("@DocumentID", DocumentId);
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
								StaticHierarchyMetaType = reader.GetStringSafe(12),
								TableTypeDescription = reader.GetStringSafe(13),
								Cells = new List<SCARAPITableCell>()
							};
							StaticHierarchies.Add(document);
							SHLookup.Add(document.Id, document);
							SHChildLookup.Add(document.Id, new List<StaticHierarchy>());
							if (document.ParentID != null) {
								SHChildLookup[document.ParentID.Value].Add(document);
							}
						}
						reader.NextResult();

						int shix = 0;
						int i = 0;
						int adjustedOrder = 0;
						#region read CellsQuery

						while (reader.Read()) {
							if (reader.GetInt64(29) == 1) {
								SCARAPITableCell cell;
								if (reader.GetNullable<int>(0).HasValue) {
									cell = new SCARAPITableCell
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
										DocumentID = reader.IsDBNull(23) ? new Guid("00000000-0000-0000-0000-000000000000") : reader.GetGuid(23),
										//	DocumentID = reader.GetGuid(23),
										Label = reader.GetStringSafe(24),
										ScalingFactorValue = reader.GetDouble(25),
										ARDErrorTypeId = reader.GetNullable<int>(26),
										MTMWErrorTypeId = reader.GetNullable<int>(27)
									};

									adjustedOrder = reader.GetInt32(28);
								} else {
									cell = new SCARAPITableCell();
									adjustedOrder = reader.GetInt32(28);
									cell.CompanyFinancialTermID = reader.GetNullable<int>(34);
								}
								if (adjustedOrder < 0) {
									var negSh = StaticHierarchies.FirstOrDefault(x => x.CompanyFinancialTermId == cell.CompanyFinancialTermID && x.AdjustedOrder < 0);
									if (negSh == null) continue;
									if (cell.ID == 0) {
										BlankCells.Add(cell, new Tuple<StaticHierarchy, int>(negSh, negSh.Cells.Count));
									}

									CellLookup.Add(cell, new Tuple<StaticHierarchy, int>(negSh, negSh.Cells.Count));

									if (cell.ID == 0 || cell.CompanyFinancialTermID == negSh.CompanyFinancialTermId) {
										negSh.Cells.Add(cell);
									} else {
										throw new Exception();
									}

								} else {
									while (adjustedOrder != StaticHierarchies[shix].AdjustedOrder) {
										shix++;
									}

									if (cell.ID == 0) {
										BlankCells.Add(cell, new Tuple<StaticHierarchy, int>(StaticHierarchies[shix], StaticHierarchies[shix].Cells.Count));
									}
									i++;
									CellLookup.Add(cell, new Tuple<StaticHierarchy, int>(StaticHierarchies[shix], StaticHierarchies[shix].Cells.Count));

									if (cell.ID == 0 || cell.CompanyFinancialTermID == StaticHierarchies[shix].CompanyFinancialTermId) {
										StaticHierarchies[shix].Cells.Add(cell);
									} else {
										throw new Exception();
									}
								}
							}
						}
#endregion
						reader.NextResult();

				temp.TimeSlices = new List<TimeSlice>();
				List<TimeSlice> TimeSlices = temp.TimeSlices;
				#region Read TimeSlice

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
								PublicationDate = reader.GetDateTime(19),
								DamDocumentId = reader.GetGuid(20)
							};

							TimeSlices.Add(slice);
							Tuple<DateTime, string> tup = new Tuple<DateTime, string>(slice.TimeSlicePeriodEndDate, slice.PeriodType);//TODO: Is this sufficient for Like Period?
							if (!TimeSliceMap.ContainsKey(tup)) {
								TimeSliceMap.Add(tup, new List<int>());
							}

							TimeSliceMap[tup].Add(TimeSlices.Count - 1);

							foreach (StaticHierarchy sh in temp.StaticHierarchies) {
								try {
									CellMap.Add(new Tuple<StaticHierarchy, TimeSlice>(sh, slice), sh.Cells[TimeSlices.Count - 1]);
								} catch { }
							}


				}
				#endregion

				reader.NextResult();
						while (reader.Read()) {
							int TimeSliceID = reader.GetInt32(0);

							if (!IsSummaryLookup.ContainsKey(TimeSliceID)) {
								IsSummaryLookup.Add(TimeSliceID, new List<string>());
							}

							IsSummaryLookup[TimeSliceID].Add(reader.GetStringSafe(1));
						}
					}
				}

			}


			foreach (StaticHierarchy sh in StaticHierarchies) {//Finds likeperiod validation failures. Currently failing with virtual cells

				if (!sh.ParentID.HasValue) {
					sh.Level = 0;
				}
				foreach (StaticHierarchy ch in SHChildLookup[sh.Id]) {
					ch.Level = sh.Level + 1;
				}

				for (int i = 0; i < sh.Cells.Count; i++) {
					try {
						TimeSlice ts = temp.TimeSlices[i];

						SCARAPITableCell tc = sh.Cells[i];
						if (ts.Cells == null) {
							ts.Cells = new List<SCARAPITableCell>();
						}
						ts.Cells.Add(tc);
						List<int> matches = TimeSliceMap[new Tuple<DateTime, string>(ts.TimeSlicePeriodEndDate, ts.PeriodType)].Where(j => sh.Cells[j] != tc).ToList();

						bool whatever = false;
						decimal cellValue = CalculateCellValue(tc, BlankCells, SHChildLookup, IsSummaryLookup, ref whatever, temp.TimeSlices);

						List<int> sortedLessThanPubDate = matches.Where(m2 => temp.TimeSlices[m2].PublicationDate < temp.TimeSlices[i].PublicationDate).OrderByDescending(c => temp.TimeSlices[c].PublicationDate).ToList();

						if (LPV(BlankCells, CellLookup, SHChildLookup, IsSummaryLookup, sh, tc, matches, ref whatever, cellValue, sortedLessThanPubDate, temp.TimeSlices)
						) {
							tc.LikePeriodValidationFlag = true;
							tc.StaticHierarchyID = sh.Id;
							tc.DocumentTimeSliceID = ts.Id;
						}

						bool hasChildren = false;
						bool whatever2 = false;

						tc.MTMWValidationFlag = tc.ValueNumeric.HasValue && SHChildLookup[sh.Id].Count > 0 &&
								(CalculateCellValue(tc, BlankCells, SHChildLookup, IsSummaryLookup, ref whatever2, temp.TimeSlices) != CalculateChildSum(tc, CellLookup, SHChildLookup, IsSummaryLookup, ref hasChildren, temp.TimeSlices)) &&
										!tc.MTMWErrorTypeId.HasValue && hasChildren && sh.UnitTypeId != 2
										&& !(IsSummaryLookup.ContainsKey(ts.Id) && IsSummaryLookup[ts.Id].Contains(sh.TableTypeDescription));
					} catch { break; }
				}
			}

			return temp;
		}

		private bool LPV(Dictionary<SCARAPITableCell, Tuple<StaticHierarchy, int>> BlankCells, Dictionary<SCARAPITableCell, Tuple<StaticHierarchy, int>> CellLookup, 
			Dictionary<int, List<StaticHierarchy>> SHChildLookup, Dictionary<int, List<string>> IsSummaryLookup, StaticHierarchy sh, SCARAPITableCell tc, 
			List<int> matches, ref bool whatever, decimal cellValue, List<int> sortedLessThanPubDate, List<TimeSlice> timeSlices) {


				bool whatever2 = false;



				foreach (int i in matches)
					CalculateCellValue(sh.Cells[i], BlankCells, SHChildLookup, IsSummaryLookup, ref whatever2, timeSlices);

				if (tc.ARDErrorTypeId.HasValue || (tc.VirtualValueNumeric.HasValue && GetChildren(tc, CellLookup, SHChildLookup).Any(c => c.ARDErrorTypeId.HasValue)))
				return false;

			if (tc.ID > 0 && !tc.ValueNumeric.HasValue)
				return true;

			if (!tc.ValueNumeric.HasValue && !tc.VirtualValueNumeric.HasValue)
				return false;

			if (matches.Where(m=> CalculateCellValue(sh.Cells[m], BlankCells, SHChildLookup, IsSummaryLookup, ref whatever2, timeSlices) == cellValue).Any(m => sh.Cells[m].ARDErrorTypeId.HasValue ||
				(GetChildren(sh.Cells[m], CellLookup, SHChildLookup).Any(c => c.ARDErrorTypeId.HasValue) && sh.Cells[m].VirtualValueNumeric.HasValue)))
				return false;

			if (matches.Any(m => (tc.ValueNumeric.HasValue || tc.VirtualValueNumeric.HasValue)
																	&& (sortedLessThanPubDate.Count > 0 && sortedLessThanPubDate.First() == m)
																	&& (!sh.Cells[m].ValueNumeric.HasValue && !sh.Cells[m].VirtualValueNumeric.HasValue)))
				return true;


			var matchGroups = matches.Where(m=> sh.Cells[m].VirtualValueNumeric.HasValue || sh.Cells[m].ValueNumeric.HasValue).Where(m => CalculateCellValue(sh.Cells[m], BlankCells, SHChildLookup, IsSummaryLookup, ref whatever2, timeSlices) != cellValue).GroupBy(m => CalculateCellValue(sh.Cells[m], BlankCells, SHChildLookup, IsSummaryLookup, ref whatever2, timeSlices));

			bool AllTagged = true;
			foreach (var g in matchGroups)
				if (g.All(m => !sh.Cells[m].ARDErrorTypeId.HasValue &&
				!(sh.Cells[m].VirtualValueNumeric.HasValue && GetChildren(sh.Cells[m], CellLookup, SHChildLookup).Any(c => c.ARDErrorTypeId.HasValue)))) {
					AllTagged = false;
					break;
				}

			return !AllTagged;

			//return matches.Any(m => CalculateCellValue(sh.Cells[m], BlankCells, SHChildLookup, IsSummaryLookup, ref whatever2, timeSlices) != cellValue && ((sh.Cells[m].ValueNumeric.HasValue || sh.Cells[m].VirtualValueNumeric.HasValue)));
		}


		private decimal CalculateCellValue(SCARAPITableCell cell, Dictionary<SCARAPITableCell, Tuple<StaticHierarchy, int>> BlankCells, Dictionary<int, List<StaticHierarchy>> SHChildLookup, 
			Dictionary<int, List<string>> IsSummaryLookup, ref bool hasChildren, List<TimeSlice> timeSlices) {
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
					TimeSlice ts = timeSlices[timesliceIndex];
					bool subChildren = false;

					foreach (StaticHierarchy child in SHChildLookup[sh.Id]) {
						if (child.StaticHierarchyMetaId != 2 && child.StaticHierarchyMetaId != 5 && child.StaticHierarchyMetaId != 6 && child.UnitTypeId != 2){
							sum += CalculateCellValue(child.Cells[timesliceIndex], BlankCells, SHChildLookup, IsSummaryLookup, ref subChildren, timeSlices);
							if (subChildren)
								hasChildren = true;
						}
					}
					if (SHChildLookup[sh.Id].Where(c=> c.StaticHierarchyMetaId != 2 && c.StaticHierarchyMetaId != 5 && c.StaticHierarchyMetaId != 6 && c.UnitTypeId != 2).Count() > 0) {
						if (!cell.ValueNumeric.HasValue && subChildren && !(IsSummaryLookup.ContainsKey(ts.Id) && IsSummaryLookup[ts.Id].Contains(sh.TableTypeDescription)))
							cell.VirtualValueNumeric = sum;

						return sum;
					}
				}
			}
			return 0;
		}

		private IEnumerable<SCARAPITableCell> GetChildren(SCARAPITableCell cell, Dictionary<SCARAPITableCell, Tuple<StaticHierarchy, int>> CellLookup, Dictionary<int, List<StaticHierarchy>> SHChildLookup) {

			StaticHierarchy sh = CellLookup[cell].Item1;
			int timesliceIndex = CellLookup[cell].Item2;

			foreach (StaticHierarchy child in SHChildLookup[sh.Id]) {
				if (child.Cells[timesliceIndex].VirtualValueNumeric.HasValue)
					foreach (SCARAPITableCell c in GetChildren(child.Cells[timesliceIndex], CellLookup, SHChildLookup))
						yield return c;
				else {
					yield return child.Cells[timesliceIndex];
				}
			}
		}

		private decimal CalculateChildSum(SCARAPITableCell cell, Dictionary<SCARAPITableCell, Tuple<StaticHierarchy, int>> CellLookup, Dictionary<int, List<StaticHierarchy>> SHChildLookup, Dictionary<int, List<string>> IsSummaryLookup, ref bool hasChildren, List<TimeSlice> TimeSlices) {
			if (CellLookup.ContainsKey(cell)) {
				decimal sum = 0;
				StaticHierarchy sh = CellLookup[cell].Item1;
				int timesliceIndex = CellLookup[cell].Item2;
				TimeSlice ts = TimeSlices[timesliceIndex];

				bool subChildren = false;

				if (sh.StaticHierarchyMetaId != 2 && sh.StaticHierarchyMetaId != 5 && sh.StaticHierarchyMetaId != 6) {

					foreach (StaticHierarchy child in SHChildLookup[sh.Id].Where(s => s.StaticHierarchyMetaId != 2 && s.StaticHierarchyMetaId != 5 && s.StaticHierarchyMetaId != 6)) {
						sum += CalculateCellValue(child.Cells[timesliceIndex], CellLookup, SHChildLookup, IsSummaryLookup, ref subChildren, TimeSlices);
						if (subChildren)
							hasChildren = true;
					}
					if (SHChildLookup[sh.Id].Where(s => s.StaticHierarchyMetaId != 2 && s.StaticHierarchyMetaId != 5 && s.StaticHierarchyMetaId != 6).Count() > 0) {
						if (!cell.ValueNumeric.HasValue && subChildren)
							cell.VirtualValueNumeric = sum;

						return sum;
					}
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
	JOIN DocumentTimeSlice dts on dtstc.DocumentTimeSliceID = dts.ID and dts.DocumentSeriesId = ds.ID 
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
							Cells = new List<SCARAPITableCell>()
						};
					}
				}

				using (SqlCommand cmd = new SqlCommand(CellsQuery, conn)) {
					cmd.Parameters.AddWithValue("@id", id);

					using (SqlDataReader reader = cmd.ExecuteReader()) {

						while (reader.Read()) {
							SCARAPITableCell cell = new SCARAPITableCell
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

		public ScarResult UpdateStaticHierarchySeperator(int id, bool isGroup) {

			string query = @"
UPDATE StaticHierarchy SET SeperatorFlag = @newValue 
WHERE id = @TargetSHID;

select * 
FROM StaticHierarchy
where id = @TargetSHID;
";

			ScarResult response = new ScarResult();
			response.StaticHierarchies = new List<StaticHierarchy>();

			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {


				using (SqlCommand cmd = new SqlCommand(query, conn)) {
					conn.Open();
					cmd.Parameters.AddWithValue("@TargetSHID", id);
					cmd.Parameters.AddWithValue("@newValue", isGroup ? 1 : 0);
					using (SqlDataReader reader = cmd.ExecuteReader()) {
						while (reader.Read()) {
							StaticHierarchy sh = new StaticHierarchy
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
								Cells = new List<SCARAPITableCell>()
							};
							response.StaticHierarchies.Add(sh);
						}
					}
				}
			}
			return response;
		}

		public ScarResult UpdateStaticHierarchyUnitType(int id, string newValue) {

			string query = @"
UPDATE StaticHierarchy SET UnitTypeId = @newValue 
WHERE id = @TargetSHID;

select * 
FROM StaticHierarchy
where id = @TargetSHID;
";

			ScarResult response = new ScarResult();
			response.StaticHierarchies = new List<StaticHierarchy>();

			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {


				using (SqlCommand cmd = new SqlCommand(query, conn)) {
					conn.Open();
					cmd.Parameters.AddWithValue("@TargetSHID", id);
					cmd.Parameters.AddWithValue("@newValue", newValue);
					using (SqlDataReader reader = cmd.ExecuteReader()) {
						while (reader.Read()) {
							StaticHierarchy sh = new StaticHierarchy
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
								Cells = new List<SCARAPITableCell>()
							};
							response.StaticHierarchies.Add(sh);
						}
					}
				}
			}
			return response;
		}

		public ScarResult UpdateStaticHierarchyMeta(int id, string newValue) {

			string query = @"
UPDATE StaticHierarchy SET StaticHierarchyMetaId = @newValue 
WHERE id = @TargetSHID;

select * 
FROM StaticHierarchy
where id = @TargetSHID;
";

			ScarResult response = new ScarResult();
			response.StaticHierarchies = new List<StaticHierarchy>();

			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {


				using (SqlCommand cmd = new SqlCommand(query, conn)) {
					conn.Open();
					cmd.Parameters.AddWithValue("@TargetSHID", id);
					cmd.Parameters.AddWithValue("@newValue", newValue);
					using (SqlDataReader reader = cmd.ExecuteReader()) {
						while (reader.Read()) {
							StaticHierarchy sh = new StaticHierarchy
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
								Cells = new List<SCARAPITableCell>()
							};
							response.StaticHierarchies.Add(sh);
						}
					}
				}
			}
			return response;
		}

		public ScarResult UpdateStaticHierarchyAddHeader(int id) {

			string query = @"

DECLARE @OrigDescription varchar(1024) = (SELECT Description FROM StaticHierarchy WHERE ID = @TargetSHID)
DECLARE @OrigHierarchyLabel varchar(1024) 
DECLARE @NewHierarchyLabel varchar(1024)  


 SET @OrigHierarchyLabel = (SELECT dbo.GetHierarchyLabelSafe(Description) + '[' + dbo.GetEndLabelSafe(Description) + ']' FROM StaticHierarchy WHERE ID = @TargetSHID)
 SET @NewHierarchyLabel = (SELECT dbo.GetHierarchyLabelSafe	(Description) + '[ ][' + dbo.GetEndLabelSafe(Description) + ']'  FROM StaticHierarchy WHERE ID = @TargetSHID)
	 UPDATE StaticHierarchy
	SET Description = (dbo.GetHierarchyLabelSafe(@OrigDescription) + '[ ]' + dbo.GetEndLabelSafe(Description))
	WHERE ID = @TargetSHID

;WITH CTE_Children(ID) AS(
	SELECT ID FROM StaticHierarchy WHERE ID = @TargetSHID
	UNION ALL
	SELECT sh.Id 
	FROM StaticHierarchy sh
	JOIN CTE_Children cte on sh.ParentID = cte.ID
) UPDATE sh
   SET sh.Description = REPLACE(sh.description, @OrigHierarchyLabel, @NewHierarchyLabel)
FROM CTE_Children cte
JOIN StaticHierarchy sh on cte.ID = SH.Id   


;WITH CTE_Children ([Id]
      ,[CompanyFinancialTermId]
      ,[AdjustedOrder]
      ,[TableTypeId]
      ,[Description]
      ,[HierarchyTypeId]
      ,[SeperatorFlag]
      ,[StaticHierarchyMetaId]
      ,[UnitTypeId]
      ,[IsIncomePositive]
      ,[ChildrenExpandDown]
      ,[ParentID]) AS(
	SELECT [Id]
      ,[CompanyFinancialTermId]
      ,[AdjustedOrder]
      ,[TableTypeId]
      ,[Description]
      ,[HierarchyTypeId]
      ,[SeperatorFlag]
      ,[StaticHierarchyMetaId]
      ,[UnitTypeId]
      ,[IsIncomePositive]
      ,[ChildrenExpandDown]
      ,[ParentID] FROM StaticHierarchy WHERE ID = @TargetSHID
	UNION ALL
 
	SELECT sh.[Id]
      ,sh.[CompanyFinancialTermId]
      ,sh.[AdjustedOrder]
      ,sh.[TableTypeId]
      ,sh.[Description]
      ,sh.[HierarchyTypeId]
      ,sh.[SeperatorFlag]
      ,sh.[StaticHierarchyMetaId]
      ,sh.[UnitTypeId]
      ,sh.[IsIncomePositive]
      ,sh.[ChildrenExpandDown]
      ,sh.[ParentID] 
	FROM StaticHierarchy sh
	JOIN CTE_Children cte on sh.ParentID = cte.ID
)
 
SELECT *
  FROM CTE_Children
  order by AdjustedOrder

";

			ScarResult response = new ScarResult();
			response.StaticHierarchies = new List<StaticHierarchy>();

			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {


				using (SqlCommand cmd = new SqlCommand(query, conn)) {
					conn.Open();
					cmd.Parameters.AddWithValue("@TargetSHID", id);
					using (SqlDataReader reader = cmd.ExecuteReader()) {
						while (reader.Read()) {
							StaticHierarchy sh = new StaticHierarchy
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
								Cells = new List<SCARAPITableCell>()
							};
							response.StaticHierarchies.Add(sh);
						}
					}
				}
			}
			return response;
		}


		public ScarResult UpdateStaticHierarchyDeleteHeader(string headerText, List<int> StaticHierarchyIds) {
			ScarResult response = new ScarResult();
			response.StaticHierarchies = new List<StaticHierarchy>();
			string query = @"SCARRemoveHeader";

			DataTable dt = new DataTable();
			dt.Columns.Add("StaticHierarchyID", typeof(Int32));
			foreach (int i in StaticHierarchyIds) {
				dt.Rows.Add(i);
			}

			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {
				conn.Open();
				using (SqlCommand cmd = new SqlCommand(query, conn)) {
					cmd.CommandType = System.Data.CommandType.StoredProcedure;
					cmd.Parameters.AddWithValue("@HeaderText", headerText);
					cmd.Parameters.AddWithValue("@StaticHierarchyList", dt);
					using (SqlDataReader reader = cmd.ExecuteReader()) {
						while (reader.Read()) {
							StaticHierarchy sh = new StaticHierarchy
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
								Cells = new List<SCARAPITableCell>()
							};
							response.StaticHierarchies.Add(sh);
						}
					}
				}
			}
			return response;
		}

		public ScarResult UpdateStaticHierarchyLabel(int id, string newLabel) {

			string query = @"

DECLARE @OrigDescription varchar(1024) = (SELECT Description FROM StaticHierarchy WHERE ID = @TargetSHID)
DECLARE @OrigHierarchyLabel varchar(1024) 
DECLARE @NewHierarchyLabel varchar(1024)  

 SET @OrigHierarchyLabel = (SELECT dbo.GetHierarchyLabelSafe(Description) + '[' + dbo.GetEndLabelSafe(Description) + ']' FROM StaticHierarchy WHERE ID = @TargetSHID)
 SET @NewHierarchyLabel = dbo.GetHierarchyLabelSafe(@OrigDescription) + '[' + @NewEndLabel + ']'
	 UPDATE StaticHierarchy
	SET Description = (dbo.GetHierarchyLabelSafe(@OrigDescription) + @NewEndLabel)
	WHERE ID = @TargetSHID


;WITH CTE_Children(ID) AS(
	SELECT ID FROM StaticHierarchy WHERE ID = @TargetSHID
	UNION ALL
	SELECT sh.Id 
	FROM StaticHierarchy sh
	JOIN CTE_Children cte on sh.ParentID = cte.ID
) UPDATE sh
   SET sh.Description = REPLACE(sh.description, @OrigHierarchyLabel, @NewHierarchyLabel)
FROM CTE_Children cte
JOIN StaticHierarchy sh on cte.ID = SH.Id   


;WITH CTE_Children ([Id]
      ,[CompanyFinancialTermId]
      ,[AdjustedOrder]
      ,[TableTypeId]
      ,[Description]
      ,[HierarchyTypeId]
      ,[SeperatorFlag]
      ,[StaticHierarchyMetaId]
      ,[UnitTypeId]
      ,[IsIncomePositive]
      ,[ChildrenExpandDown]
      ,[ParentID]) AS(
	SELECT [Id]
      ,[CompanyFinancialTermId]
      ,[AdjustedOrder]
      ,[TableTypeId]
      ,[Description]
      ,[HierarchyTypeId]
      ,[SeperatorFlag]
      ,[StaticHierarchyMetaId]
      ,[UnitTypeId]
      ,[IsIncomePositive]
      ,[ChildrenExpandDown]
      ,[ParentID] FROM StaticHierarchy WHERE ID = @TargetSHID
	UNION ALL
 
	SELECT sh.[Id]
      ,sh.[CompanyFinancialTermId]
      ,sh.[AdjustedOrder]
      ,sh.[TableTypeId]
      ,sh.[Description]
      ,sh.[HierarchyTypeId]
      ,sh.[SeperatorFlag]
      ,sh.[StaticHierarchyMetaId]
      ,sh.[UnitTypeId]
      ,sh.[IsIncomePositive]
      ,sh.[ChildrenExpandDown]
      ,sh.[ParentID] 
	FROM StaticHierarchy sh
	JOIN CTE_Children cte on sh.ParentID = cte.ID
)
 
SELECT *
  FROM CTE_Children
  order by AdjustedOrder


";

			ScarResult response = new ScarResult();
			response.StaticHierarchies = new List<StaticHierarchy>();

			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {


				using (SqlCommand cmd = new SqlCommand(query, conn)) {
					conn.Open();
					cmd.Parameters.AddWithValue("@TargetSHID", id);
					cmd.Parameters.AddWithValue("@NewEndLabel", newLabel);
					using (SqlDataReader reader = cmd.ExecuteReader()) {
						while (reader.Read()) {
							StaticHierarchy sh = new StaticHierarchy
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
								Cells = new List<SCARAPITableCell>()
							};
							response.StaticHierarchies.Add(sh);
						}
					}
				}
			}
			return response;
		}

		public ScarResult UpdateStaticHierarchyAddParent(int id) {

			string query = @"SCAR_InsertStaticHierarchy_AddParent";

			ScarResult response = new ScarResult();
			response.StaticHierarchies = new List<StaticHierarchy>();

			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {


				using (SqlCommand cmd = new SqlCommand(query, conn)) {
					conn.Open();
					cmd.CommandType = System.Data.CommandType.StoredProcedure;
					cmd.Parameters.AddWithValue("@TargetSHID", id);
					using (SqlDataReader reader = cmd.ExecuteReader()) {
						reader.NextResult(); // skip select statement from CreateCFT
						while (reader.Read()) {
							StaticHierarchy sh = new StaticHierarchy
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
								Cells = new List<SCARAPITableCell>()
							};
							response.StaticHierarchies.Add(sh);
						}
					}
				}
			}
			return response;
		}

		public ScarResult UpdateStaticHierarchySwitchChildrenOrientation(int id) {
			const string SQL_SwitchChildrenOrientation = @"

UPDATE StaticHierarchy set ChildrenExpandDown = CASE WHEN ChildrenExpandDown = 1 THEN 0 ELSE 1 END
																WHERE ID = @TargetSHID; 

SELECT * FROM StaticHierarchy WHERE ID = @TargetSHID; 
";
			ScarResult response = new ScarResult();
			response.StaticHierarchies = new List<StaticHierarchy>();

			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {


				using (SqlCommand cmd = new SqlCommand(SQL_SwitchChildrenOrientation, conn)) {
					conn.Open();
					cmd.Parameters.AddWithValue("@TargetSHID", id);
					using (SqlDataReader reader = cmd.ExecuteReader()) {
						while (reader.Read()) {
							StaticHierarchy sh = new StaticHierarchy
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
								Cells = new List<SCARAPITableCell>()
							};
							response.StaticHierarchies.Add(sh);
						}
					}
				}
			}
			return response;
		}

		public ScarResult UpdateStaticHierarchyMove(int id, string direction) {
			string BeginTran = @" 
";
			string RollbackTran = @" 
";
			string SQL_MoveDown = @"
 
DECLARE @tableTypeId INT  = (SELECT TOP 1 [TableTypeId] from [StaticHierarchy] where id = @DraggedSHID)
DECLARE @adjustedOrder INT = (SELECT TOP 1 AdjustedOrder from [StaticHierarchy] where id = @DraggedSHID)
DECLARE @Description varchar(60) = (SELECT TOP 1 Description from [StaticHierarchy] where id = @DraggedSHID)
DECLARE @maxTargetId INT = (SELECT TOP 1 Id from [StaticHierarchy] where TableTypeId = @tableTypeId order by AdjustedOrder desc)
DECLARE @TargetSHID INT;

	select TOP 1 @TargetSHID= sh.id
		FROM  StaticHierarchy sh WITH (NOLOCK) 
		where   SH.TableTypeId=  @tableTypeId  
		and  sh.AdjustedOrder > @adjustedOrder and sh.description not like '%' + dbo.GetEndLabelSafe(@Description) + '%'
		ORDER BY sh.AdjustedOrder  

  SET @TargetSHID = ISNULL(@TargetSHID, @maxTargetId)
  EXEC prcUpd_FFDocHist_UpdateStaticHierarchy_DragDrop @DraggedSHID, @TargetSHID, 'BOTTOM'

";
			string SQL_MoveUp = @"
 
DECLARE @tableTypeId INT  = (SELECT TOP 1 [TableTypeId] from [StaticHierarchy] where id = @DraggedSHID)
DECLARE @adjustedOrder INT = (SELECT TOP 1 AdjustedOrder from [StaticHierarchy] where id = @DraggedSHID)
DECLARE @Description varchar(60) = (SELECT TOP 1 Description from [StaticHierarchy] where id = @DraggedSHID)
DECLARE @maxTargetId INT = (SELECT TOP 1 Id from [StaticHierarchy] where TableTypeId = @tableTypeId order by AdjustedOrder)
DECLARE @TargetSHID INT;

	select TOP 1 @TargetSHID= sh.id
		FROM  StaticHierarchy sh WITH (NOLOCK) 
		where   SH.TableTypeId=  @tableTypeId  
		and  sh.AdjustedOrder < @adjustedOrder and sh.description not like '%' + dbo.GetEndLabelSafe(@Description) + '%'
		ORDER BY sh.AdjustedOrder desc  

  SET @TargetSHID = ISNULL(@TargetSHID, @maxTargetId)
  EXEC prcUpd_FFDocHist_UpdateStaticHierarchy_DragDrop @DraggedSHID, @TargetSHID, 'TOP'

";
			string SQL_MoveLeft = @"
 
DECLARE @tableTypeId INT  = (SELECT TOP 1 [TableTypeId] from [StaticHierarchy] where id = @DraggedSHID)
DECLARE @adjustedOrder INT = (SELECT TOP 1 AdjustedOrder from [StaticHierarchy] where id = @DraggedSHID)
DECLARE @Description varchar(60) = (SELECT TOP 1 Description from [StaticHierarchy] where id = @DraggedSHID)
DECLARE @maxTargetId INT = (SELECT TOP 1 Id from [StaticHierarchy] where TableTypeId = @tableTypeId order by AdjustedOrder)
DECLARE @TargetSHID INT;

DECLARE @DraggedParentId int = (SELECT TOP 1 ParentID from [StaticHierarchy] where id = @DraggedSHID)
DECLARE @DraggedParentParentId int = (SELECT TOP 1 ParentID from [StaticHierarchy] where id = @DraggedParentId)


SELECT *
  FROM [ffdocumenthistory].[dbo].[StaticHierarchy] where tabletypeid = @tableTypeId
  order by AdjustedOrder


IF (@DraggedParentId IS NOT NULL)
BEGIN
	EXEC prcUpd_FFDocHist_UpdateStaticHierarchy_DragDrop @DraggedSHID, @DraggedParentId, 'BOTTOM'
END
ELSE
BEGIN

	DECLARE cur CURSOR LOCAL FAST_FORWARD FOR
		SELECT ID FROM [StaticHierarchy] WHERE id <> @DraggedSHID and parentid = @DraggedParentId
	OPEN cur
	FETCH NEXT FROM cur INTO @TargetSHID
	WHILE @@FETCH_STATUS = 0 
	BEGIN
		EXEC prcUpd_FFDocHist_UpdateStaticHierarchy_DragDrop @DraggedSHID, @DraggedParentId, 'MIDDLE'
	END
END

";

			string query = @"
DECLARE @tableTypeId2 INT = (SELECT TOP 1 [TableTypeId] from [StaticHierarchy] where id = @DraggedSHID)
SELECT *
  FROM [ffdocumenthistory].[dbo].[StaticHierarchy] where tabletypeid = @tableTypeId2
  order by AdjustedOrder

			";
			switch (direction.ToUpper()) {
				case "UP": query = SQL_MoveUp + query; break;
				case "DOWN": query = SQL_MoveDown + query; break;
				case "LEFT": query = SQL_MoveLeft + query; break;
			}
			query = BeginTran + query + RollbackTran;

			ScarResult response = new ScarResult();
			response.StaticHierarchies = new List<StaticHierarchy>();

			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {


				using (SqlCommand cmd = new SqlCommand(query, conn)) {
					conn.Open();
					cmd.Parameters.AddWithValue("@DraggedSHID", id);
					using (SqlDataReader reader = cmd.ExecuteReader()) {
						while (reader.Read()) {
							StaticHierarchy sh = new StaticHierarchy
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
								Cells = new List<SCARAPITableCell>()
							};
							response.StaticHierarchies.Add(sh);
						}
					}
				}
			}
			return response;
		}

		public ScarResult DragDropStaticHierarchyLabel(int DraggedId, int TargetId, string Location) {

			string query = @"
DECLARE @TargetParentId INT = (select ParentID from StaticHierarchy where id = @TargetSHID)

exec prcUpd_FFDocHist_UpdateStaticHierarchy_DragDrop @DraggedSHID, @TargetSHID , @Location

;WITH CTE_Children ([Id]
      ,[CompanyFinancialTermId]
      ,[AdjustedOrder]
      ,[TableTypeId]
      ,[Description]
      ,[HierarchyTypeId]
      ,[SeperatorFlag]
      ,[StaticHierarchyMetaId]
      ,[UnitTypeId]
      ,[IsIncomePositive]
      ,[ChildrenExpandDown]
      ,[ParentID]) AS(
	SELECT [Id]
      ,[CompanyFinancialTermId]
      ,[AdjustedOrder]
      ,[TableTypeId]
      ,[Description]
      ,[HierarchyTypeId]
      ,[SeperatorFlag]
      ,[StaticHierarchyMetaId]
      ,[UnitTypeId]
      ,[IsIncomePositive]
      ,[ChildrenExpandDown]
      ,[ParentID] FROM StaticHierarchy WHERE ID = @DraggedSHID
	UNION ALL
 
	SELECT sh.[Id]
      ,sh.[CompanyFinancialTermId]
      ,sh.[AdjustedOrder]
      ,sh.[TableTypeId]
      ,sh.[Description]
      ,sh.[HierarchyTypeId]
      ,sh.[SeperatorFlag]
      ,sh.[StaticHierarchyMetaId]
      ,sh.[UnitTypeId]
      ,sh.[IsIncomePositive]
      ,sh.[ChildrenExpandDown]
      ,sh.[ParentID] 
	FROM StaticHierarchy sh
	JOIN CTE_Children cte on sh.ParentID = cte.ID
)
 
SELECT *
  FROM CTE_Children
";

			ScarResult response = new ScarResult();
			response.StaticHierarchies = new List<StaticHierarchy>();

			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {


				using (SqlCommand cmd = new SqlCommand(query, conn)) {
					conn.Open();
					cmd.Parameters.AddWithValue("@DraggedSHID", DraggedId);
					cmd.Parameters.AddWithValue("@TargetSHID", TargetId);
					cmd.Parameters.AddWithValue("@Location", Location);
					using (SqlDataReader reader = cmd.ExecuteReader()) {
						while (reader.Read()) {
							StaticHierarchy sh = new StaticHierarchy
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
								Cells = new List<SCARAPITableCell>()
							};
							response.StaticHierarchies.Add(sh);
						}
					}
				}
			}
			return response;
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

		public ScarResult UpdateTimeSliceIsSummary(int id, string TableType) {

			string query = @"


IF EXISTS(SELECT TOP 1 DocumentTimeSliceID FROM DocumentTimeSliceTableTypeIsSummary WHERE DocumentTimeSliceID = @id and TableType = @TableType)
BEGIN
	DELETE FROM DocumentTimeSliceTableTypeIsSummary WHERE DocumentTimeSliceID = @id and TableType = @TableType
END
ELSE
BEGIN
	INSERT DocumentTimeSliceTableTypeIsSummary ([DocumentTimeSliceID],[TableType])
	VALUES (@Id, @TableType)
	SELECT * FROM DocumentTimeSlice WHERE ID = @id
END



";
			ScarResult response = new ScarResult();
			response.TimeSlices = new List<TimeSlice>();
			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {

				using (SqlCommand cmd = new SqlCommand(query, conn)) {
					conn.Open();
					cmd.Parameters.AddWithValue("@id", id);
					cmd.Parameters.AddWithValue("@TableType", TableType);
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
								ManualOrgSet = reader.GetBoolean(18)
							};
							response.TimeSlices.Add(slice);
						}
					}
				}
			}
			return response;
		}

		public ScarResult UpdateTimeSlicePeriodNote(int id, string PeriodNoteId) {

			string query = @"


IF @PeriodNoteId is null
BEGIN
	DELETE FROM DocumentTimeSlicePeriodNotes WHERE DocumentTimeSliceID = @id  
END
ELSE
BEGIN
	MERGE INTO DocumentTimeSlicePeriodNotes d
	USING (VALUES (@id)) AS s(id) ON  d.DocumentTimeSliceID = s.id
	WHEN NOT MATCHED THEN
			INSERT ([DocumentTimeSliceID] ,[PeriodNoteID])
				VALUES (@Id, @PeriodNoteId)
	WHEN MATCHED THEN
		UPDATE SET [PeriodNoteID] = @PeriodNoteId ;

   SELECT * FROM DocumentTimeSlice WHERE ID = @id
END



";
			ScarResult response = new ScarResult();
			response.TimeSlices = new List<TimeSlice>();
			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {

				using (SqlCommand cmd = new SqlCommand(query, conn)) {
					conn.Open();
					int periodNoteId = -1;
					bool isSuccess = false;
					if (!string.IsNullOrEmpty(PeriodNoteId)) {
						isSuccess = Int32.TryParse(PeriodNoteId, out periodNoteId);
					}
					cmd.Parameters.AddWithValue("@id", id);
					cmd.Parameters.Add(new SqlParameter("@PeriodNoteId", SqlDbType.Int)
					{
						Value = (!isSuccess ? DBNull.Value : (object)periodNoteId)
					});
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
								ManualOrgSet = reader.GetBoolean(18)
							};
							response.TimeSlices.Add(slice);
						}
					}
				}
			}
			return response;
		}

		public ScarResult UpdateTimeSliceReportType(int id, string ReportType) {

			string query = @"

declare @docid uniqueidentifier = (select documentid from DocumentTimeSlice where id = @id)
UPDATE DocumentTimeSlice SET ReportType = @ReportType where DocumentId = @docid;

SELECT * FROM DocumentTimeSlice WHERE DocumentId = @docid;

";
			ScarResult response = new ScarResult();
			response.TimeSlices = new List<TimeSlice>();
			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {

				using (SqlCommand cmd = new SqlCommand(query, conn)) {
					conn.Open();
					cmd.Parameters.AddWithValue("@id", id);
					cmd.Parameters.AddWithValue("@ReportType", ReportType);
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
								ManualOrgSet = reader.GetBoolean(18)
							};
							response.TimeSlices.Add(slice);
						}
					}
				}
			}
			return response;
		}

		public ScarResult UpdateTimeSliceManualOrgSet(int id, string newValue) {

			string query = @"

UPDATE DocumentTimeSlice SET ManualOrgSet = @newValue where Id = @id;

SELECT * FROM DocumentTimeSlice WHERE id = @id;

";
			ScarResult response = new ScarResult();
			response.TimeSlices = new List<TimeSlice>();
			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {

				using (SqlCommand cmd = new SqlCommand(query, conn)) {
					conn.Open();
					cmd.Parameters.AddWithValue("@id", id);
					cmd.Parameters.AddWithValue("@newValue", newValue);
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
								ManualOrgSet = reader.GetBoolean(18)
							};
							response.TimeSlices.Add(slice);
						}
					}
				}
			}
			return response;
		}

		public ScarResult GetTableCell(string id) {

			string query = @"

SELECT 'x', * FROM TableCell WHERE ID = @id;

";
			ScarResult response = new ScarResult();
			response.StaticHierarchies = new List<StaticHierarchy>();
			var sh = new StaticHierarchy();
			response.StaticHierarchies.Add(sh);
			sh.Description = @"";
			sh.Cells = new List<SCARAPITableCell>();
			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {

				using (SqlCommand cmd = new SqlCommand(query, conn)) {
					conn.Open();
					cmd.Parameters.AddWithValue("@id", id);
					using (SqlDataReader reader = cmd.ExecuteReader()) {
						while (reader.Read()) {
							SCARAPITableCell cell;
							if (reader.GetNullable<int>(1).HasValue) {
								cell = new SCARAPITableCell
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
								};
								cell.ScarUpdated = reader.GetBoolean(20);
								cell.IsIncomePositive = reader.GetBoolean(21);
								cell.XBRLTag = reader.GetStringSafe(22);
								//cell.UpdateStampUTC = reader.GetNullable<DateTime>(23);
								cell.DocumentID = reader.IsDBNull(23) ? Guid.Empty : reader.GetGuid(23);
								cell.Label = reader.GetStringSafe(24);
								sh.Cells.Add(cell);
							}
						}
					}
				}
			}
			return response;
		}

		public ScarResult UpdateTableCellMetaNumericValue(string id, string NumericValue) {

			string query = @"

UPDATE TableCell SET ValueNumeric = @NumericValue where ID = @id;

SELECT 'x', * FROM TableCell WHERE ID = @id;

";
			ScarResult response = new ScarResult();
			response.StaticHierarchies = new List<StaticHierarchy>();
			var sh = new StaticHierarchy();
			response.StaticHierarchies.Add(sh);
			sh.Description = @"";
			sh.Cells = new List<SCARAPITableCell>();
			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {

				using (SqlCommand cmd = new SqlCommand(query, conn)) {
					conn.Open();
					cmd.Parameters.AddWithValue("@id", id);
					cmd.Parameters.AddWithValue("@NumericValue", NumericValue);
					using (SqlDataReader reader = cmd.ExecuteReader()) {
						while (reader.Read()) {
							SCARAPITableCell cell;
							if (reader.GetNullable<int>(1).HasValue) {
								cell = new SCARAPITableCell
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
								};
								cell.ScarUpdated = reader.GetBoolean(20);
								cell.IsIncomePositive = reader.GetBoolean(21);
								cell.XBRLTag = reader.GetStringSafe(22);
								//cell.UpdateStampUTC = reader.GetNullable<DateTime>(23);
								cell.DocumentID = reader.IsDBNull(23) ? Guid.Empty : reader.GetGuid(23);
								cell.Label = reader.GetStringSafe(24);
								sh.Cells.Add(cell);
							}
						}
					}
				}
			}
			return response;
		}
		public ScarResult UpdateTableCellMetaScalingFactor(string id, string newValue) {

			string query = @"

UPDATE TableCell SET ScalingFactorID = @ScalingFactorID where ID = @id;

SELECT 'x', * FROM TableCell WHERE ID = @id;

";
			ScarResult response = new ScarResult();
			response.StaticHierarchies = new List<StaticHierarchy>();
			var sh = new StaticHierarchy();
			response.StaticHierarchies.Add(sh);
			sh.Description = @"";
			sh.Cells = new List<SCARAPITableCell>();
			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {

				using (SqlCommand cmd = new SqlCommand(query, conn)) {
					conn.Open();
					cmd.Parameters.AddWithValue("@id", id);
					cmd.Parameters.AddWithValue("@ScalingFactorID", newValue);
					using (SqlDataReader reader = cmd.ExecuteReader()) {
						while (reader.Read()) {
							SCARAPITableCell cell;
							if (reader.GetNullable<int>(1).HasValue) {
								cell = new SCARAPITableCell
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
								};
								cell.ScarUpdated = reader.GetBoolean(20);
								cell.IsIncomePositive = reader.GetBoolean(21);
								cell.XBRLTag = reader.GetStringSafe(22);
								//cell.UpdateStampUTC = reader.GetNullable<DateTime>(23);
								cell.DocumentID = reader.IsDBNull(23) ? Guid.Empty : reader.GetGuid(23);
								cell.Label = reader.GetStringSafe(24);
								sh.Cells.Add(cell);
							}
						}
					}
				}
			}
			return response;
		}
		public ScarResult UpdateTableCellMetaPeriodDate(string id, string newValue) {

			string query = @"

UPDATE TableCell SET CellDate = @CellDate where ID = @id;
UPDATE TableCell SET CellDay = DATEPART(day, @CellDate) where ID = @id;
UPDATE TableCell SET CellMonth = DATEPART(month, @CellDate) where ID = @id;
UPDATE TableCell SET CellYear = DATEPART(year, @CellDate) where ID = @id;

SELECT 'x', * FROM TableCell WHERE ID = @id;

";
			ScarResult response = new ScarResult();
			response.StaticHierarchies = new List<StaticHierarchy>();
			var sh = new StaticHierarchy();
			response.StaticHierarchies.Add(sh);
			sh.Description = @"";
			sh.Cells = new List<SCARAPITableCell>();
			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {

				using (SqlCommand cmd = new SqlCommand(query, conn)) {
					conn.Open();
					cmd.Parameters.AddWithValue("@id", id);
					cmd.Parameters.AddWithValue("@CellDate", newValue);
					using (SqlDataReader reader = cmd.ExecuteReader()) {
						while (reader.Read()) {
							SCARAPITableCell cell;
							if (reader.GetNullable<int>(1).HasValue) {
								cell = new SCARAPITableCell
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
								};
								cell.ScarUpdated = reader.GetBoolean(20);
								cell.IsIncomePositive = reader.GetBoolean(21);
								cell.XBRLTag = reader.GetStringSafe(22);
								//cell.UpdateStampUTC = reader.GetNullable<DateTime>(23);
								cell.DocumentID = reader.IsDBNull(23) ? Guid.Empty : reader.GetGuid(23);
								cell.Label = reader.GetStringSafe(24);
								sh.Cells.Add(cell);
							}
						}
					}
				}
			}
			return response;
		}
		public ScarResult UpdateTableCellMetaPeriodType(string id, string newValue) {

			string query = @"

UPDATE TableCell SET PeriodTypeID = @PeriodTypeID where ID = @id;
UPDATE TableCell SET CellPeriodType = (select top 1 [Description] from [PeriodType] where ID = @PeriodTypeID) where ID = @id;


SELECT 'x', * FROM TableCell WHERE ID = @id;

";
			ScarResult response = new ScarResult();
			response.StaticHierarchies = new List<StaticHierarchy>();
			var sh = new StaticHierarchy();
			response.StaticHierarchies.Add(sh);
			sh.Description = @"";
			sh.Cells = new List<SCARAPITableCell>();
			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {

				using (SqlCommand cmd = new SqlCommand(query, conn)) {
					conn.Open();
					cmd.Parameters.AddWithValue("@id", id);
					cmd.Parameters.AddWithValue("@PeriodTypeID", newValue);
					using (SqlDataReader reader = cmd.ExecuteReader()) {
						while (reader.Read()) {
							SCARAPITableCell cell;
							if (reader.GetNullable<int>(1).HasValue) {
								cell = new SCARAPITableCell
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
								};
								cell.ScarUpdated = reader.GetBoolean(20);
								cell.IsIncomePositive = reader.GetBoolean(21);
								cell.XBRLTag = reader.GetStringSafe(22);
								//cell.UpdateStampUTC = reader.GetNullable<DateTime>(23);
								cell.DocumentID = reader.IsDBNull(23) ? Guid.Empty : reader.GetGuid(23);
								cell.Label = reader.GetStringSafe(24);
								sh.Cells.Add(cell);
							}
						}
					}
				}
			}
			return response;
		}
		public ScarResult UpdateTableCellMetaPeriodLength(string id, string newValue) {

			string query = @"

UPDATE TableCell SET PeriodLength = @PeriodLength where ID = @id;

SELECT 'x', * FROM TableCell WHERE ID = @id;

";
			ScarResult response = new ScarResult();
			response.StaticHierarchies = new List<StaticHierarchy>();
			var sh = new StaticHierarchy();
			response.StaticHierarchies.Add(sh);
			sh.Description = @"";
			sh.Cells = new List<SCARAPITableCell>();
			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {

				using (SqlCommand cmd = new SqlCommand(query, conn)) {
					conn.Open();
					cmd.Parameters.AddWithValue("@id", id);
					cmd.Parameters.AddWithValue("@PeriodLength", newValue);
					using (SqlDataReader reader = cmd.ExecuteReader()) {
						while (reader.Read()) {
							SCARAPITableCell cell;
							if (reader.GetNullable<int>(1).HasValue) {
								cell = new SCARAPITableCell
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
								};
								cell.ScarUpdated = reader.GetBoolean(20);
								cell.IsIncomePositive = reader.GetBoolean(21);
								cell.XBRLTag = reader.GetStringSafe(22);
								//cell.UpdateStampUTC = reader.GetNullable<DateTime>(23);
								cell.DocumentID = reader.IsDBNull(23) ? Guid.Empty : reader.GetGuid(23);
								cell.Label = reader.GetStringSafe(24);
								sh.Cells.Add(cell);
							}
						}
					}
				}
			}
			return response;
		}
		public ScarResult UpdateTableCellMetaCurrency(string id, string newValue) {

			string query = @"

UPDATE TableCell SET CurrencyCode = @CurrencyCode where ID = @id;
UPDATE TableCell SET Currency = (select top 1 [Description] from [Currencies] where [Code] = @CurrencyCode) where ID = @id;

SELECT 'x', * FROM TableCell WHERE ID = @id;

";
			ScarResult response = new ScarResult();
			response.StaticHierarchies = new List<StaticHierarchy>();
			var sh = new StaticHierarchy();
			response.StaticHierarchies.Add(sh);
			sh.Description = @"";
			sh.Cells = new List<SCARAPITableCell>();
			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {

				using (SqlCommand cmd = new SqlCommand(query, conn)) {
					conn.Open();
					cmd.Parameters.AddWithValue("@id", id);
					cmd.Parameters.AddWithValue("@CurrencyCode", newValue);
					using (SqlDataReader reader = cmd.ExecuteReader()) {
						while (reader.Read()) {
							SCARAPITableCell cell;
							if (reader.GetNullable<int>(1).HasValue) {
								cell = new SCARAPITableCell
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
								};
								cell.ScarUpdated = reader.GetBoolean(20);
								cell.IsIncomePositive = reader.GetBoolean(21);
								cell.XBRLTag = reader.GetStringSafe(22);
								//cell.UpdateStampUTC = reader.GetNullable<DateTime>(23);
								cell.DocumentID = reader.IsDBNull(23) ? Guid.Empty : reader.GetGuid(23);
								cell.Label = reader.GetStringSafe(24);
								sh.Cells.Add(cell);
							}
						}
					}
				}
			}
			return response;
		}

		public ScarResult UpdateTableRowMetaCusip(string id, string newValue) {

			string query = @"

update tc
set cusip = @cusip
from [DimensionToCell] dtc 
JOIN [TableDimension] td on dtc.TableDimensionID = td.ID and td.DimensionTypeID = 1
JOIN [DimensionToCell] dtc2 on td.ID = dtc2.TableDimensionID
JOIN [TableCell] tc on tc.id = dtc2.TableCellID
where dtc.TableCellID = @id


select  'x', tc.* 
from [DimensionToCell] dtc 
JOIN [TableDimension] td on dtc.TableDimensionID = td.ID and td.DimensionTypeID = 1
JOIN [DimensionToCell] dtc2 on td.ID = dtc2.TableDimensionID
JOIN [TableCell] tc on tc.id = dtc2.TableCellID
where dtc.TableCellID = @id


";
			ScarResult response = new ScarResult();
			response.StaticHierarchies = new List<StaticHierarchy>();
			var sh = new StaticHierarchy();
			response.StaticHierarchies.Add(sh);
			sh.Description = @"";
			sh.Cells = new List<SCARAPITableCell>();
			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {

				using (SqlCommand cmd = new SqlCommand(query, conn)) {
					conn.Open();
					cmd.Parameters.AddWithValue("@id", id);
					cmd.Parameters.AddWithValue("@cusip", newValue);
					using (SqlDataReader reader = cmd.ExecuteReader()) {
						while (reader.Read()) {
							SCARAPITableCell cell;
							if (reader.GetNullable<int>(1).HasValue) {
								cell = new SCARAPITableCell
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
								};
								cell.ScarUpdated = reader.GetBoolean(20);
								cell.IsIncomePositive = reader.GetBoolean(21);
								cell.XBRLTag = reader.GetStringSafe(22);
								//cell.UpdateStampUTC = reader.GetNullable<DateTime>(23);
								cell.DocumentID = reader.IsDBNull(23) ? Guid.Empty : reader.GetGuid(23);
								cell.Label = reader.GetStringSafe(24);
								sh.Cells.Add(cell);
							}
						}
					}
				}
			}
			return response;
		}
		public ScarResult UpdateTableRowMetaPit(string id, string newValue) {

			string query = @"

update tc
set PeriodTypeID = 'P', CellPeriodType = 'PIT', PeriodLength = 0
from [DimensionToCell] dtc 
JOIN [TableDimension] td on dtc.TableDimensionID = td.ID and td.DimensionTypeID = 1
JOIN [DimensionToCell] dtc2 on td.ID = dtc2.TableDimensionID
JOIN [TableCell] tc on tc.id = dtc2.TableCellID
where dtc.TableCellID = @id


select  'x', tc.* 
from [DimensionToCell] dtc 
JOIN [TableDimension] td on dtc.TableDimensionID = td.ID and td.DimensionTypeID = 1
JOIN [DimensionToCell] dtc2 on td.ID = dtc2.TableDimensionID
JOIN [TableCell] tc on tc.id = dtc2.TableCellID
where dtc.TableCellID = @id


";
			ScarResult response = new ScarResult();
			response.StaticHierarchies = new List<StaticHierarchy>();
			var sh = new StaticHierarchy();
			response.StaticHierarchies.Add(sh);
			sh.Description = @"";
			sh.Cells = new List<SCARAPITableCell>();
			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {

				using (SqlCommand cmd = new SqlCommand(query, conn)) {
					conn.Open();
					cmd.Parameters.AddWithValue("@id", id);
					using (SqlDataReader reader = cmd.ExecuteReader()) {
						while (reader.Read()) {
							SCARAPITableCell cell;
							if (reader.GetNullable<int>(1).HasValue) {
								cell = new SCARAPITableCell
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
								};
								cell.ScarUpdated = reader.GetBoolean(20);
								cell.IsIncomePositive = reader.GetBoolean(21);
								cell.XBRLTag = reader.GetStringSafe(22);
								//cell.UpdateStampUTC = reader.GetNullable<DateTime>(23);
								cell.DocumentID = reader.IsDBNull(23) ? Guid.Empty : reader.GetGuid(23);
								cell.Label = reader.GetStringSafe(24);
								sh.Cells.Add(cell);
							}
						}
					}
				}
			}
			return response;
		}
		public ScarResult UpdateTableRowMetaScalingFactor(string id, string newValue) {

			string query = @"

update tc
set ScalingFactorID = @ScalingFactorID
from [DimensionToCell] dtc 
JOIN [TableDimension] td on dtc.TableDimensionID = td.ID and td.DimensionTypeID = 1
JOIN [DimensionToCell] dtc2 on td.ID = dtc2.TableDimensionID
JOIN [TableCell] tc on tc.id = dtc2.TableCellID
where dtc.TableCellID = @id


select  'x', tc.* 
from [DimensionToCell] dtc 
JOIN [TableDimension] td on dtc.TableDimensionID = td.ID and td.DimensionTypeID = 1
JOIN [DimensionToCell] dtc2 on td.ID = dtc2.TableDimensionID
JOIN [TableCell] tc on tc.id = dtc2.TableCellID
where dtc.TableCellID = @id


";
			ScarResult response = new ScarResult();
			response.StaticHierarchies = new List<StaticHierarchy>();
			var sh = new StaticHierarchy();
			response.StaticHierarchies.Add(sh);
			sh.Description = @"";
			sh.Cells = new List<SCARAPITableCell>();
			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {

				using (SqlCommand cmd = new SqlCommand(query, conn)) {
					conn.Open();
					cmd.Parameters.AddWithValue("@id", id);
					cmd.Parameters.AddWithValue("@ScalingFactorID", newValue);
					using (SqlDataReader reader = cmd.ExecuteReader()) {
						while (reader.Read()) {
							SCARAPITableCell cell;
							if (reader.GetNullable<int>(1).HasValue) {
								cell = new SCARAPITableCell
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
								};
								cell.ScarUpdated = reader.GetBoolean(20);
								cell.IsIncomePositive = reader.GetBoolean(21);
								cell.XBRLTag = reader.GetStringSafe(22);
								//cell.UpdateStampUTC = reader.GetNullable<DateTime>(23);
								cell.DocumentID = reader.IsDBNull(23) ? Guid.Empty : reader.GetGuid(23);
								cell.Label = reader.GetStringSafe(24);
								sh.Cells.Add(cell);
							}
						}
					}
				}
			}
			return response;
		}

		public ScarResult UpdateTableColumnMetaPeriodDate(string id, string newValue) {

			string query = @"
update tc
set CellDate = @cellDate,  CellDay = DATEPART(day, @CellDate), CellMonth = DATEPART(month, @CellDate), CellYear = DATEPART(year, @CellDate)
from [DimensionToCell] dtc 
JOIN [TableDimension] td on dtc.TableDimensionID = td.ID and td.DimensionTypeID = 2
JOIN [DimensionToCell] dtc2 on td.ID = dtc2.TableDimensionID
JOIN [TableCell] tc on tc.id = dtc2.TableCellID
where dtc.TableCellID = @id

";

			string select_query = @"

select  'x', tc.* 
from [DimensionToCell] dtc 
JOIN [TableDimension] td on dtc.TableDimensionID = td.ID and td.DimensionTypeID = 2
JOIN [DimensionToCell] dtc2 on td.ID = dtc2.TableDimensionID
JOIN [TableCell] tc on tc.id = dtc2.TableCellID
where dtc.TableCellID = @id

";


			ScarResult response = new ScarResult();
			string sql_query = select_query;
			DateTime newDate;
			if (DateTime.TryParse(newValue, out newDate)) {
				sql_query = query + select_query;
			}
			response.StaticHierarchies = new List<StaticHierarchy>();
			var sh = new StaticHierarchy();
			response.StaticHierarchies.Add(sh);
			sh.Description = @"";
			sh.Cells = new List<SCARAPITableCell>();
			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {

				using (SqlCommand cmd = new SqlCommand(sql_query, conn)) {
					conn.Open();
					cmd.Parameters.AddWithValue("@id", id);
					cmd.Parameters.AddWithValue("@cellDate", newDate);
					using (SqlDataReader reader = cmd.ExecuteReader()) {
						while (reader.Read()) {
							SCARAPITableCell cell;
							if (reader.GetNullable<int>(1).HasValue) {
								cell = new SCARAPITableCell
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
								};
								cell.ScarUpdated = reader.GetBoolean(20);
								cell.IsIncomePositive = reader.GetBoolean(21);
								cell.XBRLTag = reader.GetStringSafe(22);
								//cell.UpdateStampUTC = reader.GetNullable<DateTime>(23);
								cell.DocumentID = reader.IsDBNull(23) ? Guid.Empty : reader.GetGuid(23);
								cell.Label = reader.GetStringSafe(24);
								sh.Cells.Add(cell);
							}
						}
					}
				}
			}
			return response;
		}
		public ScarResult UpdateTableColumnMetaColumnHeader(string id, string newValue) {

			string query = @"
update td
set label = @newLabel
from [DimensionToCell] dtc 
JOIN [TableDimension] td on dtc.TableDimensionID = td.ID and td.DimensionTypeID = 2
JOIN [DimensionToCell] dtc2 on td.ID = dtc2.TableDimensionID
JOIN [TableCell] tc on tc.id = dtc2.TableCellID
where dtc.TableCellID = @id

";

			string select_query = @"

select  'x', tc.* 
from [DimensionToCell] dtc 
JOIN [TableDimension] td on dtc.TableDimensionID = td.ID and td.DimensionTypeID = 2
JOIN [DimensionToCell] dtc2 on td.ID = dtc2.TableDimensionID
JOIN [TableCell] tc on tc.id = dtc2.TableCellID
where dtc.TableCellID = @id

";


			ScarResult response = new ScarResult();
			string sql_query = query + select_query;
			response.StaticHierarchies = new List<StaticHierarchy>();
			var sh = new StaticHierarchy();
			response.StaticHierarchies.Add(sh);
			sh.Description = newValue;
			sh.Cells = new List<SCARAPITableCell>();
			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {

				using (SqlCommand cmd = new SqlCommand(sql_query, conn)) {
					conn.Open();
					cmd.Parameters.AddWithValue("@id", id);
					cmd.Parameters.AddWithValue("@newLabel", newValue);
					using (SqlDataReader reader = cmd.ExecuteReader()) {
						while (reader.Read()) {
							SCARAPITableCell cell;
							if (reader.GetNullable<int>(1).HasValue) {
								cell = new SCARAPITableCell
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
								};
								cell.ScarUpdated = reader.GetBoolean(20);
								cell.IsIncomePositive = reader.GetBoolean(21);
								cell.XBRLTag = reader.GetStringSafe(22);
								//cell.UpdateStampUTC = reader.GetNullable<DateTime>(23);
								cell.DocumentID = reader.IsDBNull(23) ? Guid.Empty : reader.GetGuid(23);
								cell.Label = reader.GetStringSafe(24);
								sh.Cells.Add(cell);
							}
						}
					}
				}
			}
			return response;
		}

		public ScarResult UpdateTableColumnMetaPeriodType(string id, string newValue) {

			string query = @"
update tc
set PeriodTypeID = @newPeriodType
from [DimensionToCell] dtc 
JOIN [TableDimension] td on dtc.TableDimensionID = td.ID and td.DimensionTypeID = 2
JOIN [DimensionToCell] dtc2 on td.ID = dtc2.TableDimensionID
JOIN [TableCell] tc on tc.id = dtc2.TableCellID
where dtc.TableCellID = @id


";

			string select_query = @"

select  'x', tc.* 
from [DimensionToCell] dtc 
JOIN [TableDimension] td on dtc.TableDimensionID = td.ID and td.DimensionTypeID = 2
JOIN [DimensionToCell] dtc2 on td.ID = dtc2.TableDimensionID
JOIN [TableCell] tc on tc.id = dtc2.TableCellID
where dtc.TableCellID = @id

";


			ScarResult response = new ScarResult();
			string sql_query = query + select_query;
			response.StaticHierarchies = new List<StaticHierarchy>();
			var sh = new StaticHierarchy();
			response.StaticHierarchies.Add(sh);
			sh.Description = @"";
			sh.Cells = new List<SCARAPITableCell>();
			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {

				using (SqlCommand cmd = new SqlCommand(sql_query, conn)) {
					conn.Open();
					cmd.Parameters.AddWithValue("@id", id);
					cmd.Parameters.AddWithValue("@newPeriodType", newValue);
					using (SqlDataReader reader = cmd.ExecuteReader()) {
						while (reader.Read()) {
							SCARAPITableCell cell;
							if (reader.GetNullable<int>(1).HasValue) {
								cell = new SCARAPITableCell
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
								};
								cell.ScarUpdated = reader.GetBoolean(20);
								cell.IsIncomePositive = reader.GetBoolean(21);
								cell.XBRLTag = reader.GetStringSafe(22);
								//cell.UpdateStampUTC = reader.GetNullable<DateTime>(23);
								cell.DocumentID = reader.IsDBNull(23) ? Guid.Empty : reader.GetGuid(23);
								cell.Label = reader.GetStringSafe(24);
								sh.Cells.Add(cell);
							}
						}
					}
				}
			}
			return response;
		}

		public ScarResult UpdateTableColumnMetaPeriodLength(string id, string newValue) {

			string query = @"
update tc
set PeriodLength =  @newPeriodLength
from [DimensionToCell] dtc 
JOIN [TableDimension] td on dtc.TableDimensionID = td.ID and td.DimensionTypeID = 2
JOIN [DimensionToCell] dtc2 on td.ID = dtc2.TableDimensionID
JOIN [TableCell] tc on tc.id = dtc2.TableCellID
where dtc.TableCellID = @id


";

			string select_query = @"

select  'x', tc.* 
from [DimensionToCell] dtc 
JOIN [TableDimension] td on dtc.TableDimensionID = td.ID and td.DimensionTypeID = 2
JOIN [DimensionToCell] dtc2 on td.ID = dtc2.TableDimensionID
JOIN [TableCell] tc on tc.id = dtc2.TableCellID
where dtc.TableCellID = @id

";


			ScarResult response = new ScarResult();
			string sql_query = query + select_query;
			response.StaticHierarchies = new List<StaticHierarchy>();
			var sh = new StaticHierarchy();
			response.StaticHierarchies.Add(sh);
			sh.Description = @"";
			sh.Cells = new List<SCARAPITableCell>();
			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {

				using (SqlCommand cmd = new SqlCommand(sql_query, conn)) {
					conn.Open();
					cmd.Parameters.AddWithValue("@id", id);
					cmd.Parameters.AddWithValue("@newPeriodLength", newValue);
					using (SqlDataReader reader = cmd.ExecuteReader()) {
						while (reader.Read()) {
							SCARAPITableCell cell;
							if (reader.GetNullable<int>(1).HasValue) {
								cell = new SCARAPITableCell
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
								};
								cell.ScarUpdated = reader.GetBoolean(20);
								cell.IsIncomePositive = reader.GetBoolean(21);
								cell.XBRLTag = reader.GetStringSafe(22);
								//cell.UpdateStampUTC = reader.GetNullable<DateTime>(23);
								cell.DocumentID = reader.IsDBNull(23) ? Guid.Empty : reader.GetGuid(23);
								cell.Label = reader.GetStringSafe(24);
								sh.Cells.Add(cell);
							}
						}
					}
				}
			}
			return response;
		}

		public ScarResult UpdateTableColumnMetaCurrencyCode(string id, string newValue) {

			string query = @"
update tc
set CurrencyCode =  @newCurrencyCode
from [DimensionToCell] dtc 
JOIN [TableDimension] td on dtc.TableDimensionID = td.ID and td.DimensionTypeID = 2
JOIN [DimensionToCell] dtc2 on td.ID = dtc2.TableDimensionID
JOIN [TableCell] tc on tc.id = dtc2.TableCellID
where dtc.TableCellID = @id


";

			string select_query = @"

select  'x', tc.* 
from [DimensionToCell] dtc 
JOIN [TableDimension] td on dtc.TableDimensionID = td.ID and td.DimensionTypeID = 2
JOIN [DimensionToCell] dtc2 on td.ID = dtc2.TableDimensionID
JOIN [TableCell] tc on tc.id = dtc2.TableCellID
where dtc.TableCellID = @id

";


			ScarResult response = new ScarResult();
			string sql_query = query + select_query;
			response.StaticHierarchies = new List<StaticHierarchy>();
			var sh = new StaticHierarchy();
			response.StaticHierarchies.Add(sh);
			sh.Description = @"";
			sh.Cells = new List<SCARAPITableCell>();
			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {

				using (SqlCommand cmd = new SqlCommand(sql_query, conn)) {
					conn.Open();
					cmd.Parameters.AddWithValue("@id", id);
					cmd.Parameters.AddWithValue("@newCurrencyCode", newValue);
					using (SqlDataReader reader = cmd.ExecuteReader()) {
						while (reader.Read()) {
							SCARAPITableCell cell;
							if (reader.GetNullable<int>(1).HasValue) {
								cell = new SCARAPITableCell
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
								};
								cell.ScarUpdated = reader.GetBoolean(20);
								cell.IsIncomePositive = reader.GetBoolean(21);
								cell.XBRLTag = reader.GetStringSafe(22);
								//cell.UpdateStampUTC = reader.GetNullable<DateTime>(23);
								cell.DocumentID = reader.IsDBNull(23) ? Guid.Empty : reader.GetGuid(23);
								cell.Label = reader.GetStringSafe(24);
								sh.Cells.Add(cell);
							}
						}
					}
				}
			}
			return response;
		}

		public ScarResult UpdateTableColumnMetaInterimType(string id, string newValue) {
			// TODO:
			// need to handle "--" interim type
			string query = @"

DECLARE @newDtsID int = 0;

update dtstc
set DocumentTimeSliceId =  @newDtsID
from [DimensionToCell] dtc 
JOIN [TableDimension] td on dtc.TableDimensionID = td.ID and td.DimensionTypeID = 2
JOIN [DimensionToCell] dtc2 on td.ID = dtc2.TableDimensionID
JOIN [TableCell] tc on tc.id = dtc2.TableCellID
JOIN DocumentTimeSliceTableCell dtstc on dtstc.TableCellId = tc.id
where dtc.TableCellID = @id



";

			string select_query = @"

select  'x', tc.* 
from [DimensionToCell] dtc 
JOIN [TableDimension] td on dtc.TableDimensionID = td.ID and td.DimensionTypeID = 2
JOIN [DimensionToCell] dtc2 on td.ID = dtc2.TableDimensionID
JOIN [TableCell] tc on tc.id = dtc2.TableCellID
where dtc.TableCellID = @id

";


			ScarResult response = new ScarResult();
			string sql_query = select_query;
			if (newValue != "--") {
				sql_query = query + select_query;
			}
			response.StaticHierarchies = new List<StaticHierarchy>();
			var sh = new StaticHierarchy();
			response.StaticHierarchies.Add(sh);
			sh.Description = @"";
			sh.Cells = new List<SCARAPITableCell>();
			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {

				using (SqlCommand cmd = new SqlCommand(sql_query, conn)) {
					conn.Open();
					cmd.Parameters.AddWithValue("@id", id);
					cmd.Parameters.AddWithValue("@newCurrencyCode", newValue);
					using (SqlDataReader reader = cmd.ExecuteReader()) {
						while (reader.Read()) {
							SCARAPITableCell cell;
							if (reader.GetNullable<int>(1).HasValue) {
								cell = new SCARAPITableCell
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
								};
								cell.ScarUpdated = reader.GetBoolean(20);
								cell.IsIncomePositive = reader.GetBoolean(21);
								cell.XBRLTag = reader.GetStringSafe(22);
								//cell.UpdateStampUTC = reader.GetNullable<DateTime>(23);
								cell.DocumentID = reader.IsDBNull(23) ? Guid.Empty : reader.GetGuid(23);
								cell.Label = reader.GetStringSafe(24);
								sh.Cells.Add(cell);
							}
						}
					}
				}
			}
			return response;
		}


		public class JsonToSQL {
			protected string _json;
			public JsonToSQL(string json) {
				_json = json;

			}

			public virtual string Translate() {
				return "--";
			}
		}


		private string ConvertJsonStringToSQL(string jsonString) {
			return "";
		}
		public class JsonToSQLCompanyFinancialTerm : JsonToSQL {
			string delete_sql = @"
DELETE FROM DimensionToCell 
where TableCellID in (SELECT id from TableCell
where CompanyFinancialTermID = {0});

DELETE FROM TableCell where CompanyFinancialTermID = {0};

				DELETE FROM CompanyFinancialTerm where id = {0};
				
				";
			string merge_sql = @"MERGE CompanyFinancialTerm
USING (VALUES ({0}, {1}, {2}, {3}, {4}, {5}, {6})) as src ( ID ,DocumentSeriesID ,TermStatusID ,Description ,NormalizedFlag ,EncoreTermFlag ,ManualUpdate)
ON CompanyFinancialTerm.id = src.ID
WHEN MATCHED THEN
	UPDATE SET DocumentSeriesID =  src.DocumentSeriesID
      ,TermStatusID =  src.TermStatusID
      ,Description =  src.Description
      ,NormalizedFlag =  src.NormalizedFlag
      ,EncoreTermFlag =  src.EncoreTermFlag
      ,ManualUpdate =  src.ManualUpdate 
WHEN NOT MATCHED THEN
	INSERT ( DocumentSeriesID
      ,TermStatusID
      ,Description
      ,NormalizedFlag
      ,EncoreTermFlag
      ,ManualUpdate) VALUES
	  (
	    src.DocumentSeriesID
      ,src.TermStatusID
      ,src.Description
      ,src.NormalizedFlag
      ,src.EncoreTermFlag
      ,src.ManualUpdate
	  )
OUTPUT $action, 'CompanyFinancialTerm', inserted.Id INTO @ChangeResult;

";
			private JArray _jarray;
			public JsonToSQLCompanyFinancialTerm(JToken jToken) : base("")
			{
				if (jToken == null) {
					_jarray = null;
				} else {
					_jarray = (JArray)jToken.SelectToken("");
				}
			}
			public override string Translate() {
				if (_jarray == null) return "";
				//JObject json = JObject.Parse(_json);
				System.Text.StringBuilder sb = new System.Text.StringBuilder();

				foreach (var elem	in _jarray) {
					try {
						if (elem["action"].ToString() == "delete") {
							sb.AppendLine(string.Format(delete_sql, elem["obj"]["ID"].ToString()));
						} else if (elem["action"].ToString() == "update") {
							sb.AppendLine(string.Format(merge_sql, elem["obj"]["ID"].ToString(),   
								elem["obj"]["DocumentSeries"]["ID"].ToString(),   
								elem["obj"]["TermStatusID"].AsValue(),
								elem["obj"]["Description"].AsString(), 
  							(string.Equals(elem["obj"]["NormalizedFlag"].ToString(), "true", StringComparison.InvariantCultureIgnoreCase) ? "1" : "0"),
								elem["obj"]["EncoreTermFlag"].ToString(),
								"0" // manual falg
								));
						} else if (elem["action"].ToString() == "insert") {
								sb.AppendLine(string.Format(merge_sql, elem["obj"]["ID"].ToString(),   
								elem["obj"]["DocumentSeries"]["ID"].ToString(),   
								elem["obj"]["TermStatusID"].AsValue(),
								elem["obj"]["Description"].AsString(), 
  							(string.Equals(elem["obj"]["NormalizedFlag"].ToString(), "true", StringComparison.InvariantCultureIgnoreCase) ? "1" : "0"),
								elem["obj"]["EncoreTermFlag"].ToString(),
								"0" // manual falg
								));
						}
					} catch (System.Exception ex) {
						sb.AppendLine(ex.Message);
					}
				}

				return sb.ToString(); ;
			}

		}

		public class JsonToSQLTableDimension : JsonToSQL {
			string delete_sql = @"
DELETE FROM DimensionToCell where TableDimensionId = {0};
DELETE FROM TableDimension where id = {0};
";
			string merge_sql = @"MERGE TableDimension
USING (VALUES ({0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9})) as src ( ID  ,DocumentTableID  ,DimensionTypeID  ,Label  ,OrigLabel  ,Location  ,EndLocation  ,Parent  ,InsertedRow  ,AdjustedOrder)
ON TableDimension.id = src.ID
WHEN MATCHED THEN
	UPDATE SET       DocumentTableID  = src.DocumentTableID 
      ,DimensionTypeID  = src.DimensionTypeID 
      ,Label  = src.Label 
      ,OrigLabel  = src.OrigLabel 
      ,Location  = src.Location 
      ,EndLocation  = src.EndLocation 
      ,Parent  = TableDimension.Parent 
      ,InsertedRow  = src.InsertedRow 
      ,AdjustedOrder  = src.AdjustedOrder 
WHEN NOT MATCHED THEN
	INSERT ( DocumentTableID
      ,DimensionTypeID
      ,Label
      ,OrigLabel
      ,Location
      ,EndLocation
      ,Parent
      ,InsertedRow
      ,AdjustedOrder) VALUES
	  (
	    src.DocumentTableID
,src.DimensionTypeID
,src.Label
,src.OrigLabel
,src.Location
,src.EndLocation
,src.Parent
,src.InsertedRow
,src.AdjustedOrder
	  )
OUTPUT $action, 'TableDimension', inserted.Id INTO @ChangeResult;

";
			private JArray _jarray;
			private string _dimensionTableId;
			public JsonToSQLTableDimension(string dimensionTableId, JToken jToken)
				: base("") {
					_dimensionTableId = dimensionTableId;
					if (jToken == null) {
						_jarray = null;
					} else {
						_jarray = (JArray)jToken.SelectToken("");
					}
			}
			public override string Translate() {
				if (_jarray == null) return "";
				//JObject json = JObject.Parse(_json);
				System.Text.StringBuilder sb = new System.Text.StringBuilder();

				foreach (var elem in _jarray) {
					try {
						if (elem["action"].ToString() == "delete") {
							sb.AppendLine(string.Format(delete_sql, elem["obj"]["ID"].AsValue()));
						} else if (elem["action"].ToString() == "update") {
							sb.AppendLine(string.Format(merge_sql, elem["obj"]["ID"].AsValue(),
								_dimensionTableId,
								elem["obj"]["DimensionTypeId"].AsValue(),
								elem["obj"]["Label"].AsString(),
								elem["obj"]["OrigLabel"].AsString(),
								elem["obj"]["Location"].AsValue(),
								elem["obj"]["EndLocation"].AsValue(),
								"NULL", //Parent
								(string.Equals(elem["obj"]["InsertedRow"].ToString(), "true", StringComparison.InvariantCultureIgnoreCase) ? "1" : "0"),
								elem["obj"]["AdjustedOrder"].AsValue()
								));
						} else if (elem["action"].ToString() == "insert") {
							sb.AppendLine(string.Format(merge_sql, elem["obj"]["ID"].AsValue(),
							_dimensionTableId,
							elem["obj"]["DimensionTypeId"].AsValue(),
							elem["obj"]["Label"].AsString(),
							elem["obj"]["OrigLabel"].AsString(),
							elem["obj"]["Location"].AsValue(),
							elem["obj"]["EndLocation"].AsValue(),
							"NULL", //Parent
							(string.Equals(elem["obj"]["InsertedRow"].ToString(), "true", StringComparison.InvariantCultureIgnoreCase) ? "1" : "0"),
							elem["obj"]["AdjustedOrder"].AsValue()
							));
						}
					} catch (System.Exception ex) {
						sb.AppendLine(@"/*" + ex.Message + elem["action"].ToString() + @"*/");
					}
				}

				return sb.ToString(); ;
			}

		}

		public class JsonToSQLTableCell : JsonToSQL {
			string delete_sql = "DELETE FROM TableCell where id = {0};";
			string merge_sql = @"MERGE TableCell
USING (VALUES ({0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}, {11}, {12}, {13}, {14}, {15}, {16}, {17}, {18}, {19}, {20}, {21}, {22}, {23})) 
	as src (  ID,Offset,CellPeriodType,PeriodTypeID,CellPeriodCount,PeriodLength,CellDay,CellMonth,CellYear,CellDate,Value,CompanyFinancialTermID,ValueNumeric,NormalizedNegativeIndicator,ScalingFactorID,AsReportedScalingFactor,Currency,CurrencyCode,Cusip,ScarUpdated,IsIncomePositive,DocumentId,Label,XBRLTag)
ON TableCell.id = src.ID
WHEN MATCHED THEN
	UPDATE SET  Offset = src.Offset
      ,CellPeriodType = src.CellPeriodType
      ,PeriodTypeID = src.PeriodTypeID
      ,CellPeriodCount = src.CellPeriodCount
      ,PeriodLength = src.PeriodLength
      ,CellDay = src.CellDay
      ,CellMonth = src.CellMonth
      ,CellYear = src.CellYear
      ,CellDate = src.CellDate
      ,Value = src.Value
      ,CompanyFinancialTermID = src.CompanyFinancialTermID
      ,ValueNumeric = src.ValueNumeric
      ,NormalizedNegativeIndicator = src.NormalizedNegativeIndicator
      ,ScalingFactorID = src.ScalingFactorID
      ,AsReportedScalingFactor = src.AsReportedScalingFactor
      ,Currency = src.Currency
      ,CurrencyCode = src.CurrencyCode
      ,Cusip = src.Cusip
      ,ScarUpdated = TableCell.ScarUpdated
      ,IsIncomePositive = src.IsIncomePositive
      ,DocumentId = src.DocumentId
      ,Label = src.Label
      ,XBRLTag = src.XBRLTag
WHEN NOT MATCHED THEN
	INSERT (Offset
      ,CellPeriodType
      ,PeriodTypeID
      ,CellPeriodCount
      ,PeriodLength
      ,CellDay
      ,CellMonth
      ,CellYear
      ,CellDate
      ,Value
      ,CompanyFinancialTermID
      ,ValueNumeric
      ,NormalizedNegativeIndicator
      ,ScalingFactorID
      ,AsReportedScalingFactor
      ,Currency
      ,CurrencyCode
      ,Cusip
      ,ScarUpdated
      ,IsIncomePositive
      ,DocumentId
      ,Label
      ,XBRLTag) VALUES
	  (
	     src.Offset
      ,src.CellPeriodType
      ,src.PeriodTypeID
      ,src.CellPeriodCount
      ,src.PeriodLength
      ,src.CellDay
      ,src.CellMonth
      ,src.CellYear
      ,src.CellDate
      ,src.Value
      ,src.CompanyFinancialTermID
      ,src.ValueNumeric
      ,src.NormalizedNegativeIndicator
      ,src.ScalingFactorID
      ,src.AsReportedScalingFactor
      ,src.Currency
      ,src.CurrencyCode
      ,src.Cusip
      ,0
      ,src.IsIncomePositive
      ,src.DocumentId
      ,src.Label
      ,src.XBRLTag
	  )
OUTPUT $action, 'TableCell', inserted.Id INTO @ChangeResult;

";
			private JArray _jarray;
			public JsonToSQLTableCell(JToken jToken)
				: base("") {
					if (jToken == null) {
						_jarray = null;
					} else {
						_jarray = (JArray)jToken.SelectToken("");
					}
			}
			public override string Translate() {
				if (_jarray == null) return "";
				//JObject json = JObject.Parse(_json);
				System.Text.StringBuilder sb = new System.Text.StringBuilder();

				foreach (var elem in _jarray) {
					if (elem == null) continue;
					try {
						var s = elem["obj"]["Offset"].AsString();
						if (elem["action"].ToString() == "delete") {
							sb.AppendLine(string.Format(delete_sql, elem["obj"]["ID"].AsValue()));
						} else if (elem["action"].ToString() == "update") {
							sb.AppendLine(string.Format(merge_sql, elem["obj"]["ID"].AsValue(),
								elem["obj"]["Offset"].AsString(),
								elem["obj"]["CellPeriodType"].AsString(),
								elem["obj"]["PeriodTypeID"].AsString(),
								elem["obj"]["CellPeriodCount"].AsString(),
								elem["obj"]["PeriodLength"].AsValue(),
								elem["obj"]["CellDay"].AsString(),
								elem["obj"]["CellMonth"].AsString(),
								elem["obj"]["CellYear"].AsString(),
								elem["obj"]["CellDate"].AsString(),
								elem["obj"]["Value"].AsString(),
								elem["obj"]["CompanyFinancialTerm"]["ID"].AsValue(),
								elem["obj"]["ValueNumeric"].AsValue(),
								elem["obj"]["NormalizedNegativeIndicator"].AsBoolean(),
								elem["obj"]["ScalingFactorID"].AsString(),
								elem["obj"]["AsReportedScalingFactor"].AsString(),
								elem["obj"]["Currency"].AsString(),
								elem["obj"]["CurrencyCode"].AsString(),
								elem["obj"]["Cusip"].AsString(),
								"0",
								elem["obj"]["IsIncomePositive"].AsBoolean(),
								elem["obj"]["DocumentId"].AsString(),
								elem["obj"]["Label"].AsString(),
								elem["obj"]["XBRLTag"].AsString()
								));
						} else if (elem["action"].ToString() == "insert") {
							sb.AppendLine(string.Format(merge_sql, elem["obj"]["ID"].AsValue(),
							elem["obj"]["Offset"].AsString(),
							elem["obj"]["CellPeriodType"].AsString(),
							elem["obj"]["PeriodTypeID"].AsString(),
							elem["obj"]["CellPeriodCount"].AsString(),
							elem["obj"]["PeriodLength"].AsValue(),
							elem["obj"]["CellDay"].AsString(),
							elem["obj"]["CellMonth"].AsString(),
							elem["obj"]["CellYear"].AsString(),
							elem["obj"]["CellDate"].AsString(),
							elem["obj"]["Value"].AsString(),
							elem["obj"]["CompanyFinancialTerm"]["ID"].AsValue(),
							elem["obj"]["ValueNumeric"].AsValue(),
							elem["obj"]["NormalizedNegativeIndicator"].AsBoolean(),
							elem["obj"]["ScalingFactorID"].AsString(),
							elem["obj"]["AsReportedScalingFactor"].AsString(),
							elem["obj"]["Currency"].AsString(),
							elem["obj"]["CurrencyCode"].AsString(),
							elem["obj"]["Cusip"].AsString(),
							"0",
							elem["obj"]["IsIncomePositive"].AsBoolean(),
							elem["obj"]["DocumentId"].AsString(),
							elem["obj"]["Label"].AsString(),
							elem["obj"]["XBRLTag"].AsString()
							));
						}
					} catch (System.Exception ex) {
						sb.AppendLine(ex.Message);
					}
				}

				return sb.ToString(); ;
			}

		}

		public ScarResult UpdateTDPByDocumentTableID(string dtid, string updateInJson) {
			ScarResult result = new ScarResult();
			result.ReturnValue["DebugMessage"] = "";
			System.Text.StringBuilder sb = new System.Text.StringBuilder();
			sb.AppendLine("BEGIN TRAN");
			sb.AppendLine("DECLARE @ChangeResult TABLE (ChangeType VARCHAR(10), TableType varchar(50), Id INTEGER)");

			try {
				JObject json = JObject.Parse(updateInJson);
				string unquotedJson = updateInJson.Replace("\"", "").Replace("'", "");
				int totalUpdates = new Regex(Regex.Escape("action: update")).Matches(unquotedJson).Count;
				int totalInsert =  new Regex(Regex.Escape("action: insert")).Matches(unquotedJson).Count;
				var cft = json["CompanyFinancialTerm"];
				var tabledimension = json["TableDimension"];
				var tablecell = json["TableCell"];
				var documentTable = json["DobumenTable"]; // typo in json
				string test = cft.GetType().ToString();
				sb.AppendLine(new JsonToSQLCompanyFinancialTerm(cft).Translate());
				sb.AppendLine(new JsonToSQLTableDimension(dtid, tabledimension).Translate());
				sb.AppendLine(new JsonToSQLTableCell(tablecell).Translate());
				sb.AppendLine("select * from @ChangeResult; DECLARE @totalInsert int, @totalUpdate int; ");
				sb.AppendLine("select @totalInsert = count(*) from @ChangeResult where ChangeType = 'INSERT';");
				sb.AppendLine("select @totalUpdate = count(*) from @ChangeResult where ChangeType = 'UPDATE'; ");
				sb.AppendLine();
				sb.AppendLine(string.Format("IF (@totalInsert = {0} and @totalUpdate = {1}) BEGIN select 'commit'; ROLLBACK TRAN END ELSE BEGIN select 'rollback'; ROLLBACK TRAN END", totalInsert, totalUpdates));
				result.ReturnValue["DebugMessage"] += sb.ToString();

//				return result;

				using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {
					using (SqlCommand cmd = new SqlCommand(sb.ToString(), conn)) {
						conn.Open();
						using (SqlDataReader reader = cmd.ExecuteReader()) {
							System.Text.StringBuilder sbRet = new System.Text.StringBuilder();
							sbRet.AppendLine("{");
							while (reader.Read()) {
								var changeType = reader.GetStringSafe(0);
								var tableType = reader.GetStringSafe(1);
								var Id = reader.GetInt32(2);
								var newline = string.Format(@"{{'changeType':'{0}', 'tableType':'{1}', 'tableType':{2} }}", changeType, tableType, Id);
								sbRet.AppendLine(newline);
							}
							sbRet.AppendLine("}");
							if (reader.NextResult() && reader.Read()) {
								if (reader.GetStringSafe(0) == "commit") {
									result.ReturnValue["Success"] = "T";
								} else {
									result.ReturnValue["Success"] = "F";
								}
							}
							result.ReturnValue["Message"] = sbRet.ToString();
						}
					}
				}
			} catch (Exception ex) {
				result.ReturnValue["DebugMessage"] += ex.Message;

			}
			return result;
		}

		public ScarResult UpdateTDP(string id, string newValue, bool obselete) {
			string SQL_MergeCft = @"

DECLARE @newDtsID int = 0;

update dtstc
set DocumentTimeSliceId =  @newDtsID
from [DimensionToCell] dtc 
JOIN [TableDimension] td on dtc.TableDimensionID = td.ID and td.DimensionTypeID = 2
JOIN [DimensionToCell] dtc2 on td.ID = dtc2.TableDimensionID
JOIN [TableCell] tc on tc.id = dtc2.TableCellID
JOIN DocumentTimeSliceTableCell dtstc on dtstc.TableCellId = tc.id
where dtc.TableCellID = @id



";

			string select_query = @"

select  'x', tc.* 
from [DimensionToCell] dtc 
JOIN [TableDimension] td on dtc.TableDimensionID = td.ID and td.DimensionTypeID = 2
JOIN [DimensionToCell] dtc2 on td.ID = dtc2.TableDimensionID
JOIN [TableCell] tc on tc.id = dtc2.TableCellID
where dtc.TableCellID = @id

";

			JObject json = JObject.Parse(newValue);

			ScarResult response = new ScarResult();
			string sql_query = select_query;
			//if (newValue != "--") {
			//	sql_query = query + select_query;
			//}
			response.StaticHierarchies = new List<StaticHierarchy>();
			var sh = new StaticHierarchy();
			response.StaticHierarchies.Add(sh);
			sh.Description = @"";
			sh.Cells = new List<SCARAPITableCell>();
			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {

				using (SqlCommand cmd = new SqlCommand(sql_query, conn)) {
					conn.Open();
					cmd.Parameters.AddWithValue("@id", id);
					cmd.Parameters.AddWithValue("@newCurrencyCode", newValue);
					using (SqlDataReader reader = cmd.ExecuteReader()) {
						while (reader.Read()) {
							SCARAPITableCell cell;
							if (reader.GetNullable<int>(1).HasValue) {
								cell = new SCARAPITableCell
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
								};
								cell.ScarUpdated = reader.GetBoolean(20);
								cell.IsIncomePositive = reader.GetBoolean(21);
								cell.XBRLTag = reader.GetStringSafe(22);
								//cell.UpdateStampUTC = reader.GetNullable<DateTime>(23);
								cell.DocumentID = reader.IsDBNull(23) ? Guid.Empty : reader.GetGuid(23);
								cell.Label = reader.GetStringSafe(24);
								sh.Cells.Add(cell);
							}
						}
					}
				}
			}
			return response;
		}


		public ScarResult CloneUpdateTimeSlice(int id, string InterimType) {

			string query = @"
DECLARE @newId int;
DECLARE @DocId uniqueidentifier = (SELECT DocumentId FROM DocumentTimeSlice where id = @id)
DECLARE @TimeSlicePeriodEndDate datetime = (SELECT TimeSlicePeriodEndDate FROM DocumentTimeSlice where id = @id)
DECLARE @oldPeriodType varchar(10) = (SELECT PeriodType FROM DocumentTimeSlice where id = @id)
DECLARE @dts int = (SELECT top 1 ID FROM DocumentTimeSlice dts where dts.PeriodType = @newPeriodType AND dts.DocumentId = @DocId and dts.TimeSlicePeriodEndDate = @TimeSlicePeriodEndDate);

if @newPeriodType <> @oldPeriodType
BEGIN
	IF (@newPeriodType <> '--')
	BEGIN
		IF (@dts IS NULL)
		BEGIN
			IF (EXISTS ( SELECT TOP 1 id FROM [InterimType] WHERE ID = @newPeriodType and Duration is not null))  
			BEGIN
				INSERT DocumentTimeSlice
						( [DocumentId]
							  ,[DocumentSeriesId]
							  ,[TimeSlicePeriodEndDate]
							  ,[ReportingPeriodEndDate]
							  ,[FiscalDistance]
							  ,[Duration]
							  ,[PeriodType]
							  ,[AcquisitionFlag]
							  ,[AccountingStandard]
							  ,[ConsolidatedFlag]
							  ,[IsProForma]
							  ,[IsRecap]
							  ,[CompanyFiscalYear]
							  ,[ReportType]
							  ,[IsAmended]
							  ,[IsRestated]
							  ,[IsAutoCalc]
							  ,[ManualOrgSet])

						SELECT TOP 1 [DocumentId]
							  ,[DocumentSeriesId]
							  ,[TimeSlicePeriodEndDate]
							  ,[ReportingPeriodEndDate]
							  ,[FiscalDistance]
							  ,[Duration]
							  ,@newPeriodType
							  ,[AcquisitionFlag]
							  ,[AccountingStandard]
							  ,[ConsolidatedFlag]
							  ,[IsProForma]
							  ,[IsRecap]
							  ,[CompanyFiscalYear]
							  ,[ReportType]
							  ,[IsAmended]
							  ,[IsRestated]
							  ,[IsAutoCalc]
							  ,[ManualOrgSet]
						  FROM [DocumentTimeSlice] where id = @id;

						select @newId =  cast(scope_identity() as int);

						UPDATE [DocumentTimeSliceTableCell] SET  DocumentTimeSliceId = @newId
						  WHERE DocumentTimeSliceId = @id;
			END
			ELSE
			BEGIN
				DELETE FROM [DocumentTimeSliceTableCell]  WHERE DocumentTimeSliceId = @id; 
				SET @newId = @Id
			END

		END
		ELSE -- if exists
		BEGIN
			UPDATE [DocumentTimeSliceTableCell] SET  DocumentTimeSliceId = @dts
				WHERE DocumentTimeSliceId = @id;
			UPDATE [DocumentTimeSlice] SET  PeriodType = @newPeriodType
			WHERE Id = @id;
			SET @newId = @Id
		END
	END
	ELSE -- if new periodtype is --
	BEGIN
		DELETE FROM [DocumentTimeSliceTableCell]  WHERE DocumentTimeSliceId = @id; 
		DELETE FROM [DocumentTimeSlice] where id = @Id
		SET @newId = @Id
	END
	
END

SELECT [Id]
	,[DocumentId]
	,[DocumentSeriesId]
	,[TimeSlicePeriodEndDate]
	,[ReportingPeriodEndDate]
	,[FiscalDistance]
	,[Duration]
	,[PeriodType]
	,[AcquisitionFlag]
	,[AccountingStandard]
	,[ConsolidatedFlag]
	,[IsProForma]
	,[IsRecap]
	,[CompanyFiscalYear]
	,[ReportType]
	,[IsAmended]
	,[IsRestated]
	,[IsAutoCalc]
	,[ManualOrgSet]
FROM [DocumentTimeSlice] where id = @newId or id = @dts or id = @id;

";


			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {
				ScarResult response = new ScarResult();
				response.TimeSlices = new List<TimeSlice>();
				using (SqlCommand cmd = new SqlCommand(query, conn)) {
					conn.Open();
					cmd.Parameters.AddWithValue("@id", id);
					cmd.Parameters.AddWithValue("@newPeriodType", InterimType);
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
								ManualOrgSet = reader.GetBoolean(18)
							};
							response.TimeSlices.Add(slice);
						}
					}
				}
				return response;
			}
		}

		public ScarResult GetReviewTimeSlice(string TemplateName, int iconum) {

			string SQL_ReviewButton = @"
select tc.CellDate as PeriodEndDate, 
case when dts.PeriodType = 'XX' THEN 'AR' ELSE dts.PeriodType END as TimeSeries, 
case when IsRecap = 1 AND ManualOrgSet = 0 THEN 'RECAP' ELSE 'ORG' END as AccountType, 
d.FormTypeID as FormType, 
case when dts.ReportType = 'P' THEN 'P' ELSE CASE WHEN dts.periodtype = 'XX' THEN 'A' ELSE 'I' END END as ReportType, 
tc.CellDate as PubDate,
dts.CompanyFiscalYear as DataYear,
dbo.GetEndLabel(sh.Description) as DataLabel,
mtm.Description as Comment
from DocumentSeries ds
join CompanyFinancialTerm cft on ds.id = cft.documentseriesid
join statichierarchy sh on sh.companyfinancialtermid = cft.id
join TableType tt on sh.tabletypeid = tt.id
join TableCell tc on tc.CompanyFinancialTermID = sh.CompanyFinancialTermId
join DocumentTimeSliceTableCell dtstc on tc.id = dtstc.tablecellid
join documenttimeslice dts on dts.id = dtstc.documenttimesliceid and dts.DocumentSeriesId = ds.ID
join Document d on dts.documentid = d.id
join MTMWErrorTypeTableCell mtmtc on tc.id = mtmtc.tablecellid
join MTMWErrorType mtm on mtmtc.MTMWErrorTypeId = mtm.ID
where companyid = @Iconum
and tt.description = @TableType

";
			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {
				ScarResult response = new ScarResult();
				response.TimeSlices = new List<TimeSlice>();

				using (SqlCommand cmd = new SqlCommand(SQL_ReviewButton, conn)) {
					conn.Open();
					cmd.Parameters.AddWithValue("@Iconum", iconum);
					cmd.Parameters.AddWithValue("@TableType", TemplateName);
					using (SqlDataReader reader = cmd.ExecuteReader()) {
						var ordinals = new
						{
							PeriodEndDate = reader.GetOrdinal("PeriodEndDate"),
							TimeSeries = reader.GetOrdinal("TimeSeries"),
							AccountType = reader.GetOrdinal("AccountType"),
							FormType = reader.GetOrdinal("FormType"),
							ReportType = reader.GetOrdinal("ReportType"),
							PubDate = reader.GetOrdinal("PubDate"),
							DataYear = reader.GetOrdinal("DataYear"),
							DataLabel = reader.GetOrdinal("DataLabel"),
							Comment = reader.GetOrdinal("Comment")
						};
						while (reader.Read()) {
							TimeSlice slice = new TimeSlice
							{
								DamDocumentId = new Guid("00000000-0000-0000-0000-000000000000"),
								TimeSlicePeriodEndDate = reader.GetDateTime(ordinals.PeriodEndDate),
								InterimType = reader.GetStringSafe(ordinals.TimeSeries),
								AccountingStandard = reader.GetStringSafe(ordinals.AccountType),
								PeriodType = reader.GetStringSafe(ordinals.FormType),
								ReportType = reader.GetStringSafe(ordinals.ReportType),
								PublicationDate = reader.GetDateTime(ordinals.PubDate),
								CompanyFiscalYear = reader.GetDecimal(ordinals.DataYear),
								ConsolidatedFlag = reader.GetStringSafe(ordinals.DataLabel),
								AcquisitionFlag = reader.GetStringSafe(ordinals.Comment)
							};
							response.TimeSlices.Add(slice);
						}
					}
				}
				return response;
			}
		}

		public ScarResult GetTimeSliceByTemplate(string TemplateName, Guid DocumentId) {

			string SQL_query = @"
DECLARE @DocumentSeriesId int;
select top 1 @DocumentSeriesId = d.DocumentSeriesID
	FROM Document d WITH (NOLOCK)
	where d.DAMDocumentId = @DocumentID;

;WITH cte_timeslice(DamDocumentID, TimeSliceId, NumberofCell, CurrencyCount, CurrencyCode, ArComponent)
AS
(
	SELECT distinct   d.damdocumentid, dts.id, count(distinct tc.id), count(distinct tc.CurrencyCode), max(tc.CurrencyCode), count(artsdc.DocumentTimeSliceID)
		FROM DocumentSeries ds WITH (NOLOCK)
		JOIN DocumentTimeSlice dts WITH (NOLOCK) on ds.ID = Dts.DocumentSeriesId
		JOIN Document d WITH (NOLOCK) on dts.DocumentId = d.ID
		JOIN DocumentTimeSliceTableCell dtstc WITH (NOLOCK) on dts.ID = dtstc.DocumentTimeSliceID
		JOIN TableCell tc WITH (NOLOCK) on dtstc.TableCellID = tc.ID
		JOIN DimensionToCell dtc WITH (NOLOCK) on tc.ID = dtc.TableCellID -- check that is in a table
		JOIN StaticHierarchy sh WITH (NOLOCK) on tc.CompanyFinancialTermID = sh.CompanyFinancialTermID
		JOIN TableType tt WITH (NOLOCK) on tt.ID = sh.TableTypeID  
		LEFT JOIN ARTimeSliceDerivationComponents artsdc WITH(NOLOCK) ON artsdc.DocumentTimeSliceID = dts.id
	WHERE 1=1
	and tt.description = @TypeTable
	and ds.id = @DocumentSeriesId and tt.DocumentSeriesID = @DocumentSeriesId
	group by d.damdocumentid, dts.id 
)
SELECT ts.*, dts.*, d.DocumentDate, d.ReportTypeID, d.PublicationDateTime
	INTO #nonempty
	FROM cte_timeslice ts WITH (NOLOCK)
	JOIN DocumentTimeSlice dts WITH(NOLOCK) on ts.TimeSliceId = dts.Id
  JOIN Document d WITH(NOLOCK) on dts.DocumentId = d.id

select d.id, d.DAMDocumentId, tt.Description, dtc.TableCellID
 INTO #tableCells
from DocumentTable dt (nolock)
join TableType tt (nolock) on dt.TableTypeId = tt.id
inner join Tablemeta tm (NOLOCK) on tt.Description= tm.ShortName and tm.IsTemplate =1
JOIN Document d WITH(NOLOCK) ON dt.DocumentId = d.id
JOIN TableDimension td (nolock) ON dt.ID = td.DocumentTableID and td.DimensionTypeID = 1
JOIN DimensionToCell dtc WITH(NOLOCK) ON dtc.TableDimensionID = td.ID
where 
  tt.Description = @TypeTable and tt.DocumentSeriesID = @DocumentSeriesId and
  d.documentseriesid =  @DocumentSeriesId
  and (d.ArdExportFlag = 1 or d.IsDocSetUpCompleted = 1 or d.ExportFlag = 1)


select d.id as DocumentId, d.DAMDocumentId, d.Description, tc.PeriodLength, tc.PeriodTypeID, tc.CellDate, count(*) as count, count(distinct tc.CurrencyCode) as CurrencyCount, max(tc.CurrencyCode) as CurrencyCode 
 INTO #alltimeslices
from #tableCells d
JOIN TableCell tc (NOLOCK) on tc.ID = d.TableCellID
 
 group by d.id, d.DAMDocumentId, d.Description, tc.PeriodLength, tc.PeriodTypeID, tc.CellDate


select 
	ISNULL(n.DamDocumentID, a.DAMDocumentId) as DamDocumentID
	,ISNULL(n.TimeSliceId, -1) as TimeSliceId
	,ISNULL(n.NumberofCell, a.count) as NumberofCell
	,ISNULL(n.CurrencyCode, a.CurrencyCode) as CurrencyCode
	,ISNULL(n.CurrencyCount, a.CurrencyCount) as CurrencyCount
	,ISNULL(n.ArComponent, 0) as ArComponent
	,ISNULL(n.DocumentId, a.DocumentId) as DocumentId
	,ISNULL(n.DocumentSeriesId, @DocumentSeriesId) as DocumentSeriesId
	,ISNULL(n.TimeSlicePeriodEndDate, a.CellDate) as TimeSlicePeriodEndDate
	,ISNULL(n.ReportingPeriodEndDate, a.CellDate) as ReportingPeriodEndDate
	,ISNULL(n.FiscalDistance, 0) as FiscalDistance
	,ISNULL(n.Duration, 0) as Duration
	,ISNULL(n.PeriodType, '--') as PeriodType
	,ISNULL(n.AcquisitionFlag, 0) as AcquisitionFlag
	,ISNULL(n.AccountingStandard, 0) as AccountingStandard
	,ISNULL(n.CompanyFiscalYear, 2017) as CompanyFiscalYear
	,ISNULL(n.DocumentDate, d.DocumentDate) as DocumentDate
	,ISNULL(n.PublicationDateTime, d.PublicationDateTime) as PublicationDateTime
	,ISNULL(n.ReportType, d.ReportTypeID) as ReportType
	,ISNULL(n.ReportTypeID, d.ReportTypeID) as ReportTypeID
	,ISNULL(n.IsAutoCalc, 0) as IsAutoCalc

from #alltimeslices  a
JOIN Document d WITH(NOLOCK) on a.DAMDocumentId  = d.DAMDocumentId
LEFT JOIN #nonempty n on a.DamDocumentID = n.DamDocumentID  and n.TimeSlicePeriodEndDate = a.CellDate
 order by a.CellDate desc

";

			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {
				StaticHierarchy sh;
				ScarResult response = new ScarResult();
				response.TimeSlices = new List<TimeSlice>();

				using (SqlCommand cmd = new SqlCommand(SQL_query, conn)) {
					conn.Open();
					cmd.Parameters.AddWithValue("@DocumentID", DocumentId);
					cmd.Parameters.AddWithValue("@TypeTable", TemplateName);
					using (SqlDataReader reader = cmd.ExecuteReader()) {
						var ordinals = new
						{
							DocumentId = reader.GetOrdinal("DocumentId"),
							DamDocumentId = reader.GetOrdinal("DamDocumentID"),
							TimeSliceId = reader.GetOrdinal("TimeSliceId"),
							DocumentDate = reader.GetOrdinal("DocumentDate"),
							PublicationDate = reader.GetOrdinal("PublicationDateTime"),
							DocumentSeriesId = reader.GetOrdinal("DocumentSeriesId"),
							ReportType = reader.GetOrdinal("ReportType"),
							ReportStatus = reader.GetOrdinal("ReportTypeId"),
							//TableType = reader.GetOrdinal(""),
							CompanyFiscalYear = reader.GetOrdinal("CompanyFiscalYear"),
							FiscalDistance = reader.GetOrdinal("FiscalDistance"),
							PeriodLength = reader.GetOrdinal("Duration"),
							PeriodType = reader.GetOrdinal("PeriodType"),
							Currency = reader.GetOrdinal("AccountingStandard"),
							PeriodEndDate = reader.GetOrdinal("TimeSlicePeriodEndDate"),
							InterimType = reader.GetOrdinal("PeriodType"),
							AutocalcStatus = reader.GetOrdinal("IsAutoCalc"),
							NumberOfCells = reader.GetOrdinal("NumberofCell"),
							CurrencyCode = reader.GetOrdinal("CurrencyCode"),
							CurrencyCount = reader.GetOrdinal("CurrencyCount"),
							ArComponent = reader.GetOrdinal("ArComponent")
						};
						while (reader.Read()) {
							TimeSlice slice = new TimeSlice();
							slice.DocumentId = reader.GetGuid(ordinals.DocumentId);
							slice.DamDocumentId = reader.GetGuid(ordinals.DamDocumentId);
							slice.Id = reader.GetInt32(ordinals.TimeSliceId);
							if (slice.Id > 0) {
								slice.TimeSlicePeriodEndDate = reader.GetDateTime(ordinals.PeriodEndDate);
								slice.ReportingPeriodEndDate = reader.GetDateTime(ordinals.DocumentDate);
							}
							slice.DocumentSeriesId = reader.GetInt32(ordinals.DocumentSeriesId);
							slice.PublicationDate = reader.GetDateTime(ordinals.PublicationDate);
							slice.FiscalDistance = reader.GetInt32(ordinals.FiscalDistance);
							slice.CompanyFiscalYear = reader.GetDecimal(ordinals.CompanyFiscalYear);
							slice.Duration = reader.GetInt32(ordinals.PeriodLength);
							slice.InterimType = reader.GetStringSafe(ordinals.PeriodType);
							slice.ReportType = reader.GetStringSafe(ordinals.ReportType);
							slice.IsAutoCalc = reader.GetBoolean(ordinals.AutocalcStatus);
							slice.NumberOfCells = reader.GetInt32(ordinals.NumberOfCells);
							slice.Currency = reader.GetInt32(ordinals.CurrencyCount) == 1 ? reader.GetStringSafe(ordinals.CurrencyCode) : null;
							slice.AccountingStandard = reader.GetInt32(ordinals.ArComponent).ToString();
							response.TimeSlices.Add(slice);
						}
					}
				}
				return response;
			}
		}


		public ScarResult FlipSign(string CellId, Guid DocumentId, int iconum, int TargetStaticHierarchyID) {
			const string SQL_UpdateFlipIncomeFlag = @"

UPDATE TableCell  set IsIncomePositive = CASE WHEN IsIncomePositive = 1 THEN 0 ELSE 1 END
																WHERE ID = @cellid; 
";

			const string SQL_ValidateCells = @"

DECLARE @DocumentSeriesId INT
SELECT TOP 1 @DocumentSeriesId = DocumentSeriesID
FROM Document WITH(NOLOCK) WHERE ID =  @DocumentID


DECLARE @TargetSH int;

SELECT top 1 @TargetSH = sh.id
  FROM  StaticHierarchy sh WITH (NOLOCK)
  JOIN [TableCell] tc WITH (NOLOCK) on sh.CompanyFinancialTermId = tc.CompanyFinancialTermId
  where tc.id = @cellid

DECLARE @SHCells CellList

INSERT @SHCells
SELECT distinct sh.ID, tc.DocumentTimeSliceID
FROM StaticHierarchy sh
JOIN vw_SCARDocumentTimeSliceTableCell tc ON sh.CompanyFinancialTermId = tc.CompanyFinancialTermID
WHERE sh.ID = @TargetSH and tc.TableCellid = @cellID

DECLARE @SHCellsMTMW TABLE(StaticHierarchyID int, DocumentTimeSliceID int, ChildrenSum decimal(28,5), CellValue decimal(28,5))
DECLARE @SHCellsLPV TABLE(StaticHierarchyID int, DocumentTimeSliceID int, LPVFail bit)
DECLARE @SHCellsError TABLE(StaticHierarchyID int, DocumentTimeSliceID int, LPVFail bit, MTMWFail bit)

DECLARE @SHParentCells CellList
INSERT INTO @SHParentCells
EXEC SCARGetTableCellMTMW_GetFirstMTMWParent @SHCells

INSERT INTO @SHCellsMTMW
EXEC SCARGetTableCellMTMWCalc @SHParentCells

INSERT INTO @SHCellsLPV
EXEC SCARGetTableCellLikePeriod @SHCells, @DocumentID

INSERT @SHCellsError 
SELECT ISNULL(lpv.StaticHierarchyID, mtmw.StaticHierarchyID), ISNULL(lpv.DocumentTimeSliceID, mtmw.DocumentTimeSliceID), ISNULL(lpv.LPVFail, 0), CASE WHEN mtmw.ChildrenSum <> mtmw.CellValue THEN 1 ELSE 0 END
from @SHCellsLPV lpv
FULL OUTER JOIN @SHCellsMTMW mtmw ON lpv.StaticHierarchyID = mtmw.StaticHierarchyID and  lpv.DocumentTimeSliceID = mtmw.DocumentTimeSliceID



;WITH cte_level(SHRootID, SHID, level)
AS
(
	SELECT @TargetSH, @TargetSH, 0
	UNION ALL
	SELECT cte.SHRootID, shp.ID, cte.level+1
	FROM cte_level cte
	JOIN StaticHierarchy sh ON cte.SHID = sh.ID
	JOIN StaticHierarchy shp ON sh.ParentID = shp.ID
)
SELECT MAX(level)
FROM cte_level
GROUP BY SHRootID

SELECT distinct 'x', tc.TableCellID, tc.Offset, tc.CellPeriodType, tc.PeriodTypeID, tc.CellPeriodCount, tc.PeriodLength, tc.CellDay, 
				tc.CellMonth, tc.CellYear, tc.CellDate, tc.Value, tc.CompanyFinancialTermID, tc.ValueNumeric, tc.NormalizedNegativeIndicator, 
				tc.ScalingFactorID, tc.AsReportedScalingFactor, tc.Currency, tc.CurrencyCode, tc.Cusip, tc.ScarUpdated, tc.IsIncomePositive, 
				tc.XBRLTag, 
				tc.DocumentId, tc.Label, sf.Value,
				(select aetc.ARDErrorTypeId from ARDErrorTypeTableCell aetc (nolock) where tc.TableCellId = aetc.TableCellId),
				(select metc.MTMWErrorTypeId from MTMWErrorTypeTableCell metc (nolock) where tc.TableCellId = metc.TableCellId), 
				lpv.LPVFail, lpv.MTMWFail,
				dts.Id, sh.AdjustedOrder, dts.Duration, dts.TimeSlicePeriodEndDate, dts.ReportingPeriodEndDate, d.PublicationDateTime
FROM StaticHierarchy sh
JOIN dbo.DocumentTimeSlice dts WITH(NOLOCK) ON dts.DocumentSeriesId = @DocumentSeriesId
JOIN Document d on dts.DocumentID = d.ID
LEFT JOIN vw_SCARDocumentTimeSliceTableCell tc ON tc.CompanyFinancialTermID = sh.CompanyFinancialTermID AND tc.DocumentTimeSliceID = dts.ID
JOIN @SHCellsError lpv ON lpv.StaticHierarchyID = sh.ID AND lpv.DocumentTimeSliceID = dts.ID
LEFT JOIN ScalingFactor sf ON tc.ScalingFactorID = sf.ID
WHERE (d.ID = @DocumentID OR d.ArdExportFlag = 1 OR d.ExportFlag = 1 OR d.IsDocSetupCompleted = 1)
ORDER BY dts.TimeSlicePeriodEndDate desc, dts.Duration desc, dts.ReportingPeriodEndDate desc, d.PublicationDateTime desc;

 
";
			ScarResult result = new ScarResult();
			result.CellToDTS = new Dictionary<SCARAPITableCell, int>();
			result.ChangedCells = new List<SCARAPITableCell>();

			string SQL_FlipSignCommand =
					SQL_UpdateFlipIncomeFlag
					+ SQL_ValidateCells
					;
			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {
				using (SqlCommand cmd = new SqlCommand(SQL_FlipSignCommand, conn)) {
					conn.Open();
					cmd.Parameters.AddWithValue("@DocumentID ", DocumentId);
					cmd.Parameters.AddWithValue("@cellid", CellId);
					cmd.Parameters.AddWithValue("@Iconum", iconum);

					using (SqlDataReader reader = cmd.ExecuteReader()) {
						reader.Read();
						int level = reader.GetInt32(0);
						reader.NextResult();
						int shix = 0;
						int adjustedOrder = 0;
						while (reader.Read()) {
							SCARAPITableCell cell;
							if (reader.GetNullable<int>(1).HasValue) {
								cell = new SCARAPITableCell
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
								};
								cell.ScarUpdated = reader.GetBoolean(20);
								cell.IsIncomePositive = reader.GetBoolean(21);
								cell.XBRLTag = reader.GetStringSafe(22);
								//cell.UpdateStampUTC = reader.GetNullable<DateTime>(23);
								cell.DocumentID = reader.IsDBNull(23) ? Guid.Empty : reader.GetGuid(23);
								cell.Label = reader.GetStringSafe(24);
								cell.ScalingFactorValue = reader.GetDouble(25);
								cell.ARDErrorTypeId = reader.GetNullable<int>(26);
								cell.MTMWErrorTypeId = reader.GetNullable<int>(27);
								cell.LikePeriodValidationFlag = reader.GetBoolean(28);
								cell.MTMWValidationFlag = reader.GetBoolean(29);
								adjustedOrder = reader.GetInt32(31);
								result.ChangedCells.Add(cell);

							} else {
								continue;
							}
							result.CellToDTS.Add(cell, adjustedOrder);
						}
					}
				}
			}
			return result;
		}

		public ScarResult FlipChildren(string CellId, Guid DocumentId, int iconum, int TargetStaticHierarchyID) {
			const string query = @"
DECLARE @TargetSHID int;

SELECT top 1 @TargetSHID = sh.id
  FROM  StaticHierarchy sh WITH (NOLOCK)
  JOIN [TableCell] tc WITH (NOLOCK) on sh.CompanyFinancialTermId = tc.CompanyFinancialTermId
  where tc.id = @cellid

DECLARE @DocumentSeriesId INT
SELECT TOP 1 @DocumentSeriesId = DocumentSeriesID
FROM Document WITH(NOLOCK) WHERE ID =  @DocumentID
 

DECLARE @OldStaticHierarchyList StaticHierarchyList

;WITH CTE_Children(ID) AS(
	SELECT ID FROM StaticHierarchy WHERE ID = @TargetSHID
	UNION ALL
	SELECT sh.Id 
	FROM StaticHierarchy sh
	JOIN CTE_Children cte on sh.ParentID = cte.ID
) INSERT @OldStaticHierarchyList ([StaticHierarchyID])
   SELECT ID 
FROM CTE_Children cte
where cte.id <> @TargetSHID
 
DECLARE @CurrentTimeSliceID int 
SELECT @CurrentTimeSliceID =  tc.DocumentTimeSliceID
FROM StaticHierarchy sh
JOIN vw_SCARDocumentTimeSliceTableCell tc ON sh.CompanyFinancialTermId = tc.CompanyFinancialTermID
WHERE sh.ID = @TargetSHID and tc.TableCellid = @cellID
 

DECLARE @OldSHCells CellList
INSERT @OldSHCells
SELECT distinct tc.TableCellID, tc.DocumentTimeSliceID
FROM StaticHierarchy sh
JOIN @OldStaticHierarchyList shl ON sh.id = shl.StaticHierarchyID
JOIN vw_SCARDocumentTimeSliceTableCell tc ON sh.CompanyFinancialTermId = tc.CompanyFinancialTermID and tc.DocumentTimeSliceID = @CurrentTimeSliceID
 
UPDATE tc 
set IsIncomePositive = CASE WHEN IsIncomePositive = 1 THEN 0 ELSE 1 END																
FROM 
TableCell tc 
JOIN @OldSHCells osh on tc.id = osh.StaticHierarchyID  --- osh staticHierarchyID is the cellID
 

 



DECLARE @SHCells CellList

INSERT @SHCells
SELECT distinct sh.ID, @CurrentTimeSliceID
FROM StaticHierarchy sh
JOIN @OldStaticHierarchyList shl ON sh.id = shl.StaticHierarchyID
JOIN vw_SCARDocumentTimeSliceTableCell tc ON sh.CompanyFinancialTermId = tc.CompanyFinancialTermID


DECLARE @SHCellsMTMW TABLE(StaticHierarchyID int, DocumentTimeSliceID int, ChildrenSum decimal(28,5), CellValue decimal(28,5))
DECLARE @SHCellsLPV TABLE(StaticHierarchyID int, DocumentTimeSliceID int, LPVFail bit)
DECLARE @SHCellsError TABLE(StaticHierarchyID int, DocumentTimeSliceID int, LPVFail bit, MTMWFail bit)

DECLARE @SHParentCells CellList
INSERT INTO @SHParentCells
EXEC SCARGetTableCellMTMW_GetFirstMTMWParent @SHCells

INSERT INTO @SHCellsMTMW
EXEC SCARGetTableCellMTMWCalc @SHParentCells

INSERT INTO @SHCellsLPV
EXEC SCARGetTableCellLikePeriod @SHCells, @DocumentID

INSERT @SHCellsError 
SELECT ISNULL(lpv.StaticHierarchyID, mtmw.StaticHierarchyID), ISNULL(lpv.DocumentTimeSliceID, mtmw.DocumentTimeSliceID), ISNULL(lpv.LPVFail, 0), CASE WHEN mtmw.ChildrenSum <> mtmw.CellValue THEN 1 ELSE 0 END
from @SHCellsLPV lpv
FULL OUTER JOIN @SHCellsMTMW mtmw ON lpv.StaticHierarchyID = mtmw.StaticHierarchyID and  lpv.DocumentTimeSliceID = mtmw.DocumentTimeSliceID



;WITH cte_level(SHRootID, SHID, level)
AS
(
	SELECT @TargetSHID, @TargetSHID, 0
	UNION ALL
	SELECT cte.SHRootID, shp.ID, cte.level+1
	FROM cte_level cte
	JOIN StaticHierarchy sh ON cte.SHID = sh.ID
	JOIN StaticHierarchy shp ON sh.ParentID = shp.ID
)
SELECT MAX(level)
FROM cte_level
GROUP BY SHRootID

SELECT distinct 'x', tc.TableCellID, tc.Offset, tc.CellPeriodType, tc.PeriodTypeID, tc.CellPeriodCount, tc.PeriodLength, tc.CellDay, 
				tc.CellMonth, tc.CellYear, tc.CellDate, tc.Value, tc.CompanyFinancialTermID, tc.ValueNumeric, tc.NormalizedNegativeIndicator, 
				tc.ScalingFactorID, tc.AsReportedScalingFactor, tc.Currency, tc.CurrencyCode, tc.Cusip, tc.ScarUpdated, tc.IsIncomePositive, 
				tc.XBRLTag, 
				tc.DocumentId, tc.Label, sf.Value,
				(select aetc.ARDErrorTypeId from ARDErrorTypeTableCell aetc (nolock) where tc.TableCellId = aetc.TableCellId),
				(select metc.MTMWErrorTypeId from MTMWErrorTypeTableCell metc (nolock) where tc.TableCellId = metc.TableCellId), 
				lpv.LPVFail, lpv.MTMWFail,
				dts.Id, sh.AdjustedOrder, dts.Duration, dts.TimeSlicePeriodEndDate, dts.ReportingPeriodEndDate, d.PublicationDateTime
FROM StaticHierarchy sh
JOIN dbo.DocumentTimeSlice dts WITH(NOLOCK) ON dts.DocumentSeriesId = @DocumentSeriesId
JOIN Document d on dts.DocumentID = d.ID
LEFT JOIN vw_SCARDocumentTimeSliceTableCell tc ON tc.CompanyFinancialTermID = sh.CompanyFinancialTermID AND tc.DocumentTimeSliceID = dts.ID
JOIN @SHCellsError lpv ON lpv.StaticHierarchyID = sh.ID AND lpv.DocumentTimeSliceID = dts.ID
LEFT JOIN ScalingFactor sf ON tc.ScalingFactorID = sf.ID
WHERE (d.ID = @DocumentID OR d.ArdExportFlag = 1 OR d.ExportFlag = 1 OR d.IsDocSetupCompleted = 1)
ORDER BY dts.TimeSlicePeriodEndDate desc, dts.Duration desc, dts.ReportingPeriodEndDate desc, d.PublicationDateTime desc;
 
";
			ScarResult result = new ScarResult();
			result.CellToDTS = new Dictionary<SCARAPITableCell, int>();
			result.ChangedCells = new List<SCARAPITableCell>();


			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {
				using (SqlCommand cmd = new SqlCommand(query, conn)) {
					conn.Open();
					cmd.Parameters.AddWithValue("@DocumentID ", DocumentId);
					cmd.Parameters.AddWithValue("@cellid", CellId);
					cmd.Parameters.AddWithValue("@Iconum", iconum);

					using (SqlDataReader reader = cmd.ExecuteReader()) {
						reader.Read();
						int level = reader.GetInt32(0);
						reader.NextResult();
						int shix = 0;
						int adjustedOrder = 0;
						while (reader.Read()) {
							SCARAPITableCell cell;
							if (reader.GetNullable<int>(1).HasValue) {
								cell = new SCARAPITableCell
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
								};
								cell.ScarUpdated = reader.GetBoolean(20);
								cell.IsIncomePositive = reader.GetBoolean(21);
								cell.XBRLTag = reader.GetStringSafe(22);
								//cell.UpdateStampUTC = reader.GetNullable<DateTime>(23);
								cell.DocumentID = reader.IsDBNull(23) ? Guid.Empty : reader.GetGuid(23);
								cell.Label = reader.GetStringSafe(24);
								cell.ScalingFactorValue = reader.GetDouble(25);
								cell.ARDErrorTypeId = reader.GetNullable<int>(26);
								cell.MTMWErrorTypeId = reader.GetNullable<int>(27);
								cell.LikePeriodValidationFlag = reader.GetBoolean(28);
								cell.MTMWValidationFlag = reader.GetBoolean(29);
								adjustedOrder = reader.GetInt32(31);
								result.ChangedCells.Add(cell);

							} else {
								continue;
							}
							result.CellToDTS.Add(cell, adjustedOrder);
						}
					}
				}
			}
			return result;
		}

		public ScarResult FlipHistorical(string CellId, Guid DocumentId, int iconum, int TargetStaticHierarchyID) {
			const string query = @"

DECLARE @TargetSHID int;

SELECT top 1 @TargetSHID = sh.id
  FROM  StaticHierarchy sh WITH (NOLOCK)
  JOIN [TableCell] tc WITH (NOLOCK) on sh.CompanyFinancialTermId = tc.CompanyFinancialTermId
  where tc.id = @cellid

DECLARE @DocumentSeriesId INT
SELECT TOP 1 @DocumentSeriesId = DocumentSeriesID
FROM Document WITH(NOLOCK) WHERE ID =  @DocumentID

DECLARE @OldStaticHierarchyList StaticHierarchyList

;WITH CTE(ID) AS(
	SELECT ID FROM StaticHierarchy WHERE ID = @TargetSHID
) INSERT @OldStaticHierarchyList ([StaticHierarchyID])
   SELECT ID 
FROM CTE cte

DECLARE @OldSHCells CellList
INSERT @OldSHCells
SELECT distinct tc.TableCellID, tc.DocumentTimeSliceID
FROM StaticHierarchy sh
JOIN @OldStaticHierarchyList shl ON sh.id = shl.StaticHierarchyID
JOIN vw_SCARDocumentTimeSliceTableCell tc ON sh.CompanyFinancialTermId = tc.CompanyFinancialTermID  

UPDATE tc 
set IsIncomePositive = CASE WHEN IsIncomePositive = 1 THEN 0 ELSE 1 END																
FROM 
TableCell tc 
JOIN @OldSHCells osh on tc.id = osh.StaticHierarchyID  --- osh staticHierarchyID is the cellID
 

DECLARE @SHCells CellList

INSERT @SHCells
SELECT distinct sh.ID, tc.DocumentTimeSliceId
FROM StaticHierarchy sh
JOIN @OldStaticHierarchyList shl ON sh.id = shl.StaticHierarchyID
JOIN vw_SCARDocumentTimeSliceTableCell tc ON sh.CompanyFinancialTermId = tc.CompanyFinancialTermID


DECLARE @SHCellsMTMW TABLE(StaticHierarchyID int, DocumentTimeSliceID int, ChildrenSum decimal(28,5), CellValue decimal(28,5))
DECLARE @SHCellsLPV TABLE(StaticHierarchyID int, DocumentTimeSliceID int, LPVFail bit)
DECLARE @SHCellsError TABLE(StaticHierarchyID int, DocumentTimeSliceID int, LPVFail bit, MTMWFail bit)

DECLARE @SHParentCells CellList
INSERT INTO @SHParentCells
EXEC SCARGetTableCellMTMW_GetFirstMTMWParent @SHCells

INSERT INTO @SHCellsMTMW
EXEC SCARGetTableCellMTMWCalc @SHParentCells

INSERT INTO @SHCellsLPV
EXEC SCARGetTableCellLikePeriod @SHCells, @DocumentID

INSERT @SHCellsError 
SELECT ISNULL(lpv.StaticHierarchyID, mtmw.StaticHierarchyID), ISNULL(lpv.DocumentTimeSliceID, mtmw.DocumentTimeSliceID), ISNULL(lpv.LPVFail, 0), CASE WHEN mtmw.ChildrenSum <> mtmw.CellValue THEN 1 ELSE 0 END
from @SHCellsLPV lpv
FULL OUTER JOIN @SHCellsMTMW mtmw ON lpv.StaticHierarchyID = mtmw.StaticHierarchyID and  lpv.DocumentTimeSliceID = mtmw.DocumentTimeSliceID



;WITH cte_level(SHRootID, SHID, level)
AS
(
	SELECT @TargetSHID, @TargetSHID, 0
	UNION ALL
	SELECT cte.SHRootID, shp.ID, cte.level+1
	FROM cte_level cte
	JOIN StaticHierarchy sh ON cte.SHID = sh.ID
	JOIN StaticHierarchy shp ON sh.ParentID = shp.ID
)
SELECT MAX(level)
FROM cte_level
GROUP BY SHRootID

SELECT distinct 'x', tc.TableCellID, tc.Offset, tc.CellPeriodType, tc.PeriodTypeID, tc.CellPeriodCount, tc.PeriodLength, tc.CellDay, 
				tc.CellMonth, tc.CellYear, tc.CellDate, tc.Value, tc.CompanyFinancialTermID, tc.ValueNumeric, tc.NormalizedNegativeIndicator, 
				tc.ScalingFactorID, tc.AsReportedScalingFactor, tc.Currency, tc.CurrencyCode, tc.Cusip, tc.ScarUpdated, tc.IsIncomePositive, 
				tc.XBRLTag, 
				tc.DocumentId, tc.Label, sf.Value,
				(select aetc.ARDErrorTypeId from ARDErrorTypeTableCell aetc (nolock) where tc.TableCellId = aetc.TableCellId),
				(select metc.MTMWErrorTypeId from MTMWErrorTypeTableCell metc (nolock) where tc.TableCellId = metc.TableCellId), 
				lpv.LPVFail, lpv.MTMWFail,
				dts.Id, sh.AdjustedOrder, dts.Duration, dts.TimeSlicePeriodEndDate, dts.ReportingPeriodEndDate, d.PublicationDateTime
FROM StaticHierarchy sh
JOIN dbo.DocumentTimeSlice dts WITH(NOLOCK) ON dts.DocumentSeriesId = @DocumentSeriesId
JOIN Document d on dts.DocumentID = d.ID
LEFT JOIN vw_SCARDocumentTimeSliceTableCell tc ON tc.CompanyFinancialTermID = sh.CompanyFinancialTermID AND tc.DocumentTimeSliceID = dts.ID
JOIN @SHCellsError lpv ON lpv.StaticHierarchyID = sh.ID AND lpv.DocumentTimeSliceID = dts.ID
LEFT JOIN ScalingFactor sf ON tc.ScalingFactorID = sf.ID
WHERE (d.ID = @DocumentID OR d.ArdExportFlag = 1 OR d.ExportFlag = 1 OR d.IsDocSetupCompleted = 1)
ORDER BY dts.TimeSlicePeriodEndDate desc, dts.Duration desc, dts.ReportingPeriodEndDate desc, d.PublicationDateTime desc;
 

";
			ScarResult result = new ScarResult();
			result.CellToDTS = new Dictionary<SCARAPITableCell, int>();
			result.ChangedCells = new List<SCARAPITableCell>();


			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {
				using (SqlCommand cmd = new SqlCommand(query, conn)) {
					conn.Open();
					cmd.Parameters.AddWithValue("@DocumentID ", DocumentId);
					cmd.Parameters.AddWithValue("@cellid", CellId);
					cmd.Parameters.AddWithValue("@Iconum", iconum);

					using (SqlDataReader reader = cmd.ExecuteReader()) {
						reader.Read();
						int level = reader.GetInt32(0);
						reader.NextResult();
						int shix = 0;
						int adjustedOrder = 0;
						while (reader.Read()) {
							SCARAPITableCell cell;
							if (reader.GetNullable<int>(1).HasValue) {
								cell = new SCARAPITableCell
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
								};
								cell.ScarUpdated = reader.GetBoolean(20);
								cell.IsIncomePositive = reader.GetBoolean(21);
								cell.XBRLTag = reader.GetStringSafe(22);
								//cell.UpdateStampUTC = reader.GetNullable<DateTime>(23);
								cell.DocumentID = reader.IsDBNull(23) ? Guid.Empty : reader.GetGuid(23);
								cell.Label = reader.GetStringSafe(24);
								cell.ScalingFactorValue = reader.GetDouble(25);
								cell.ARDErrorTypeId = reader.GetNullable<int>(26);
								cell.MTMWErrorTypeId = reader.GetNullable<int>(27);
								cell.LikePeriodValidationFlag = reader.GetBoolean(28);
								cell.MTMWValidationFlag = reader.GetBoolean(29);
								adjustedOrder = reader.GetInt32(31);
								result.ChangedCells.Add(cell);

							} else {
								continue;
							}
							result.CellToDTS.Add(cell, adjustedOrder);
						}
					}
				}
			}
			return result;
		}

		public ScarResult FlipChildrenHistorical(string CellId, Guid DocumentId, int iconum, int TargetStaticHierarchyID) {
			const string query = @"

DECLARE @TargetSHID int;
SELECT top 1 @TargetSHID = sh.id
  FROM  StaticHierarchy sh WITH (NOLOCK)
  JOIN [TableCell] tc WITH (NOLOCK) on sh.CompanyFinancialTermId = tc.CompanyFinancialTermId
  where tc.id = @cellid

DECLARE @DocumentSeriesId INT
SELECT TOP 1 @DocumentSeriesId = DocumentSeriesID
FROM Document WITH(NOLOCK) WHERE ID =  @DocumentID

DECLARE @OldStaticHierarchyList StaticHierarchyList

;WITH CTE_Children(ID) AS(
	SELECT ID FROM StaticHierarchy WHERE ID = @TargetSHID
	UNION ALL
	SELECT sh.Id 
	FROM StaticHierarchy sh
	JOIN CTE_Children cte on sh.ParentID = cte.ID
) INSERT @OldStaticHierarchyList ([StaticHierarchyID])
   SELECT ID 
FROM CTE_Children cte
where cte.id <> @TargetSHID
 


DECLARE @OldSHCells CellList
INSERT @OldSHCells
SELECT distinct tc.TableCellID, tc.DocumentTimeSliceID
FROM StaticHierarchy sh
JOIN @OldStaticHierarchyList shl ON sh.id = shl.StaticHierarchyID
JOIN vw_SCARDocumentTimeSliceTableCell tc ON sh.CompanyFinancialTermId = tc.CompanyFinancialTermID and sh.CompanyFinancialTermId = tc.CompanyFinancialTermID  
 
UPDATE tc 
set IsIncomePositive = CASE WHEN IsIncomePositive = 1 THEN 0 ELSE 1 END																
FROM 
TableCell tc 
JOIN @OldSHCells osh on tc.id = osh.StaticHierarchyID  --- osh staticHierarchyID is the cellID
 

 



DECLARE @SHCells CellList

INSERT @SHCells
SELECT distinct  sh.ID, tc.DocumentTimeSliceId
FROM StaticHierarchy sh
JOIN @OldStaticHierarchyList shl ON sh.id = shl.StaticHierarchyID
JOIN vw_SCARDocumentTimeSliceTableCell tc ON sh.CompanyFinancialTermId = tc.CompanyFinancialTermID


DECLARE @SHCellsMTMW TABLE(StaticHierarchyID int, DocumentTimeSliceID int, ChildrenSum decimal(28,5), CellValue decimal(28,5))
DECLARE @SHCellsLPV TABLE(StaticHierarchyID int, DocumentTimeSliceID int, LPVFail bit)
DECLARE @SHCellsError TABLE(StaticHierarchyID int, DocumentTimeSliceID int, LPVFail bit, MTMWFail bit)

DECLARE @SHParentCells CellList
INSERT INTO @SHParentCells
EXEC SCARGetTableCellMTMW_GetFirstMTMWParent @SHCells

INSERT INTO @SHCellsMTMW
EXEC SCARGetTableCellMTMWCalc @SHParentCells

INSERT INTO @SHCellsLPV
EXEC SCARGetTableCellLikePeriod @SHCells, @DocumentID

INSERT @SHCellsError 
SELECT ISNULL(lpv.StaticHierarchyID, mtmw.StaticHierarchyID), ISNULL(lpv.DocumentTimeSliceID, mtmw.DocumentTimeSliceID), ISNULL(lpv.LPVFail, 0), CASE WHEN mtmw.ChildrenSum <> mtmw.CellValue THEN 1 ELSE 0 END
from @SHCellsLPV lpv
FULL OUTER JOIN @SHCellsMTMW mtmw ON lpv.StaticHierarchyID = mtmw.StaticHierarchyID and  lpv.DocumentTimeSliceID = mtmw.DocumentTimeSliceID



;WITH cte_level(SHRootID, SHID, level)
AS
(
	SELECT @TargetSHID, @TargetSHID, 0
	UNION ALL
	SELECT cte.SHRootID, shp.ID, cte.level+1
	FROM cte_level cte
	JOIN StaticHierarchy sh ON cte.SHID = sh.ID
	JOIN StaticHierarchy shp ON sh.ParentID = shp.ID
)
SELECT MAX(level)
FROM cte_level
GROUP BY SHRootID

SELECT distinct 'x', tc.TableCellID, tc.Offset, tc.CellPeriodType, tc.PeriodTypeID, tc.CellPeriodCount, tc.PeriodLength, tc.CellDay, 
				tc.CellMonth, tc.CellYear, tc.CellDate, tc.Value, tc.CompanyFinancialTermID, tc.ValueNumeric, tc.NormalizedNegativeIndicator, 
				tc.ScalingFactorID, tc.AsReportedScalingFactor, tc.Currency, tc.CurrencyCode, tc.Cusip, tc.ScarUpdated, tc.IsIncomePositive, 
				tc.XBRLTag, 
				tc.DocumentId, tc.Label, sf.Value,
				(select aetc.ARDErrorTypeId from ARDErrorTypeTableCell aetc (nolock) where tc.TableCellId = aetc.TableCellId),
				(select metc.MTMWErrorTypeId from MTMWErrorTypeTableCell metc (nolock) where tc.TableCellId = metc.TableCellId), 
				lpv.LPVFail, lpv.MTMWFail,
				dts.Id, sh.AdjustedOrder, dts.Duration, dts.TimeSlicePeriodEndDate, dts.ReportingPeriodEndDate, d.PublicationDateTime
FROM StaticHierarchy sh
JOIN dbo.DocumentTimeSlice dts WITH(NOLOCK) ON dts.DocumentSeriesId = @DocumentSeriesId
JOIN Document d on dts.DocumentID = d.ID
LEFT JOIN vw_SCARDocumentTimeSliceTableCell tc ON tc.CompanyFinancialTermID = sh.CompanyFinancialTermID AND tc.DocumentTimeSliceID = dts.ID
JOIN @SHCellsError lpv ON lpv.StaticHierarchyID = sh.ID AND lpv.DocumentTimeSliceID = dts.ID
LEFT JOIN ScalingFactor sf ON tc.ScalingFactorID = sf.ID
WHERE (d.ID = @DocumentID OR d.ArdExportFlag = 1 OR d.ExportFlag = 1 OR d.IsDocSetupCompleted = 1)
ORDER BY dts.TimeSlicePeriodEndDate desc, dts.Duration desc, dts.ReportingPeriodEndDate desc, d.PublicationDateTime desc;
 
";
			ScarResult result = new ScarResult();
			result.CellToDTS = new Dictionary<SCARAPITableCell, int>();
			result.ChangedCells = new List<SCARAPITableCell>();


			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {
				using (SqlCommand cmd = new SqlCommand(query, conn)) {
					conn.Open();
					cmd.Parameters.AddWithValue("@DocumentID ", DocumentId);
					cmd.Parameters.AddWithValue("@cellid", CellId);
					cmd.Parameters.AddWithValue("@Iconum", iconum);

					using (SqlDataReader reader = cmd.ExecuteReader()) {
						reader.Read();
						int level = reader.GetInt32(0);
						reader.NextResult();
						int shix = 0;
						int adjustedOrder = 0;
						while (reader.Read()) {
							SCARAPITableCell cell;
							if (reader.GetNullable<int>(1).HasValue) {
								cell = new SCARAPITableCell
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
								};
								cell.ScarUpdated = reader.GetBoolean(20);
								cell.IsIncomePositive = reader.GetBoolean(21);
								cell.XBRLTag = reader.GetStringSafe(22);
								//cell.UpdateStampUTC = reader.GetNullable<DateTime>(23);
								cell.DocumentID = reader.IsDBNull(23) ? Guid.Empty : reader.GetGuid(23);
								cell.Label = reader.GetStringSafe(24);
								cell.ScalingFactorValue = reader.GetDouble(25);
								cell.ARDErrorTypeId = reader.GetNullable<int>(26);
								cell.MTMWErrorTypeId = reader.GetNullable<int>(27);
								cell.LikePeriodValidationFlag = reader.GetBoolean(28);
								cell.MTMWValidationFlag = reader.GetBoolean(29);
								adjustedOrder = reader.GetInt32(31);
								result.ChangedCells.Add(cell);

							} else {
								continue;
							}
							result.CellToDTS.Add(cell, adjustedOrder);
						}
					}
				}
			}
			return result;
		}

		public ScarResult SwapValue(string firstCellId, string secondCellId) {
			const string query = @"

BEGIN TRY
	BEGIN TRAN
		DECLARE @docId uniqueidentifier 
		select top 1 @docId = Documentid from TableCell where id = @firstCellId

		DECLARE @firstCFT int
		select top 1 @firstCFT = CompanyFinancialTermID from TableCell where id = @firstCellId

		DECLARE @secondCFT int
		select top 1 @secondCFT = CompanyFinancialTermID from TableCell where id = @secondCellId
 
		IF (@firstCFT is null) RAISERROR('Null CFT', 16, 1);
		if (@secondCFT is null) RAISERROR('Null CFT', 16, 1);
		DECLARE @TempCells Table(ID INT)
		INSERT @TempCells (ID)
		SELECT ID from TableCell where CompanyFinancialTermID = @secondCFT and Documentid = @docId

		Update TableCell set CompanyFinancialTermID = @secondCFT, ScarUpdated = 1 where CompanyFinancialTermID = @firstCFT and Documentid = @docId
		Update TableCell set CompanyFinancialTermID = @firstCFT , ScarUpdated = 1 where id in (select id from @TempCells)

		UPDATE CompanyFinancialTerm set TermStatusID = 1 where id = @firstCFT
		UPDATE CompanyFinancialTerm set TermStatusID = 1 where id = @secondCFT
		COMMIT 
		select 1
END TRY
BEGIN CATCH
	ROLLBACK;
	throw;
	select 0
END CATCH
";
			ScarResult result = new ScarResult();
			result.CellToDTS = new Dictionary<SCARAPITableCell, int>();
			result.ChangedCells = new List<SCARAPITableCell>();


			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {
				using (SqlCommand cmd = new SqlCommand(query, conn)) {
					conn.Open();
					cmd.Parameters.AddWithValue("@firstCellId ", firstCellId);
					cmd.Parameters.AddWithValue("@secondCellId", secondCellId);

					using (SqlDataReader reader = cmd.ExecuteReader()) {
						reader.Read();
						int success = reader.GetInt32(0);
						if (success == 1) {
							result.ReturnValue["Success"] = "T";

						} else {
							result.ReturnValue["Success"] = "F";
						}
					}

				}
			}
			return result;
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
				CellToDTS = new Dictionary<SCARAPITableCell, int>(),
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
							Cells = new List<SCARAPITableCell>(),
							Level = level
						};
						res.StaticHierarchy = document;
						sdr.NextResult();
						while (sdr.Read()) {
							SCARAPITableCell cell;
							if (sdr.GetNullable<int>(0).HasValue) {
								cell = new SCARAPITableCell
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
								cell = new SCARAPITableCell();
							}
							document.Cells.Add(cell);

							res.CellToDTS.Add(cell, sdr.GetInt32(29));
						}
					}
				}
			}

			foreach (SCARAPITableCell cell in res.StaticHierarchy.Cells) {
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

			Dictionary<Tuple<int, int>, SCARAPITableCell> CellMap = new Dictionary<Tuple<int, int>, SCARAPITableCell>();
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
								Cells = new List<SCARAPITableCell>()
							};
							res.StaticHierarchies.Add(document);
						}

						sdr.NextResult();

						int shix = 0;
						int adjustedOrder = 0;

						while (sdr.Read()) {
							SCARAPITableCell cell;
							if (sdr.GetNullable<int>(0).HasValue) {
								cell = new SCARAPITableCell
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
								cell = new SCARAPITableCell();
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
				Tuple<int, int> tup = new Tuple<int, int>(comp.RootStaticHierarchyID, comp.RootDocumentTimeSliceID);
				if (!CellValueMap.ContainsKey(tup))
					CellValueMap.Add(tup, 0);

				CellValueMap[tup] += comp.ValueNumeric * ((decimal)comp.ScalingFactorValue) * (comp.IsIncomePositive ? 1 : -1);
			}

			foreach (Tuple<int, int> key in CellMap.Keys) {
				SCARAPITableCell cell = CellMap[key];
				if (CellValueMap.ContainsKey(key))
					cell.MTMWValidationFlag = (cell.ValueNumeric * (decimal)cell.ScalingFactorValue * (cell.IsIncomePositive ? 1 : -1)) != CellValueMap[key];
			}

			foreach (StaticHierarchy sh in res.StaticHierarchies) {
				sh.Level = SHLevels[sh.Id];
			}

			return res;
		}

		#region Deprecated Methods
		public SCARAPITableCell GetCell(string CellId) {
			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {
				using (SqlCommand cmd = new SqlCommand(SQL_GetCellQuery, conn)) {
					conn.Open();
					cmd.Parameters.AddWithValue("@cellId", CellId);

					using (SqlDataReader reader = cmd.ExecuteReader()) {

						int shix = 0;

						int adjustedOrder = 0;
						while (reader.Read()) {
							SCARAPITableCell cell;
							if (reader.GetNullable<int>(0).HasValue) {
								cell = new SCARAPITableCell
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
								cell = new SCARAPITableCell();
								adjustedOrder = reader.GetInt32(28);
							}
							return cell;
						}
					}
				}
			}
			return new SCARAPITableCell();
		}

		public TableCellResult AddMakeTheMathWorkNote(string CellId, Guid DocumentId) {
			TableCellResult result = new TableCellResult();
			result.cells = new List<SCARAPITableCell>();

			SCARAPITableCell currCell = GetCell(CellId);
			currCell.MTMWErrorTypeId = 0;
			result.cells.Add(currCell);
			SCARAPITableCell[] sibilings = getSibilingsCells(CellId, DocumentId);
			result.cells.AddRange(sibilings);
			return result;
		}

		public TableCellResult AddMakeTheMathWorkNote(string CellId, Guid DocumentId, string newValue) {
			string query = @"


IF @newValue = 0
BEGIN
	DELETE FROM MTMWErrorTypeTableCell WHERE TableCellId = @id  
END
ELSE
BEGIN
	MERGE INTO MTMWErrorTypeTableCell mtmwtc
	USING (VALUES (@id)) AS s(id) ON  mtmwtc.TableCellId = s.id
	WHEN NOT MATCHED THEN
			INSERT ([MTMWErrorTypeId] ,[TableCellId])
				VALUES (@newValue, @id )
	WHEN MATCHED THEN
		UPDATE SET [MTMWErrorTypeId] = @newValue ;

END



";
			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {

				using (SqlCommand cmd = new SqlCommand(query, conn)) {
					conn.Open();
					int newInt = -1;
					bool isSuccess = false;
					if (!string.IsNullOrEmpty(newValue)) {
						isSuccess = Int32.TryParse(newValue, out newInt);
					}
					cmd.Parameters.AddWithValue("@id", CellId);
					cmd.Parameters.Add(new SqlParameter("@newValue", SqlDbType.Int)
					{
						Value = (!isSuccess ? DBNull.Value : (object)newInt)
					});
					cmd.ExecuteNonQuery();
				}
			}
			return AddMakeTheMathWorkNote(CellId, DocumentId);
		}

		public TableCellResult AddLikePeriodValidationNote(string CellId, Guid DocumentId) {
			TableCellResult result = new TableCellResult();
			result.cells = new List<SCARAPITableCell>();

			SCARAPITableCell currCell = GetCell(CellId);
			currCell.LikePeriodValidationFlag = true;
			currCell.MTMWValidationFlag = true;
			result.cells.Add(currCell);
			SCARAPITableCell[] sibilings = getSibilingsCells(CellId, DocumentId);
			result.cells.AddRange(sibilings);
			return result;
		}

		public TableCellResult AddLikePeriodValidationNote(string CellId, Guid DocumentId, string newValue) {
			string query = @"


IF @newValue = 0
BEGIN
	DELETE FROM ARDErrorTypeTableCell WHERE TableCellId = @id  
END
ELSE
BEGIN
	MERGE INTO ARDErrorTypeTableCell ardtc
	USING (VALUES (@id)) AS s(id) ON  ardtc.TableCellId = s.id
	WHEN NOT MATCHED THEN
			INSERT ([ARDErrorTypeId] ,[TableCellId])
				VALUES (@newValue, @id )
	WHEN MATCHED THEN
		UPDATE SET [ARDErrorTypeId] = @newValue ;

END



";
			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {

				using (SqlCommand cmd = new SqlCommand(query, conn)) {
					conn.Open();
					int newInt = -1;
					bool isSuccess = false;
					if (!string.IsNullOrEmpty(newValue)) {
						isSuccess = Int32.TryParse(newValue, out newInt);
					}
					cmd.Parameters.AddWithValue("@id", CellId);
					cmd.Parameters.Add(new SqlParameter("@newValue", SqlDbType.Int)
					{
						Value = (!isSuccess ? DBNull.Value : (object)newInt)
					});
					cmd.ExecuteNonQuery();
				}
			}
			return AddLikePeriodValidationNote(CellId, DocumentId);
		}

		private SCARAPITableCell[] getSibilingsCells(string CellId, Guid DocumentId) {
			return new SCARAPITableCell[0];
		}
		#endregion

		#region Zero-Minute Update
		public Dictionary<string, string> UpdateRedStarSlotting(Guid SFDocumentId) {
			string query = @"
SELECT 'redstar_result' as result, 'Red star item is not part of the static hierarchy' as msg
FROM StaticHierarchy sh (nolock)
where sh.id in ({0}) and sh.ParentId is null
UNION
SELECT 'redstar_result' as result, 'Red star item in share and per share sections' as msg
FROM StaticHierarchy sh (nolock)
where sh.id in ({0}) and (lower(sh.Description) like '%\[per share\]%'  escape '\' or lower(sh.Description) like '%\[weighted average shares\]%'   escape '\')
";
			bool isSuccess = false;
			var sb = new System.Text.StringBuilder();
			string returnMessage = "";
			try {
				using (SqlConnection sqlConn = new SqlConnection(_sfConnectionString)) {
					sqlConn.Open();
						bool isComma = false;

					using (SqlCommand cmd = new SqlCommand("prcUpd_FFDocHist_UpdateAdjustRedStar", sqlConn)) {
						cmd.CommandType = CommandType.StoredProcedure;

						cmd.Parameters.Add("@DocumentID", SqlDbType.UniqueIdentifier).Value = SFDocumentId;
						using (SqlDataReader sdr = cmd.ExecuteReader()) {
							while (sdr.Read()) {
								var firstfield = "";
								try {
									firstfield = sdr.GetStringSafe(0);
								} catch {
								}
								if (firstfield == "RedStarLabels") {
									var shId = sdr.GetInt32(1).ToString();
									sb.Append((isComma ? "," : "") + shId);
									isComma = true;
								} else {
									sdr.NextResult();
								}
							}
						}
						isSuccess = true;
					}
					if (isComma) { // at least one ID
						using (SqlCommand cmd = new SqlCommand(string.Format(query, sb.ToString()), sqlConn)) {
							//cmd.Parameters.AddWithValue("@SHIds", sb.ToString());
							using (SqlDataReader sdr = cmd.ExecuteReader()) {
								if (sdr.Read()) {
									returnMessage += sdr.GetStringSafe(1);
									isSuccess = false;
								}
							}
						}
					}
				}
			} catch (Exception ex) {
				isSuccess = false;
				returnMessage = "Exception occured during RedStar slotting";
			}
			Dictionary<string, string> returnValue = new Dictionary<string, string>();
			returnValue["Success"] = isSuccess ? "T" : "F";
			returnValue["Message"] = returnMessage;
			return returnValue;
		}

		public string GetDocumentIsoCountry(Guid SFDocumentId) {
			string query = @"
select TOP 1 isoCountry
from Document d
join PPIIconumMap pim on d.PPI = pim.PPI
WHERE d.ID = @SFDocumentID 
";
			string isoCountry = "";
			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {
				conn.Open();
				using (SqlCommand cmd = new SqlCommand(query, conn)) {
					cmd.Parameters.AddWithValue("@SFDocumentID", SFDocumentId);
					using (SqlDataReader sdr = cmd.ExecuteReader()) {
						if (sdr.Read()) {
							isoCountry = sdr.GetStringSafe(0);
						}
					}
				}
			}
			return isoCountry;
		}

		public string CheckParsedTableInterimTypeAndCurrency(Guid SFDocumentId, int Iconum) {
			string query = @"

 DECLARE @BigThree Table (Description varchar(64))
 INSERT  @BigThree (Description)
 VALUES 
 ('IS'), ('BS'), ('CF')

 SELECT TOP 1 'Missing Table. ' as Error, *
 FROM DocumentTable dt
 JOIN TableType tt ON dt.TableTypeID = tt.id
 RIGHT JOIN @BigThree bt on bt.Description = tt.description
 where dt.DocumentID = @DocumentId and tt.description is null


 SELECT TOP 1 'Missing InterimType. ' as Error, * 
 FROM DocumentTimeSlice dts
 JOIN DocumentTimeSliceTableCell dtstc on dtstc.DocumentTimeSliceId = dts.Id
 JOIN TableCell tc ON dtstc.TableCellID = tc.id
  where dts.DocumentID = @DocumentId and dts.PeriodType is null

 SELECT TOP 1 'Missing InterimType. ' as Error, * 
  FROM DocumentTable dt
 JOIN TableType tt ON dt.TableTypeID = tt.id
 JOIN TableDimension td on dt.TableIntID = td.DocumentTableID
 JOIN DimensionToCell dtc on dtc.TableDimensionID = td.ID
 JOIN TableCell tc ON dtc.TableCellID = tc.id
 JOIN DocumentTimeSliceTableCell dtstc on dtstc.TableCellId = tc.Id
 JOIN DocumentTimeSlice dts on dtstc.DocumentTimeSliceId = dts.Id
  where dt.DocumentID = @DocumentId and dts.PeriodType is null


 ;WITH cte (id) as
(
 SELECT  dtc.TableCellid
 FROM DocumentTable dt
 JOIN TableType tt ON dt.TableTypeID = tt.id
 JOIN TableDimension td on dt.TableIntID = td.DocumentTableID
 JOIN DimensionToCell dtc on dtc.TableDimensionID = td.ID
 where dt.DocumentID = @DocumentId 
)
 SELECT TOP 1 'Missing Currency. ' as Error, * 
 FROM cte
 JOIN TableCell tc ON cte.id = tc.id 
 where  tc.currencycode is null
";
			string errorMessage = "";
			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {
				conn.Open();

				using (SqlCommand cmd = new SqlCommand("prcInsert_CreateDocumentTimeSlices", conn)) {
					cmd.CommandType = CommandType.StoredProcedure;
					cmd.Parameters.AddWithValue("@DocID", SFDocumentId);
					cmd.ExecuteNonQuery();
				}

				using (SqlCommand cmd = new SqlCommand("SCARSetParentIDs", conn)) {
					cmd.CommandType = CommandType.StoredProcedure;
					cmd.Parameters.AddWithValue("@Iconum", Iconum);
					cmd.ExecuteNonQuery();
				}


				using (SqlCommand cmd = new SqlCommand(query, conn)) {
					cmd.Parameters.AddWithValue("@DocumentID", SFDocumentId);


					using (SqlDataReader sdr = cmd.ExecuteReader()) {
						if (sdr.Read()) {
							errorMessage += sdr.GetStringSafe(0);
						}
						sdr.NextResult();
						if (sdr.Read()) {
							errorMessage += sdr.GetStringSafe(0);
						}
						sdr.NextResult();
						if (sdr.Read()) {
							errorMessage += sdr.GetStringSafe(0);
						}
						sdr.NextResult();
						if (sdr.Read()) {
							errorMessage += sdr.GetStringSafe(0);
						}
					}
				}
			}
			return errorMessage;
		}

		public bool GetMtmwTableCells(int iconum, Guid DocumentId) {
			var sw = System.Diagnostics.Stopwatch.StartNew();


			string CellsQuery =
				@"

 DECLARE @iconum INT 

 DECLARE  @IconumList TABLE(CompanyId INT)
 INSERT @IconumList(CompanyId )
 SELECT ds.CompanyID 
 FROM Document d
 JOIN DocumentSeries ds on d.DocumentSeriesID = ds.id
 WHERE d.id = @DocumentId
 IF (@GuessedIconum not in (SELECT CompanyId from @IconumList))
 BEGIN
	SELECT TOP 1 @Iconum = Companyid 
	FROM @IconumList
 END
 ELSE
 BEGIN
	SET @Iconum = @GuessedIconum
 END

DECLARE @SHCells CellList

INSERT @SHCells
SELECT distinct sh.ID, dts.Id
FROM vw_SCARDocumentTimeSlices dts
JOIN StaticHierarchy sh ON sh.TableTypeId = dts.TableTypeID
WHERE CompanyID = @iconum



DECLARE @SHCellsMTMW TABLE(StaticHierarchyID int, DocumentTimeSliceID int, ChildrenSum decimal(28,5), CellValue decimal(28,5))



INSERT INTO @SHCellsMTMW
EXEC SCARGetTableCellMTMWCalc @SHCells

select StaticHierarchyID, DocumentTimeSliceID from @SHCellsMTMW
WHERE ChildrenSum is not null
AND ChildrenSum <> CellValue

";//I hate this query, it is so bad



			ScarResult result = new ScarResult();

			result.ChangedCells = new List<SCARAPITableCell>();

			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {
				conn.Open();
				using (SqlCommand cmd = new SqlCommand(CellsQuery, conn)) {
					cmd.Parameters.AddWithValue("@GuessedIconum", iconum);
					cmd.Parameters.AddWithValue("@DocumentID", DocumentId);

					using (SqlDataReader reader = cmd.ExecuteReader()) {
						return !reader.HasRows;
					}
				}
			}
			return false;
		}

		public ScarResult GetLpvTableCells(int iconum, Guid DocumentId) {
			var sw = System.Diagnostics.Stopwatch.StartNew();


			string CellsQuery =
				@"
 DECLARE @iconum INT 

 DECLARE  @IconumList TABLE(CompanyId INT)
 INSERT @IconumList(CompanyId )
 SELECT ds.CompanyID 
 FROM Document d
 JOIN DocumentSeries ds on d.DocumentSeriesID = ds.id
 WHERE d.id = @DocumentId
 IF (@GuessedIconum not in (SELECT CompanyId from @IconumList))
 BEGIN
	SELECT TOP 1 @Iconum = Companyid 
	FROM @IconumList
 END
 ELSE
 BEGIN
	SET @Iconum = @GuessedIconum
 END
 
  SELECT DISTINCT tc.ID, tc.Offset, tc.CellPeriodType, tc.PeriodTypeID, tc.CellPeriodCount, tc.PeriodLength, tc.CellDay, 
				tc.CellMonth, tc.CellYear, tc.CellDate, tc.Value, tc.CompanyFinancialTermID, tc.ValueNumeric, tc.NormalizedNegativeIndicator, 
				tc.ScalingFactorID, tc.AsReportedScalingFactor, tc.Currency, tc.CurrencyCode, tc.Cusip, tc.ScarUpdated, tc.IsIncomePositive, 
tc.XBRLTag, 
--tc.UpdateStampUTC
null
, tc.DocumentId, tc.Label, tc.ScalingFactorValue,
				(select aetc.ARDErrorTypeId from ARDErrorTypeTableCell aetc (nolock) where tc.Id = aetc.TableCellId) as ArdError,
				(select metc.MTMWErrorTypeId from MTMWErrorTypeTableCell metc (nolock) where tc.Id = metc.TableCellId) as MtmwError, 
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
	AND (d.ID = @DocumentID OR d.ArdExportFlag = 1 OR d.ExportFlag = 1 OR d.IsDocSetupCompleted = 1)
) as ts on 1=1
JOIN DocumentTimeSlice dts on dts.ID = ts.ID
JOIN(
	SELECT tc.*, dtstc.DocumentTimeSliceID, sf.Value as ScalingFactorValue
	FROM DocumentSeries ds
	JOIN CompanyFinancialTerm cft ON cft.DocumentSeriesId = ds.Id
	JOIN StaticHierarchy sh on cft.ID = sh.CompanyFinancialTermID
	JOIN TableType tt on sh.TableTypeID = tt.ID
	JOIN TableCell tc on tc.CompanyFinancialTermID = cft.ID
	JOIN ARDErrorTypeTableCell aetc ON tc.Id = aetc.TableCellId
	JOIN DocumentTimeSliceTableCell dtstc on dtstc.TableCellID = tc.ID
	JOIN ScalingFactor sf on sf.ID = tc.ScalingFactorID
	WHERE ds.CompanyID = @iconum
) as tc ON tc.DocumentTimeSliceID = ts.ID AND tc.CompanyFinancialTermID = cft.ID
JOIN Document d on dts.documentid = d.ID
WHERE ds.CompanyID = @iconum
ORDER BY sh.AdjustedOrder asc, dts.TimeSlicePeriodEndDate desc, dts.Duration desc, dts.ReportingPeriodEndDate desc, d.PublicationDateTime desc

";//I hate this query, it is so bad



			ScarResult result = new ScarResult();

			result.ChangedCells = new List<SCARAPITableCell>();

			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {
				conn.Open();
				using (SqlCommand cmd = new SqlCommand(CellsQuery, conn)) {
					cmd.Parameters.AddWithValue("@GuessedIconum", iconum);
					cmd.Parameters.AddWithValue("@DocumentID", DocumentId);

					using (SqlDataReader reader = cmd.ExecuteReader()) {

						int shix = 0;
						int i = 0;
						int adjustedOrder = 0;
						while (reader.Read()) {
							SCARAPITableCell cell;
							if (reader.GetNullable<int>(0).HasValue) {
								cell = new SCARAPITableCell
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
									DocumentID = reader.IsDBNull(23) ? new Guid("00000000-0000-0000-0000-000000000000") : reader.GetGuid(23),
									//	DocumentID = reader.GetGuid(23),
									Label = reader.GetStringSafe(24),
									ScalingFactorValue = reader.GetDouble(25),
									ARDErrorTypeId = reader.GetNullable<int>(26),
									MTMWErrorTypeId = reader.GetNullable<int>(27)
								};

								result.ChangedCells.Add(cell);
							} else {

							}
						}
					}
				}

			}
			return result;
		}
		#endregion

		internal IEnumerable<string> GetAllTemplates(string ConnectionString, int Iconum) {

			using (SqlConnection conn = new SqlConnection(ConnectionString)) {
				conn.Open();
				using (SqlCommand cmd = new SqlCommand(@"SELECT tt.Description FROM DocumentSeries ds JOIN TableType tt ON tt.DocumentSeriesID = ds.ID WHERE ds.CompanyID = @Iconum", conn)) {
					cmd.Parameters.AddWithValue(@"@Iconum", Iconum);
					using (SqlDataReader sdr = cmd.ExecuteReader()) {
						return sdr.Cast<IDataRecord>().Select(r => r.GetStringSafe(0)).ToList();
					}
				}
			}
		}

		public void SetIncomeOrientation(Guid DocumentID) {
			var url = ConfigurationManager.AppSettings["SetIncomeOrientationURL"];

			List<Tuple<int, int>> Tables;

			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {
				conn.Open();
				using (SqlCommand cmd = new SqlCommand("select ID, TableTypeID from [dbo].[vw_SCARDocumentTimeSlices] WHERE DocumentID = @DocumentID", conn)) {
					cmd.Parameters.AddWithValue("@DocumentID", DocumentID);
					using (SqlDataReader sdr = cmd.ExecuteReader()) {
						 Tables = sdr.Cast<IDataRecord>().Select(r => new Tuple<int, int>(r.GetInt32(0), r.GetInt32(1))).ToList();
					}
				}
			}

			foreach (Tuple<int, int> table in Tables) {
				HttpWebRequest request = (HttpWebRequest)WebRequest.Create(url + table.Item1 + "/" + table.Item2);
				request.ContentType = "application/json";
				request.Method = "GET";
				var response = (HttpWebResponse)request.GetResponse();
				//We can get the response if we care but for now we can just let it run
				//if (response.StatusCode == HttpStatusCode.OK) {
				//	using (var streamReader = new StreamReader(response.GetResponseStream())) {
				//		var outputresult = streamReader.ReadToEnd();
				//		//result = JsonConvert.DeserializeObject<CompleteTestResult>(outputresult);

				//	}
				//}
			}


		}

		public void LogError(Guid damDocumentId, string startReason, DateTime startTimeUtc, string iconum, bool IsSuccess, string Message) {
			string query =
	@"
DECLARE @log_id int
DECLARE @loginname varchar(500)
DECLARE @hostname varchar(500)
select 
       @loginname = convert(sysname, rtrim(sp.loginame))  
        , @hostname =CASE sp.hostname
                 When Null  Then '  .'
                 When ' ' Then '  .'
                 Else    rtrim(sp.hostname)
              END  
 
        from master.dbo.sysprocesses sp (nolock) 
		where spid = @@SPID

 
INSERT [dbo].[LogAutoStitchingAgent] (
       [Hostname]
      ,[StartTimeUTC]
      ,[EndTimeUTC]
      ,[DocumentId]
      ,[Iconum]
      ,[StartReason]
      ,[IsSuccess]
      ,[Comment]) values
	  ( @hostname
	  , @startTime
	  , getutcdate() 
		, @DocumentId
		, @iconum
		, @startReason
		, @IsSuccess
	  , @Message)
 set @log_id = scope_identity();
";
			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {
				conn.Open();
				using (SqlCommand cmd = new SqlCommand(query, conn)) {
					cmd.Parameters.AddWithValue("@DocumentID", damDocumentId);
					cmd.Parameters.AddWithValue("@iconum", iconum);
					cmd.Parameters.AddWithValue("@startTime", startTimeUtc);
					cmd.Parameters.AddWithValue("@startReason", startReason);
					cmd.Parameters.AddWithValue("@IsSuccess", IsSuccess);
					cmd.Parameters.AddWithValue("@Message", Message);

					using (SqlDataReader reader = cmd.ExecuteReader()) {
						//return !reader.HasRows;
					}
				}
			}
		}

		public class CompleteTestResult {
			public IndividualTestResult[] Results { get; set; }
		}
		public class IndividualTestResult {
			public Guid documentid { get; set; }
			public string Criticality { get; set; }

			public string test { get; set; }

			public bool isissue { get; set; }
			public List<ErrorDetail> testoutput { get; set; }
			public string testlevel { get; set; }
		}

		public class ErrorDetail {
			public string errortext { get; set; }
			public int tablecellid { get; set; }
			public int documenttimesliceid { get; set; }
			public DateTime timestamp { get; set; }
		}

		public Dictionary<string, string> ARDValidation(Guid DocumentID) {

			string url = ConfigurationManager.AppSettings["ARDValidationURL"];
			//string url =  @"https://data-wellness-orchestrator-staging.factset.io/Check/SCAR_ZeroMinute/92C6C824-0F9A-4A5C-BC62-000095729E1B";
			url = url + DocumentID.ToString(); ;
			HttpWebRequest request = (HttpWebRequest)WebRequest.Create(url);
			request.ContentType = "application/json";
			request.Timeout = 120000;
			request.Method = "GET";
			var response = (HttpWebResponse)request.GetResponse();

			CompleteTestResult result = null;
			if (response.StatusCode == HttpStatusCode.OK) {
				using (var streamReader = new StreamReader(response.GetResponseStream())) {
					var outputresult = streamReader.ReadToEnd();
					result = Newtonsoft.Json.JsonConvert.DeserializeObject<CompleteTestResult>(outputresult);

				}
			}
			bool hasNoError = true;
			Dictionary<string, string> returnValue = new Dictionary<string, string>();
			returnValue["Success"] = "T";
			returnValue["Message"] = "";
			System.Text.StringBuilder errorBuilder = new System.Text.StringBuilder("ArdValidation: ");
			if (result != null) {
				foreach (var test in result.Results) {
					if (test.isissue) {
						hasNoError = false;
						foreach (var error in test.testoutput) {
							if (!string.IsNullOrWhiteSpace(error.errortext)) {
								errorBuilder.Append(string.Format("{0}:{1};", test.test, error.errortext));
							}
						}
					}
				}
			} else {
				returnValue["Success"] = "F";
				returnValue["Message"] = "ArdValidation did not run successfully";
				return returnValue;
			}
			if (hasNoError) {
				errorBuilder = new System.Text.StringBuilder();
			}
			returnValue["Success"] = hasNoError ? "T" : "F";
			returnValue["Message"] = errorBuilder.ToString();
			return returnValue;
		}
	
	}
	public static class JValueExtension {
		public static string AsString(this JToken jValue) {
			string jString = jValue.ToString();
			string result = "";
			if (string.Equals(jString, "null", StringComparison.InvariantCultureIgnoreCase)) {
				result = "NULL";
			} else {
				result = "'" + jString.Replace("'", "''") +"'";
			}
			return result;
		}
		public static string AsValue(this JToken jValue) {
			string jString = jValue.ToString();
			string result = "";
			if (string.Equals(jString, "null", StringComparison.InvariantCultureIgnoreCase)) {
				result = "NULL";
			} else {
				result = jString;
			}
			return result;
		}
		public static string AsBoolean(this JToken jValue) {
			string jString = jValue.ToString();
			string result = "0";
			if (string.Equals(jString, "null", StringComparison.InvariantCultureIgnoreCase)) {
				result = "NULL";
			} else if (string.Equals(jString, "true", StringComparison.InvariantCultureIgnoreCase)) {
				result = "1";
			}
			return result;
		}
	}
}