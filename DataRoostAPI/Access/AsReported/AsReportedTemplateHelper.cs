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

		#endregion

		public AsReportedTemplate GetTemplate(int iconum, string TemplateName, Guid DocumentId) {
			var sw = System.Diagnostics.Stopwatch.StartNew();
			string query =
                @"
SELECT DISTINCT sh.*, shm.Code
FROM DocumentSeries ds
	JOIN CompanyFinancialTerm cft ON cft.DocumentSeriesId = ds.Id
	JOIN StaticHierarchy sh on cft.ID = sh.CompanyFinancialTermID
	JOIN TableType tt on sh.TableTypeID = tt.ID
    JOIN HierarchyMetaTypes shm on sh.StaticHierarchyMetaId = shm.id
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

			string pdata = "";

			CCS.Fundamentals.DataRoostAPI.Controllers.PerformanceInfo1 pInfo1 = new Controllers.PerformanceInfo1();
			CCS.Fundamentals.DataRoostAPI.Controllers.PerformanceInfo1.Load();
			pdata += "Enter method: " + sw.Elapsed.TotalSeconds.ToString() + pInfo1.GetPerformanceData();
			Dictionary<Tuple<StaticHierarchy, TimeSlice>, SCARAPITableCell> CellMap = new Dictionary<Tuple<StaticHierarchy, TimeSlice>, SCARAPITableCell>();
			Dictionary<Tuple<DateTime, string>, List<int>> TimeSliceMap = new Dictionary<Tuple<DateTime, string>, List<int>>();//int is index into timeslices for fast lookup


			AsReportedTemplate temp = new AsReportedTemplate();

			temp.StaticHierarchies = new List<StaticHierarchy>();
            Dictionary<SCARAPITableCell, Tuple<StaticHierarchy, int>> BlankCells = new Dictionary<SCARAPITableCell, Tuple<StaticHierarchy, int>>();
            Dictionary<SCARAPITableCell, Tuple<StaticHierarchy, int>> CellLookup = new Dictionary<SCARAPITableCell, Tuple<StaticHierarchy, int>>();
			Dictionary<int, StaticHierarchy> SHLookup = new Dictionary<int, StaticHierarchy>();
			Dictionary<int, List<StaticHierarchy>> SHChildLookup = new Dictionary<int, List<StaticHierarchy>>();
			List<StaticHierarchy> StaticHierarchies = temp.StaticHierarchies;

			pdata += "Before connection: " + sw.Elapsed.TotalSeconds.ToString() + pInfo1.GetPerformanceData();
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
                                StaticHierarchyMetaType = reader.GetStringSafe(12),
                                Cells = new List<SCARAPITableCell>()
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
				pdata += "after connection 1: " + sw.Elapsed.TotalSeconds.ToString() + pInfo1.GetPerformanceData();
				using (SqlCommand cmd = new SqlCommand(CellsQuery, conn)) {
					cmd.Parameters.AddWithValue("@iconum", iconum);
					cmd.Parameters.AddWithValue("@templateName", TemplateName);
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

								adjustedOrder = reader.GetInt32(28);
							} else {
                                cell = new SCARAPITableCell();
								adjustedOrder = reader.GetInt32(28);
							}

							while (adjustedOrder != StaticHierarchies[shix].AdjustedOrder) {
								shix++;
							}

							if (cell.ID == 0) {
								BlankCells.Add(cell, new Tuple<StaticHierarchy, int>(StaticHierarchies[shix], StaticHierarchies[shix].Cells.Count));
							}
							i++;
							if (i % 20 == 9) {
								pdata += "after connection 1.2: " + sw.Elapsed.TotalSeconds.ToString() + pInfo1.GetPerformanceData();
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
				pdata += "after connection 2: " + sw.Elapsed.TotalSeconds.ToString() + pInfo1.GetPerformanceData();

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
							pdata += "after connection 3.1 timeslice: " + sw.Elapsed.TotalSeconds.ToString() + pInfo1.GetPerformanceData();
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
				pdata += "after connection 4: " + sw.Elapsed.TotalSeconds.ToString() + pInfo1.GetPerformanceData();
				CCS.Fundamentals.DataRoostAPI.Controllers.PerformanceInfo1.SendEmail("DataRoost Performance GetTemplate", pdata);
			}


			foreach (StaticHierarchy sh in StaticHierarchies) {//Finds likeperiod validation failures. Currently failing with virtual cells

				if (!sh.ParentID.HasValue) {
					sh.Level = 0;
				}
				foreach (StaticHierarchy ch in SHChildLookup[sh.Id]) {
					ch.Level = sh.Level + 1;
				}

				for (int i = 0; i < sh.Cells.Count; i++) {
                    try
                    {
                        TimeSlice ts = temp.TimeSlices[i];

                        SCARAPITableCell tc = sh.Cells[i];
                        List<int> matches = TimeSliceMap[new Tuple<DateTime, string>(ts.TimeSlicePeriodEndDate, ts.PeriodType)];
                        foreach (int j in matches)
                        {
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
                                )
                            {
                                tc.LikePeriodValidationFlag = true;
                            }
                        }

                        bool hasChildren = false;
                        bool whatever2 = false;

                        tc.MTMWValidationFlag = SHChildLookup[sh.Id].Count > 0 && 
                            (CalculateCellValue(tc, BlankCells, SHChildLookup, ref whatever2) != CalculateChildSum(tc, CellLookup, SHChildLookup, ref hasChildren)) && 
                                !tc.MTMWErrorTypeId.HasValue && hasChildren;
                    }
                    catch { break; }
				}
			}

			return temp;
		}

        private decimal CalculateCellValue(SCARAPITableCell cell, Dictionary<SCARAPITableCell, Tuple<StaticHierarchy, int>> BlankCells, Dictionary<int, List<StaticHierarchy>> SHChildLookup, ref bool hasChildren)
        {
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

        private decimal CalculateChildSum(SCARAPITableCell cell, Dictionary<SCARAPITableCell, Tuple<StaticHierarchy, int>> CellLookup, Dictionary<int, List<StaticHierarchy>> SHChildLookup, ref bool hasChildren)
        {
			if (CellLookup.ContainsKey(cell)) {
				decimal sum = 0;
				StaticHierarchy sh = CellLookup[cell].Item1;
				int timesliceIndex = CellLookup[cell].Item2;

                if (sh.StaticHierarchyMetaId != 2 && sh.StaticHierarchyMetaId != 5 && sh.StaticHierarchyMetaId != 6)
                {

                    foreach (StaticHierarchy child in SHChildLookup[sh.Id].Where(s => s.StaticHierarchyMetaId != 2 && s.StaticHierarchyMetaId != 5 && s.StaticHierarchyMetaId != 6))
                    {
                        sum += CalculateCellValue(child.Cells[timesliceIndex], CellLookup, SHChildLookup, ref hasChildren);
                    }
                    if (SHChildLookup[sh.Id].Count > 0)
                    {
                        if (!cell.ValueNumeric.HasValue)
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


		public ScarResult UpdateStaticHierarchyAddHeader(int id) {

			string query = @"
BEGIN TRAN

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
ROLLBACK TRAN
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
BEGIN TRAN

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


ROLLBACK TRAN
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

		public TimeSlice CloneUpdateTimeSlice(int id, string InterimType) {

			string query = @"
DECLARE @newId int;

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
      ,@PeriodType
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
  FROM [DocumentTimeSlice] where id = @newId;
";


			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {

				using (SqlCommand cmd = new SqlCommand(query, conn)) {
					conn.Open();
					cmd.Parameters.AddWithValue("@id", id);
					cmd.Parameters.AddWithValue("@PeriodType", InterimType);
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
	and ds.id = @DocumentSeriesId
	group by d.damdocumentid, dts.id 
)
SELECT ts.*, dts.*, d.DocumentDate, d.ReportTypeID, d.PublicationDateTime
	FROM cte_timeslice ts WITH (NOLOCK)
	JOIN DocumentTimeSlice dts WITH(NOLOCK) on ts.TimeSliceId = dts.Id
  JOIN Document d WITH(NOLOCK) on dts.DocumentId = d.id
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
							ReportType = reader.GetOrdinal("ReportTypeId"),
							ReportStatus = reader.GetOrdinal("ReportType"),
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
							TimeSlice slice = new TimeSlice
							{
								DocumentId = reader.GetGuid(ordinals.DocumentId),
								DamDocumentId = reader.GetGuid(ordinals.DamDocumentId),
								Id = reader.GetInt32(ordinals.TimeSliceId),
								DocumentSeriesId = reader.GetInt32(ordinals.DocumentSeriesId),
								PublicationDate = reader.GetDateTime(ordinals.PublicationDate),
								TimeSlicePeriodEndDate = reader.GetDateTime(ordinals.PeriodEndDate),
								ReportingPeriodEndDate = reader.GetDateTime(ordinals.DocumentDate),
								FiscalDistance = reader.GetInt32(ordinals.FiscalDistance),
								CompanyFiscalYear = reader.GetDecimal(ordinals.CompanyFiscalYear),
								Duration = reader.GetInt32(ordinals.PeriodLength),
								InterimType = reader.GetStringSafe(ordinals.PeriodType),
								ReportType = reader.GetStringSafe(ordinals.ReportType),
								IsAutoCalc = reader.GetBoolean(ordinals.AutocalcStatus),
								NumberOfCells = reader.GetInt32(ordinals.NumberOfCells),
								Currency = reader.GetInt32(ordinals.CurrencyCount) == 1 ? reader.GetStringSafe(ordinals.CurrencyCode) : null,
								AccountingStandard = reader.GetInt32(ordinals.ArComponent).ToString(),
							};
							response.TimeSlices.Add(slice);
						}
					}
				}
				return response;
			}
		}


        public ScarResult FlipSign(string CellId, Guid DocumentId, int iconum, int TargetStaticHierarchyID)
        {
            const string SQL_UpdateFlipIncomeFlag = @"

UPDATE TableCell  set IsIncomePositive = CASE WHEN IsIncomePositive = 1 THEN 0 ELSE 1 END
																WHERE ID = @cellid; 
";

            const string SQL_ValidateCells= @"
DECLARE @TargetSH int;

SELECT top 1 @TargetSH = sh.id
  FROM  StaticHierarchy sh WITH (NOLOCK)
  JOIN [TableCell] tc WITH (NOLOCK) on sh.CompanyFinancialTermId = tc.CompanyFinancialTermId
  where tc.id = @cellid

DECLARE @SHCells CellList

INSERT @SHCells
SELECT sh.ID, tc.DocumentTimeSliceID
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
JOIN vw_SCARDocumentTimeSlices dts ON dts.CompanyID = @Iconum
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
            using (SqlConnection conn = new SqlConnection(_sfConnectionString))
            {
                using (SqlCommand cmd = new SqlCommand(SQL_FlipSignCommand, conn))
                {
                    conn.Open();
                    cmd.Parameters.AddWithValue("@DocumentID ", DocumentId);
                    cmd.Parameters.AddWithValue("@cellid", CellId);
                    cmd.Parameters.AddWithValue("@Iconum", iconum);

                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
												reader.Read();
                        int level = reader.GetInt32(0);
                        reader.NextResult();
                        int shix = 0;
                        int adjustedOrder = 0;
                        while (reader.Read())
                        {
                            SCARAPITableCell cell;
                            if (reader.GetNullable<int>(1).HasValue)
                            {
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

                            }
                            else
                            {
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
SELECT tc.TableCellID, tc.DocumentTimeSliceID
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
SELECT sh.ID, @CurrentTimeSliceID
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
JOIN vw_SCARDocumentTimeSlices dts ON dts.CompanyID = @Iconum
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
BEGIN TRAN

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
SELECT tc.TableCellID, tc.DocumentTimeSliceID
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
SELECT sh.ID, tc.DocumentTimeSliceId
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
 
ROLLBACK TRAN
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

            foreach (SCARAPITableCell cell in res.StaticHierarchy.Cells)
            {
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
				Tuple<int,int> tup = new Tuple<int,int>(comp.RootStaticHierarchyID, comp.RootDocumentTimeSliceID);
				if(!CellValueMap.ContainsKey(tup))
					CellValueMap.Add(tup, 0);

				CellValueMap[tup] += comp.ValueNumeric * ((decimal)comp.ScalingFactorValue) * (comp.IsIncomePositive ? 1 : -1);
			}

			foreach(Tuple<int, int> key in CellMap.Keys){
                SCARAPITableCell cell = CellMap[key];
				if(CellValueMap.ContainsKey(key))
				cell.MTMWValidationFlag = (cell.ValueNumeric * (decimal)cell.ScalingFactorValue * (cell.IsIncomePositive ? 1 : -1)) != CellValueMap[key];
			}

			foreach (StaticHierarchy sh in res.StaticHierarchies) {
				sh.Level = SHLevels[sh.Id];
			}

			return res;
		}

        #region Deprecated Methods
        public SCARAPITableCell GetCell(string CellId)
        {
            using (SqlConnection conn = new SqlConnection(_sfConnectionString))
            {
                using (SqlCommand cmd = new SqlCommand(SQL_GetCellQuery, conn))
                {
                    conn.Open();
                    cmd.Parameters.AddWithValue("@cellId", CellId);

                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {

                        int shix = 0;

                        int adjustedOrder = 0;
                        while (reader.Read())
                        {
                            SCARAPITableCell cell;
                            if (reader.GetNullable<int>(0).HasValue)
                            {
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
                            }
                            else
                            {
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

        public TableCellResult AddMakeTheMathWorkNote(string CellId, Guid DocumentId)
        {
            TableCellResult result = new TableCellResult();
            result.cells = new List<SCARAPITableCell>();

            SCARAPITableCell currCell = GetCell(CellId);
            currCell.MTMWErrorTypeId = 0;
            result.cells.Add(currCell);
            SCARAPITableCell[] sibilings = getSibilingsCells(CellId, DocumentId);
            result.cells.AddRange(sibilings);
            return result;
        }

        public TableCellResult AddLikePeriodValidationNote(string CellId, Guid DocumentId)
        {
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

        private SCARAPITableCell[] getSibilingsCells(string CellId, Guid DocumentId)
        {
            return null;
        }
        #endregion
	}
}