using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Net;
using System.Web;
using System.Net.Mail;
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
FROM DocumentSeries ds WITH (NOLOCK) 
JOIN CompanyFinancialTerm cft  WITH (NOLOCK) ON cft.DocumentSeriesId = ds.Id
JOIN StaticHierarchy sh WITH (NOLOCK)  on cft.ID = sh.CompanyFinancialTermID
JOIN TableType tt WITH (NOLOCK)  on sh.TableTypeID = tt.ID
JOIN(
	SELECT distinct dts.ID
	FROM DocumentSeries ds WITH (NOLOCK) 
	JOIN dbo.DocumentTimeSlice dts WITH (NOLOCK)  on ds.ID = Dts.DocumentSeriesId
	JOIN Document d WITH (NOLOCK)  on dts.DocumentId = d.ID
	JOIN DocumentTimeSliceTableCell dtstc WITH (NOLOCK)  on dts.ID = dtstc.DocumentTimeSliceID
	JOIN TableCell tc WITH (NOLOCK)  on dtstc.TableCellID = tc.ID
	JOIN DimensionToCell dtc WITH (NOLOCK)  on tc.ID = dtc.TableCellID -- check that is in a table
	JOIN StaticHierarchy sh WITH (NOLOCK)  on tc.CompanyFinancialTermID = sh.CompanyFinancialTermID
	JOIN TableType tt WITH (NOLOCK)  on tt.ID = sh.TableTypeID
	WHERE tc.ID = @cellId
	AND (d.ArdExportFlag = 1 OR d.ExportFlag = 1 OR d.IsDocSetupCompleted = 1)
) as ts on 1=1
JOIN dbo.DocumentTimeSlice dts WITH (NOLOCK)  on dts.ID = ts.ID and dts.DocumentSeriesId = ds.ID 
JOIN(
	SELECT tc.*, dtstc.DocumentTimeSliceID, sf.Value as ScalingFactorValue
	FROM DocumentSeries ds WITH (NOLOCK) 
	JOIN CompanyFinancialTerm cft WITH (NOLOCK)  ON cft.DocumentSeriesId = ds.Id
	JOIN StaticHierarchy sh WITH (NOLOCK)  on cft.ID = sh.CompanyFinancialTermID
	JOIN TableType tt WITH (NOLOCK)  on sh.TableTypeID = tt.ID
	JOIN TableCell tc WITH (NOLOCK)  on tc.CompanyFinancialTermID = cft.ID
	JOIN DocumentTimeSliceTableCell dtstc WITH (NOLOCK)  on dtstc.TableCellID = tc.ID
	JOIN ScalingFactor sf WITH (NOLOCK)  on sf.ID = tc.ScalingFactorID
	WHERE tc.ID = @cellId
) as tc ON tc.DocumentTimeSliceID = ts.ID AND tc.CompanyFinancialTermID = cft.ID
JOIN Document d WITH (NOLOCK)  on dts.documentid = d.ID
WHERE 1=1
ORDER BY sh.AdjustedOrder asc, dts.TimeSlicePeriodEndDate desc, dts.Duration desc, dts.ReportingPeriodEndDate desc, d.PublicationDateTime desc

";//I hate this query, it is so bad

		#endregion

		public ScarResult CreateStaticHierarchyForTemplate(int iconum, string TemplateName, Guid DocumentId) {

			ScarResult temp = new ScarResult();
			string query_sproc = @"prcUpd_FFDocHist_UpdateStaticHierarchy_CreateStaticHierarchyIfNecessary";
			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {
				#region Using SqlConnection
				using (SqlCommand cmd = new SqlCommand(query_sproc, conn)) {
					conn.Open();
					temp.Message += "ConnOpen.";
					cmd.CommandType = System.Data.CommandType.StoredProcedure;
					cmd.CommandTimeout = 120;
					//cmd.Parameters.AddWithValue("@iconum", iconum);
					//cmd.Parameters.AddWithValue("@templateName", TemplateName);
					cmd.Parameters.AddWithValue("@DocumentID", DocumentId);
					using (SqlDataReader reader = cmd.ExecuteReader()) {
						temp.Message += "StaticHierarchy.";
						while (reader.Read()) {
							break;
						}

					}
				}
				#endregion
			}
			return temp;
		}


		public ScarResult GetTemplateInScarResult(int iconum, string TemplateName, Guid DocumentId) {
			ScarResult newFormat = new ScarResult();
			AsReportedTemplate oldFormat = GetTemplate(iconum, TemplateName, DocumentId);
			newFormat.StaticHierarchies = oldFormat.StaticHierarchies;
			newFormat.TimeSlices = oldFormat.TimeSlices;
			return newFormat;
		}
		public ScarResult GetTemplateInScarResultJune(int iconum, string TemplateName, Guid DocumentId) {
			ScarResult newFormat = new ScarResult();
			AsReportedTemplate oldFormat = GetTemplateWithSqlDataReader(iconum, TemplateName, DocumentId);
			newFormat.StaticHierarchies = oldFormat.StaticHierarchies;
			newFormat.TimeSlices = oldFormat.TimeSlices;
			return newFormat;
		}
		public AsReportedTemplate GetTemplate(int iconum, string TemplateName, Guid DocumentId) {
			var sw = System.Diagnostics.Stopwatch.StartNew();

			Dictionary<Tuple<StaticHierarchy, TimeSlice>, SCARAPITableCell> CellMap = new Dictionary<Tuple<StaticHierarchy, TimeSlice>, SCARAPITableCell>();
			Dictionary<Tuple<DateTime, string>, List<int>> TimeSliceMap = new Dictionary<Tuple<DateTime, string>, List<int>>();//int is index into timeslices for fast lookup

			AsReportedTemplate temp = new AsReportedTemplate();
			try {
				temp.Message = "Start." + DateTime.UtcNow.ToString();
				string query_sproc = @"SCARGetTemplate";
				temp.StaticHierarchies = new List<StaticHierarchy>();
				Dictionary<SCARAPITableCell, Tuple<StaticHierarchy, int>> BlankCells = new Dictionary<SCARAPITableCell, Tuple<StaticHierarchy, int>>();
				Dictionary<SCARAPITableCell, Tuple<StaticHierarchy, int>> CellLookup = new Dictionary<SCARAPITableCell, Tuple<StaticHierarchy, int>>();
				Dictionary<int, StaticHierarchy> SHLookup = new Dictionary<int, StaticHierarchy>();
				Dictionary<int, List<StaticHierarchy>> SHChildLookup = new Dictionary<int, List<StaticHierarchy>>();
				List<StaticHierarchy> StaticHierarchies = temp.StaticHierarchies;
				Dictionary<int, List<string>> IsSummaryLookup = new Dictionary<int, List<string>>();
				DataSet dataSet = new DataSet();
				using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {
					#region Using SqlConnection
					using (SqlCommand cmd = new SqlCommand(query_sproc, conn)) {
						cmd.CommandType = System.Data.CommandType.StoredProcedure;
						cmd.CommandTimeout = 120;
						cmd.Parameters.AddWithValue("@iconum", iconum);
						cmd.Parameters.AddWithValue("@templateName", TemplateName);
						cmd.Parameters.AddWithValue("@DocumentID", DocumentId);
						conn.Open();
						temp.Message += "ConnOpen." + DateTime.UtcNow.ToString();
						//using (DataTable dt = new DataTable()) {
						//	dt.Load(reader);
						//	Console.WriteLine(dt.Rows.Count);
						//}
						SqlDataAdapter da = new SqlDataAdapter(cmd);
						da.Fill(dataSet);
						conn.Close();
					}
					#endregion
				}
				#region In-Memory Processing
				temp.Message += "Filled." + DateTime.UtcNow.ToString();
				var shTable = dataSet.Tables[0];
				if (shTable != null) {
					temp.Message += "StaticHierarchy." + DateTime.UtcNow.ToString();
					foreach (DataRow row in shTable.Rows) {
						StaticHierarchy shs = new StaticHierarchy
						{
							Id = row[0].AsInt32(),
							CompanyFinancialTermId = row[1].AsInt32(),
							AdjustedOrder = row[2].AsInt32(),
							TableTypeId = row[3].AsInt32(),
							Description = row[4].AsString()
						};
						shs.HierarchyTypeId = row[5].AsString()[0];
						shs.SeparatorFlag = row[6].AsBoolean();
						shs.StaticHierarchyMetaId = row[7].AsInt32();
						shs.UnitTypeId = row[8].AsInt32();
						shs.IsIncomePositive = row[9].AsBoolean();
						shs.ChildrenExpandDown = row[10].AsBoolean();
						shs.ParentID = row[11].AsInt32Nullable();
						shs.StaticHierarchyMetaType = row[12].AsString();
						shs.TableTypeDescription = row[13].ToString();
						shs.Cells = new List<SCARAPITableCell>();
						StaticHierarchies.Add(shs);
						SHLookup.Add(shs.Id, shs);
						if (!SHChildLookup.ContainsKey(shs.Id))
							SHChildLookup.Add(shs.Id, new List<StaticHierarchy>());

						if (shs.ParentID != null) {
							if (!SHChildLookup.ContainsKey(shs.ParentID.Value))
								SHChildLookup.Add(shs.ParentID.Value, new List<StaticHierarchy>());

							SHChildLookup[shs.ParentID.Value].Add(shs);
						}
					}
				}
				var cellTable = dataSet.Tables[1];
				temp.Message += "Cells." + DateTime.UtcNow.ToString();
				temp.Message += "Cells Next Result." + DateTime.UtcNow.ToString();
				int shix = 0;
				int adjustedOrder = 0;

				if (cellTable != null) {
					#region read CellsQuery
					temp.Message += "Cell2." + DateTime.UtcNow.ToString();
					foreach (DataRow row in cellTable.Rows) {
						if (shix >= StaticHierarchies.Count())
							break;
						if (row[29].AsInt64() == 1) {
							SCARAPITableCell cell;
							if (row[0].AsInt32Nullable().HasValue) {
								cell = new SCARAPITableCell
								{
									ID = row[0].AsInt32(),
									Offset = row[1].AsString(),
									CellPeriodType = row[2].AsString(),
									PeriodTypeID = row[3].AsString(),
									CellPeriodCount = row[4].AsString(),
									PeriodLength = row[5].AsInt32Nullable(),
									CellDay = row[6].AsString(),
									CellMonth = row[7].AsString(),
									CellYear = row[8].AsString(),
									CellDate = row[9].AsDateTimeNullable(),
									Value = row[10].AsString(),
									CompanyFinancialTermID = row[11].AsInt32Nullable(),
									ValueNumeric = row[12].AsDecimalNullable(),
									NormalizedNegativeIndicator = row[13].AsBoolean(),
									ScalingFactorID = row[14].AsString(),
									AsReportedScalingFactor = row[15].AsString(),
									Currency = row[16].AsString(),
									CurrencyCode = row[17].AsString(),
									Cusip = row[18].AsString(),
									ScarUpdated = row[19].AsBoolean(),
									IsIncomePositive = row[20].AsBoolean(),
									XBRLTag = row[21].AsString(),
									UpdateStampUTC = row[22].AsDateTimeNullable(),
									DocumentID = row[23].AsGuid(),
									Label = row[24].AsString(),
									ScalingFactorValue = row[25].AsDouble(),
									ARDErrorTypeId = row[26].AsInt32Nullable(),
									MTMWErrorTypeId = row[27].AsInt32Nullable()
								};
								adjustedOrder = row[28].AsInt32();
							} else {
								cell = new SCARAPITableCell();
								adjustedOrder = row[28].AsInt32();
								cell.CompanyFinancialTermID = row[34].AsInt32Nullable();
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
									if (shix >= StaticHierarchies.Count())
										break;
								}
								var currSh = StaticHierarchies.FirstOrDefault(x => x.AdjustedOrder == adjustedOrder && x.CompanyFinancialTermId == cell.CompanyFinancialTermID);
								if (currSh == null) {
									continue;
								}
								//while (adjustedOrder == StaticHierarchies[shix].AdjustedOrder && cell.CompanyFinancialTermID != StaticHierarchies[shix].CompanyFinancialTermId) {
								//	shix++;
								//	if (shix >= StaticHierarchies.Count())
								//		break;
								//}
								if (shix >= StaticHierarchies.Count())
									break;
								if (cell.ID == 0) {
									BlankCells.Add(cell, new Tuple<StaticHierarchy, int>(currSh, currSh.Cells.Count));
								}
								CellLookup.Add(cell, new Tuple<StaticHierarchy, int>(currSh, currSh.Cells.Count));

								if (cell.ID == 0 || cell.CompanyFinancialTermID == currSh.CompanyFinancialTermId) {
									currSh.Cells.Add(cell);
								} else {
									throw new Exception();
								}
							}
						}
					}
					#endregion
				}
				var timesliceTable = dataSet.Tables[2];
				temp.Message += "TimeSlice." + DateTime.UtcNow.ToString();
				temp.TimeSlices = new List<TimeSlice>();
				List<TimeSlice> TimeSlices = temp.TimeSlices;
				if (timesliceTable != null) {
					temp.Message += "TimeSlice.2" + DateTime.UtcNow.ToString();
					#region Read TimeSlice
					foreach (DataRow row in timesliceTable.Rows) {
						TimeSlice slice = new TimeSlice
						{
							Id = row[0].AsInt32(),
							DocumentId = row[1].AsGuid(),
							DocumentSeriesId = row[2].AsInt32(),
							TimeSlicePeriodEndDate = row[3].AsDateTime(),
							ReportingPeriodEndDate = row[4].AsDateTime(),
							FiscalDistance = row[5].AsInt32(),
							Duration = row[6].AsInt32(),
							PeriodType = row[7].AsString(),
							AcquisitionFlag = row[8].AsString(),
							AccountingStandard = row[9].AsString(),
							ConsolidatedFlag = row[10].AsString(),
							IsProForma = row[11].AsBoolean(),
							IsRecap = row[12].AsBoolean(),
							CompanyFiscalYear = row[13].AsDecimal(),
							ReportType = row[14].AsString(),
							IsAmended = row[15].AsBoolean(),
							IsRestated = row[16].AsBoolean(),
							IsAutoCalc = row[17].AsBoolean(),
							ManualOrgSet = row[18].AsBoolean(),
							TableTypeID = row[19].AsInt32(),
							PublicationDate = row[20].AsDateTime(),
							DamDocumentId = row[21].AsGuid(),
							PeriodNoteID = row[22].AsByteNullable()
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
				}
				var issummaryTable = dataSet.Tables[3];
				temp.Message += "IsSummary." + DateTime.UtcNow.ToString();
				if (issummaryTable != null) {
					temp.Message += "IsSummary2." + DateTime.UtcNow.ToString();
					foreach (DataRow row in issummaryTable.Rows) {
						int TimeSliceID = row[0].AsInt32();
						if (TimeSlices.FirstOrDefault(t => t.Id == TimeSliceID) != null) {
							TimeSlices.FirstOrDefault(t => t.Id == TimeSliceID).IsSummary = true;
						}
						if (!IsSummaryLookup.ContainsKey(TimeSliceID)) {
							IsSummaryLookup.Add(TimeSliceID, new List<string>());
						}

						IsSummaryLookup[TimeSliceID].Add(row[1].AsString());
					}
				}

				temp.Message += "Calculate.";
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

							bool hasValidChild = false;
							decimal calcChildSum = CalculateChildSum(tc, CellLookup, SHChildLookup, IsSummaryLookup, ref hasValidChild, temp.TimeSlices);
							if (hasValidChild && tc.ID == 0 && !tc.ValueNumeric.HasValue && !tc.VirtualValueNumeric.HasValue && !IsSummaryLookup.ContainsKey(ts.Id)) {
								tc.VirtualValueNumeric = calcChildSum;
							}


							//bool whatever = false;
							//decimal cellValue = CalculateCellValue(tc, BlankCells, SHChildLookup, IsSummaryLookup, ref whatever, temp.TimeSlices);

							//List<int> sortedLessThanPubDate = matches.Where(m2 => temp.TimeSlices[m2].PublicationDate < temp.TimeSlices[i].PublicationDate).OrderByDescending(c => temp.TimeSlices[c].PublicationDate).ToList();

							//if (LPV(BlankCells, CellLookup, SHChildLookup, IsSummaryLookup, sh, tc, matches, ref whatever, cellValue, sortedLessThanPubDate, temp.TimeSlices)
							//) {
							//	tc.LikePeriodValidationFlag = true;
							//	tc.StaticHierarchyID = sh.Id;
							//	tc.DocumentTimeSliceID = ts.Id;
							//}

							//bool ChildrenSumEqual = false;
							//if (!tc.ValueNumeric.HasValue || !hasValidChild)
							//	ChildrenSumEqual = true;
							//else {
							//	decimal diff = cellValue - calcChildSum;
							//	diff = Math.Abs(diff);

							//	if (tc.ScalingFactorValue == 1.0)
							//		ChildrenSumEqual = tc.ValueNumeric.HasValue && ((diff == 0) || (diff < 0.01m));
							//	else
							//		ChildrenSumEqual = tc.ValueNumeric.HasValue && ((diff == 0) || (diff < 0.1m && Math.Abs(cellValue) > 100));
							//}

							//tc.MTMWValidationFlag = tc.ValueNumeric.HasValue && SHChildLookup[sh.Id].Count > 0 &&
							//		!ChildrenSumEqual &&
							//				!tc.MTMWErrorTypeId.HasValue && sh.UnitTypeId != 2;

						} catch (Exception ex) {
							Console.WriteLine(ex.Message);
							break;
						}
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

							bool hasValidChild = false;
							decimal calcChildSum = CalculateChildSum(tc, CellLookup, SHChildLookup, IsSummaryLookup, ref hasValidChild, temp.TimeSlices);
							if (hasValidChild && tc.ID == 0 && !tc.ValueNumeric.HasValue && !tc.VirtualValueNumeric.HasValue && !IsSummaryLookup.ContainsKey(ts.Id)) {
								tc.VirtualValueNumeric = calcChildSum;
							}

							bool whatever = false;
							decimal cellValue = CalculateCellValue(tc, BlankCells, SHChildLookup, IsSummaryLookup, ref whatever, temp.TimeSlices);

							List<int> sortedLessThanPubDate = matches.Where(m2 => temp.TimeSlices[m2].PublicationDate < temp.TimeSlices[i].PublicationDate).OrderByDescending(c => temp.TimeSlices[c].PublicationDate).ToList();

							if (LPV(BlankCells, CellLookup, SHChildLookup, IsSummaryLookup, sh, tc, matches, ref whatever, cellValue, sortedLessThanPubDate, temp.TimeSlices)
							) {
								tc.LikePeriodValidationFlag = true;
								tc.StaticHierarchyID = sh.Id;
								tc.DocumentTimeSliceID = ts.Id;
							}

							bool ChildrenSumEqual = false;
							if (!tc.ValueNumeric.HasValue || !hasValidChild)
								ChildrenSumEqual = true;
							else {
								decimal diff = cellValue - calcChildSum;
								diff = Math.Abs(diff);

								if (tc.ScalingFactorValue == 1.0)
									ChildrenSumEqual = tc.ValueNumeric.HasValue && ((diff == 0) || (diff < 0.01m));
								else
									ChildrenSumEqual = tc.ValueNumeric.HasValue && ((diff == 0) || (diff < 0.1m && Math.Abs(cellValue) > 100));
							}

							tc.MTMWValidationFlag = tc.ValueNumeric.HasValue && SHChildLookup[sh.Id].Count > 0 &&
									!ChildrenSumEqual &&
											!tc.MTMWErrorTypeId.HasValue && sh.UnitTypeId != 2;

						} catch (Exception ex) {
							Console.WriteLine(ex.Message);
							break;
						}
					}
				}
				temp.Message += "Finished.";
				#endregion
			} catch (Exception ex) {
				throw new Exception(temp.Message + "ExceptionTime:" + DateTime.UtcNow.ToString() + ex.Message, ex);
			}
			return temp;
		}

		public AsReportedTemplate GetTemplateWithSqlDataReader(int iconum, string TemplateName, Guid DocumentId) {
			var sw = System.Diagnostics.Stopwatch.StartNew();
			#region Old Queries
			string query =
								@"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED

BEGIN TRY
DROP TABLE #StaticHierarchy
END TRY
BEGIN CATCH
END CATCH 


SELECT DISTINCT sh.*, shm.Code, tt.Description as 'TableTypeDescription'
INTO #StaticHierarchy
FROM DocumentSeries ds WITH (NOLOCK) 
	JOIN CompanyFinancialTerm cft WITH (NOLOCK) ON cft.DocumentSeriesId = ds.Id
	JOIN StaticHierarchy sh WITH (NOLOCK) on cft.ID = sh.CompanyFinancialTermID
	JOIN TableType tt WITH (NOLOCK) on sh.TableTypeID = tt.ID
    JOIN HierarchyMetaTypes shm WITH (NOLOCK) on sh.StaticHierarchyMetaId = shm.id
WHERE ds.CompanyID = @iconum
AND tt.Description = @templateName
ORDER BY sh.AdjustedOrder asc

select * from #StaticHierarchy
";

			string CellsQuery =
				@"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED

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
	JOIN dbo.DocumentTimeSlice dts  WITH (NOLOCK) on ds.ID = Dts.DocumentSeriesId
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
--			JOIN dbo.DocumentTimeSlice dts on dtstc.DocumentTimeSliceID = dts.ID
--			JOIN Document d on dts.DocumentId = d.ID
--		WHERE ds.CompanyID = @iconum
--		AND tt.Description = @templateName
--		AND (d.ID = @DocumentID OR d.ArdExportFlag = 1 OR d.ExportFlag = 1 OR d.IsDocSetupCompleted = 1) 
--	)dts
join dbo.DocumentTimeSlice dts WITH (NOLOCK) 
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
				@"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED

SELECT DISTINCT dts.*, d.PublicationDateTime, d.damdocumentid, dtspn.PeriodNoteID, CHARINDEX(dts.PeriodType, '""XX"", ""AR"", ""IF"", ""T3"", ""Q4"", ""Q3"", ""T2"", ""I1"", ""Q2"", ""T1"", ""Q1"", ""Q9"", ""Q8"", ""Q6""') as CHARINDEX
FROM DocumentSeries ds WITH (NOLOCK) 
	JOIN CompanyFinancialTerm cft WITH (NOLOCK)  ON cft.DocumentSeriesId = ds.Id
	JOIN #StaticHierarchy sh  WITH (NOLOCK) on cft.ID = sh.CompanyFinancialTermID
	JOIN TableType tt  WITH (NOLOCK) on sh.TableTypeID = tt.ID
	JOIN TableCell tc  WITH (NOLOCK) on tc.CompanyFinancialTermID = cft.ID
	JOIN DimensionToCell dtc WITH (NOLOCK)  on tc.ID = dtc.TableCellID -- check that is in a table
	JOIN DocumentTimeSliceTableCell dtstc  WITH (NOLOCK) on tc.ID = dtstc.TableCellID
	JOIN dbo.DocumentTimeSlice dts  WITH (NOLOCK) on dtstc.DocumentTimeSliceID = dts.ID  and dts.DocumentSeriesId = ds.ID 
	JOIN Document d  WITH (NOLOCK) on dts.DocumentId = d.ID
  LEFT JOIN DocumentTimeSlicePeriodNotes dtspn WITH (nolock) on dts.ID = dtspn.DocumentTimeSliceID
WHERE ds.CompanyID = @iconum
AND tt.Description = @templateName
AND (d.ID = @DocumentID OR d.ArdExportFlag = 1 OR d.ExportFlag = 1 OR d.IsDocSetupCompleted = 1)
--ORDER BY dts.TimeSlicePeriodEndDate desc, dts.Duration desc, dts.ReportingPeriodEndDate desc, d.PublicationDateTime desc
ORDER BY dts.TimeSlicePeriodEndDate desc, CHARINDEX(dts.PeriodType, '""XX"", ""AR"", ""IF"", ""T3"", ""Q4"", ""Q3"", ""T2"", ""I1"", ""Q2"", ""T1"", ""Q1"", ""Q9"", ""Q8"", ""Q6""') asc, dts.Duration desc, d.PublicationDateTime desc, dts.ReportingPeriodEndDate desc
";

			string TimeSliceIsSummaryQuery = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED

select distinct DocumentTimeSliceID, TableType
from DocumentSeries ds WITH (NOLOCK) 
JOIN Document d WITH (NOLOCK) on ds.ID = d.DocumentSeriesID
JOIN dbo.DocumentTimeSlice dts WITH (NOLOCK) on dts.DocumentId = d.ID and dts.DocumentSeriesId = ds.ID 
join DocumentTimeSliceTableTypeIsSummary dtsis WITH (NOLOCK) on dts.id = dtsis.DocumentTimeSliceID
WHERE  CompanyID = @Iconum";
			#endregion

			Dictionary<Tuple<StaticHierarchy, TimeSlice>, SCARAPITableCell> CellMap = new Dictionary<Tuple<StaticHierarchy, TimeSlice>, SCARAPITableCell>();
			Dictionary<Tuple<DateTime, string>, List<int>> TimeSliceMap = new Dictionary<Tuple<DateTime, string>, List<int>>();//int is index into timeslices for fast lookup

			AsReportedTemplate temp = new AsReportedTemplate();
			try {
				temp.Message = "Start." + DateTime.UtcNow.ToString();
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
					#region Using SqlConnection
					using (SqlCommand cmd = new SqlCommand(query_sproc, conn)) {
						cmd.CommandType = System.Data.CommandType.StoredProcedure;
						cmd.CommandTimeout = 120;
						cmd.Parameters.AddWithValue("@iconum", iconum);
						cmd.Parameters.AddWithValue("@templateName", TemplateName);
						cmd.Parameters.AddWithValue("@DocumentID", DocumentId);
						conn.Open();
						temp.Message += "ConnOpen." + DateTime.UtcNow.ToString();
						//using (DataTable dt = new DataTable()) {
						//	dt.Load(reader);
						//	Console.WriteLine(dt.Rows.Count);
						//}
						using (SqlDataReader reader = cmd.ExecuteReader()) {
							temp.Message += "StaticHierarchy." + DateTime.UtcNow.ToString();
							while (reader.Read()) {
								temp.Message += "Read." + DateTime.UtcNow.ToString();
								StaticHierarchy shs = new StaticHierarchy
								{
									Id = reader.GetInt32(0),
									CompanyFinancialTermId = reader.GetInt32(1),
									AdjustedOrder = reader.GetInt32(2),
									TableTypeId = reader.GetInt32(3),
									Description = reader.GetStringSafe(4)
								};
								temp.Message += "HierarchyTypeId." + DateTime.UtcNow.ToString();
								shs.HierarchyTypeId = reader.GetStringSafe(5)[0];
								shs.SeparatorFlag = reader.GetBoolean(6);
								shs.StaticHierarchyMetaId = reader.GetInt32(7);
								shs.UnitTypeId = reader.GetInt32(8);
								temp.Message += "shsIsIncomePositive." + DateTime.UtcNow.ToString();
								shs.IsIncomePositive = reader.GetBoolean(9);
								shs.ChildrenExpandDown = reader.GetBoolean(10);
								shs.ParentID = reader.GetNullable<int>(11);
								shs.StaticHierarchyMetaType = reader.GetStringSafe(12);
								shs.TableTypeDescription = reader.GetStringSafe(13);
								temp.Message += "shsCell." + DateTime.UtcNow.ToString();
								shs.Cells = new List<SCARAPITableCell>();
								temp.Message += "Shid: " + shs.Id.ToString() + " utc" + DateTime.UtcNow.ToString();
								StaticHierarchies.Add(shs);
								SHLookup.Add(shs.Id, shs);
								temp.Message += "SHLookup." + DateTime.UtcNow.ToString();
								if (!SHChildLookup.ContainsKey(shs.Id))
									SHChildLookup.Add(shs.Id, new List<StaticHierarchy>());

								if (shs.ParentID != null) {
									temp.Message += "ParentID." + DateTime.UtcNow.ToString();
									if (!SHChildLookup.ContainsKey(shs.ParentID.Value))
										SHChildLookup.Add(shs.ParentID.Value, new List<StaticHierarchy>());

									SHChildLookup[shs.ParentID.Value].Add(shs);
									temp.Message += "ChildLookup." + DateTime.UtcNow.ToString();
								}
							}
							temp.Message += "Cells." + DateTime.UtcNow.ToString();
							reader.NextResult();
							temp.Message += "Cells Next Result." + DateTime.UtcNow.ToString();
							int shix = 0;
							int i = 0;
							int adjustedOrder = 0;
							#region read CellsQuery
							temp.Message += "Cell2." + DateTime.UtcNow.ToString();
							while (reader.Read()) {
								if (shix >= StaticHierarchies.Count())
									break;
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
											if (shix >= StaticHierarchies.Count())
												break;
										}
										var currSh = StaticHierarchies.FirstOrDefault(x => x.AdjustedOrder == adjustedOrder && x.CompanyFinancialTermId == cell.CompanyFinancialTermID);
										if (currSh == null) {
											continue;
										}
										//while (adjustedOrder == StaticHierarchies[shix].AdjustedOrder && cell.CompanyFinancialTermID != StaticHierarchies[shix].CompanyFinancialTermId) {
										//	shix++;
										//	if (shix >= StaticHierarchies.Count())
										//		break;
										//}
										if (shix >= StaticHierarchies.Count())
											break;
										if (cell.ID == 0) {
											BlankCells.Add(cell, new Tuple<StaticHierarchy, int>(currSh, currSh.Cells.Count));
										}
										i++;
										CellLookup.Add(cell, new Tuple<StaticHierarchy, int>(currSh, currSh.Cells.Count));

										if (cell.ID == 0 || cell.CompanyFinancialTermID == currSh.CompanyFinancialTermId) {
											currSh.Cells.Add(cell);
										} else {
											throw new Exception();
										}
									}
								}
							}
							#endregion
							temp.Message += "TimeSlice." + DateTime.UtcNow.ToString();
							reader.NextResult();
							temp.Message += "TimeSlice.2" + DateTime.UtcNow.ToString();
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
									TableTypeID = reader.GetInt32(19),
									PublicationDate = reader.GetDateTime(20),
									DamDocumentId = reader.GetGuid(21),
									PeriodNoteID = reader.GetNullable<byte>(22)
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
							temp.Message += "IsSummary." + DateTime.UtcNow.ToString();
							while (reader.Read()) {
								int TimeSliceID = reader.GetInt32(0);
								if (TimeSlices.FirstOrDefault(t => t.Id == TimeSliceID) != null) {
									TimeSlices.FirstOrDefault(t => t.Id == TimeSliceID).IsSummary = true;
								}
								if (!IsSummaryLookup.ContainsKey(TimeSliceID)) {
									IsSummaryLookup.Add(TimeSliceID, new List<string>());
								}

								IsSummaryLookup[TimeSliceID].Add(reader.GetStringSafe(1));
							}
						}
					}
					#endregion
				}

				temp.Message += "Calculate.";
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

							bool hasValidChild = false;
							decimal calcChildSum = CalculateChildSum(tc, CellLookup, SHChildLookup, IsSummaryLookup, ref hasValidChild, temp.TimeSlices);
							if (hasValidChild && tc.ID == 0 && !tc.ValueNumeric.HasValue && !tc.VirtualValueNumeric.HasValue && !IsSummaryLookup.ContainsKey(ts.Id)) {
								tc.VirtualValueNumeric = calcChildSum;
							}


							//bool whatever = false;
							//decimal cellValue = CalculateCellValue(tc, BlankCells, SHChildLookup, IsSummaryLookup, ref whatever, temp.TimeSlices);

							//List<int> sortedLessThanPubDate = matches.Where(m2 => temp.TimeSlices[m2].PublicationDate < temp.TimeSlices[i].PublicationDate).OrderByDescending(c => temp.TimeSlices[c].PublicationDate).ToList();

							//if (LPV(BlankCells, CellLookup, SHChildLookup, IsSummaryLookup, sh, tc, matches, ref whatever, cellValue, sortedLessThanPubDate, temp.TimeSlices)
							//) {
							//	tc.LikePeriodValidationFlag = true;
							//	tc.StaticHierarchyID = sh.Id;
							//	tc.DocumentTimeSliceID = ts.Id;
							//}

							//bool ChildrenSumEqual = false;
							//if (!tc.ValueNumeric.HasValue || !hasValidChild)
							//	ChildrenSumEqual = true;
							//else {
							//	decimal diff = cellValue - calcChildSum;
							//	diff = Math.Abs(diff);

							//	if (tc.ScalingFactorValue == 1.0)
							//		ChildrenSumEqual = tc.ValueNumeric.HasValue && ((diff == 0) || (diff < 0.01m));
							//	else
							//		ChildrenSumEqual = tc.ValueNumeric.HasValue && ((diff == 0) || (diff < 0.1m && Math.Abs(cellValue) > 100));
							//}

							//tc.MTMWValidationFlag = tc.ValueNumeric.HasValue && SHChildLookup[sh.Id].Count > 0 &&
							//		!ChildrenSumEqual &&
							//				!tc.MTMWErrorTypeId.HasValue && sh.UnitTypeId != 2;

						} catch (Exception ex) {
							Console.WriteLine(ex.Message);
							break;
						}
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

							bool hasValidChild = false;
							decimal calcChildSum = CalculateChildSum(tc, CellLookup, SHChildLookup, IsSummaryLookup, ref hasValidChild, temp.TimeSlices);
							if (hasValidChild && tc.ID == 0 && !tc.ValueNumeric.HasValue && !tc.VirtualValueNumeric.HasValue && !IsSummaryLookup.ContainsKey(ts.Id)) {
								tc.VirtualValueNumeric = calcChildSum;
							}

							bool whatever = false;
							decimal cellValue = CalculateCellValue(tc, BlankCells, SHChildLookup, IsSummaryLookup, ref whatever, temp.TimeSlices);

							List<int> sortedLessThanPubDate = matches.Where(m2 => temp.TimeSlices[m2].PublicationDate < temp.TimeSlices[i].PublicationDate).OrderByDescending(c => temp.TimeSlices[c].PublicationDate).ToList();

							if (LPV(BlankCells, CellLookup, SHChildLookup, IsSummaryLookup, sh, tc, matches, ref whatever, cellValue, sortedLessThanPubDate, temp.TimeSlices)
							) {
								tc.LikePeriodValidationFlag = true;
								tc.StaticHierarchyID = sh.Id;
								tc.DocumentTimeSliceID = ts.Id;
							}

							bool ChildrenSumEqual = false;
							if (!tc.ValueNumeric.HasValue || !hasValidChild)
								ChildrenSumEqual = true;
							else {
								decimal diff = cellValue - calcChildSum;
								diff = Math.Abs(diff);

								if (tc.ScalingFactorValue == 1.0)
									ChildrenSumEqual = tc.ValueNumeric.HasValue && ((diff == 0) || (diff < 0.01m));
								else
									ChildrenSumEqual = tc.ValueNumeric.HasValue && ((diff == 0) || (diff < 0.1m && Math.Abs(cellValue) > 100));
							}

							tc.MTMWValidationFlag = tc.ValueNumeric.HasValue && SHChildLookup[sh.Id].Count > 0 &&
									!ChildrenSumEqual &&
											!tc.MTMWErrorTypeId.HasValue && sh.UnitTypeId != 2;

						} catch (Exception ex) {
							Console.WriteLine(ex.Message);
							break;
						}
					}
				}
				temp.Message += "Finished.";

			} catch (Exception ex) {
				throw new Exception(temp.Message + "ExceptionTime:" + DateTime.UtcNow.ToString() + ex.Message, ex);
			}
			return temp;
		}
		public decimal getDecimal(decimal value, double ScalingFactorValue, Boolean ispositive) {
			decimal factor = Decimal.Parse("" + ScalingFactorValue);
			int m = 1;
			if ((value > 0 && !ispositive) || (value < 0 && ispositive))
				m = -1;
			decimal t1 = Math.Abs(value);
			decimal test = Decimal.Parse("" + Math.Abs(value));
			decimal tmp = Decimal.Parse("" + Math.Abs(value)) * factor;
			if (m < 0) {
				tmp = -tmp;
			}
			return tmp;
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

			if (matches.Where(m => CalculateCellValue(sh.Cells[m], BlankCells, SHChildLookup, IsSummaryLookup, ref whatever2, timeSlices) == cellValue).Any(m => sh.Cells[m].ARDErrorTypeId.HasValue ||
				 (GetChildren(sh.Cells[m], CellLookup, SHChildLookup).Any(c => c.ARDErrorTypeId.HasValue) && sh.Cells[m].VirtualValueNumeric.HasValue)))
				return false;

			if (matches.Any(m => (tc.ValueNumeric.HasValue || tc.VirtualValueNumeric.HasValue)
																	&& (sortedLessThanPubDate.Count > 0 && sortedLessThanPubDate.First() == m)
																	&& (!sh.Cells[m].ValueNumeric.HasValue && !sh.Cells[m].VirtualValueNumeric.HasValue)))
				return true;


			var matchGroups = matches.Where(m => sh.Cells[m].VirtualValueNumeric.HasValue || sh.Cells[m].ValueNumeric.HasValue).Where(m => CalculateCellValue(sh.Cells[m], BlankCells, SHChildLookup, IsSummaryLookup, ref whatever2, timeSlices) != cellValue).GroupBy(m => CalculateCellValue(sh.Cells[m], BlankCells, SHChildLookup, IsSummaryLookup, ref whatever2, timeSlices));

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
				//hasChildren = true;
				if (!cell.VirtualValueNumeric.HasValue)
					return cell.ValueNumeric.Value * (cell.IsIncomePositive ? 1 : -1) * (decimal)cell.ScalingFactorValue;
			} else if (cell.ID == 0 && cell.VirtualValueNumeric.HasValue) {
				//hasChildren = true;
				return cell.VirtualValueNumeric.Value;
			} else {
				/*
				if (BlankCells.ContainsKey(cell)) {
					decimal sum = 0;
					StaticHierarchy sh = BlankCells[cell].Item1;
					int timesliceIndex = BlankCells[cell].Item2;
					TimeSlice ts = timeSlices[timesliceIndex];
					bool subChildren = false;

					foreach (StaticHierarchy child in SHChildLookup[sh.Id]) {
						hasChildren = true;
						if (
							(((child.StaticHierarchyMetaId != 2 && child.StaticHierarchyMetaId != 5 && child.StaticHierarchyMetaId != 6) && (sh.StaticHierarchyMetaId != 2 && sh.StaticHierarchyMetaId != 5 && sh.StaticHierarchyMetaId != 6))
								|| ((child.StaticHierarchyMetaId == 2 || child.StaticHierarchyMetaId == 5 || child.StaticHierarchyMetaId == 6) && child.StaticHierarchyMetaId == sh.StaticHierarchyMetaId))
							&& child.UnitTypeId != 2
							) {
							sum += CalculateCellValue(child.Cells[timesliceIndex], BlankCells, SHChildLookup, IsSummaryLookup, ref subChildren, timeSlices);
							if (subChildren)
								hasChildren = true;

						}
					}
					if (SHChildLookup[sh.Id].Where(c =>
						(((c.StaticHierarchyMetaId != 2 && c.StaticHierarchyMetaId != 5 && c.StaticHierarchyMetaId != 6) && (sh.StaticHierarchyMetaId != 2 && sh.StaticHierarchyMetaId != 5 && sh.StaticHierarchyMetaId != 6))
								|| ((c.StaticHierarchyMetaId == 2 || c.StaticHierarchyMetaId == 5 || c.StaticHierarchyMetaId == 6) && c.StaticHierarchyMetaId == sh.StaticHierarchyMetaId))
							&& c.UnitTypeId != 2
						).Count() > 0) {
						if (!cell.ValueNumeric.HasValue && hasChildren && !(IsSummaryLookup.ContainsKey(ts.Id) && IsSummaryLookup[ts.Id].Contains(sh.TableTypeDescription))) {
							cell.VirtualValueNumeric = sum;
						}
						return sum;
					}
				}
				*/
			}
			return 0;
		}


		/*
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


						if (
							(((child.StaticHierarchyMetaId != 2 && child.StaticHierarchyMetaId != 5 && child.StaticHierarchyMetaId != 6) && (sh.StaticHierarchyMetaId != 2 && sh.StaticHierarchyMetaId != 5 && sh.StaticHierarchyMetaId != 6))
								|| ((child.StaticHierarchyMetaId == 2 || child.StaticHierarchyMetaId == 5 || child.StaticHierarchyMetaId == 6) && child.StaticHierarchyMetaId == sh.StaticHierarchyMetaId))
							&& child.UnitTypeId != 2
							) {
							sum += CalculateCellValue(child.Cells[timesliceIndex], BlankCells, SHChildLookup, IsSummaryLookup, ref subChildren, timeSlices);
							if (subChildren)
								hasChildren = true;

						}
					}
					if (SHChildLookup[sh.Id].Where(c =>
						(((c.StaticHierarchyMetaId != 2 && c.StaticHierarchyMetaId != 5 && c.StaticHierarchyMetaId != 6) && (sh.StaticHierarchyMetaId != 2 && sh.StaticHierarchyMetaId != 5 && sh.StaticHierarchyMetaId != 6))
								|| ((c.StaticHierarchyMetaId == 2 || c.StaticHierarchyMetaId == 5 || c.StaticHierarchyMetaId == 6) && c.StaticHierarchyMetaId == sh.StaticHierarchyMetaId))
							&& c.UnitTypeId != 2
						).Count() > 0) {
						if (!cell.ValueNumeric.HasValue && subChildren && !(IsSummaryLookup.ContainsKey(ts.Id) && IsSummaryLookup[ts.Id].Contains(sh.TableTypeDescription)))
							cell.VirtualValueNumeric = sum;

						return sum;
					}
				}
			}
			return 0;
		}

		*/

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

		private decimal CalculateChildSum(SCARAPITableCell cell, Dictionary<SCARAPITableCell, Tuple<StaticHierarchy, int>> CellLookup, Dictionary<int, List<StaticHierarchy>> SHChildLookup, Dictionary<int, List<string>> IsSummaryLookup, ref bool hasValidChild, List<TimeSlice> TimeSlices) {
			decimal vv = 0;
			if (CellLookup.ContainsKey(cell)) {
				StaticHierarchy sh = CellLookup[cell].Item1;
				int timesliceIndex = CellLookup[cell].Item2;
				TimeSlice ts = TimeSlices[timesliceIndex];

				foreach (StaticHierarchy child in SHChildLookup[sh.Id]) {
					if (sh.StaticHierarchyMetaId == 2 || sh.StaticHierarchyMetaId == 5 || sh.StaticHierarchyMetaId == 6) {
						if (sh.StaticHierarchyMetaId != child.StaticHierarchyMetaId)
							continue;
					}
					if (!(sh.StaticHierarchyMetaId == 2 || sh.StaticHierarchyMetaId == 5 || sh.StaticHierarchyMetaId == 6)) {
						if (child.StaticHierarchyMetaId == 2 || child.StaticHierarchyMetaId == 5 || child.StaticHierarchyMetaId == 6)
							continue;
					}

					if (child.TableTypeDescription != "NG-IS") {
						if (child.UnitTypeId == 2)
							continue;
					} else {
						if ((sh.UnitTypeId == 2 && child.UnitTypeId != 2) || (sh.UnitTypeId != 2 && child.UnitTypeId == 2))
							continue;
					}

					SCARAPITableCell cc = child.Cells[timesliceIndex];
					if (cc == null)
						continue;

					if (cc.ValueNumeric.HasValue) {
						decimal tmp = getDecimal(cc.ValueNumeric.Value, cc.ScalingFactorValue, cc.IsIncomePositive); //   cc.ValueNumeric.Value;
						vv += tmp;
						hasValidChild = true;
						continue;
					}
					if (cc.ID > 0) {
						continue;
					}

					if (SHChildLookup[child.Id].Count() > 0) {
						if (!IsSummaryLookup.ContainsKey(ts.Id)) {
							decimal headerTotal = CalculateChildSum(cc, CellLookup, SHChildLookup, IsSummaryLookup, ref hasValidChild, TimeSlices);
							if (headerTotal != 0) {
								cc.VirtualValueNumeric = headerTotal;
								hasValidChild = true;
								vv += headerTotal;
							}
						}
					}
				}
			}
			return vv;
		}

		public AsReportedTemplateSkeleton GetTemplateSkeleton(int iconum, string TemplateName) {

			string query =
				@"
SELECT DISTINCT sh.ID, sh.AdjustedOrder
FROM DocumentSeries ds WITH (NOLOCK)
	JOIN CompanyFinancialTerm cft WITH (NOLOCK) ON cft.DocumentSeriesId = ds.Id
	JOIN StaticHierarchy sh WITH (NOLOCK) on cft.ID = sh.CompanyFinancialTermID
	JOIN TableType tt WITH (NOLOCK) on sh.TableTypeID = tt.ID
WHERE ds.CompanyID = @iconum
AND tt.Description = @templateName
ORDER BY sh.AdjustedOrder asc";

			string TimeSliceQuery =
				@"SELECT DISTINCT dts.ID, sh.AdjustedOrder, dts.Duration, dts.TimeSlicePeriodEndDate, dts.ReportingPeriodEndDate
FROM DocumentSeries ds WITH (NOLOCK)
	JOIN CompanyFinancialTerm cft WITH (NOLOCK) ON cft.DocumentSeriesId = ds.Id
	JOIN StaticHierarchy sh WITH (NOLOCK) on cft.ID = sh.CompanyFinancialTermID
	JOIN TableType tt WITH (NOLOCK) on sh.TableTypeID = tt.ID
	JOIN TableCell tc WITH (NOLOCK) on tc.CompanyFinancialTermID = cft.ID
	JOIN DocumentTimeSliceTableCell dtstc WITH (NOLOCK) on tc.ID = dtstc.TableCellID
	JOIN dbo.DocumentTimeSlice dts WITH (NOLOCK) on dtstc.DocumentTimeSliceID = dts.ID and dts.DocumentSeriesId = ds.ID 
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
FROM DocumentSeries ds WITH (NOLOCK)
	JOIN CompanyFinancialTerm cft WITH (NOLOCK) ON cft.DocumentSeriesId = ds.Id
	JOIN StaticHierarchy sh WITH (NOLOCK) on cft.ID = sh.CompanyFinancialTermID
	JOIN TableType tt WITH (NOLOCK) on sh.TableTypeID = tt.ID
	JOIN TableCell tc WITH (NOLOCK) on tc.CompanyFinancialTermID = cft.ID
	JOIN DocumentTimeSliceTableCell dtstc WITH (NOLOCK) on tc.ID = dtstc.TableCellID
	JOIN dbo.DocumentTimeSlice dts WITH (NOLOCK) on dtstc.DocumentTimeSliceID = dts.ID

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

		public ScarResult CopyDocumentHierarchy(int iconum, int TableTypeid, Guid DocumentId) {
			string query = @"prcUpd_FFDocHist_UpdateStaticHierarchy_CopyHierarchy";
			string text_query = @"
DECLARE @newDocumentTableId int;
BEGIN TRY
	BEGIN TRAN

				Declare @DocSeriesId int;
				Declare @DocumentDate DateTime;
				Declare @LatestScalingFactor Varchar(6);
				

				select @DocumentDate= DocumentDate,@DocSeriesId=DocumentSeriesID from Document Where Id=@DocumentId;

				select TOP 1 @LatestScalingFactor = ISNULL(dt.ScalingFactorID, 'A')
				from DocumentTable dt  WITH (NOLOCK)
				inner join Document d WITH (NOLOCK) on d.ID=dt.DocumentID and dt.TableTypeId = @TableTypeId
				where dt.ScalingFactorID<>'A' and d.DocumentDate<@DocumentDate
				order by d.DocumentDate DESC, d.ReportTypeID ASC

				INSERT DocumentTable(DocumentID,TableOrganizationID,TableTypeID,Consolidated,Unit,ScalingFactorID,TableIntID,ExceptShare)
				VALUES (@DocumentId, 1, @TableTypeId, 1, @LatestScalingFactor, @LatestScalingFactor, -1, 0)

				select @newDocumentTableId =  cast(scope_identity() as int);
				select * from DocumentTable WITH (NOLOCK) where id = @newDocumentTableId

				DECLARE @newTableDimensionColumn int;
				INSERT TableDimension (DocumentTableID,DimensionTypeID,Label,OrigLabel,Location,EndLocation,Parent,InsertedRow,AdjustedOrder)
				VALUES (@newDocumentTableId, 2, '', '', -1, -1, NULL, 0, -1)
				select @newTableDimensionColumn =  cast(scope_identity() as int);
				select * from TableDimension WITH (NOLOCK) where id = @newTableDimensionColumn

				DECLARE @newTableDimensionRows TABLE(Id int, CFT int, AdjustedOrder int)
				MERGE INTO TableDimension
				USING (	SELECT sh.Description, sh.AdjustedOrder, sh.CompanyFinancialTermId
					FROM StaticHierarchy sh where sh.TableTypeId = @TableTypeId) as Src ON 1=0
				WHEN NOT MATCHED THEN
					INSERT (DocumentTableID,DimensionTypeID,Label,OrigLabel,Location,EndLocation,Parent,InsertedRow,AdjustedOrder)
					VALUES (@newDocumentTableId, 1, src.Description, 'Description', -1, -1, NULL, 0, src.AdjustedOrder)
				OUTPUT inserted.ID, src.CompanyFinancialTermId, inserted.AdjustedOrder into @newTableDimensionRows(Id, CFT, AdjustedOrder);
				select * FROM TableDimension where id = @newDocumentTableId


				DECLARE @newTableCells TABLE(Id int, CFT int, AdjustedOrder int)
				MERGE INTO TableCell
				USING (	SELECT sh.Description, sh.CompanyFinancialTermId, sh.AdjustedOrder
					FROM StaticHierarchy sh where sh.TableTypeId = @TableTypeId) as Src ON 1=0
				WHEN NOT MATCHED THEN
					INSERT (Offset,CompanyFinancialTermId,NormalizedNegativeIndicator,ScalingFactorID,AsReportedScalingFactor,ScarUpdated,IsIncomePositive,DocumentId,Label)
					VALUES ('', src.CompanyFinancialTermId, 0, @LatestScalingFactor, @LatestScalingFactor, 0, 1, @DocumentId, src.description)
				OUTPUT inserted.ID, inserted.CompanyFinancialTermId, src.AdjustedOrder into @newTableCells(Id, CFT, AdjustedOrder);
				select * from @newTableCells

				INSERT INTO DimensionToCell(TableDimensionID,TableCellID)
				SELECT r.id, tc.id FROM
				@newTableDimensionRows r 
				JOIN @newTableCells tc ON r.CFT = tc.CFT and r.AdjustedOrder = tc.AdjustedOrder

				INSERT INTO DimensionToCell (TableDimensionID,TableCellID)
				SELECT @newTableDimensionColumn, tc.ID FROM
				@newTableCells tc

				SELECT * FROM DimensionToCell WITH (NOLOCK) where TableDimensionID = @newTableDimensionColumn or
				TableDimensionID in (select id from @newTableDimensionRows)

		COMMIT; 
		select * from DocumentTable where id = @newDocumentTableId
END TRY
BEGIN CATCH
	ROLLBACK;
  select 0;
END CATCH
";

			ScarResult response = new ScarResult();
			response.StaticHierarchies = new List<StaticHierarchy>();

			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {
				conn.Open();
				using (SqlCommand cmd = new SqlCommand(query, conn)) {
					cmd.CommandType = System.Data.CommandType.StoredProcedure;
					cmd.Parameters.AddWithValue("@DocumentId", DocumentId);
					cmd.Parameters.AddWithValue("@TableTypeId", TableTypeid);
					using (SqlDataReader reader = cmd.ExecuteReader()) {
						if (reader.Read()) {
							int newDocumentTableId = reader.GetInt32(0);
							if (newDocumentTableId > 0) {
								response.ReturnValue["Success"] = "T";
								response.ReturnValue["DocumentTableId"] = newDocumentTableId.ToString();
							} else {
								response.ReturnValue["Success"] = "F";
							}
						}
					}
				}
			}
			return response;
		}

		public ScarResult UpdateStaticHierarchySeperator(int id, bool isGroup) {

			string query = @"
UPDATE StaticHierarchy SET SeperatorFlag = @newValue 
WHERE id = @TargetSHID;

select * 
FROM StaticHierarchy WITH (NOLOCK)
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
FROM StaticHierarchy WITH (NOLOCK)
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
FROM StaticHierarchy WITH (NOLOCK)
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
DECLARE @TableTypeID INT

SET @TableTypeID = (SELECT TableTypeId  FROM StaticHierarchy WHERE ID = @TargetSHID)
 SET @OrigHierarchyLabel = (SELECT dbo.GetHierarchyLabelSafe(Description) + '[' + dbo.GetEndLabelSafe(Description) + ']' FROM StaticHierarchy WHERE ID = @TargetSHID)
 SET @NewHierarchyLabel = (SELECT dbo.GetHierarchyLabelSafe	(Description) + '[New Header][' + dbo.GetEndLabelSafe(Description) + ']'  FROM StaticHierarchy WHERE ID = @TargetSHID)
	 UPDATE StaticHierarchy
	SET Description = (dbo.GetHierarchyLabelSafe(@OrigDescription) + '[New Header]' + dbo.GetEndLabelSafe(Description))
	WHERE ID = @TargetSHID

;WITH CTE_Children(ID) AS(
	SELECT ID FROM StaticHierarchy WITH (NOLOCK) WHERE ID = @TargetSHID
	UNION ALL
	SELECT sh.Id 
	FROM StaticHierarchy sh WITH (NOLOCK)
	JOIN CTE_Children cte on sh.ParentID = cte.ID
) UPDATE sh
   SET sh.Description = REPLACE(sh.description, @OrigHierarchyLabel, @NewHierarchyLabel)
FROM CTE_Children cte
JOIN StaticHierarchy sh on cte.ID = SH.Id   

exec prcUpd_FFDocHist_UpdateStaticHierarchy_Cleanup @TableTypeID

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

		public static void SendEmail(string subject, string emailBody) {
			try {
				SmtpClient mySMTP = new SmtpClient("mail.factset.com");
				MailAddress mailFrom = new MailAddress("myself@factset.com", "IMA DataRoost");
				MailMessage message = new MailMessage();
				message.From = mailFrom;
				message.To.Add(new MailAddress("ljiang@factset.com", "Lun Jiang"));
				message.Subject = subject + " from " + Environment.MachineName;
				message.DeliveryNotificationOptions = DeliveryNotificationOptions.OnFailure;
				message.Body = emailBody;
				message.IsBodyHtml = true;
				mySMTP.Send(message);
			} catch { }
		}

		public ScarResult DeleteStaticHierarchy(List<int> StaticHierarchyIds) {
			ScarResult response = new ScarResult();
			response.StaticHierarchies = new List<StaticHierarchy>();
			var inclause = string.Join(",", StaticHierarchyIds);
			string query1 = @"delete from dbo.ARTimeSliceDerivationMeta where StaticHierarchyID in ({0})";
			string query2 = @"delete from dbo.ARTimeSliceDerivationMetaNodes where StaticHierarchyID in ({0})";
			string query3 = @"delete from dbo.statichierarchy where Id in ({0})";
			String q1 = string.Format(query1, inclause);
			String q2 = string.Format(query2, inclause);
			String q3 = string.Format(query3, inclause);
			String finalquery = q1 + q2 + q3;
			SendEmail("DataRoost Bulk StaticHierarchy Delete - DeleteStaticHierarchy", q3);
			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {
				conn.Open();
				using (SqlCommand cmd = new SqlCommand(finalquery, conn)) {
					using (SqlDataReader reader = cmd.ExecuteReader()) {
						reader.NextResult();
						reader.Read();
						reader.NextResult();
						reader.Read();
					}
				}
			}
			return response;
		}

		public ScarResult CleanupStaticHierarchy(List<int> StaticHierarchyIds) {
			ScarResult response = new ScarResult();
			response.StaticHierarchies = new List<StaticHierarchy>();
			var inclause = string.Join(",", StaticHierarchyIds);
			string query = @"
DECLARE @AllStaticHierarchy TABLE
(ID int, CompanyFinancialTermID int);
INSERT @AllStaticHierarchy 
SELECT distinct ID, CompanyFinancialTermID from StaticHierarchy where id in ({0});


DECLARE @GoodStaticHierarchy TABLE
(ID int, CFT int);

INSERT @GoodStaticHierarchy
Select distinct sh.id, sh.CompanyFinancialTermID from 
@AllStaticHierarchy sh 
JOIN TableCell tc on tc.CompanyFinancialTermID = sh.CompanyFinancialTermID
JOIN DocumentTimeSliceTableCell dtstc on dtstc.TableCellId = tc.ID 

INSERT @GoodStaticHierarchy
Select distinct sh.id, sh.CompanyFinancialTermID from 
@AllStaticHierarchy sh 
JOIN TableCell tc on tc.CompanyFinancialTermID = sh.CompanyFinancialTermID
JOIN DimensionToCell dtc on dtc.TableCellId = tc.ID 

DELETE FROM @AllStaticHierarchy where id in (select id from @GoodStaticHierarchy)
delete from dbo.ARTimeSliceDerivationMeta where StaticHierarchyID in (select id from @AllStaticHierarchy);
delete from dbo.ARTimeSliceDerivationMetaNodes where StaticHierarchyID in (select id from @AllStaticHierarchy);
delete from dbo.statichierarchy where Id in (select id from @AllStaticHierarchy);
SELECT id From @AllStaticHierarchy
";
			String finalquery = string.Format(query, inclause);
			System.Text.StringBuilder sb = new System.Text.StringBuilder();
			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {
				conn.Open();
				using (SqlCommand cmd = new SqlCommand(finalquery, conn)) {
					using (SqlDataReader reader = cmd.ExecuteReader()) {
						while (reader.Read()) {
							sb.AppendLine(reader.GetInt32(0).AsInt32().ToString() + ",");
						}
					}
				}
			}
			response.Message += "StaticHierarchy Deleted: " + sb.ToString();
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

		public ScarResult UpdateStaticHierarchyDeleteParent(string headerText, List<int> StaticHierarchyIds) {
			ScarResult response = new ScarResult();
			response.StaticHierarchies = new List<StaticHierarchy>();
			string query = @"prcUpd_FFDocHist_UpdateStaticHierarchy_DeleteParent";

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

		public ScarResult UpdateStaticHierarchyCusip(int id, string newCusip) {

			string deletequery = @"delete from dbo.StaticHierarchySecurity where StaticHierarchyId in ({0})";
			string insertquery = @"insert into dbo.StaticHierarchySecurity (StaticHierarchyId,SecPermId) values ({0},'{1}')";

			String q1 = string.Format(deletequery, id);
			String q2 = string.Format(insertquery, id, newCusip);
			SendEmail("DataRoost Bulk StaticHierarchy Delete - UpdateStaticHierarchyCusip", id.ToString());
			ScarResult response = new ScarResult();
			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {
				using (SqlCommand cmd = new SqlCommand(q1, conn)) {
					conn.Open();
					using (SqlDataReader reader = cmd.ExecuteReader()) {
						reader.Read();
					}
				}
				if (newCusip.Length > 5) {
					using (SqlCommand cmd = new SqlCommand(q2, conn)) {
						using (SqlDataReader reader = cmd.ExecuteReader()) {
							reader.Read();
						}
					}
				}
			}
			return response;
		}

		public ScarResult UpdateStaticHierarchyLabel(int id, string newLabel) {

			string query = @"

DECLARE @TableTypeId INT = (SELECT TableTypeId FROM StaticHierarchy WHERE ID = @TargetSHID)
exec prcUpd_FFDocHist_UpdateStaticHierarchy_Cleanup @TableTypeId

DECLARE @OrigDescription varchar(1024) = (SELECT Description FROM StaticHierarchy WHERE ID = @TargetSHID)
DECLARE @OrigHierarchyLabel varchar(1024) 
DECLARE @NewHierarchyLabel varchar(1024)  

 SET @OrigHierarchyLabel = (SELECT dbo.GetHierarchyLabelSafe(Description) + '[' + dbo.GetEndLabelSafe(Description) + ']' FROM StaticHierarchy WHERE ID = @TargetSHID)
 SET @NewHierarchyLabel = dbo.GetHierarchyLabelSafe(@OrigDescription) + '[' + @NewEndLabel + ']'
	 UPDATE StaticHierarchy
	SET Description = (dbo.GetHierarchyLabelSafe(@OrigDescription) + @NewEndLabel)
	WHERE ID = @TargetSHID


;WITH CTE_Children(ID) AS(
	SELECT ID FROM StaticHierarchy WITH (NOLOCK) WHERE ID = @TargetSHID
	UNION ALL
	SELECT sh.Id 
	FROM StaticHierarchy sh WITH (NOLOCK)
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
      ,[ParentID] FROM StaticHierarchy WITH (NOLOCK) WHERE ID = @TargetSHID
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
	FROM StaticHierarchy sh WITH (NOLOCK)
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

		public ScarResult UpdateStaticHierarchyHeaderLabel(int id, string newLabel) {

			string query = @"
DECLARE @TableTypeId int = (SELECT Top 1 TableTypeId  FROM StaticHierarchy WHERE ID = @TargetSHID)
exec prcUpd_FFDocHist_UpdateStaticHierarchy_Cleanup @TableTypeId

DECLARE @OrigDescription varchar(1024) = (SELECT dbo.GetHierarchyLabelSafe(Description)  FROM StaticHierarchy WHERE ID = @TargetSHID)
DECLARE @OrigHierarchyLabel varchar(1024) 
DECLARE @NewHierarchyLabel varchar(1024)  
DECLARE @OrigParent varchar(1024)

IF (DATALENGTH(@OrigDescription) > 2)
BEGIN 
SET @OrigParent = dbo.GetHierarchyLabelSafe(SUBSTRING(@OrigDescription, 0, DATALENGTH(@OrigDescription)))
SET @NewHierarchyLabel = @OrigParent + '[' + @NewEndLabel + ']'
	 UPDATE StaticHierarchy
	SET Description = Replace(Description, @OrigDescription,@NewHierarchyLabel) 
	WHERE TableTypeId = @TableTypeId   
END



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

			string query = @"prcUpd_FFDocHist_UpdateStaticHierarchy_AddParent";

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

		public ScarResult UpdateStaticHierarchyConvertDanglingHeader(int id, string newValue) {

			string query = @"prcUpd_FFDocHist_UpdateStaticHierarchy_ConvertDanglingHeader";

			ScarResult response = new ScarResult();
			response.StaticHierarchies = new List<StaticHierarchy>();

			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {


				using (SqlCommand cmd = new SqlCommand(query, conn)) {
					conn.Open();
					cmd.CommandType = System.Data.CommandType.StoredProcedure;
					cmd.Parameters.AddWithValue("@TargetSHID", id);
					cmd.Parameters.AddWithValue("@newLabel", newValue);
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

DECLARE @TableTypeId int = (SELECT Top 1 TableTypeId  FROM StaticHierarchy WHERE ID = @TargetSHID)
exec prcUpd_FFDocHist_UpdateStaticHierarchy_Cleanup @TableTypeId

UPDATE StaticHierarchy set ChildrenExpandDown = CASE WHEN ChildrenExpandDown = 1 THEN 0 ELSE 1 END
																WHERE ID = @TargetSHID; 

exec prcUpd_FFDocHist_UpdateStaticHierarchy_Cleanup @TableTypeId

SELECT * FROM StaticHierarchy WITH (NOLOCK) WHERE ID = @TargetSHID; 
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
 
DECLARE @tableTypeId INT  = (SELECT TOP 1 [TableTypeId] from [StaticHierarchy] WITH (NOLOCK) where id = @DraggedSHID)
DECLARE @adjustedOrder INT = (SELECT TOP 1 AdjustedOrder from [StaticHierarchy] WITH (NOLOCK) where id = @DraggedSHID)
DECLARE @Description varchar(60) = (SELECT TOP 1 Description from [StaticHierarchy] WITH (NOLOCK) where id = @DraggedSHID)
DECLARE @maxTargetId INT = (SELECT TOP 1 Id from [StaticHierarchy] WITH (NOLOCK) where TableTypeId = @tableTypeId order by AdjustedOrder desc)
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
 
DECLARE @tableTypeId INT  = (SELECT TOP 1 [TableTypeId] from [StaticHierarchy] WITH (NOLOCK) where id = @DraggedSHID)
DECLARE @adjustedOrder INT = (SELECT TOP 1 AdjustedOrder from [StaticHierarchy] WITH (NOLOCK) where id = @DraggedSHID)
DECLARE @Description varchar(60) = (SELECT TOP 1 Description from [StaticHierarchy] WITH (NOLOCK) where id = @DraggedSHID)
DECLARE @maxTargetId INT = (SELECT TOP 1 Id from [StaticHierarchy] WITH (NOLOCK) where TableTypeId = @tableTypeId order by AdjustedOrder)
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
 
DECLARE @tableTypeId INT  = (SELECT TOP 1 [TableTypeId] from [StaticHierarchy] WITH (NOLOCK) where id = @DraggedSHID)
DECLARE @adjustedOrder INT = (SELECT TOP 1 AdjustedOrder from [StaticHierarchy] WITH (NOLOCK) where id = @DraggedSHID)
DECLARE @Description varchar(60) = (SELECT TOP 1 Description from [StaticHierarchy] WITH (NOLOCK) where id = @DraggedSHID)
DECLARE @maxTargetId INT = (SELECT TOP 1 Id from [StaticHierarchy] WITH (NOLOCK) where TableTypeId = @tableTypeId order by AdjustedOrder)
DECLARE @TargetSHID INT;

DECLARE @DraggedParentId int = (SELECT TOP 1 ParentID from [StaticHierarchy] WITH (NOLOCK) where id = @DraggedSHID)
DECLARE @DraggedParentParentId int = (SELECT TOP 1 ParentID from [StaticHierarchy] WITH (NOLOCK) where id = @DraggedParentId)


SELECT *
  FROM [ffdocumenthistory].[dbo].[StaticHierarchy] WITH (NOLOCK) where tabletypeid = @tableTypeId
  order by AdjustedOrder


IF (@DraggedParentId IS NOT NULL)
BEGIN
	EXEC prcUpd_FFDocHist_UpdateStaticHierarchy_DragDrop @DraggedSHID, @DraggedParentId, 'BOTTOM'
END
ELSE
BEGIN

	DECLARE cur CURSOR LOCAL FAST_FORWARD FOR
		SELECT ID FROM [StaticHierarchy] WITH (NOLOCK) WHERE id <> @DraggedSHID and parentid = @DraggedParentId
	OPEN cur
	FETCH NEXT FROM cur INTO @TargetSHID
	WHILE @@FETCH_STATUS = 0 
	BEGIN
		EXEC prcUpd_FFDocHist_UpdateStaticHierarchy_DragDrop @DraggedSHID, @DraggedParentId, 'MIDDLE'
	END
END

";

			string query = @"
DECLARE @tableTypeId2 INT = (SELECT TOP 1 [TableTypeId] from [StaticHierarchy] WITH (NOLOCK) where id = @DraggedSHID)
SELECT *
  FROM [ffdocumenthistory].[dbo].[StaticHierarchy] WITH (NOLOCK) where tabletypeid = @tableTypeId2
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
BEGIN TRY
	BEGIN TRAN
DECLARE @TargetParentId INT = (select ParentID from StaticHierarchy WITH (NOLOCK) where id = @TargetSHID)

exec prcUpd_FFDocHist_UpdateStaticHierarchy_DragDrop @DraggedSHID, @TargetSHID , @Location
	COMMIT 
END TRY
BEGIN CATCH
	ROLLBACK;
END CATCH

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
							//StaticHierarchy sh = new StaticHierarchy
							//{
							//	Id = reader.GetInt32(0),
							//	CompanyFinancialTermId = reader.GetInt32(1),
							//	AdjustedOrder = reader.GetInt32(2),
							//	TableTypeId = reader.GetInt32(3),
							//	Description = reader.GetStringSafe(4),
							//	HierarchyTypeId = reader.GetStringSafe(5)[0],
							//	SeparatorFlag = reader.GetBoolean(6),
							//	StaticHierarchyMetaId = reader.GetInt32(7),
							//	UnitTypeId = reader.GetInt32(8),
							//	IsIncomePositive = reader.GetBoolean(9),
							//	ChildrenExpandDown = reader.GetBoolean(10),
							//	Cells = new List<SCARAPITableCell>()
							//};
							//response.StaticHierarchies.Add(sh);
						}
					}
				}
			}
			return response;
		}

		public ScarResult DragDropStaticHierarchyLabelByString(int tableTypeId, string DraggedLabel, string TargetLabel, string Location) {

			string query = @"prcUpd_FFDocHist_UpdateStaticHierarchy_DragDrop_ByLabel";

			ScarResult response = new ScarResult();
			response.StaticHierarchies = new List<StaticHierarchy>();

			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {


				using (SqlCommand cmd = new SqlCommand(query, conn)) {
					conn.Open();
					cmd.CommandType = System.Data.CommandType.StoredProcedure;
					cmd.Parameters.AddWithValue("@DraggedLabel", DraggedLabel);
					cmd.Parameters.AddWithValue("@TargetLabel", TargetLabel);
					cmd.Parameters.AddWithValue("@TableTypeID", tableTypeId);
					cmd.Parameters.AddWithValue("@Location", Location);
					using (SqlDataReader reader = cmd.ExecuteReader()) {
						int i = 0;
						while (reader.Read()) {
							i++;
						}
					}
				}
			}
			return response;
		}

		public TimeSlice GetTimeSlice(int id) {

			string query = @"SELECT * FROM dbo.DocumentTimeSlice WITH (NOLOCK) WHERE ID = @id";

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
							ManualOrgSet = reader.GetBoolean(18),
							TableTypeID = reader.GetInt32(19)
						};
						return slice;
					}
				}
			}
		}

		public ScarResult UpdateTimeSliceIsSummary(int id, string TableType) {

			string query = @"


IF EXISTS(SELECT TOP 1 DocumentTimeSliceID FROM DocumentTimeSliceTableTypeIsSummary WITH (NOLOCK) WHERE DocumentTimeSliceID = @id and TableType = @TableType)
BEGIN
	DELETE FROM DocumentTimeSliceTableTypeIsSummary WHERE DocumentTimeSliceID = @id
END
ELSE
BEGIN
	INSERT DocumentTimeSliceTableTypeIsSummary ([DocumentTimeSliceID],[TableType])
	VALUES (@Id, @TableType)
	SELECT * FROM dbo.DocumentTimeSlice WHERE ID = @id
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
								ManualOrgSet = reader.GetBoolean(18),
								TableTypeID = reader.GetInt32(19)
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

   SELECT * FROM dbo.DocumentTimeSlice WHERE ID = @id
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
								ManualOrgSet = reader.GetBoolean(18),
								TableTypeID = reader.GetInt32(19)
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

declare @docid uniqueidentifier = (select documentid from dbo.DocumentTimeSlice WITH (NOLOCK) where id = @id)
UPDATE dbo.DocumentTimeSlice SET ReportType = @ReportType where DocumentId = @docid;

SELECT * FROM dbo.DocumentTimeSlice WITH (NOLOCK) WHERE DocumentId = @docid;

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
								ManualOrgSet = reader.GetBoolean(18),
								TableTypeID = reader.GetInt32(19)
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

UPDATE dbo.DocumentTimeSlice SET ManualOrgSet = @newValue where id = @id;

SELECT * FROM dbo.DocumentTimeSlice WITH (NOLOCK) WHERE id = @id;

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
								ManualOrgSet = reader.GetBoolean(18),
								TableTypeID = reader.GetInt32(19)
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

SELECT 'x', * FROM TableCell WITH (NOLOCK) WHERE ID = @id;

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

SELECT 'x', * FROM TableCell WITH (NOLOCK) WHERE ID = @id;

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

SELECT 'x', * FROM TableCell WITH (NOLOCK) WHERE ID = @id;

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

SELECT 'x', * FROM TableCell WITH (NOLOCK) WHERE ID = @id;

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


SELECT 'x', * FROM TableCell WITH (NOLOCK) WHERE ID = @id;

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

SELECT 'x', * FROM TableCell WITH (NOLOCK) WHERE ID = @id;

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

SELECT 'x', * FROM TableCell WITH (NOLOCK) WHERE ID = @id;

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
from [DimensionToCell] dtc  WITH (NOLOCK)
JOIN [TableDimension] td WITH (NOLOCK) on dtc.TableDimensionID = td.ID and td.DimensionTypeID = 1
JOIN [DimensionToCell] dtc2 WITH (NOLOCK) on td.ID = dtc2.TableDimensionID
JOIN [TableCell] tc WITH (NOLOCK) on tc.id = dtc2.TableCellID
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
from [DimensionToCell] dtc  WITH (NOLOCK)
JOIN [TableDimension] td WITH (NOLOCK) on dtc.TableDimensionID = td.ID and td.DimensionTypeID = 1
JOIN [DimensionToCell] dtc2 WITH (NOLOCK) on td.ID = dtc2.TableDimensionID
JOIN [TableCell] tc WITH (NOLOCK) on tc.id = dtc2.TableCellID
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
from [DimensionToCell] dtc  WITH (NOLOCK)
JOIN [TableDimension] td WITH (NOLOCK) on dtc.TableDimensionID = td.ID and td.DimensionTypeID = 1
JOIN [DimensionToCell] dtc2 WITH (NOLOCK) on td.ID = dtc2.TableDimensionID
JOIN [TableCell] tc WITH (NOLOCK) on tc.id = dtc2.TableCellID
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
from [DimensionToCell] dtc  WITH (NOLOCK)
JOIN [TableDimension] td WITH (NOLOCK) on dtc.TableDimensionID = td.ID and td.DimensionTypeID = 2
JOIN [DimensionToCell] dtc2 WITH (NOLOCK) on td.ID = dtc2.TableDimensionID
JOIN [TableCell] tc WITH (NOLOCK) on tc.id = dtc2.TableCellID
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
from [DimensionToCell] dtc  WITH (NOLOCK)
JOIN [TableDimension] td WITH (NOLOCK) on dtc.TableDimensionID = td.ID and td.DimensionTypeID = 2
JOIN [DimensionToCell] dtc2 WITH (NOLOCK) on td.ID = dtc2.TableDimensionID
JOIN [TableCell] tc WITH (NOLOCK) on tc.id = dtc2.TableCellID
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
from [DimensionToCell] dtc  WITH (NOLOCK)
JOIN [TableDimension] td WITH (NOLOCK) on dtc.TableDimensionID = td.ID and td.DimensionTypeID = 2
JOIN [DimensionToCell] dtc2 WITH (NOLOCK) on td.ID = dtc2.TableDimensionID
JOIN [TableCell] tc WITH (NOLOCK) on tc.id = dtc2.TableCellID
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
from [DimensionToCell] dtc  WITH (NOLOCK)
JOIN [TableDimension] td WITH (NOLOCK) on dtc.TableDimensionID = td.ID and td.DimensionTypeID = 2
JOIN [DimensionToCell] dtc2 WITH (NOLOCK) on td.ID = dtc2.TableDimensionID
JOIN [TableCell] tc WITH (NOLOCK) on tc.id = dtc2.TableCellID
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
from [DimensionToCell] dtc  WITH (NOLOCK)
JOIN [TableDimension] td WITH (NOLOCK) on dtc.TableDimensionID = td.ID and td.DimensionTypeID = 2
JOIN [DimensionToCell] dtc2 WITH (NOLOCK) on td.ID = dtc2.TableDimensionID
JOIN [TableCell] tc WITH (NOLOCK) on tc.id = dtc2.TableCellID
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

			string query = @"

UPDATE dbo.DocumentTimeSlice SET PeriodType = @newValue
where id = @id

SELECT * from dbo.DocumentTimeSlice WITH (NOLOCK) where id = @id and PeriodType = @newValue;

";


			ScarResult response = new ScarResult();
			response.StaticHierarchies = new List<StaticHierarchy>();
			var sh = new StaticHierarchy();
			response.StaticHierarchies.Add(sh);
			sh.Description = @"";
			sh.Cells = new List<SCARAPITableCell>();
			response.ReturnValue = new Dictionary<string, string>();
			response.ReturnValue["Success"] = "F";
			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {

				using (SqlCommand cmd = new SqlCommand(query, conn)) {
					conn.Open();
					cmd.Parameters.AddWithValue("@oldDtsID", id);
					cmd.Parameters.AddWithValue("@newDtsID", newValue);
					using (SqlDataReader reader = cmd.ExecuteReader()) {
						if (reader.Read()) {
							response.ReturnValue["Success"] = "T";
						}
					}
				}
			}
			return response;
		}

		public ScarResult UpdateTableColumnMetaInterimType(string id, string newValue, bool obselete) {
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
from [DimensionToCell] dtc  WITH (NOLOCK)
JOIN [TableDimension] td WITH (NOLOCK) on dtc.TableDimensionID = td.ID and td.DimensionTypeID = 2
JOIN [DimensionToCell] dtc2 WITH (NOLOCK) on td.ID = dtc2.TableDimensionID
JOIN [TableCell] tc WITH (NOLOCK) on tc.id = dtc2.TableCellID
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


		public ScarResult UpdateDocumentTimeSliceTableCell(string id, string newValue) {

			string query = @"

UPDATE DocumentTimeSliceTableCell 
SET DocumentTimeSliceId = @newDtsID WHERE DocumentTimeSliceId = @oldDtsID

SELECT * from DocumentTimeSliceTableCell WITH (NOLOCK) WHERE DocumentTimeSliceId = @newDtsID
";


			ScarResult response = new ScarResult();
			response.StaticHierarchies = new List<StaticHierarchy>();
			var sh = new StaticHierarchy();
			response.StaticHierarchies.Add(sh);
			sh.Description = @"";
			sh.Cells = new List<SCARAPITableCell>();
			response.ReturnValue = new Dictionary<string, string>();
			response.ReturnValue["Success"] = "F";
			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {

				using (SqlCommand cmd = new SqlCommand(query, conn)) {
					conn.Open();
					cmd.Parameters.AddWithValue("@oldDtsID", id);
					cmd.Parameters.AddWithValue("@newDtsID", newValue);
					using (SqlDataReader reader = cmd.ExecuteReader()) {
						if (reader.Read()) {
							response.ReturnValue["Success"] = "T";
						}
					}
				}
			}
			return response;
		}

		public ScarResult CopyDocumentTimeSliceTableCell(string id, string newValue) {

			string query = @"

INSERT DocumentTimeSliceTableCell
SELECT @newDtsID, TableCellId FROM  DocumentTimeSliceTableCell WITH (NOLOCK)
 WHERE DocumentTimeSliceId = @oldDtsID

SELECT * from DocumentTimeSliceTableCell WITH (NOLOCK) WHERE DocumentTimeSliceId = @newDtsID
";


			ScarResult response = new ScarResult();
			response.StaticHierarchies = new List<StaticHierarchy>();
			var sh = new StaticHierarchy();
			response.StaticHierarchies.Add(sh);
			sh.Description = @"";
			sh.Cells = new List<SCARAPITableCell>();
			response.ReturnValue = new Dictionary<string, string>();
			response.ReturnValue["Success"] = "F";
			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {

				using (SqlCommand cmd = new SqlCommand(query, conn)) {
					conn.Open();
					cmd.Parameters.AddWithValue("@oldDtsID", id);
					cmd.Parameters.AddWithValue("@newDtsID", newValue);
					using (SqlDataReader reader = cmd.ExecuteReader()) {
						if (reader.Read()) {
							response.ReturnValue["Success"] = "T";
						}
					}
				}
			}
			return response;
		}

		public ScarResult DeleteDocumentTimeSliceTableCell(string id, string newValue) {

			string query = @"

DELETE FROM  DocumentTimeSliceTableCell
 WHERE DocumentTimeSliceId = @oldDtsID

SELECT * from DocumentTimeSliceTableCell WITH (NOLOCK) WHERE DocumentTimeSliceId = @oldDtsID
";


			ScarResult response = new ScarResult();
			response.StaticHierarchies = new List<StaticHierarchy>();
			var sh = new StaticHierarchy();
			response.StaticHierarchies.Add(sh);
			sh.Description = @"";
			sh.Cells = new List<SCARAPITableCell>();
			response.ReturnValue = new Dictionary<string, string>();
			response.ReturnValue["Success"] = "T";
			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {

				using (SqlCommand cmd = new SqlCommand(query, conn)) {
					conn.Open();
					cmd.Parameters.AddWithValue("@oldDtsID", id);
					using (SqlDataReader reader = cmd.ExecuteReader()) {
						if (reader.Read()) {
							response.ReturnValue["Success"] = "F";
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
where CompanyFinancialTermID in ({0}));

DELETE FROM TableCell where CompanyFinancialTermID in ({0});

				DELETE FROM CompanyFinancialTerm where id in ({0});
				
				";
			string merge_sql = @"MERGE CompanyFinancialTerm
USING ({0}) as src ( ID ,DocumentSeriesID ,TermStatusID ,Description ,NormalizedFlag ,EncoreTermFlag ,ManualUpdate)
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
OUTPUT $action, 'CompanyFinancialTerm', inserted.Id,0 INTO @ChangeResult;

";
			private JArray _jarray;
			public JsonToSQLCompanyFinancialTerm(JToken jToken) : base("") {
				if (jToken == null) {
					_jarray = null;
				} else {
					_jarray = (JArray)jToken.SelectToken("");
				}
			}
			public override string Translate() {
				if (_jarray == null) return "";
				System.Text.StringBuilder merging_ids = new System.Text.StringBuilder();
				System.Text.StringBuilder deleting_ids = new System.Text.StringBuilder();
				bool is_deleting = false;
				bool is_merging = false;
				foreach (var elem in _jarray) {
					try {
						if (elem["action"].ToString() == "delete") {
							if (!is_deleting) {
								deleting_ids.Append(string.Format("{0}", elem["obj"]["ID"].AsValue()));
								is_deleting = true;
							} else {
								deleting_ids.Append(string.Format(",{0}", elem["obj"]["ID"].AsValue()));
							}
						} else if (elem["action"].ToString() == "update") {
							if (!is_merging) {
								merging_ids.AppendLine(string.Format("VALUES ({0}, {1}, {2}, {3}, {4}, {5}, {6})", elem["obj"]["ID"].ToString(),
								elem["obj"]["DocumentSeries"]["ID"].ToString(),
								elem["obj"]["TermStatusID"].AsValue(),
								elem["obj"]["Description"].AsString(),
								(string.Equals(elem["obj"]["NormalizedFlag"].ToString(), "true", StringComparison.InvariantCultureIgnoreCase) ? "1" : "0"),
								elem["obj"]["EncoreTermFlag"].ToString(),
								"0" // manual falg
								));
								is_merging = true;
							} else {
								merging_ids.AppendLine(string.Format(",({0}, {1}, {2}, {3}, {4}, {5}, {6})", elem["obj"]["ID"].ToString(),
elem["obj"]["DocumentSeries"]["ID"].ToString(),
elem["obj"]["TermStatusID"].AsValue(),
elem["obj"]["Description"].AsString(),
(string.Equals(elem["obj"]["NormalizedFlag"].ToString(), "true", StringComparison.InvariantCultureIgnoreCase) ? "1" : "0"),
elem["obj"]["EncoreTermFlag"].ToString(),
"0" // manual flag
));
							}
						} else if (elem["action"].ToString() == "insert") {
							if (!is_merging) {
								merging_ids.AppendLine(string.Format("VALUES ({0}, {1}, {2}, {3}, {4}, {5}, {6})", elem["obj"]["ID"].ToString(),
								elem["obj"]["DocumentSeries"]["ID"].ToString(),
								elem["obj"]["TermStatusID"].AsValue(),
								elem["obj"]["Description"].AsString(),
								(string.Equals(elem["obj"]["NormalizedFlag"].ToString(), "true", StringComparison.InvariantCultureIgnoreCase) ? "1" : "0"),
								elem["obj"]["EncoreTermFlag"].ToString(),
								"0" // manual falg
								));
								is_merging = true;
							} else {
								merging_ids.AppendLine(string.Format(",({0}, {1}, {2}, {3}, {4}, {5}, {6})", elem["obj"]["ID"].ToString(),
elem["obj"]["DocumentSeries"]["ID"].ToString(),
elem["obj"]["TermStatusID"].AsValue(),
elem["obj"]["Description"].AsString(),
(string.Equals(elem["obj"]["NormalizedFlag"].ToString(), "true", StringComparison.InvariantCultureIgnoreCase) ? "1" : "0"),
elem["obj"]["EncoreTermFlag"].ToString(),
"0" // manual flag
));
							}
						}
					} catch (System.Exception ex) {
						merging_ids.AppendLine(@"/*" + ex.Message + @"*/");
					}
				}

				string result = "";
				if (is_deleting) {
					result += string.Format(delete_sql, deleting_ids.ToString());
				}
				if (is_merging) {
					result += string.Format(merge_sql, merging_ids.ToString());
				}
				return result;
			}

		}

		public class JsonToSQLTableDimension : JsonToSQL {
			string delete_sql = @"
DELETE FROM DimensionToCell where TableDimensionId in ({0});
DELETE FROM TableDimension where id in ({0});
";
			string merge_sql = @"MERGE TableDimension
USING ({0}) as src ( ID  ,DocumentTableID  ,DimensionTypeID  ,Label  ,OrigLabel  ,Location  ,EndLocation  ,Parent  ,InsertedRow  ,AdjustedOrder)
ON TableDimension.id = src.ID
WHEN MATCHED THEN
	UPDATE SET       DocumentTableID  = TableDimension.DocumentTableID 
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
OUTPUT $action, 'TableDimension', inserted.Id,0 INTO @ChangeResult;

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
				System.Text.StringBuilder merging_ids = new System.Text.StringBuilder();
				System.Text.StringBuilder deleting_ids = new System.Text.StringBuilder();
				bool is_deleting = false;
				bool is_merging = false;
				foreach (var elem in _jarray) {
					try {
						if (elem["action"].ToString() == "delete") {
							if (!is_deleting) {
								deleting_ids.Append(string.Format("{0}", elem["obj"]["ID"].AsValue()));
								is_deleting = true;
							} else {
								deleting_ids.Append(string.Format(",{0}", elem["obj"]["ID"].AsValue()));
							}
						} else if (elem["action"].ToString() == "update") {
							if (!is_merging) {
								merging_ids.AppendLine(string.Format("VALUES ({0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9})", elem["obj"]["ID"].AsValue(),
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
								is_merging = true;
							} else {
								merging_ids.AppendLine(string.Format(",({0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9})", elem["obj"]["ID"].AsValue(),
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
						} else if (elem["action"].ToString() == "insert") {
							if (!is_merging) {
								merging_ids.AppendLine(string.Format("VALUES ({0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9})", elem["obj"]["ID"].AsValue(),
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
								is_merging = true;
							} else {
								merging_ids.AppendLine(string.Format(",({0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9})", elem["obj"]["ID"].AsValue(),
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
						}
					} catch (System.Exception ex) {
						merging_ids.AppendLine(@"/*" + ex.Message + elem["action"].ToString() + @"*/");
					}
				}
				string result = "";
				if (is_deleting) {
					result += string.Format(delete_sql, deleting_ids.ToString());
				}
				if (is_merging) {
					result += string.Format(merge_sql, merging_ids.ToString());
				}
				return result;
			}

		}

		public class JsonToSQLTableCell : JsonToSQL {
			string delete_sql = @"
DELETE FROM DimensionToCell where TableCellId in ({0});
DELETE FROM TableCell where id in ({0});";
			string merge_sql = @"MERGE TableCell
USING ({0}) 
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
      ,IsIncomePositive = TableCell.IsIncomePositive
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
OUTPUT $action, 'TableCell', inserted.Id,0 INTO @ChangeResult;

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
				System.Text.StringBuilder merging_ids = new System.Text.StringBuilder();
				System.Text.StringBuilder deleting_ids = new System.Text.StringBuilder();
				bool is_deleting = false;
				bool is_merging = false;
				foreach (var elem in _jarray) {
					if (elem == null) continue;
					try {
						if (elem["action"].ToString() == "delete") {
							if (!is_deleting) {
								deleting_ids.Append(string.Format("{0}", elem["obj"]["ID"].AsValue()));
								is_deleting = true;
							} else {
								deleting_ids.Append(string.Format(",{0}", elem["obj"]["ID"].AsValue()));
							}
						} else if (elem["action"].ToString() == "update") {
							if (!is_merging) {
								merging_ids.AppendLine(string.Format("VALUES ({0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}, {11}, {12}, {13}, {14}, {15}, {16}, {17}, {18}, {19}, {20}, {21}, {22}, {23})", elem["obj"]["ID"].AsValue(),
								elem["obj"]["Offset"].AsString(),
								elem["obj"]["CellPeriodType"].AsString(),
								elem["obj"]["PeriodTypeID"].AsString().Length > 0 ? elem["obj"]["PeriodTypeID"].AsString() : elem["obj"]["PeriodTypeID"].AsValue(),
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
								(elem["obj"]["DocumentId"].AsString().Length > 5 ? elem["obj"]["DocumentId"].AsString() : elem["obj"]["DocumentId"].AsValue()),
								elem["obj"]["Label"].AsString(),
								elem["obj"]["XBRLTag"].AsString()
								));
								is_merging = true;
							} else {
								merging_ids.AppendLine(string.Format(",({0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}, {11}, {12}, {13}, {14}, {15}, {16}, {17}, {18}, {19}, {20}, {21}, {22}, {23})", elem["obj"]["ID"].AsValue(),
								elem["obj"]["Offset"].AsString(),
								elem["obj"]["CellPeriodType"].AsString(),
								elem["obj"]["PeriodTypeID"].AsSafeString(),
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
								elem["obj"]["CurrencyCode"].AsSafeString(),
								elem["obj"]["Cusip"].AsString(),
								"0",
								elem["obj"]["IsIncomePositive"].AsBoolean(),
								(elem["obj"]["DocumentId"].AsString().Length > 5 ? elem["obj"]["DocumentId"].AsString() : elem["obj"]["DocumentId"].AsValue()),
								elem["obj"]["Label"].AsString(),
								elem["obj"]["XBRLTag"].AsString()
								));
							}
						} else if (elem["action"].ToString() == "insert") {
							if (!is_merging) {
								merging_ids.AppendLine(string.Format("VALUES ({0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}, {11}, {12}, {13}, {14}, {15}, {16}, {17}, {18}, {19}, {20}, {21}, {22}, {23})", elem["obj"]["ID"].AsValue(),
								elem["obj"]["Offset"].AsString(),
								elem["obj"]["CellPeriodType"].AsString(),
								elem["obj"]["PeriodTypeID"].AsString().Length > 0 ? elem["obj"]["PeriodTypeID"].AsString() : elem["obj"]["PeriodTypeID"].AsValue(),
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
								(elem["obj"]["DocumentId"].AsString().Length > 5 ? elem["obj"]["DocumentId"].AsString() : elem["obj"]["DocumentId"].AsValue()),
								elem["obj"]["Label"].AsString(),
								elem["obj"]["XBRLTag"].AsString()
								));
								is_merging = true;
							} else {
								merging_ids.AppendLine(string.Format(",({0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}, {11}, {12}, {13}, {14}, {15}, {16}, {17}, {18}, {19}, {20}, {21}, {22}, {23})", elem["obj"]["ID"].AsValue(),
								elem["obj"]["Offset"].AsString(),
								elem["obj"]["CellPeriodType"].AsString(),
							elem["obj"]["PeriodTypeID"].AsSafeString(),
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
							elem["obj"]["CurrencyCode"].AsSafeString(),
								elem["obj"]["Cusip"].AsString(),
								"0",
								elem["obj"]["IsIncomePositive"].AsBoolean(),
								(elem["obj"]["DocumentId"].AsString().Length > 5 ? elem["obj"]["DocumentId"].AsString() : elem["obj"]["DocumentId"].AsValue()),
								elem["obj"]["Label"].AsString(),
								elem["obj"]["XBRLTag"].AsString()
								));
							}
						}
					} catch (System.Exception ex) {
						merging_ids.AppendLine(@"/*" + ex.Message + @"*/");
					}
				}
				string result = "";
				if (is_deleting) {
					result += string.Format(delete_sql, deleting_ids.ToString());
				}
				if (is_merging) {
					result += string.Format(merge_sql, merging_ids.ToString());
				}
				return result;
			}

		}

		public class JsonToSQLDimensionToCell : JsonToSQL {
			string delete_sql = @"
DECLARE @TempDTS TABLE(TableDimensionID int, TableCellID int)
INSERT INTO @TempDTS (TableDimensionID, TableCellID)
VALUES  {0}

DELETE DimensionToCell
FROM @TempDTS tdts
JOIN DimensionToCell dts ON tdts.TableDimensionID = dts.TableDimensionID AND tdts.TableCellID = dts.TableCellID; 
				";
						
			string merge_sql = @"MERGE DimensionToCell
USING ({0}) as src ( TableDimensionID,TableCellID)
ON DimensionToCell.TableDimensionID = src.TableDimensionID AND DimensionToCell.TableCellID = src.TableCellID
WHEN MATCHED THEN
	UPDATE SET TableCellID =  DimensionToCell.TableCellID
WHEN NOT MATCHED THEN
	INSERT ( TableDimensionID
      ,TableCellID
      ) VALUES
	  (
	    src.TableDimensionID
      ,src.TableCellID
	  )
OUTPUT $action, 'DimensionToCell', inserted.TableCellID,0 INTO @ChangeResult;

";
			private JArray _jarray;
			private string _dimensionTableId;
			public JsonToSQLDimensionToCell(string dimensionTableId, JToken jToken)
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
				System.Text.StringBuilder merging_ids = new System.Text.StringBuilder();
				System.Text.StringBuilder deleting_ids = new System.Text.StringBuilder();
				bool is_deleting = false;
				bool is_merging = false;
				foreach (var elem in _jarray) {
					try {
						if (elem["action"].ToString() == "delete") {
							if (!is_deleting) {
								deleting_ids.Append(string.Format("({0}, {1})", elem["obj"]["TableDimension"]["ID"].ToString(), elem["obj"]["TableCell"]["ID"].ToString()));
								is_deleting = true;
							} else {
								deleting_ids.Append(string.Format(",({0}, {1})", elem["obj"]["TableDimension"]["ID"].ToString(), elem["obj"]["TableCell"]["ID"].ToString()));
							}
						} else if (elem["action"].ToString() == "update") { // we still need this to pass the UpdateCheck
							if (!is_merging) {
								merging_ids.AppendLine(string.Format("VALUES ({0}, {1})", elem["obj"]["TableDimension"]["ID"].ToString(), elem["obj"]["TableCell"]["ID"].ToString()));
								is_merging = true;
							} else {
								merging_ids.AppendLine(string.Format(",({0}, {1})", elem["obj"]["TableDimension"]["ID"].ToString(), elem["obj"]["TableCell"]["ID"].ToString()));
							}
						} else if (elem["action"].ToString() == "insert") {
							if (!is_merging) {
								merging_ids.AppendLine(string.Format("VALUES ({0}, {1})", elem["obj"]["TableDimension"]["ID"].ToString(), elem["obj"]["TableCell"]["ID"].ToString()));
								is_merging = true;
							} else {
								merging_ids.AppendLine(string.Format(",({0}, {1})", elem["obj"]["TableDimension"]["ID"].ToString(), elem["obj"]["TableCell"]["ID"].ToString()));
							}
						}
					} catch (System.Exception ex) {
						merging_ids.AppendLine(ex.Message);
					}
				}

				string result = "";
				if (is_deleting) {
					result += string.Format(delete_sql, deleting_ids.ToString());
				} 
				if (is_merging) {
					result += string.Format(merge_sql, merging_ids.ToString());
				}
				return result;
			}

		}

		public class JsonToSQLDocumentTimeSlice : JsonToSQL {
			string delete_sql = @"
DELETE FROM dbo.DocumentTimeSlice where id in ({0});
";
			string merge_sql = @"MERGE dbo.DocumentTimeSlice
USING ({0}) as src (Id,DocumentId,DocumentSeriesId,TimeSlicePeriodEndDate,ReportingPeriodEndDate,FiscalDistance,Duration,PeriodType,AcquisitionFlag,AccountingStandard,ConsolidatedFlag,IsProForma,IsRecap,CompanyFiscalYear,ReportType,IsAmended,IsRestated,IsAutoCalc,ManualOrgSet,TableTypeID)
ON dbo.DocumentTimeSlice.id = src.ID
WHEN MATCHED THEN
	UPDATE SET DocumentId = src.DocumentId
      ,DocumentSeriesId = src.DocumentSeriesId
      ,TimeSlicePeriodEndDate = src.TimeSlicePeriodEndDate
      ,ReportingPeriodEndDate = src.ReportingPeriodEndDate
      ,FiscalDistance = src.FiscalDistance
      ,Duration = src.Duration
      ,PeriodType = src.PeriodType
      ,AcquisitionFlag = src.AcquisitionFlag
      ,AccountingStandard = src.AccountingStandard
      ,ConsolidatedFlag = src.ConsolidatedFlag
      ,IsProForma = src.IsProForma
      ,IsRecap = src.IsRecap
      ,CompanyFiscalYear = src.CompanyFiscalYear
      ,ReportType = src.ReportType
      ,IsAmended = src.IsAmended
      ,IsRestated = src.IsRestated
      ,IsAutoCalc = src.IsAutoCalc
      ,ManualOrgSet = src.ManualOrgSet
      ,TableTypeID = src.TableTypeID
WHEN NOT MATCHED THEN
	INSERT (DocumentId
,DocumentSeriesId
,TimeSlicePeriodEndDate
,ReportingPeriodEndDate
,FiscalDistance
,Duration
,PeriodType
,AcquisitionFlag
,AccountingStandard
,ConsolidatedFlag
,IsProForma
,IsRecap
,CompanyFiscalYear
,ReportType
,IsAmended
,IsRestated
,IsAutoCalc
,ManualOrgSet
,TableTypeID) VALUES
	  (
src.DocumentId 
,src.DocumentSeriesId 
,src.TimeSlicePeriodEndDate 
,src.ReportingPeriodEndDate 
,src.FiscalDistance 
,src.Duration 
,src.PeriodType 
,src.AcquisitionFlag 
,src.AccountingStandard 
,src.ConsolidatedFlag 
,src.IsProForma 
,src.IsRecap 
,src.CompanyFiscalYear 
,src.ReportType 
,src.IsAmended 
,src.IsRestated 
,src.IsAutoCalc 
,src.ManualOrgSet 
,src.TableTypeID
	  )
OUTPUT $action, 'DocumentTimeSlice', inserted.Id,0 INTO @ChangeResult;

";
			private JArray _jarray;
			private string _dimensionTableId;
			public JsonToSQLDocumentTimeSlice(JToken jToken)
				: base("") {
				if (jToken == null) {
					_jarray = null;
				} else {
					_jarray = (JArray)jToken.SelectToken("");
				}
			}
			public override string Translate() {
				if (_jarray == null) return "";
				System.Text.StringBuilder merging_ids = new System.Text.StringBuilder();
				System.Text.StringBuilder deleting_ids = new System.Text.StringBuilder();
				bool is_deleting = false;
				bool is_merging = false;
				foreach (var elem in _jarray) {
					try {
						if (elem["action"].ToString() == "delete") {
							if (!is_deleting) {
								deleting_ids.Append(string.Format("{0}", elem["obj"]["ID"].AsValue()));
								is_deleting = true;
							} else {
								deleting_ids.Append(string.Format(",{0}", elem["obj"]["ID"].AsValue()));
							}
						} else if (elem["action"].ToString() == "update") {
							if (!is_merging) {
								merging_ids.AppendLine(string.Format("VALUES ({0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}, {11}, {12}, {13}, {14}, {15}, {16}, {17}, {18}, {19})", elem["obj"]["ID"].AsValue(),
								elem["obj"]["Document"]["ID"].AsString(),
								elem["obj"]["DocumentSeries"]["ID"].AsValue(),
								elem["obj"]["TimeSlicePeriodEndDate"].AsString(),
								elem["obj"]["ReportingPeriodEndDate"].AsString(),
								elem["obj"]["FiscalDistance"].AsValue(),
								elem["obj"]["Duration"].AsValue(),
								elem["obj"]["PeriodType"].AsString(),
								elem["obj"]["AcquisitionFlag"].AsString().Length > 0 ? elem["obj"]["AcquisitionFlag"].AsString() : elem["obj"]["AcquisitionFlag"].AsValue(),
								elem["obj"]["AccountingStandard"].AsString(),
								elem["obj"]["ConsolidatedFlag"].AsString(),
								elem["obj"]["IsProForma"].AsBoolean(),
								elem["obj"]["IsRecap"].AsBoolean(),
								elem["obj"]["CompanyFiscalYear"].AsValue(),
								elem["obj"]["ReportStatus"].AsString(),
								elem["obj"]["IsAmended"].AsBoolean(),
								elem["obj"]["IsRestated"].AsBoolean(),
								elem["obj"]["IsAutoCalc"].AsBoolean(),
								elem["obj"]["ManualOrgSet"].AsBoolean(),
								elem["obj"]["TableTypeID"].AsValue()
								));
								is_merging = true;
							} else {
								merging_ids.AppendLine(string.Format(",({0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}, {11}, {12}, {13}, {14}, {15}, {16}, {17}, {18}, {19})", elem["obj"]["ID"].AsValue(),
								elem["obj"]["Document"]["ID"].AsString(),
								elem["obj"]["DocumentSeries"]["ID"].AsValue(),
								elem["obj"]["TimeSlicePeriodEndDate"].AsString(),
								elem["obj"]["ReportingPeriodEndDate"].AsString(),
								elem["obj"]["FiscalDistance"].AsValue(),
								elem["obj"]["Duration"].AsValue(),
								elem["obj"]["PeriodType"].AsString(),
								elem["obj"]["AcquisitionFlag"].AsString().Length > 0 ? elem["obj"]["AcquisitionFlag"].AsString() : elem["obj"]["AcquisitionFlag"].AsValue(),
								elem["obj"]["AccountingStandard"].AsString(),
								elem["obj"]["ConsolidatedFlag"].AsString(),
								elem["obj"]["IsProForma"].AsBoolean(),
								elem["obj"]["IsRecap"].AsBoolean(),
								elem["obj"]["CompanyFiscalYear"].AsValue(),
								elem["obj"]["ReportStatus"].AsString(),
								elem["obj"]["IsAmended"].AsBoolean(),
								elem["obj"]["IsRestated"].AsBoolean(),
								elem["obj"]["IsAutoCalc"].AsBoolean(),
								elem["obj"]["ManualOrgSet"].AsBoolean(),
								elem["obj"]["TableTypeID"].AsValue()
								));
							}
						} else if (elem["action"].ToString() == "insert") {
							if (!is_merging) {
								merging_ids.AppendLine(string.Format("VALUES ({0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}, {11}, {12}, {13}, {14}, {15}, {16}, {17}, {18}, {19})", elem["obj"]["ID"].AsValue(),
								elem["obj"]["Document"]["ID"].AsString(),
								elem["obj"]["DocumentSeries"]["ID"].AsValue(),
								elem["obj"]["TimeSlicePeriodEndDate"].AsString(),
								elem["obj"]["ReportingPeriodEndDate"].AsString(),
								elem["obj"]["FiscalDistance"].AsValue(),
								elem["obj"]["Duration"].AsValue(),
								elem["obj"]["PeriodType"].AsString(),
								elem["obj"]["AcquisitionFlag"].AsString().Length > 0 ? elem["obj"]["AcquisitionFlag"].AsString() : elem["obj"]["AcquisitionFlag"].AsValue(),
								elem["obj"]["AccountingStandard"].AsString(),
								elem["obj"]["ConsolidatedFlag"].AsString(),
								elem["obj"]["IsProForma"].AsBoolean(),
								elem["obj"]["IsRecap"].AsBoolean(),
								elem["obj"]["CompanyFiscalYear"].AsValue(),
								elem["obj"]["ReportStatus"].AsString(),
								elem["obj"]["IsAmended"].AsBoolean(),
								elem["obj"]["IsRestated"].AsBoolean(),
								elem["obj"]["IsAutoCalc"].AsBoolean(),
								elem["obj"]["ManualOrgSet"].AsBoolean(),
								elem["obj"]["TableTypeID"].AsValue()
								));
								is_merging = true;
							} else {
								merging_ids.AppendLine(string.Format(",({0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}, {11}, {12}, {13}, {14}, {15}, {16}, {17}, {18}, {19})", elem["obj"]["ID"].AsValue(),
								elem["obj"]["Document"]["ID"].AsString(),
								elem["obj"]["DocumentSeries"]["ID"].AsValue(),
								elem["obj"]["TimeSlicePeriodEndDate"].AsString(),
								elem["obj"]["ReportingPeriodEndDate"].AsString(),
								elem["obj"]["FiscalDistance"].AsValue(),
								elem["obj"]["Duration"].AsValue(),
								elem["obj"]["PeriodType"].AsString(),
								elem["obj"]["AcquisitionFlag"].AsString().Length > 0 ? elem["obj"]["AcquisitionFlag"].AsString() : elem["obj"]["AcquisitionFlag"].AsValue(),
								elem["obj"]["AccountingStandard"].AsString(),
								elem["obj"]["ConsolidatedFlag"].AsString(),
								elem["obj"]["IsProForma"].AsBoolean(),
								elem["obj"]["IsRecap"].AsBoolean(),
								elem["obj"]["CompanyFiscalYear"].AsValue(),
								elem["obj"]["ReportStatus"].AsString(),
								elem["obj"]["IsAmended"].AsBoolean(),
								elem["obj"]["IsRestated"].AsBoolean(),
								elem["obj"]["IsAutoCalc"].AsBoolean(),
								elem["obj"]["ManualOrgSet"].AsBoolean(),
								elem["obj"]["TableTypeID"].AsValue()
								));
							}
						}
					} catch (System.Exception ex) {
						merging_ids.AppendLine(@"/*" + ex.Message + elem["action"].ToString() + @"*/");
					}
				}
				string result = "";
				if (is_deleting) {
					result += string.Format(delete_sql, deleting_ids.ToString());
				}
				if (is_merging) {
					result += string.Format(merge_sql, merging_ids.ToString());
				}
				return result;
			}

			public string TranslateInsert() {
				if (_jarray == null) return "";
				System.Text.StringBuilder merging_ids = new System.Text.StringBuilder();
				bool is_merging = false;

				foreach (var elem in _jarray) {
					try {
						if (elem["action"].ToString() == "insert" || elem["action"].ToString() == null) {
							if (!is_merging) {
								merging_ids.AppendLine(string.Format("VALUES ({0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}, {11}, {12}, {13}, {14}, {15}, {16}, {17}, {18}, {19})", elem["obj"]["ID"].AsValue(),
								elem["obj"]["Document"]["ID"].AsString(),
								elem["obj"]["DocumentSeries"]["ID"].AsValue(),
								elem["obj"]["TimeSlicePeriodEndDate"].AsString(),
								elem["obj"]["ReportingPeriodEndDate"].AsString(),
								elem["obj"]["FiscalDistance"].AsValue(),
								elem["obj"]["Duration"].AsValue(),
								elem["obj"]["PeriodType"].AsString(),
								elem["obj"]["AcquisitionFlag"].AsString().Length > 0 ? elem["obj"]["AcquisitionFlag"].AsString() : elem["obj"]["AcquisitionFlag"].AsValue(),
								elem["obj"]["AccountingStandard"].AsString(),
								elem["obj"]["ConsolidatedFlag"].AsString(),
								elem["obj"]["IsProForma"].AsBoolean(),
								elem["obj"]["IsRecap"].AsBoolean(),
								elem["obj"]["CompanyFiscalYear"].AsValue(),
								elem["obj"]["ReportStatus"].AsString(),
								elem["obj"]["IsAmended"].AsBoolean(),
								elem["obj"]["IsRestated"].AsBoolean(),
								elem["obj"]["IsAutoCalc"].AsBoolean(),
								elem["obj"]["ManualOrgSet"].AsBoolean(),
								elem["obj"]["TableTypeID"].AsValue()
								));
								is_merging = true;
							} else {
								merging_ids.AppendLine(string.Format(",({0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}, {11}, {12}, {13}, {14}, {15}, {16}, {17}, {18}, {19})", elem["obj"]["ID"].AsValue(),
								elem["obj"]["Document"]["ID"].AsString(),
								elem["obj"]["DocumentSeries"]["ID"].AsValue(),
								elem["obj"]["TimeSlicePeriodEndDate"].AsString(),
								elem["obj"]["ReportingPeriodEndDate"].AsString(),
								elem["obj"]["FiscalDistance"].AsValue(),
								elem["obj"]["Duration"].AsValue(),
								elem["obj"]["PeriodType"].AsString(),
								elem["obj"]["AcquisitionFlag"].AsString().Length > 0 ? elem["obj"]["AcquisitionFlag"].AsString() : elem["obj"]["AcquisitionFlag"].AsValue(),
								elem["obj"]["AccountingStandard"].AsString(),
								elem["obj"]["ConsolidatedFlag"].AsString(),
								elem["obj"]["IsProForma"].AsBoolean(),
								elem["obj"]["IsRecap"].AsBoolean(),
								elem["obj"]["CompanyFiscalYear"].AsValue(),
								elem["obj"]["ReportStatus"].AsString(),
								elem["obj"]["IsAmended"].AsBoolean(),
								elem["obj"]["IsRestated"].AsBoolean(),
								elem["obj"]["IsAutoCalc"].AsBoolean(),
								elem["obj"]["ManualOrgSet"].AsBoolean(),
								elem["obj"]["TableTypeID"].AsValue()
								));
							}
						}
					} catch (System.Exception ex) {
						merging_ids.AppendLine(@"/*" + ex.Message + elem["action"].ToString() + @"*/");
					}
				}

				string result = "";
				if (is_merging) {
					result += string.Format(merge_sql, merging_ids.ToString());
				}
				return result;
			}
		}

		public class JsonToSQLStaticHierarchy : JsonToSQL {
			string delete_sql = @"
DELETE FROM dbo.StaticHierarchy where id in ({0});
";
			string merge_sql = @"MERGE dbo.StaticHierarchy
USING ( select {0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}, {11} ,ISNULL(max(AdjustedOrder), -1)  from dbo.StaticHierarchy where tabletypeID = {3} ) as src (Id,CompanyFinancialTermId,AdjustedOrder,TableTypeId
,Description,HierarchyTypeId,SeperatorFlag,StaticHierarchyMetaId,UnitTypeId,IsIncomePositive,ChildrenExpandDown,ParentID,MaxAdjustedOrder)
ON dbo.StaticHierarchy.Id = src.Id
WHEN MATCHED THEN
	UPDATE SET CompanyFinancialTermId = src.CompanyFinancialTermId
      ,AdjustedOrder = src.AdjustedOrder
      ,TableTypeId = src.TableTypeId
      ,Description = src.Description
      ,HierarchyTypeId = src.HierarchyTypeId
      ,SeperatorFlag = src.SeperatorFlag
      ,StaticHierarchyMetaId = src.StaticHierarchyMetaId
      ,UnitTypeId = src.UnitTypeId
      ,IsIncomePositive = src.IsIncomePositive
      ,ChildrenExpandDown = src.ChildrenExpandDown
      ,ParentID = src.ParentID
WHEN NOT MATCHED THEN
	INSERT (CompanyFinancialTermId,AdjustedOrder,TableTypeId
,Description,HierarchyTypeId,SeperatorFlag,StaticHierarchyMetaId,UnitTypeId,IsIncomePositive,ChildrenExpandDown) VALUES
	  (
src.CompanyFinancialTermId 
,CASE src.AdjustedOrder WHEN -1 THEN -101 ELSE src.MaxAdjustedOrder+1 END
,src.TableTypeId 
,src.Description 
,src.HierarchyTypeId 
,src.SeperatorFlag 
,src.StaticHierarchyMetaId 
,src.UnitTypeId 
,src.IsIncomePositive 
,src.ChildrenExpandDown 
	  )
OUTPUT $action, 'StaticHierarchy', inserted.Id, inserted.AdjustedOrder INTO @ChangeResult;

";
			string cleanup_sql = @"
exec prcUpd_FFDocHist_UpdateStaticHierarchy_Cleanup {0};
";
			private JArray _jarray;
			public JsonToSQLStaticHierarchy(JToken jToken)
				: base("") {
				if (jToken == null) {
					_jarray = null;
				} else {
					_jarray = (JArray)jToken.SelectToken("");
				}
			}
			public override string Translate() {
				if (_jarray == null) return "";
				System.Text.StringBuilder sb = new System.Text.StringBuilder();
				List<string> deleted_ids = new List<string>();
				string tableTypeId = null;
				foreach (var elem in _jarray) {
					try {
						if (elem["action"].ToString() == "delete") {
							deleted_ids.Add(elem["obj"]["ID"].AsValue());
						} else if (elem["action"].ToString() == "update") {
							sb.AppendLine(string.Format(merge_sql, elem["obj"]["ID"].AsValue(),
								elem["obj"]["CompanyFinancialTerm"]["ID"].AsValue(),
								elem["obj"]["AdjustedOrder"].AsValue(),
								elem["obj"]["TableType"]["ID"].AsValue(),
								elem["obj"]["Description"].AsString(),
								elem["obj"]["Hierarchy"].AsString(),
								elem["obj"]["SeperatorFlag"].AsBoolean(),
								elem["obj"]["StaticHierarchyMetaId"].AsValue(),
								elem["obj"]["UnitTypeId"].AsValue(),
								elem["obj"]["IsIncomePositive"].AsBoolean(),
								elem["obj"]["ChildrenExpandDown"].AsBoolean(),
								elem["obj"]["ParentID"].AsValue()
								));
							if (string.IsNullOrEmpty(tableTypeId)) {
								tableTypeId = elem["obj"]["TableType"]["ID"].AsValue();
							}
						} else if (elem["action"].ToString() == "insert") {
							sb.AppendLine(string.Format(merge_sql, 0,
								elem["obj"]["CompanyFinancialTerm"]["ID"].AsValue(),
								elem["obj"]["AdjustedOrder"].AsValue(),
								elem["obj"]["TableType"]["ID"].AsValue(),
								elem["obj"]["Description"].AsString(),
								elem["obj"]["Hierarchy"].AsString(),
								elem["obj"]["SeperatorFlag"].AsBoolean(),
								elem["obj"]["StaticHierarchyMetaId"].AsValue(),
								elem["obj"]["UnitTypeId"].AsValue(),
								elem["obj"]["IsIncomePositive"].AsBoolean(),
								elem["obj"]["ChildrenExpandDown"].AsBoolean(),
								"NULL"
								));
							if (string.IsNullOrEmpty(tableTypeId)) {
								tableTypeId = elem["obj"]["TableType"]["ID"].AsValue();
							}
						}
					} catch (System.Exception ex) {
						sb.AppendLine(@"/*" + ex.Message + elem["action"].ToString() + @"*/");
					}
				}
				if (!string.IsNullOrEmpty(tableTypeId)) {
					sb.AppendLine(string.Format(cleanup_sql, tableTypeId));
				}
				string result = "";
				if (deleted_ids.Count > 0) {
					result = string.Format(delete_sql, string.Join(",", deleted_ids)) + sb.ToString();
					SendEmail("DataRoost Bulk StaticHierarchy Delete - JsonToSQLStaticHierarchy", result);
				} else {
					result = sb.ToString();
				}
				return result;
			}

			public string TranslateInsert() {
				if (_jarray == null) return "";
				System.Text.StringBuilder sb = new System.Text.StringBuilder();

				foreach (var elem in _jarray) {
					try {
						if (elem["action"].ToString() == "insert" || elem["action"].ToString() == null) {
							sb.AppendLine(string.Format(merge_sql, elem["obj"]["ID"].AsValue(),
								elem["obj"]["CompanyFinancialTerm"]["ID"].AsValue(),
								elem["obj"]["AdjustedOrder"].AsValue(),
								elem["obj"]["TableType"]["ID"].AsValue(),
								elem["obj"]["Description"].AsString(),
								elem["obj"]["HierarchyTypeId"].AsString(),
								elem["obj"]["SeperatorFlag"].AsBoolean(),
								elem["obj"]["StaticHierarchyMetaId"].AsValue(),
								elem["obj"]["UnitTypeId"].AsValue(),
								elem["obj"]["IsIncomePositive"].AsBoolean(),
								elem["obj"]["ChildrenExpandDown"].AsBoolean(),
								elem["obj"]["ParentID"].AsValue()
	));
						}
					} catch (System.Exception ex) {
						sb.AppendLine(@"/*" + ex.Message + elem["action"].ToString() + @"*/");
					}
				}

				return sb.ToString(); ;
			}
		}

		public class JsonToSQLDocumentTimeSliceTableCell : JsonToSQL {
			string delete_sql = @"
DELETE FROM DocumentTimeSliceTableCell where TableCellId in ({0});
";
			string merge_sql = @"MERGE DocumentTimeSliceTableCell
USING ({0}) as src (DocumentTimeSliceId,TableCellId)
ON DocumentTimeSliceTableCell.TableCellId = src.TableCellId
WHEN MATCHED THEN
	UPDATE SET DocumentTimeSliceId = src.DocumentTimeSliceId
WHEN NOT MATCHED THEN
	INSERT (DocumentTimeSliceId
,TableCellId) VALUES
	  (
src.DocumentTimeSliceId 
,src.TableCellId 
	  )
OUTPUT $action, 'DocumentTimeSliceTableCell', inserted.TableCellId,0 INTO @ChangeResult;

";
			private JArray _jarray;
			public JsonToSQLDocumentTimeSliceTableCell(JToken jToken)
				: base("") {
				if (jToken == null) {
					_jarray = null;
				} else {
					_jarray = (JArray)jToken.SelectToken("");
				}
			}
			public override string Translate() {
				if (_jarray == null) return "";
				System.Text.StringBuilder merging_ids = new System.Text.StringBuilder();
				System.Text.StringBuilder deleting_ids = new System.Text.StringBuilder();
				bool is_deleting = false;
				bool is_merging = false;
				foreach (var elem in _jarray) {
					try {
						if (elem["action"].ToString() == "delete") {
							if (!is_deleting) {
								deleting_ids.Append(string.Format("{0}", elem["obj"]["TableCell"]["ID"].AsValue()));
								is_deleting = true;
							} else {
								deleting_ids.Append(string.Format(",{0}", elem["obj"]["TableCell"]["ID"].AsValue()));
							}
						} else if (elem["action"].ToString() == "update") {
							if (!is_merging) {
								merging_ids.AppendLine(string.Format("VALUES ({0}, {1})", elem["obj"]["DocumentTimeSlice"]["ID"].AsValue(),
								elem["obj"]["TableCell"]["ID"].AsValue()
								));
								is_merging = true;
							} else {
								merging_ids.AppendLine(string.Format(",({0}, {1})", elem["obj"]["DocumentTimeSlice"]["ID"].AsValue(),
								elem["obj"]["TableCell"]["ID"].AsValue()
								));
							}
						} else if (elem["action"].ToString() == "insert") {
							if (!is_merging) {
								merging_ids.AppendLine(string.Format("VALUES ({0}, {1})", elem["obj"]["DocumentTimeSlice"]["ID"].AsValue(),
								elem["obj"]["TableCell"]["ID"].AsValue()
								));
								is_merging = true;
							} else {
								merging_ids.AppendLine(string.Format(",({0}, {1})", elem["obj"]["DocumentTimeSlice"]["ID"].AsValue(),
								elem["obj"]["TableCell"]["ID"].AsValue()
								));
							}
						}
					} catch (System.Exception ex) {
						merging_ids.AppendLine(@"/*" + ex.Message + elem["action"].ToString() + @"*/");
					}
				}
				string result = "";
				if (is_deleting) {
					result += string.Format(delete_sql, deleting_ids.ToString());
				}
				if (is_merging) {
					result += string.Format(merge_sql, merging_ids.ToString());
				}
				return result;
			}

		}

		public class JsonToSQLDocumentTable : JsonToSQL {
			string delete_sql = @"
DELETE FROM DocumentTable where ID in ({0});
";
			string merge_sql = @"MERGE DocumentTable
USING ({0}) as src (ID, DocumentID,TableOrganizationID,TableTypeID,Consolidated,Unit,ScalingFactorID,TableIntID,ExceptShare)
ON DocumentTable.ID = src.ID
WHEN MATCHED THEN
 
	UPDATE SET 
				  DocumentID = DocumentTable.DocumentID
					,TableOrganizationID = src.TableOrganizationID
					,TableTypeID = DocumentTable.TableTypeID
					,Consolidated = src.Consolidated
					,Unit = src.Unit
					,ScalingFactorID = src.ScalingFactorID
					,TableIntID = src.TableIntID
					,ExceptShare = src.ExceptShare
WHEN NOT MATCHED THEN
	INSERT ( DocumentID
      ,TableOrganizationID
      ,TableTypeID
      ,Consolidated
      ,Unit
      ,ScalingFactorID
      ,TableIntID
      ,ExceptShare) VALUES
	  (
			src.DocumentID
		,src.TableOrganizationID
		,src.TableTypeID
		,src.Consolidated
		,src.Unit
		,src.ScalingFactorID
		,src.TableIntID
		,src.ExceptShare
	  )
OUTPUT $action, 'DocumentTable', inserted.Id,0 INTO @ChangeResult;
 

";
			private JArray _jarray;
			public JsonToSQLDocumentTable(JToken jToken)
				: base("") {
				if (jToken == null) {
					_jarray = null;
				} else {
					_jarray = (JArray)jToken.SelectToken("");
				}
			}
			public override string Translate() {
				if (_jarray == null) return "";
				System.Text.StringBuilder merging_ids = new System.Text.StringBuilder();
				System.Text.StringBuilder deleting_ids = new System.Text.StringBuilder();
				bool is_deleting = false;
				bool is_merging = false;
				foreach (var elem in _jarray) {
					try {
						if (elem["action"].ToString() == "delete") {
							if (!is_deleting) {
								deleting_ids.Append(string.Format("{0}", elem["obj"]["ID"].AsValue()));
								is_deleting = true;
							} else {
								deleting_ids.Append(string.Format(",{0}", elem["obj"]["ID"].AsValue()));
							}
						} else if (elem["action"].ToString() == "update") {
							if (!is_merging) {
								merging_ids.AppendLine(string.Format("VALUES ({0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8})", elem["obj"]["ID"].AsValue(),
								"'00000000-0000-0000-0000-000000000000'",
								elem["obj"]["TableOrganizationID"].AsValue(),
								"0",
								elem["obj"]["Consolidated"].AsBoolean(),
								elem["obj"]["Unit"].AsString(),
								elem["obj"]["ScalingFactorID"].AsString(),
								elem["obj"]["TableIntID"].AsValue(),
								elem["obj"]["ExceptShare"].AsBoolean()
								));
								is_merging = true;
							} else {
								merging_ids.AppendLine(string.Format(",({0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8})", elem["obj"]["ID"].AsValue(),
								"'00000000-0000-0000-0000-000000000000'",
								elem["obj"]["TableOrganizationID"].AsValue(),
								"0",
								elem["obj"]["Consolidated"].AsBoolean(),
								elem["obj"]["Unit"].AsString(),
								elem["obj"]["ScalingFactorID"].AsString(),
								elem["obj"]["TableIntID"].AsValue(),
								elem["obj"]["ExceptShare"].AsBoolean()
								));
							}
						} else if (elem["action"].ToString() == "insert") {
							if (!is_merging) {
								merging_ids.AppendLine(string.Format("VALUES ({0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8})", "-1",
								elem["obj"]["TableOrganizationID"].AsValue(),
								"0", // ?????
								elem["obj"]["Consolidated"].AsBoolean(),
								elem["obj"]["Unit"].AsString(),
								elem["obj"]["ScalingFactorID"].AsString(),
								elem["obj"]["TableIntID"].AsValue(),
								elem["obj"]["ExceptShare"].AsBoolean()
								));
								is_merging = true;
							} else {
								merging_ids.AppendLine(string.Format(",({0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8})", "-1",
								elem["obj"]["TableOrganizationID"].AsValue(),
								"0", // ??????
								elem["obj"]["Consolidated"].AsBoolean(),
								elem["obj"]["Unit"].AsString(),
								elem["obj"]["ScalingFactorID"].AsString(),
								elem["obj"]["TableIntID"].AsValue(),
								elem["obj"]["ExceptShare"].AsBoolean()
								));
							}
						}
					} catch (System.Exception ex) {
						merging_ids.AppendLine(@"/*" + ex.Message + elem["action"].ToString() + @"*/");
					}
				}
				string result = "";
				if (is_deleting) {
					result += string.Format(delete_sql, deleting_ids.ToString());
				}
				if (is_merging) {
					result += string.Format(merge_sql, merging_ids.ToString());
				}
				return result;
			}

		}

		public ScarResult UpdateTDPByDocumentTableID(string dtid, string updateInJson) {
			updateInJson = updateInJson.Replace("\\", "\\\\").Replace("&quotx;", "\\\"");
			ScarResult result = new ScarResult();
			result.ReturnValue["DebugMessage"] = "";
			System.Text.StringBuilder sb = new System.Text.StringBuilder();
			sb.AppendLine("BEGIN TRAN");
			sb.AppendLine("DECLARE @ChangeResult TABLE (ChangeType VARCHAR(10), TableType varchar(50), Id INTEGER, Info INTEGER)");

			try {
				JObject json = JObject.Parse(updateInJson);
				string unquotedJson = updateInJson.Replace("\"", "").Replace("'", "");
				int totalUpdates = new Regex(Regex.Escape("action: update")).Matches(unquotedJson).Count;
				int totalInsert = new Regex(Regex.Escape("action: insert")).Matches(unquotedJson).Count;
				var cft = json["CompanyFinancialTerm"];
				var shs = json["StaticHierarchy"];
				var tabledimension = json["TableDimension"];
				var tablecell = json["TableCell"];
				var documentTable = json["DocumentTable"];
				var dimensionToCel = json["DimensionToCell"];
				var documentTimeSlice = json["DocumentTimeSlice"];
				var documentTimeSliceTableCell = json["DocumentTimeSliceTableCell"];
				sb.AppendLine(new JsonToSQLCompanyFinancialTerm(cft).Translate());
				sb.AppendLine(new JsonToSQLStaticHierarchy(shs).Translate());
				sb.AppendLine(new JsonToSQLTableDimension(dtid, tabledimension).Translate());
				sb.AppendLine(new JsonToSQLTableCell(tablecell).Translate());
				sb.AppendLine(new JsonToSQLDimensionToCell(dtid, dimensionToCel).Translate());
				sb.AppendLine(new JsonToSQLDocumentTimeSlice(documentTimeSlice).Translate());
				sb.AppendLine(new JsonToSQLDocumentTimeSliceTableCell(documentTimeSliceTableCell).Translate());
				sb.AppendLine(new JsonToSQLDocumentTable(documentTable).Translate());
				sb.AppendLine("select * from @ChangeResult; DECLARE @totalInsert int, @totalUpdate int; ");
				sb.AppendLine("select @totalInsert = count(*) from @ChangeResult where ChangeType = 'INSERT';");
				sb.AppendLine("select @totalUpdate = count(*) from @ChangeResult where ChangeType = 'UPDATE'; ");
				sb.AppendLine();
				sb.AppendLine(string.Format("IF (@totalInsert + @totalUpdate = ({0} + {1})) BEGIN select 'commit'; COMMIT TRAN END ELSE BEGIN select 'rollback'; ROLLBACK TRAN END", totalInsert, totalUpdates));
				result.ReturnValue["DebugMessage"] += sb.ToString();

				//				return result;

				using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {
					using (SqlCommand cmd = new SqlCommand(sb.ToString(), conn)) {
						cmd.CommandTimeout = 0;
						conn.Open();
						using (SqlDataReader reader = cmd.ExecuteReader()) {
							List<object> aList = new List<object>();

							while (reader.Read()) {
								var changeType = reader.GetStringSafe(0);
								var tableType = reader.GetStringSafe(1);
								var Id = reader.GetInt32(2);
								var Info = -1;
								try {
									Info = reader.GetInt32(3);
								} catch (Exception ex) {

								}

								var returnStatus2 = new { returnDetails = "", isError = false, mainId = Guid.Empty, eventId = default(Guid) };
								aList.Add(new { ChangeType = changeType, TableType = tableType, Id = Id, Info = Info });
							}
							if (reader.NextResult() && reader.Read()) {
								if (reader.GetStringSafe(0) == "commit") {
									result.ReturnValue["Success"] = "T";
								} else {
									result.ReturnValue["Success"] = "F";
								}
							}
							result.ReturnValue["Message"] = Newtonsoft.Json.JsonConvert.SerializeObject(aList, Newtonsoft.Json.Formatting.Indented);
						}
					}
				}
			} catch (Exception ex) {
				result.ReturnValue["DebugMessage"] += ex.Message;

			}
			return result;
		}

		public ScarResult DeleteRowColumnTDPByDocumentTableID(string dtid, string updateInJson) {
			updateInJson = updateInJson.Replace("&quot;", "\\\"");
			ScarResult result = new ScarResult();
			result.ReturnValue["DebugMessage"] = "";
			System.Text.StringBuilder sb = new System.Text.StringBuilder();
			sb.AppendLine("BEGIN TRAN");
			sb.AppendLine("DECLARE @ChangeResult TABLE (ChangeType VARCHAR(10), TableType varchar(50), Id INTEGER)");

			try {
				JObject json = JObject.Parse(updateInJson);
				string unquotedJson = updateInJson.Replace("\"", "").Replace("'", "");
				int totalUpdates = new Regex(Regex.Escape("action: update")).Matches(unquotedJson).Count;
				int totalInsert = new Regex(Regex.Escape("action: insert")).Matches(unquotedJson).Count;
				var tabledimension = json["TableDimension"];
				var tablecell = json["TableCell"];
				var dimensionToCel = json["DimensionToCell"];
				var documentTimeSliceTableCell = json["DocumentTimeSliceTableCell"];
				sb.AppendLine(new JsonToSQLDimensionToCell(dtid, dimensionToCel).Translate());
				sb.AppendLine(new JsonToSQLDocumentTimeSliceTableCell(documentTimeSliceTableCell).Translate());
				sb.AppendLine(new JsonToSQLTableDimension(dtid, tabledimension).Translate());
				sb.AppendLine(new JsonToSQLTableCell(tablecell).Translate());
				sb.AppendLine("select * from @ChangeResult; DECLARE @totalInsert int, @totalUpdate int; ");
				sb.AppendLine("select @totalInsert = count(*) from @ChangeResult where ChangeType = 'INSERT';");
				sb.AppendLine("select @totalUpdate = count(*) from @ChangeResult where ChangeType = 'UPDATE'; ");
				sb.AppendLine();
				sb.AppendLine(string.Format("IF (@totalInsert = {0} and @totalUpdate = {1}) BEGIN select 'commit'; COMMIT TRAN END ELSE BEGIN select 'rollback'; ROLLBACK TRAN END", totalInsert, totalUpdates));
				result.ReturnValue["DebugMessage"] += sb.ToString();

				//				return result;

				using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {
					using (SqlCommand cmd = new SqlCommand(sb.ToString(), conn)) {
						cmd.CommandTimeout = 0;
						conn.Open();
						using (SqlDataReader reader = cmd.ExecuteReader()) {
							List<object> aList = new List<object>();

							while (reader.Read()) {
								var changeType = reader.GetStringSafe(0);
								var tableType = reader.GetStringSafe(1);
								var Id = reader.GetInt32(2);
								var returnStatus2 = new { returnDetails = "", isError = false, mainId = Guid.Empty, eventId = default(Guid) };
								aList.Add(new { ChangeType = changeType, TableType = tableType, Id = Id });
							}
							if (reader.NextResult() && reader.Read()) {
								if (reader.GetStringSafe(0) == "commit") {
									result.ReturnValue["Success"] = "T";
								} else {
									result.ReturnValue["Success"] = "F";
								}
							}
							result.ReturnValue["Message"] = Newtonsoft.Json.JsonConvert.SerializeObject(aList, Newtonsoft.Json.Formatting.Indented);
						}
					}
				}
			} catch (Exception ex) {
				result.ReturnValue["DebugMessage"] += ex.Message;

			}
			return result;
		}

		public ScarResult DeleteDocumentTableID(string dtid) {
			string SQL_Delete = @"
BEGIN TRY
	BEGIN TRAN
			delete from DimensionToCell where TableDimensionID in
			(select id from TableDimension where DocumentTableID = @id);
			delete from tabledimension where DocumentTableID = @id;
			delete from documenttable where id = @id;
		COMMIT 
		select 1
END TRY
BEGIN CATCH
	ROLLBACK;
  select 0;
END CATCH
";
			ScarResult result = new ScarResult();
			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {

				using (SqlCommand cmd = new SqlCommand(SQL_Delete, conn)) {
					conn.Open();
					cmd.Parameters.AddWithValue("@id", dtid);
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
from [DimensionToCell] dtc  WITH (NOLOCK)
JOIN [TableDimension] td WITH (NOLOCK) on dtc.TableDimensionID = td.ID and td.DimensionTypeID = 2
JOIN [DimensionToCell] dtc2 WITH (NOLOCK) on td.ID = dtc2.TableDimensionID
JOIN [TableCell] tc WITH (NOLOCK) on tc.id = dtc2.TableCellID
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

		public ScarResult CreateTimeSlice(string updateInJson) {
			ScarResult result = new ScarResult();
			result.ReturnValue["DebugMessage"] = "";
			System.Text.StringBuilder sb = new System.Text.StringBuilder();
			sb.AppendLine("BEGIN TRAN");
			sb.AppendLine("DECLARE @ChangeResult TABLE (ChangeType VARCHAR(10), TableType varchar(50), Id INTEGER, Info INTEGER)");

			try {
				JObject json = JObject.Parse(updateInJson);
				string unquotedJson = updateInJson.Replace("\"", "").Replace("'", "");
				int totalUpdates = new Regex(Regex.Escape("action: update")).Matches(unquotedJson).Count;
				int totalInsert = new Regex(Regex.Escape("action: insert")).Matches(unquotedJson).Count;
				var documentTimeSlice = json["DocumentTimeSlice"];
				sb.AppendLine(new JsonToSQLDocumentTimeSlice(documentTimeSlice).TranslateInsert());
				sb.AppendLine("select * from @ChangeResult; DECLARE @totalInsert int, @totalUpdate int; ");
				sb.AppendLine("select @totalInsert = count(*) from @ChangeResult where ChangeType = 'INSERT';");
				sb.AppendLine("select @totalUpdate = count(*) from @ChangeResult where ChangeType = 'UPDATE'; ");
				sb.AppendLine();
				sb.AppendLine(string.Format("IF (@totalInsert = {0} and @totalUpdate = {1}) BEGIN select 'commit'; COMMIT TRAN END ELSE BEGIN select 'rollback'; ROLLBACK TRAN END", totalInsert, totalUpdates));
				result.ReturnValue["DebugMessage"] += sb.ToString();

				//				return result;
				String cmd1 = sb.ToString();
				Console.WriteLine(cmd1);
				using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {
					using (SqlCommand cmd = new SqlCommand(sb.ToString(), conn)) {
						conn.Open();
						using (SqlDataReader reader = cmd.ExecuteReader()) {
							List<object> aList = new List<object>();

							while (reader.Read()) {
								var changeType = reader.GetStringSafe(0);
								var tableType = reader.GetStringSafe(1);
								var Id = reader.GetInt32(2);
								var returnStatus2 = new { returnDetails = "", isError = false, mainId = Guid.Empty, eventId = default(Guid) };
								aList.Add(new { ChangeType = changeType, TableType = tableType, Id = Id, Info = -1 });
							}
							if (reader.NextResult() && reader.Read()) {
								if (reader.GetStringSafe(0) == "commit") {
									result.ReturnValue["Success"] = "T";
								} else {
									result.ReturnValue["Success"] = "F";
								}
							}
							result.ReturnValue["Message"] = Newtonsoft.Json.JsonConvert.SerializeObject(aList, Newtonsoft.Json.Formatting.Indented);
						}
					}
				}
			} catch (Exception ex) {
				result.ReturnValue["DebugMessage"] += ex.Message;

			}
			return result;
		}

		public ScarResult CloneUpdateTimeSlice(int id, string InterimType) {

			string query = @"
DECLARE @newId int;
DECLARE @DocId uniqueidentifier = (SELECT DocumentId FROM dbo.DocumentTimeSlice WITH (NOLOCK) where id = @id)
DECLARE @TimeSlicePeriodEndDate datetime = (SELECT TimeSlicePeriodEndDate WITH (NOLOCK) FROM dbo.DocumentTimeSlice where id = @id)
DECLARE @oldPeriodType varchar(10) = (SELECT PeriodType FROM dbo.DocumentTimeSlice WITH (NOLOCK) where id = @id)
DECLARE @dts int = (SELECT top 1 ID FROM dbo.DocumentTimeSlice dts WITH (NOLOCK) where dts.PeriodType = @newPeriodType AND dts.DocumentId = @DocId and dts.TimeSlicePeriodEndDate = @TimeSlicePeriodEndDate);

if @newPeriodType <> @oldPeriodType
BEGIN
	IF (@newPeriodType <> '--')
	BEGIN
		IF (@dts IS NULL)
		BEGIN
			IF (EXISTS ( SELECT TOP 1 id FROM [InterimType] WITH (NOLOCK) WHERE ID = @newPeriodType and Duration is not null))  
			BEGIN
				INSERT dbo.DocumentTimeSlice
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
							  ,[ManualOrgSet]
                ,[TableTypeID])

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
                ,[TableTypeID]  
						  FROM dbo.DocumentTimeSlice where id = @id;

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
			UPDATE dbo.DocumentTimeSlice SET  PeriodType = @newPeriodType
			WHERE Id = @id;
			SET @newId = @Id
		END
	END
	ELSE -- if new periodtype is --
	BEGIN
		DELETE FROM [DocumentTimeSliceTableCell]  WHERE DocumentTimeSliceId = @id; 
		DELETE FROM dbo.DocumentTimeSlice where id = @Id
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
  ,[TableTypeID] 
FROM dbo.DocumentTimeSlice WITH (NOLOCK) where id = @newId or id = @dts or id = @id;

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
								ManualOrgSet = reader.GetBoolean(18),
								TableTypeID = reader.GetInt32(19),
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
from DocumentSeries ds WITH (NOLOCK)
join CompanyFinancialTerm cft WITH (NOLOCK) on ds.id = cft.documentseriesid
join statichierarchy sh WITH (NOLOCK) on sh.companyfinancialtermid = cft.id
join TableType tt WITH (NOLOCK) on sh.tabletypeid = tt.id
join TableCell tc WITH (NOLOCK) on tc.CompanyFinancialTermID = sh.CompanyFinancialTermId
join DocumentTimeSliceTableCell dtstc WITH (NOLOCK) on tc.id = dtstc.tablecellid
join dbo.documenttimeslice dts WITH (NOLOCK) on dts.id = dtstc.documenttimesliceid and dts.DocumentSeriesId = ds.ID
join Document d WITH (NOLOCK) on dts.documentid = d.id
join MTMWErrorTypeTableCell mtmtc WITH (NOLOCK) on tc.id = mtmtc.tablecellid
join MTMWErrorType mtm WITH (NOLOCK) on mtmtc.MTMWErrorTypeId = mtm.ID
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

		public ScarResult GetTimeSliceByTemplate(string companyId, string TemplateName, Guid DocumentId) {

			string SQL_query = @"
DECLARE @DocumentSeriesId int;
select top 1 @DocumentSeriesId = d.DocumentSeriesID
	FROM Document d WITH (NOLOCK)
join DocumentSeries ds with(nolock)
  on d.DocumentSeriesID = ds.ID
	where d.DAMDocumentId = @DocumentID
  and ds.companyId = @companyId;

;WITH cte_timeslice(DamDocumentID, TimeSliceId, NumberofCell, CurrencyCount, CurrencyCode, ArComponent, PeriodLength)
AS
(
	SELECT distinct   d.damdocumentid, dts.id, count(distinct tc.id), count(distinct tc.CurrencyCode), max(tc.CurrencyCode), count(artsdc.DocumentTimeSliceID), max(tc.PeriodLength)
		FROM DocumentSeries ds WITH (NOLOCK)
		JOIN dbo.DocumentTimeSlice dts WITH (NOLOCK) on ds.ID = Dts.DocumentSeriesId
		JOIN Document d WITH (NOLOCK) on dts.DocumentId = d.ID
		JOIN DocumentTimeSliceTableCell dtstc WITH (NOLOCK) on dts.ID = dtstc.DocumentTimeSliceID
		JOIN TableCell tc WITH (NOLOCK) on dtstc.TableCellID = tc.ID
		JOIN DimensionToCell dtc WITH (NOLOCK) on tc.ID = dtc.TableCellID -- check that is in a table
		JOIN StaticHierarchy sh WITH (NOLOCK) on tc.CompanyFinancialTermID = sh.CompanyFinancialTermID
		JOIN TableType tt WITH (NOLOCK) on tt.ID = sh.TableTypeID  
		LEFT JOIN ARTimeSliceDerivationComponents artsdc WITH(NOLOCK) ON artsdc.DocumentTimeSliceID = dts.id
	WHERE tt.description = @TypeTable
	and ds.id = @DocumentSeriesId and tt.DocumentSeriesID = @DocumentSeriesId
	group by d.damdocumentid, dts.id 
)
SELECT ts.*, dts.*, d.DocumentDate, d.ReportTypeID, d.PublicationDateTime
	INTO #nonempty
	FROM cte_timeslice ts WITH (NOLOCK)
	JOIN dbo.DocumentTimeSlice dts WITH(NOLOCK) on ts.TimeSliceId = dts.Id
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
  d.documentseriesid =  @DocumentSeriesId and 
(d.DAMDocumentId = @DocumentID or d.ArdExportFlag = 1 or d.IsDocSetUpCompleted = 1 or d.ExportFlag = 1) 
  
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
	,ISNULL(n.TableTypeID, -1) as TableTypeID
	,ISNULL(n.IsAutoCalc, 0) as IsAutoCalc
  ,a.PeriodLength
INTO #tmptimeslices
from #alltimeslices  a
JOIN Document d WITH(NOLOCK) on a.DAMDocumentId  = d.DAMDocumentId
LEFT JOIN #nonempty n on a.DamDocumentID = n.DamDocumentID and n.TimeSlicePeriodEndDate = a.CellDate and a.PeriodLength = n.PeriodLength
 order by a.CellDate desc

select distinct ts.*
from #tmptimeslices ts 
";

			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {
				StaticHierarchy sh;
				ScarResult response = new ScarResult();
				response.TimeSlices = new List<TimeSlice>();

				using (SqlCommand cmd = new SqlCommand(SQL_query, conn)) {
					conn.Open();
					cmd.Parameters.AddWithValue("@DocumentID", DocumentId);
					cmd.Parameters.AddWithValue("@TypeTable", TemplateName);
					cmd.Parameters.AddWithValue("@companyId", companyId);
					
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
							ArComponent = reader.GetOrdinal("ArComponent"),
							TableTypeID = reader.GetOrdinal("TableTypeID")
						};
						while (reader.Read()) {
							TimeSlice slice = new TimeSlice();
							slice.DocumentId = reader.GetGuid(ordinals.DocumentId);
							slice.DamDocumentId = reader.GetGuid(ordinals.DamDocumentId);
							slice.Id = reader.GetInt32(ordinals.TimeSliceId);
							if (!reader.IsDBNull(ordinals.PeriodEndDate))
								slice.TimeSlicePeriodEndDate = reader.GetDateTime(ordinals.PeriodEndDate);

							if (slice.Id > 0) {
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
							slice.TableTypeID = reader.GetInt32(ordinals.TableTypeID);
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

DECLARE @CellsForLPV CellList
DECLARE @CellsForMTMW CellList

INSERT @CellsForLPV
SELECT distinct sh.ID, tc.DocumentTimeSliceID
FROM StaticHierarchy sh WITH (NOLOCK)
JOIN vw_SCARDocumentTimeSliceTableCell tc WITH (NOLOCK) ON sh.CompanyFinancialTermId = tc.CompanyFinancialTermID
WHERE sh.ID = @TargetSH and tc.TableCellid = @cellID

INSERT @CellsForMTMW
SELECT distinct sh.ID, tc.DocumentTimeSliceID
FROM StaticHierarchy sh WITH (NOLOCK)
JOIN vw_SCARDocumentTimeSliceTableCell tc WITH (NOLOCK) ON sh.CompanyFinancialTermId = tc.CompanyFinancialTermID
WHERE sh.ID = @TargetSH and tc.TableCellid = @cellID

DECLARE @ParentCells TABLE(StaticHierarchyID int, DocumentTimeSliceID int, TablecellID int)

;WITH cte_sh(StaticHierarchyID, CompanyFinancialTermID, ParentID, DocumentTimeSliceID, TableCellID, IsRoot, RootStaticHierarchyID, RootDocumentTimeSliceID)
AS
(
       SELECT sh.ID, sh.CompanyFinancialTermID, sh.ParentID, c.DocumentTimeSliceID, tc.TableCellID, 1, c.StaticHierarchyID, c.DocumentTimeSliceID
       FROM @CellsForLPV c
       JOIN StaticHierarchy sh WITH (NOLOCK) ON sh.ID = c.StaticHierarchyID
       LEFT JOIN vw_SCARDocumentTimeSliceTableCell2 tc WITH (NOLOCK) on c.DocumentTimeSliceID = tc.DocumentTimeSliceID AND sh.CompanyFinancialTermID = tc.CompanyFinancialTermID
       UNION ALL
       SELECT ID, sh.CompanyFinancialTermID, sh.ParentID, cte.DocumentTimeSliceID, dtc.TableCellID, 0, cte.RootStaticHierarchyID, cte.RootDocumentTimeSliceID
       FROM cte_sh cte
       JOIN StaticHierarchy sh WITH (NOLOCK) on sh.ID = cte.ParentID
       OUTER APPLY(SELECT dtc.TableCellID FROM vw_SCARDocumentTimeSliceTableCell2 dtc WHERE sh.CompanyFinancialTermID = dtc.CompanyFinancialTermID 
                                  AND dtc.DocumentTimeSliceID = cte.DocumentTimeSliceID)dtc
       WHERE cte.IsRoot = 1 OR (cte.IsRoot = 0 AND cte.TableCellID IS NULL)
)
INSERT @ParentCells
select StaticHierarchyID, DocumentTimeSliceID, TableCellID from  cte_sh where IsRoot = 0


INSERT @CellsForLPV
Select StaticHierarchyID, DocumentTimeSliceID 
FROM @ParentCells
WHERE TableCellID is null and DocumentTimeSliceID NOT IN (Select DocumentTimeSliceID FROM DocumentTimeSliceTableTypeIsSummary WITH (NOLOCK))

INSERT @CellsForLPV
EXEC SCARGetTableCellLikePeriod_GetSibilingTableCells @CellsForLPV, @DocumentID

INSERT @CellsForMTMW
Select StaticHierarchyID, DocumentTimeSliceID 
FROM @ParentCells
WHERE TableCellID is not null

DECLARE @SHCellsMTMW TABLE(StaticHierarchyID int, DocumentTimeSliceID int, ChildrenSum decimal(28,5), CellValue decimal(28,5))
DECLARE @SHCellsLPV TABLE(StaticHierarchyID int, DocumentTimeSliceID int, LPVFail bit)
DECLARE @SHCellsError TABLE(StaticHierarchyID int, DocumentTimeSliceID int, LPVFail bit, MTMWFail bit)

DELETE FROM MTMWErrorTypeTableCell 
WHERE TableCellid in(
select tc.TableCellID from
@CellsForMTMW e 
JOIN StaticHierarchy sh on e.StaticHierarchyid = sh.id
JOIN vw_SCARDocumentTimeSliceTableCell tc ON e.DocumentTimeSliceID = tc.DocumentTimeSliceId AND sh.CompanyFinancialTermID = tc.CompanyFinancialTermID
)


INSERT INTO @SHCellsMTMW
EXEC SCARGetTableCellMTMWCalc @CellsForMTMW

INSERT INTO @SHCellsLPV
EXEC SCARGetTableCellLikePeriod_ByTableCell @CellsForLPV, @DocumentID

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
	JOIN StaticHierarchy sh WITH (NOLOCK) ON cte.SHID = sh.ID
	JOIN StaticHierarchy shp WITH (NOLOCK) ON sh.ParentID = shp.ID
)
SELECT MAX(level)
FROM cte_level
GROUP BY SHRootID

SELECT distinct 'x', ISNULL(tc.TableCellID,0), tc.Offset, tc.CellPeriodType, tc.PeriodTypeID, tc.CellPeriodCount, tc.PeriodLength, tc.CellDay, 
				tc.CellMonth, tc.CellYear, tc.CellDate, tc.Value, ISNULL(tc.CompanyFinancialTermID, sh.CompanyFinancialTermId), ISNULL(tc.ValueNumeric, dbo.GetTableCellDisplayValue(lpv.StaticHierarchyID, lpv.DocumentTimeSliceID)), tc.NormalizedNegativeIndicator, 
				tc.ScalingFactorID, tc.AsReportedScalingFactor, tc.Currency, tc.CurrencyCode, tc.Cusip, tc.ScarUpdated, tc.IsIncomePositive, 
				tc.XBRLTag, 
				tc.DocumentId, tc.Label, sf.Value,
				(select aetc.ARDErrorTypeId from ARDErrorTypeTableCell aetc (nolock) where tc.TableCellId = aetc.TableCellId),
				(select metc.MTMWErrorTypeId from MTMWErrorTypeTableCell metc (nolock) where tc.TableCellId = metc.TableCellId), 
				lpv.LPVFail, lpv.MTMWFail,
				dts.Id, sh.AdjustedOrder, dts.Duration, dts.TimeSlicePeriodEndDate, dts.ReportingPeriodEndDate, d.PublicationDateTime,
				sh.id as 'StaticHierarchyId'
FROM StaticHierarchy sh WITH (NOLOCK)
JOIN dbo.DocumentTimeSlice dts WITH(NOLOCK) ON dts.DocumentSeriesId = @DocumentSeriesId
JOIN Document d WITH (NOLOCK) on dts.DocumentID = d.ID
LEFT JOIN vw_SCARDocumentTimeSliceTableCell tc WITH (NOLOCK) ON tc.CompanyFinancialTermID = sh.CompanyFinancialTermID AND tc.DocumentTimeSliceID = dts.ID
JOIN @SHCellsError lpv ON lpv.StaticHierarchyID = sh.ID AND lpv.DocumentTimeSliceID = dts.ID
LEFT JOIN ScalingFactor sf WITH (NOLOCK) ON tc.ScalingFactorID = sf.ID
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
								if (reader.GetNullable<int>(1).Value > 0) {
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
								} else {
									cell = new SCARAPITableCell
									{
										ID = reader.GetInt32(1),
										CompanyFinancialTermID = reader.GetNullable<int>(12),
										VirtualValueNumeric = reader.GetNullable<decimal>(13),
									};
									cell.ARDErrorTypeId = reader.GetNullable<int>(26);
									cell.MTMWErrorTypeId = reader.GetNullable<int>(27);
									cell.LikePeriodValidationFlag = reader.GetBoolean(28);
									cell.MTMWValidationFlag = reader.GetBoolean(29);
									cell.DocumentTimeSliceID = reader.GetInt32(30);
									adjustedOrder = reader.GetInt32(31);
									cell.StaticHierarchyID = reader.GetInt32(36);
								}
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
	SELECT ID FROM StaticHierarchy WITH (NOLOCK) WHERE ID = @TargetSHID
	UNION ALL
	SELECT sh.Id 
	FROM StaticHierarchy sh WITH (NOLOCK)
	JOIN CTE_Children cte on sh.ParentID = cte.ID
) INSERT @OldStaticHierarchyList ([StaticHierarchyID])
   SELECT ID 
FROM CTE_Children cte
where cte.id <> @TargetSHID
 
DECLARE @CurrentTimeSliceID int 
SELECT @CurrentTimeSliceID =  tc.DocumentTimeSliceID
FROM StaticHierarchy sh WITH (NOLOCK)
JOIN vw_SCARDocumentTimeSliceTableCell tc WITH (NOLOCK) ON sh.CompanyFinancialTermId = tc.CompanyFinancialTermID
WHERE sh.ID = @TargetSHID and tc.TableCellid = @cellID
 

DECLARE @OldSHCells CellList
INSERT @OldSHCells
SELECT distinct tc.TableCellID, tc.DocumentTimeSliceID
FROM StaticHierarchy sh WITH (NOLOCK)
JOIN @OldStaticHierarchyList shl ON sh.id = shl.StaticHierarchyID
JOIN vw_SCARDocumentTimeSliceTableCell tc WITH (NOLOCK) ON sh.CompanyFinancialTermId = tc.CompanyFinancialTermID and tc.DocumentTimeSliceID = @CurrentTimeSliceID
 
UPDATE tc 
set IsIncomePositive = CASE WHEN IsIncomePositive = 1 THEN 0 ELSE 1 END																
FROM 
TableCell tc 
JOIN @OldSHCells osh on tc.id = osh.StaticHierarchyID  --- osh staticHierarchyID is the cellID
 

DECLARE @CellsForLPV CellList
DECLARE @CellsForMTMW CellList

INSERT @CellsForLPV
SELECT distinct sh.ID, @CurrentTimeSliceID
FROM StaticHierarchy sh WITH (NOLOCK)
JOIN @OldStaticHierarchyList shl ON sh.id = shl.StaticHierarchyID
JOIN vw_SCARDocumentTimeSliceTableCell tc WITH (NOLOCK) ON sh.CompanyFinancialTermId = tc.CompanyFinancialTermID

INSERT @CellsForMTMW
SELECT distinct sh.ID, @CurrentTimeSliceID
FROM StaticHierarchy sh WITH (NOLOCK)
JOIN @OldStaticHierarchyList shl ON sh.id = shl.StaticHierarchyID
JOIN vw_SCARDocumentTimeSliceTableCell tc WITH (NOLOCK) ON sh.CompanyFinancialTermId = tc.CompanyFinancialTermID


DECLARE @ParentCells TABLE(StaticHierarchyID int, DocumentTimeSliceID int, TablecellID int)

;WITH cte_sh(StaticHierarchyID, CompanyFinancialTermID, ParentID, DocumentTimeSliceID, TableCellID, IsRoot, RootStaticHierarchyID, RootDocumentTimeSliceID)
AS
(
       SELECT sh.ID, sh.CompanyFinancialTermID, sh.ParentID, c.DocumentTimeSliceID, tc.TableCellID, 1, c.StaticHierarchyID, c.DocumentTimeSliceID
       FROM @CellsForLPV c
       JOIN StaticHierarchy sh WITH (NOLOCK) ON sh.ID = c.StaticHierarchyID
       LEFT JOIN vw_SCARDocumentTimeSliceTableCell2 tc WITH (NOLOCK) on c.DocumentTimeSliceID = tc.DocumentTimeSliceID AND sh.CompanyFinancialTermID = tc.CompanyFinancialTermID
       UNION ALL
       SELECT ID, sh.CompanyFinancialTermID, sh.ParentID, cte.DocumentTimeSliceID, dtc.TableCellID, 0, cte.RootStaticHierarchyID, cte.RootDocumentTimeSliceID
       FROM cte_sh cte
       JOIN StaticHierarchy sh WITH (NOLOCK) on sh.ID = cte.ParentID
       OUTER APPLY(SELECT dtc.TableCellID FROM vw_SCARDocumentTimeSliceTableCell2 dtc WHERE sh.CompanyFinancialTermID = dtc.CompanyFinancialTermID 
                                  AND dtc.DocumentTimeSliceID = cte.DocumentTimeSliceID)dtc
       WHERE cte.IsRoot = 1 OR (cte.IsRoot = 0 AND cte.TableCellID IS NULL)
)
INSERT @ParentCells
select StaticHierarchyID, DocumentTimeSliceID, TableCellID from  cte_sh where IsRoot = 0


INSERT @CellsForLPV
Select StaticHierarchyID, DocumentTimeSliceID 
FROM @ParentCells
WHERE TableCellID is null and DocumentTimeSliceID NOT IN (Select DocumentTimeSliceID FROM DocumentTimeSliceTableTypeIsSummary WITH (NOLOCK))

INSERT @CellsForLPV
EXEC SCARGetTableCellLikePeriod_GetSibilingTableCells @CellsForLPV, @DocumentID

INSERT @CellsForMTMW
Select StaticHierarchyID, DocumentTimeSliceID 
FROM @ParentCells
WHERE TableCellID is not null


DECLARE @SHCellsMTMW TABLE(StaticHierarchyID int, DocumentTimeSliceID int, ChildrenSum decimal(28,5), CellValue decimal(28,5))
DECLARE @SHCellsLPV TABLE(StaticHierarchyID int, DocumentTimeSliceID int, LPVFail bit)
DECLARE @SHCellsError TABLE(StaticHierarchyID int, DocumentTimeSliceID int, LPVFail bit, MTMWFail bit)

DELETE FROM MTMWErrorTypeTableCell 
WHERE TableCellid in(
select tc.TableCellID from
@CellsForMTMW e 
JOIN StaticHierarchy sh on e.StaticHierarchyid = sh.id
JOIN vw_SCARDocumentTimeSliceTableCell tc ON e.DocumentTimeSliceID = tc.DocumentTimeSliceId AND sh.CompanyFinancialTermID = tc.CompanyFinancialTermID
)

INSERT INTO @SHCellsMTMW
EXEC SCARGetTableCellMTMWCalc @CellsForMTMW

INSERT INTO @SHCellsLPV
EXEC SCARGetTableCellLikePeriod_ByTableCell @CellsForLPV, @DocumentID

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
	JOIN StaticHierarchy sh WITH (NOLOCK) ON cte.SHID = sh.ID
	JOIN StaticHierarchy shp WITH (NOLOCK) ON sh.ParentID = shp.ID
)
SELECT MAX(level)
FROM cte_level
GROUP BY SHRootID

SELECT distinct 'x', ISNULL(tc.TableCellID,0), tc.Offset, tc.CellPeriodType, tc.PeriodTypeID, tc.CellPeriodCount, tc.PeriodLength, tc.CellDay, 
				tc.CellMonth, tc.CellYear, tc.CellDate, tc.Value, ISNULL(tc.CompanyFinancialTermID, sh.CompanyFinancialTermId), ISNULL(tc.ValueNumeric, dbo.GetTableCellDisplayValue(lpv.StaticHierarchyID, lpv.DocumentTimeSliceID)), tc.NormalizedNegativeIndicator, 
				tc.ScalingFactorID, tc.AsReportedScalingFactor, tc.Currency, tc.CurrencyCode, tc.Cusip, tc.ScarUpdated, tc.IsIncomePositive, 
				tc.XBRLTag, 
				tc.DocumentId, tc.Label, sf.Value,
				(select aetc.ARDErrorTypeId from ARDErrorTypeTableCell aetc (nolock) where tc.TableCellId = aetc.TableCellId),
				(select metc.MTMWErrorTypeId from MTMWErrorTypeTableCell metc (nolock) where tc.TableCellId = metc.TableCellId), 
				lpv.LPVFail, lpv.MTMWFail,
				dts.Id, sh.AdjustedOrder, dts.Duration, dts.TimeSlicePeriodEndDate, dts.ReportingPeriodEndDate, d.PublicationDateTime,
				sh.id as 'StaticHierarchyId'
FROM StaticHierarchy sh WITH (NOLOCK)
JOIN dbo.DocumentTimeSlice dts WITH(NOLOCK) ON dts.DocumentSeriesId = @DocumentSeriesId
JOIN Document d WITH (NOLOCK) on dts.DocumentID = d.ID
LEFT JOIN vw_SCARDocumentTimeSliceTableCell tc WITH (NOLOCK) ON tc.CompanyFinancialTermID = sh.CompanyFinancialTermID AND tc.DocumentTimeSliceID = dts.ID
JOIN @SHCellsError lpv ON lpv.StaticHierarchyID = sh.ID AND lpv.DocumentTimeSliceID = dts.ID
LEFT JOIN ScalingFactor sf WITH (NOLOCK) ON tc.ScalingFactorID = sf.ID
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
								if (reader.GetNullable<int>(1).Value > 0) {
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
								} else {
									cell = new SCARAPITableCell
									{
										ID = reader.GetInt32(1),
										CompanyFinancialTermID = reader.GetNullable<int>(12),
										VirtualValueNumeric = reader.GetNullable<decimal>(13),
									};
									cell.ARDErrorTypeId = reader.GetNullable<int>(26);
									cell.MTMWErrorTypeId = reader.GetNullable<int>(27);
									cell.LikePeriodValidationFlag = reader.GetBoolean(28);
									cell.MTMWValidationFlag = reader.GetBoolean(29);
									cell.DocumentTimeSliceID = reader.GetInt32(30);
									adjustedOrder = reader.GetInt32(31);
									cell.StaticHierarchyID = reader.GetInt32(36);
								}
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
	SELECT ID FROM StaticHierarchy WITH (NOLOCK) WHERE ID = @TargetSHID
) INSERT @OldStaticHierarchyList ([StaticHierarchyID])
   SELECT ID 
FROM CTE cte

DECLARE @OldSHCells CellList
INSERT @OldSHCells
SELECT distinct tc.TableCellID, tc.DocumentTimeSliceID
FROM StaticHierarchy sh WITH (NOLOCK)
JOIN @OldStaticHierarchyList shl ON sh.id = shl.StaticHierarchyID
JOIN vw_SCARDocumentTimeSliceTableCell tc WITH (NOLOCK) ON sh.CompanyFinancialTermId = tc.CompanyFinancialTermID  

UPDATE tc 
set IsIncomePositive = CASE WHEN IsIncomePositive = 1 THEN 0 ELSE 1 END																
FROM 
TableCell tc 
JOIN @OldSHCells osh on tc.id = osh.StaticHierarchyID  --- osh staticHierarchyID is the cellID
 

DECLARE @CellsForLPV CellList
DECLARE @CellsForMTMW CellList

INSERT @CellsForLPV
SELECT distinct sh.ID, tc.DocumentTimeSliceId
FROM StaticHierarchy sh WITH (NOLOCK)
JOIN @OldStaticHierarchyList shl ON sh.id = shl.StaticHierarchyID
JOIN vw_SCARDocumentTimeSliceTableCell tc WITH (NOLOCK) ON sh.CompanyFinancialTermId = tc.CompanyFinancialTermID

INSERT @CellsForMTMW
SELECT distinct sh.ID, tc.DocumentTimeSliceId
FROM StaticHierarchy sh WITH (NOLOCK)
JOIN @OldStaticHierarchyList shl ON sh.id = shl.StaticHierarchyID
JOIN vw_SCARDocumentTimeSliceTableCell tc WITH (NOLOCK) ON sh.CompanyFinancialTermId = tc.CompanyFinancialTermID

DECLARE @ParentCells TABLE(StaticHierarchyID int, DocumentTimeSliceID int, TablecellID int)

;WITH cte_sh(StaticHierarchyID, CompanyFinancialTermID, ParentID, DocumentTimeSliceID, TableCellID, IsRoot, RootStaticHierarchyID, RootDocumentTimeSliceID)
AS
(
       SELECT sh.ID, sh.CompanyFinancialTermID, sh.ParentID, c.DocumentTimeSliceID, tc.TableCellID, 1, c.StaticHierarchyID, c.DocumentTimeSliceID
       FROM @CellsForLPV c
       JOIN StaticHierarchy sh WITH (NOLOCK) ON sh.ID = c.StaticHierarchyID
       LEFT JOIN vw_SCARDocumentTimeSliceTableCell2 tc WITH (NOLOCK) on c.DocumentTimeSliceID = tc.DocumentTimeSliceID AND sh.CompanyFinancialTermID = tc.CompanyFinancialTermID
       UNION ALL
       SELECT ID, sh.CompanyFinancialTermID, sh.ParentID, cte.DocumentTimeSliceID, dtc.TableCellID, 0, cte.RootStaticHierarchyID, cte.RootDocumentTimeSliceID
       FROM cte_sh cte
       JOIN StaticHierarchy sh WITH (NOLOCK) on sh.ID = cte.ParentID
       OUTER APPLY(SELECT dtc.TableCellID FROM vw_SCARDocumentTimeSliceTableCell2 dtc WHERE sh.CompanyFinancialTermID = dtc.CompanyFinancialTermID 
                                  AND dtc.DocumentTimeSliceID = cte.DocumentTimeSliceID)dtc
       WHERE cte.IsRoot = 1 OR (cte.IsRoot = 0 AND cte.TableCellID IS NULL)
)
INSERT @ParentCells
select StaticHierarchyID, DocumentTimeSliceID, TableCellID from  cte_sh where IsRoot = 0

INSERT @CellsForLPV
Select StaticHierarchyID, DocumentTimeSliceID 
FROM @ParentCells
WHERE TableCellID is null and DocumentTimeSliceID NOT IN (Select DocumentTimeSliceID FROM DocumentTimeSliceTableTypeIsSummary WITH (NOLOCK))

INSERT @CellsForLPV
EXEC SCARGetTableCellLikePeriod_GetSibilingTableCells @CellsForLPV, @DocumentID

INSERT @CellsForMTMW
Select StaticHierarchyID, DocumentTimeSliceID 
FROM @ParentCells
WHERE TableCellID is not null

DECLARE @SHCellsMTMW TABLE(StaticHierarchyID int, DocumentTimeSliceID int, ChildrenSum decimal(28,5), CellValue decimal(28,5))
DECLARE @SHCellsLPV TABLE(StaticHierarchyID int, DocumentTimeSliceID int, LPVFail bit)
DECLARE @SHCellsError TABLE(StaticHierarchyID int, DocumentTimeSliceID int, LPVFail bit, MTMWFail bit)

DELETE FROM MTMWErrorTypeTableCell 
WHERE TableCellid in(
select tc.TableCellID from
@CellsForMTMW e 
JOIN StaticHierarchy sh on e.StaticHierarchyid = sh.id
JOIN vw_SCARDocumentTimeSliceTableCell tc ON e.DocumentTimeSliceID = tc.DocumentTimeSliceId AND sh.CompanyFinancialTermID = tc.CompanyFinancialTermID
)

INSERT INTO @SHCellsMTMW
EXEC SCARGetTableCellMTMWCalc @CellsForMTMW

INSERT INTO @SHCellsLPV
EXEC SCARGetTableCellLikePeriod_ByTableCell @CellsForLPV, @DocumentID

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
	JOIN StaticHierarchy sh WITH (NOLOCK) ON cte.SHID = sh.ID
	JOIN StaticHierarchy shp WITH (NOLOCK) ON sh.ParentID = shp.ID
)
SELECT MAX(level)
FROM cte_level
GROUP BY SHRootID

SELECT distinct 'x', ISNULL(tc.TableCellID,0), tc.Offset, tc.CellPeriodType, tc.PeriodTypeID, tc.CellPeriodCount, tc.PeriodLength, tc.CellDay, 
				tc.CellMonth, tc.CellYear, tc.CellDate, tc.Value, ISNULL(tc.CompanyFinancialTermID, sh.CompanyFinancialTermId), ISNULL(tc.ValueNumeric, dbo.GetTableCellDisplayValue(lpv.StaticHierarchyID, lpv.DocumentTimeSliceID)), tc.NormalizedNegativeIndicator, 
				tc.ScalingFactorID, tc.AsReportedScalingFactor, tc.Currency, tc.CurrencyCode, tc.Cusip, tc.ScarUpdated, tc.IsIncomePositive, 
				tc.XBRLTag, 
				tc.DocumentId, tc.Label, sf.Value,
				(select aetc.ARDErrorTypeId from ARDErrorTypeTableCell aetc (nolock) where tc.TableCellId = aetc.TableCellId),
				(select metc.MTMWErrorTypeId from MTMWErrorTypeTableCell metc (nolock) where tc.TableCellId = metc.TableCellId), 
				lpv.LPVFail, lpv.MTMWFail,
				dts.Id, sh.AdjustedOrder, dts.Duration, dts.TimeSlicePeriodEndDate, dts.ReportingPeriodEndDate, d.PublicationDateTime,
				sh.id as 'StaticHierarchyId'
FROM StaticHierarchy sh WITH (NOLOCK)
JOIN dbo.DocumentTimeSlice dts WITH(NOLOCK) ON dts.DocumentSeriesId = @DocumentSeriesId
JOIN Document d WITH (NOLOCK) on dts.DocumentID = d.ID
LEFT JOIN vw_SCARDocumentTimeSliceTableCell tc WITH (NOLOCK) ON tc.CompanyFinancialTermID = sh.CompanyFinancialTermID AND tc.DocumentTimeSliceID = dts.ID
JOIN @SHCellsError lpv ON lpv.StaticHierarchyID = sh.ID AND lpv.DocumentTimeSliceID = dts.ID
LEFT JOIN ScalingFactor sf WITH (NOLOCK) ON tc.ScalingFactorID = sf.ID
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
								if (reader.GetNullable<int>(1).Value > 0) {
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
								} else {
									cell = new SCARAPITableCell
									{
										ID = reader.GetInt32(1),
										CompanyFinancialTermID = reader.GetNullable<int>(12),
										VirtualValueNumeric = reader.GetNullable<decimal>(13),
									};
									cell.ARDErrorTypeId = reader.GetNullable<int>(26);
									cell.MTMWErrorTypeId = reader.GetNullable<int>(27);
									cell.LikePeriodValidationFlag = reader.GetBoolean(28);
									cell.MTMWValidationFlag = reader.GetBoolean(29);
									cell.DocumentTimeSliceID = reader.GetInt32(30);
									adjustedOrder = reader.GetInt32(31);
									cell.StaticHierarchyID = reader.GetInt32(36);
								}
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
	SELECT ID FROM StaticHierarchy WITH (NOLOCK) WHERE ID = @TargetSHID
	UNION ALL
	SELECT sh.Id 
	FROM StaticHierarchy sh WITH (NOLOCK)
	JOIN CTE_Children cte on sh.ParentID = cte.ID
) INSERT @OldStaticHierarchyList ([StaticHierarchyID])
   SELECT ID 
FROM CTE_Children cte
where cte.id <> @TargetSHID
 


DECLARE @OldSHCells CellList
INSERT @OldSHCells
SELECT distinct tc.TableCellID, tc.DocumentTimeSliceID
FROM StaticHierarchy sh WITH (NOLOCK)
JOIN @OldStaticHierarchyList shl ON sh.id = shl.StaticHierarchyID
JOIN vw_SCARDocumentTimeSliceTableCell tc WITH (NOLOCK) ON sh.CompanyFinancialTermId = tc.CompanyFinancialTermID and sh.CompanyFinancialTermId = tc.CompanyFinancialTermID  
 
UPDATE tc 
set IsIncomePositive = CASE WHEN IsIncomePositive = 1 THEN 0 ELSE 1 END																
FROM 
TableCell tc 
JOIN @OldSHCells osh on tc.id = osh.StaticHierarchyID  --- osh staticHierarchyID is the cellID
 

DECLARE @CellsForLPV CellList
DECLARE @CellsForMTMW CellList

INSERT @CellsForLPV
SELECT distinct  sh.ID, tc.DocumentTimeSliceId
FROM StaticHierarchy sh WITH (NOLOCK)
JOIN @OldStaticHierarchyList shl ON sh.id = shl.StaticHierarchyID
JOIN vw_SCARDocumentTimeSliceTableCell tc WITH (NOLOCK) ON sh.CompanyFinancialTermId = tc.CompanyFinancialTermID

INSERT @CellsForMTMW
SELECT distinct  sh.ID, tc.DocumentTimeSliceId
FROM StaticHierarchy sh WITH (NOLOCK)
JOIN @OldStaticHierarchyList shl ON sh.id = shl.StaticHierarchyID
JOIN vw_SCARDocumentTimeSliceTableCell tc WITH (NOLOCK) ON sh.CompanyFinancialTermId = tc.CompanyFinancialTermID

DECLARE @ParentCells TABLE(StaticHierarchyID int, DocumentTimeSliceID int, TablecellID int)

;WITH cte_sh(StaticHierarchyID, CompanyFinancialTermID, ParentID, DocumentTimeSliceID, TableCellID, IsRoot, RootStaticHierarchyID, RootDocumentTimeSliceID)
AS
(
       SELECT sh.ID, sh.CompanyFinancialTermID, sh.ParentID, c.DocumentTimeSliceID, tc.TableCellID, 1, c.StaticHierarchyID, c.DocumentTimeSliceID
       FROM @CellsForLPV c
       JOIN StaticHierarchy sh WITH (NOLOCK) ON sh.ID = c.StaticHierarchyID
       LEFT JOIN vw_SCARDocumentTimeSliceTableCell2 tc WITH (NOLOCK) on c.DocumentTimeSliceID = tc.DocumentTimeSliceID AND sh.CompanyFinancialTermID = tc.CompanyFinancialTermID
       UNION ALL
       SELECT ID, sh.CompanyFinancialTermID, sh.ParentID, cte.DocumentTimeSliceID, dtc.TableCellID, 0, cte.RootStaticHierarchyID, cte.RootDocumentTimeSliceID
       FROM cte_sh cte
       JOIN StaticHierarchy sh WITH (NOLOCK) on sh.ID = cte.ParentID
       OUTER APPLY(SELECT dtc.TableCellID FROM vw_SCARDocumentTimeSliceTableCell2 dtc WHERE sh.CompanyFinancialTermID = dtc.CompanyFinancialTermID 
                                  AND dtc.DocumentTimeSliceID = cte.DocumentTimeSliceID)dtc
       WHERE cte.IsRoot = 1 OR (cte.IsRoot = 0 AND cte.TableCellID IS NULL)
)
INSERT @ParentCells
select StaticHierarchyID, DocumentTimeSliceID, TableCellID from  cte_sh where IsRoot = 0


INSERT @CellsForLPV
Select StaticHierarchyID, DocumentTimeSliceID 
FROM @ParentCells
WHERE TableCellID is null and DocumentTimeSliceID NOT IN (Select DocumentTimeSliceID FROM DocumentTimeSliceTableTypeIsSummary WITH (NOLOCK))

INSERT @CellsForLPV
EXEC SCARGetTableCellLikePeriod_GetSibilingTableCells @CellsForLPV, @DocumentID

INSERT @CellsForMTMW
Select StaticHierarchyID, DocumentTimeSliceID 
FROM @ParentCells
WHERE TableCellID is not null

DECLARE @SHCellsMTMW TABLE(StaticHierarchyID int, DocumentTimeSliceID int, ChildrenSum decimal(28,5), CellValue decimal(28,5))
DECLARE @SHCellsLPV TABLE(StaticHierarchyID int, DocumentTimeSliceID int, LPVFail bit)
DECLARE @SHCellsError TABLE(StaticHierarchyID int, DocumentTimeSliceID int, LPVFail bit, MTMWFail bit)

DELETE FROM MTMWErrorTypeTableCell 
WHERE TableCellid in(
select tc.TableCellID from
@CellsForMTMW e 
JOIN StaticHierarchy sh on e.StaticHierarchyid = sh.id
JOIN vw_SCARDocumentTimeSliceTableCell tc ON e.DocumentTimeSliceID = tc.DocumentTimeSliceId AND sh.CompanyFinancialTermID = tc.CompanyFinancialTermID
)

INSERT INTO @SHCellsMTMW
EXEC SCARGetTableCellMTMWCalc @CellsForMTMW

INSERT INTO @SHCellsLPV
EXEC SCARGetTableCellLikePeriod_ByTableCell @CellsForLPV, @DocumentID

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
	JOIN StaticHierarchy sh WITH (NOLOCK) ON cte.SHID = sh.ID
	JOIN StaticHierarchy shp WITH (NOLOCK) ON sh.ParentID = shp.ID
)
SELECT MAX(level)
FROM cte_level
GROUP BY SHRootID


SELECT distinct 'x', ISNULL(tc.TableCellID,0), tc.Offset, tc.CellPeriodType, tc.PeriodTypeID, tc.CellPeriodCount, tc.PeriodLength, tc.CellDay, 
				tc.CellMonth, tc.CellYear, tc.CellDate, tc.Value, ISNULL(tc.CompanyFinancialTermID, sh.CompanyFinancialTermId), ISNULL(tc.ValueNumeric, dbo.GetTableCellDisplayValue(lpv.StaticHierarchyID, lpv.DocumentTimeSliceID)), tc.NormalizedNegativeIndicator, 
				tc.ScalingFactorID, tc.AsReportedScalingFactor, tc.Currency, tc.CurrencyCode, tc.Cusip, tc.ScarUpdated, tc.IsIncomePositive, 
				tc.XBRLTag, 
				tc.DocumentId, tc.Label, sf.Value,
				(select aetc.ARDErrorTypeId from ARDErrorTypeTableCell aetc (nolock) where tc.TableCellId = aetc.TableCellId),
				(select metc.MTMWErrorTypeId from MTMWErrorTypeTableCell metc (nolock) where tc.TableCellId = metc.TableCellId), 
				lpv.LPVFail, lpv.MTMWFail,
				dts.Id, sh.AdjustedOrder, dts.Duration, dts.TimeSlicePeriodEndDate, dts.ReportingPeriodEndDate, d.PublicationDateTime,
				sh.id as 'StaticHierarchyId'
FROM StaticHierarchy sh WITH (NOLOCK)
JOIN dbo.DocumentTimeSlice dts WITH(NOLOCK) ON dts.DocumentSeriesId = @DocumentSeriesId
JOIN Document d WITH (NOLOCK) on dts.DocumentID = d.ID
LEFT JOIN vw_SCARDocumentTimeSliceTableCell tc WITH (NOLOCK) ON tc.CompanyFinancialTermID = sh.CompanyFinancialTermID AND tc.DocumentTimeSliceID = dts.ID
JOIN @SHCellsError lpv ON lpv.StaticHierarchyID = sh.ID AND lpv.DocumentTimeSliceID = dts.ID
LEFT JOIN ScalingFactor sf WITH (NOLOCK) ON tc.ScalingFactorID = sf.ID
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
								if (reader.GetNullable<int>(1).Value > 0) {
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
								} else {
									cell = new SCARAPITableCell
									{
										ID = reader.GetInt32(1),
										CompanyFinancialTermID = reader.GetNullable<int>(12),
										VirtualValueNumeric = reader.GetNullable<decimal>(13),
									};
									cell.ARDErrorTypeId = reader.GetNullable<int>(26);
									cell.MTMWErrorTypeId = reader.GetNullable<int>(27);
									cell.LikePeriodValidationFlag = reader.GetBoolean(28);
									cell.MTMWValidationFlag = reader.GetBoolean(29);
									cell.DocumentTimeSliceID = reader.GetInt32(30);
									adjustedOrder = reader.GetInt32(31);
									cell.StaticHierarchyID = reader.GetInt32(36);
								}
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
		select top 1 @docId = Documentid from TableCell WITH (NOLOCK) where id = @firstCellId

		DECLARE @firstCFT int
		select top 1 @firstCFT = CompanyFinancialTermID from TableCell WITH (NOLOCK) where id = @firstCellId

		DECLARE @secondCFT int
		select top 1 @secondCFT = CompanyFinancialTermID from TableCell WITH (NOLOCK) where id = @secondCellId
 
		IF (@firstCFT is null) RAISERROR('Null CFT', 16, 1);
		if (@secondCFT is null) RAISERROR('Null CFT', 16, 1);
		DECLARE @TempCells Table(ID INT)
		INSERT @TempCells (ID)
		SELECT ID from TableCell WITH (NOLOCK) where CompanyFinancialTermID = @secondCFT and Documentid = @docId

		DECLARE @TempCells2 Table(ID INT)
		INSERT @TempCells2 (ID)
		SELECT ID from TableCell WITH (NOLOCK) where CompanyFinancialTermID = @firstCFT and Documentid = @docId


		Update TableCell set CompanyFinancialTermID = @secondCFT, ScarUpdated = 1 where id in (select id from @TempCells2)
		Update TableCell set CompanyFinancialTermID = @firstCFT , ScarUpdated = 1 where id in (select id from @TempCells)

		UPDATE CompanyFinancialTerm set TermStatusID = 1 where id = @firstCFT
		UPDATE CompanyFinancialTerm set TermStatusID = 1 where id = @secondCFT
		COMMIT 
		select 1
END TRY
BEGIN CATCH
	ROLLBACK;
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
					cmd.CommandTimeout = 180;
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
							ValueNumeric = r.GetNullable<Decimal>(3).HasValue ? r.GetDecimal(3) : 0,
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
							ValueNumeric = r.GetNullable<Decimal>(3).HasValue ? r.GetDecimal(3) : 0,
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
									DocumentID = sdr.GetNullable<Guid>(23).HasValue ? sdr.GetGuid(23) : Guid.Empty,
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
				decimal value = (cell.ValueNumeric.HasValue ? cell.ValueNumeric.Value : 0) * (cell.IsIncomePositive ? 1 : -1) * (decimal)cell.ScalingFactorValue;
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

		public UnStitchResult UnstitchStaticHierarchy(int StaticHierarchyID, Guid DocumentID, int Iconum, List<int> DocumentTimeSliceIDs) {

			DataTable dt = new DataTable();
			dt.Columns.Add("DocumentTimeSliceID", typeof(Int32));
			foreach (int i in DocumentTimeSliceIDs) {
				dt.Rows.Add(i);
			}

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
					cmd.CommandTimeout = 120;
					cmd.Parameters.AddWithValue("@TargetSH", StaticHierarchyID);
					cmd.Parameters.AddWithValue("@DocumentID", DocumentID);
					cmd.Parameters.AddWithValue("@Iconum", Iconum);
					cmd.Parameters.AddWithValue("@DocumentTimeSliceList", dt);


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
									DocumentID = sdr.GetNullable<Guid>(23).HasValue ? sdr.GetGuid(23) : Guid.Empty,
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
							ValueNumeric = r.GetNullable<Decimal>(8).HasValue ? r.GetDecimal(8) : 0,
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
FROM StaticHierarchy sh WITH (NOLOCK)
where sh.id in ({0}) and sh.ParentId is null
UNION
SELECT 'redstar_result' as result, 'Red star item in share and per share sections' as msg
FROM StaticHierarchy sh WITH (NOLOCK)
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
						cmd.CommandTimeout = 300;

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
from Document d WITH (NOLOCK)
join PPIIconumMap pim WITH (NOLOCK) on d.PPI = pim.PPI
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

		public string CheckParsedTableInterimTypeAndCurrency(Guid SFDocumentId, int Iconum, string ContentType = "Full") {
			string query = @"

 DECLARE @BigThree Table (Description varchar(64))
 INSERT  @BigThree (Description)
 VALUES 
 ('IS'), ('BS'), ('CF')

 SELECT TOP 1 'Missing Table. ' as Error, *
 FROM DocumentTable dt WITH (NOLOCK)
 JOIN TableType tt WITH (NOLOCK) ON dt.TableTypeID = tt.id
 RIGHT JOIN @BigThree bt on bt.Description = tt.description
 where dt.DocumentID = @DocumentId and tt.description is null


 SELECT TOP 1 'Missing InterimType. ' as Error, * 
 FROM dbo.DocumentTimeSlice dts WITH (NOLOCK)
 JOIN DocumentTimeSliceTableCell dtstc WITH (NOLOCK) on dtstc.DocumentTimeSliceId = dts.Id
 JOIN TableCell tc WITH (NOLOCK) ON dtstc.TableCellID = tc.id
  where dts.DocumentID = @DocumentId and dts.PeriodType is null

 SELECT TOP 1 'Missing InterimType. ' as Error, * 
  FROM DocumentTable dt WITH (NOLOCK)
 JOIN TableType tt WITH (NOLOCK) ON dt.TableTypeID = tt.id
 JOIN TableDimension td WITH (NOLOCK) on dt.TableIntID = td.DocumentTableID
 JOIN DimensionToCell dtc WITH (NOLOCK) on dtc.TableDimensionID = td.ID
 JOIN TableCell tc WITH (NOLOCK) ON dtc.TableCellID = tc.id
 JOIN DocumentTimeSliceTableCell dtstc WITH (NOLOCK) on dtstc.TableCellId = tc.Id
 JOIN dbo.DocumentTimeSlice dts WITH (NOLOCK) on dtstc.DocumentTimeSliceId = dts.Id
  where dt.DocumentID = @DocumentId and dts.PeriodType is null


 ;WITH cte (id) as
(
 SELECT  dtc.TableCellid
 FROM DocumentTable dt WITH (NOLOCK)
 JOIN TableType tt WITH (NOLOCK) ON dt.TableTypeID = tt.id
 JOIN TableDimension td WITH (NOLOCK) on dt.TableIntID = td.DocumentTableID
 JOIN DimensionToCell dtc WITH (NOLOCK) on dtc.TableDimensionID = td.ID
 where dt.DocumentID = @DocumentId 
)
 SELECT TOP 1 'Missing Currency. ' as Error, * 
 FROM cte
 JOIN TableCell tc WITH (NOLOCK) ON cte.id = tc.id 
 where  tc.currencycode is null
";
			string errorMessage = "";
			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {
				conn.Open();

				using (SqlCommand cmd = new SqlCommand("prcInsert_CreateDocumentTimeSlices", conn)) {
					cmd.CommandType = CommandType.StoredProcedure;
					cmd.Parameters.AddWithValue("@DocID", SFDocumentId);
					cmd.Parameters.AddWithValue("@ContentType", ContentType);
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
 FROM Document d WITH (NOLOCK)
 JOIN DocumentSeries ds WITH (NOLOCK) on d.DocumentSeriesID = ds.id
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
FROM vw_SCARDocumentTimeSlices dts WITH (NOLOCK)
JOIN StaticHierarchy sh WITH (NOLOCK) ON sh.TableTypeId = dts.TableTypeID
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
 FROM Document d WITH (NOLOCK)
 JOIN DocumentSeries ds WITH (NOLOCK) on d.DocumentSeriesID = ds.id
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
FROM DocumentSeries ds WITH (NOLOCK)
JOIN CompanyFinancialTerm cft WITH (NOLOCK) ON cft.DocumentSeriesId = ds.Id
JOIN StaticHierarchy sh WITH (NOLOCK) on cft.ID = sh.CompanyFinancialTermID
JOIN TableType tt WITH (NOLOCK) on sh.TableTypeID = tt.ID
JOIN(
	SELECT distinct dts.ID
	FROM DocumentSeries ds WITH (NOLOCK)
	JOIN dbo.DocumentTimeSlice dts WITH (NOLOCK) on ds.ID = Dts.DocumentSeriesId
	JOIN Document d WITH (NOLOCK) on dts.DocumentId = d.ID
	JOIN DocumentTimeSliceTableCell dtstc WITH (NOLOCK) on dts.ID = dtstc.DocumentTimeSliceID
	JOIN TableCell tc WITH (NOLOCK) on dtstc.TableCellID = tc.ID
	JOIN DimensionToCell dtc WITH (NOLOCK) on tc.ID = dtc.TableCellID -- check that is in a table
	JOIN StaticHierarchy sh WITH (NOLOCK) on tc.CompanyFinancialTermID = sh.CompanyFinancialTermID
	JOIN TableType tt WITH (NOLOCK) on tt.ID = sh.TableTypeID
	WHERE ds.CompanyID = @iconum
	AND (d.ID = @DocumentID OR d.ArdExportFlag = 1 OR d.ExportFlag = 1 OR d.IsDocSetupCompleted = 1)
) as ts on 1=1
JOIN dbo.DocumentTimeSlice dts on dts.ID = ts.ID
JOIN(
	SELECT tc.*, dtstc.DocumentTimeSliceID, sf.Value as ScalingFactorValue
	FROM DocumentSeries ds WITH (NOLOCK)
	JOIN CompanyFinancialTerm cft WITH (NOLOCK) ON cft.DocumentSeriesId = ds.Id
	JOIN StaticHierarchy sh WITH (NOLOCK) on cft.ID = sh.CompanyFinancialTermID
	JOIN TableType tt WITH (NOLOCK) on sh.TableTypeID = tt.ID
	JOIN TableCell tc WITH (NOLOCK) on tc.CompanyFinancialTermID = cft.ID
	JOIN ARDErrorTypeTableCell aetc WITH (NOLOCK) ON tc.Id = aetc.TableCellId
	JOIN DocumentTimeSliceTableCell dtstc WITH (NOLOCK) on dtstc.TableCellID = tc.ID
	JOIN ScalingFactor sf WITH (NOLOCK) on sf.ID = tc.ScalingFactorID
	WHERE ds.CompanyID = @iconum
) as tc ON tc.DocumentTimeSliceID = ts.ID AND tc.CompanyFinancialTermID = cft.ID
JOIN Document d WITH (NOLOCK) on dts.documentid = d.ID
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
				using (SqlCommand cmd = new SqlCommand(@"SELECT tt.Description FROM DocumentSeries ds WITH (NOLOCK) JOIN TableType tt WITH (NOLOCK) ON tt.DocumentSeriesID = ds.ID WHERE ds.CompanyID = @Iconum", conn)) {
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
				using (SqlCommand cmd = new SqlCommand("select ID, TableTypeID from [dbo].[vw_SCARDocumentTimeSlices] WITH (NOLOCK) WHERE DocumentID = @DocumentID", conn)) {
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
			//string url =  @"https://data-wellness-orchestrator-staging.factset.io/Check/Full/92C6C824-0F9A-4A5C-BC62-000095729E1B";
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
		public static string AsSafeString(this JToken jValue) {
			string jString = jValue.ToString();
			string result = "''";
			if (string.Equals(jString, "null", StringComparison.InvariantCultureIgnoreCase) || jString.Length < 1) {
				result = "NULL";
			} else {
				result = "'" + jString.Replace("'", "''").Replace("\\\\", "\\") + "'";
			}
			return result;
		}
		public static string AsString(this JToken jValue) {
			string jString = jValue.ToString();
			string result = "''";
			if (string.Equals(jString, "null", StringComparison.InvariantCultureIgnoreCase)) {
				result = "NULL";
			} else {
				result = "'" + jString.Replace("'", "''").Replace("\\\\", "\\") + "'";
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
			if (string.IsNullOrWhiteSpace(result)) {
				result = "NULL";
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
		public static class ObjectExtension {
		public static string AsString(this object o) {
			if (Convert.IsDBNull(o)) {
				return null;
			}
			string jString = o.ToString();
			string result = jString;
			return result;
		}
		public static byte? AsByteNullable(this object o) {
			string jString = o.ToString();
			byte temp;
			byte? result;
			if (!byte.TryParse(jString, out temp)) {
				result = null;
			} else {
				result = temp;
			}
			return result;
		}
		public static int AsInt32(this object o) {
			string jString = o.ToString();
			int result;
			if (!int.TryParse(jString, out result)) {
				result = 0;
			}
			return result;
		}
		public static int? AsInt32Nullable(this object o) {
			string jString = o.ToString();
			int temp;
			int? result;
			if (!int.TryParse(jString, out temp)) {
				result = null;
			} else {
				result = temp;
			}
			return result;
		}
		public static Int64 AsInt64(this object o) {
			string jString = o.ToString();
			long result;
			if (!long.TryParse(jString, out result)) {
				result = 0;
			}
			return result;
		}
		public static double AsDouble(this object o) {
			string jString = o.ToString();
			double result;
			if (!double.TryParse(jString, out result)) {
				result = default(double);
			}
			return result;
		}
		public static decimal AsDecimal(this object o) {
			string jString = o.ToString();
			decimal result;
			if (!decimal.TryParse(jString, out result)) {
				result = default(decimal);
			}
			return result;
		}
		public static decimal? AsDecimalNullable(this object o) {
			string jString = o.ToString();
			decimal temp;
			decimal? result;
			if (!decimal.TryParse(jString, out temp)) {
				result = null;
			} else {
				result = temp;
			}
			return result;
		}

		public static bool AsBoolean(this object o) {
			string jString = o.ToString();
			bool result;
			if (!bool.TryParse(jString, out result)) {
				result = false;
			}
			return result;
		}
		public static Guid AsGuid(this object o) {
			if (Convert.IsDBNull(o)) {
				return new Guid("00000000-0000-0000-0000-000000000000");
			}
			string jString = o.ToString();
			Guid result;
			if (!Guid.TryParse(jString, out result)) {
				result = new Guid("00000000-0000-0000-0000-000000000000");
			}
			return result;
		}
		public static DateTime AsDateTime(this object o) {
			string jString = o.ToString();
			DateTime result;
			if (!DateTime.TryParse(jString, out result)) {
				result = DateTime.MinValue;
			} 
			return result;
		}
		public static DateTime? AsDateTimeNullable(this object o) {
			string jString = o.ToString();
			DateTime temp;
			DateTime? result;
			if (!DateTime.TryParse(jString, out temp)) {
				result = null;
			} else {
				result = temp;
			}
			return result;
		}
	}
}