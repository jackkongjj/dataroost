using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Data;
using System.Data.SqlClient;
using System.Linq;

using DataRoostAPI.Common.Models;
using DataRoostAPI.Common.Models.SuperFast;
using DataRoostAPI.Common.Models.TimeseriesValues;

using FactSet.Data.SqlClient;

namespace CCS.Fundamentals.DataRoostAPI.Access.SuperFast {
	public class TimeseriesHelper {
		private readonly string connectionString;

		public TimeseriesHelper(string connectionString) {
			this.connectionString = connectionString;
		}

		public TimeseriesDTO[] QuerySDBTimeseries(int iconum, TemplateIdentifier templateId, TimeseriesIdentifier timeseriesId, StandardizationType dataType, NameValueCollection queryFilter = null) {
			string preQuery_timeseriesIdentification = dataType == StandardizationType.SDB ? @"
WITH TemplateMasterID AS
(
	select distinct sdm.Id
	from CompanyIndustry ci 
	join SDBtemplateDetail std on ci.IndustryDetailID = std.IndustryDetailId
	join DocumentSeries ds on ci.Iconum = ds.CompanyId
	join Document d on ds.Id = d.DocumentSeriesId
	join FDSTriPPIMap f on d.PPI = f.PPI
	join SDBCountryGroupCountries sc on sc.CountriesIsoCountry = f.IsoCountry
	join SDBCountryGroup sg on sc.SDBCountryGroupID = sg.Id
		and std.SDBCountryGroupID = sg.ID
	join SDBTemplateMaster sdm on std.SDBTemplateMasterId = sdm.Id
	where ci.Iconum = @iconum
		and std.ReportTypeID = @reportTypeId
		and std.UpdateTypeID = @updateTypeId
		and std.TemplateTypeId = @templateTypeId
)
select distinct ts.id, cast(tmi.id as varchar(5))
from Timeseries ts
join Document d on ts.DocumentID = d.id
join DocumentSeries ds on d.DocumentSeriesID = ds.ID
join vw_SDBTimeSeriesDetail sdbd on ts.id = sdbd.TimeSeriesId
join SDBTemplateItem sdbti on sdbd.sdbitemId = sdbti.SDBItemID
join TemplateMasterID tmi on tmi.Id = sdbti.SDBTemplateMasterID
where ds.CompanyID = @iconum	
" : @"WITH TemplateMasterID AS
(
	select distinct sdm.Code
	from CompanyIndustry ci 
	join STDTemplateDetail std on ci.IndustryDetailID = std.IndustryDetailId
	join DocumentSeries ds on ci.Iconum = ds.CompanyId
	join Document d on ds.Id = d.DocumentSeriesId
	join FDSTriPPIMap f on d.PPI = f.PPI
	join STDCountryGroupCountries sc on sc.CountriesIsoCountry = f.IsoCountry
	join STDCountryGroup sg on sc.STDCountryGroupID = sg.Id
		and std.STDCountryGroupID = sg.ID
	join STDTemplateMaster sdm on std.STDTemplateMasterCode = sdm.Code
	where ci.Iconum = @iconum
		and std.ReportTypeID = @reportTypeId
		and std.UpdateTypeID = @updateTypeId
		and std.TemplateTypeId = @templateTypeId
)
select distinct ts.id, tmi.Code
from Timeseries ts
join document d on ts.DocumentID = d.ID
join DocumentSeries ds on d.DocumentSeriesID = ds.ID
join vw_STDTimeSeriesDetail stdd on ts.ID = stdd.TimeSeriesId
join STDTemplateItem stdti on stdd.STDItemId = stdti.STDItemID
join TemplateMasterID tmi on tmi.Code = stdti.STDTemplateMasterCode
where ds.CompanyID = @iconum";

			bool requestedSpecificTimeSerie = (timeseriesId != null);
			string templateMasterId = string.Empty;

			if (requestedSpecificTimeSerie) {
					 preQuery_timeseriesIdentification += @" and d.id = isnull(@SFDocumentId, d.id)
	and ts.CompanyFiscalYear = isnull(@FiscalYear, ts.CompanyFiscalYear)
	and ts.TimeSeriesDate = isnull(@PeriodEndDate, ts.TimeSeriesDate)
	and ts.InterimTypeID = isnull(@InterimType, ts.InterimTypeID)
	and ts.AutoCalcFlag = isnull(@AutoCalcFlag, ts.AutoCalcFlag)";
			} else if (!string.IsNullOrEmpty(queryFilter["years"])) {
				preQuery_timeseriesIdentification += " and ts.companyfiscalyear in (select id from @years)";
			} else if (!string.IsNullOrEmpty(queryFilter["startyear"]) && !string.IsNullOrEmpty(queryFilter["endyear"])) {
				preQuery_timeseriesIdentification += " and ts.companyfiscalyear between @startyear AND @endyear";
			}
			using (SqlConnection conn = new SqlConnection(connectionString)) {
				conn.Open();

				HashSet<Guid> timeseries = new HashSet<Guid>();

				using (SqlCommand cmd = new SqlCommand(preQuery_timeseriesIdentification, conn)) {
					cmd.Parameters.Add(new SqlParameter("@iconum", SqlDbType.Int) { Value = iconum });
					cmd.Parameters.Add(new SqlParameter("@reportTypeId", SqlDbType.NVarChar, 64) { Value = templateId.ReportType });
					cmd.Parameters.Add(new SqlParameter("@updateTypeId", SqlDbType.NVarChar, 64) { Value = templateId.UpdateType });
					cmd.Parameters.Add(new SqlParameter("@templateTypeId", SqlDbType.Int) { Value = templateId.TemplateType });
					if (requestedSpecificTimeSerie) {
						cmd.Parameters.Add(new SqlParameter("@SFDocumentId", SqlDbType.UniqueIdentifier) { Value = timeseriesId.SFDocumentId });
						cmd.Parameters.Add(new SqlParameter("@FiscalYear", SqlDbType.Decimal) { Value = timeseriesId.CompanyFiscalYear });
						cmd.Parameters.Add(new SqlParameter("@PeriodEndDate", SqlDbType.DateTime) { Value = timeseriesId.PeriodEndDate });
						cmd.Parameters.Add(new SqlParameter("@InterimType", SqlDbType.Char, 2) { Value = timeseriesId.InterimType });
						cmd.Parameters.Add(new SqlParameter("@AutoCalcFlag", SqlDbType.Int) { Value = (timeseriesId.IsAutoCalc ? 1 : 0) });
					} else if (!string.IsNullOrEmpty(queryFilter["years"])) {
						DataTable dtYears = new DataTable();
						DataColumn col = new DataColumn("year", typeof(Int32));
						dtYears.Columns.Add(col);

						var filter = from x in queryFilter["years"].Split(new string[] { "," }, StringSplitOptions.RemoveEmptyEntries).AsEnumerable()
												 select new { year = int.Parse(x) };
						foreach (var years in filter) {
							DataRow row = dtYears.NewRow();
							row["year"] = years.year;
							dtYears.Rows.Add(row);
						}
						cmd.Parameters.Add(new SqlParameter("@years", SqlDbType.Structured)
												{
													TypeName = "tblType_IntList",
													Value = dtYears
												});

					} else if (!string.IsNullOrEmpty(queryFilter["startyear"]) && !string.IsNullOrEmpty(queryFilter["endyear"])) {
						cmd.Parameters.Add(new SqlParameter("@startyear", SqlDbType.Int) { Value = Int32.Parse(queryFilter["startyear"]) });
						cmd.Parameters.Add(new SqlParameter("@endyear", SqlDbType.Int) { Value = Int32.Parse(queryFilter["endyear"]) });
					}
					using (SqlDataReader reader = cmd.ExecuteReader(CommandBehavior.SingleResult | CommandBehavior.SequentialAccess)) {
						while (reader.Read()) {
							int c = 0;

							timeseries.Add(reader.GetGuid(c++));
							if (string.IsNullOrEmpty(templateMasterId))
								templateMasterId = reader.GetString(c++);
						}
					}
				}

				if(dataType == StandardizationType.SDB)
					return ConvertSDBTimeseries(conn, timeseries, requestedSpecificTimeSerie ? (int?)Int32.Parse(templateMasterId) : null);
				return ConvertSTDTimeseries(conn, timeseries, requestedSpecificTimeSerie ? templateMasterId : null);
			}
		}

		private TimeseriesDTO[] ConvertSDBTimeseries(SqlConnection conn, HashSet<Guid> timeseriesIds, int? templateMasterId) {
			List<SFTimeseriesDTO> timeseries = new List<SFTimeseriesDTO>();
			foreach (Guid timeseriesId in timeseriesIds) {
				SFTimeseriesDTO ts = _GetTimeseriesDTO(conn, timeseriesId);

				if (templateMasterId.HasValue) {
					ts.Values = _GetTimeseriesSDBValues(conn, templateMasterId.Value, timeseriesId);
				}

				timeseries.Add(ts);
			}

			// Condense our PIT/Duration Timeseries into single instance
			SFTimeseriesDTO[] condensed = timeseries
				.GroupBy(x => new TimeseriesIdentifier(x).GetToken())
				.Select(x =>
				{
					SFTimeseriesDTO t = null;

					using (IEnumerator<SFTimeseriesDTO> e = x.GetEnumerator()) {
						while (e.MoveNext()) {
							if (t == null) {
								t = e.Current;
								t.Id = new TimeseriesIdentifier(t).GetToken();
							} else if (templateMasterId.HasValue) {
								t.Values = t.Values.Union(e.Current.Values).ToDictionary(k => k.Key, v => v.Value);
							}
						}
					}

					return t;
				}).ToArray();

			// Return our results
			return condensed;
		}

		private TimeseriesDTO[] ConvertSTDTimeseries(SqlConnection conn, HashSet<Guid> timeseriesIds, string templateMasterId) {
			List<SFTimeseriesDTO> timeseries = new List<SFTimeseriesDTO>();
			foreach (Guid timeseriesId in timeseriesIds) {
				SFTimeseriesDTO ts = _GetTimeseriesDTO(conn, timeseriesId);

				if (templateMasterId != null) {
					ts.Values = _GetTimeseriesSTDValues(conn, templateMasterId, timeseriesId);
				}

				timeseries.Add(ts);
			}

			// Condense our PIT/Duration Timeseries into single instance
			SFTimeseriesDTO[] condensed = timeseries
				.GroupBy(x => new TimeseriesIdentifier(x).GetToken())
				.Select(x =>
				{
					SFTimeseriesDTO t = null;

					using (IEnumerator<SFTimeseriesDTO> e = x.GetEnumerator()) {
						while (e.MoveNext()) {
							if (t == null) {
								t = e.Current;
								t.Id = new TimeseriesIdentifier(t).GetToken();
							} else if (templateMasterId != null) {
								t.Values = t.Values.Union(e.Current.Values).ToDictionary(k => k.Key, v => v.Value);
							}
						}
					}

					return t;
				}).ToArray();

			// Return our results
			return condensed;
		}

		private SFTimeseriesDTO _GetTimeseriesDTO(SqlConnection conn, Guid timeseriesId) {
			string preQuery_timeseriesComponent = @"
select
	-- Timeseries Components
	ts.PeriodLength, ts.PeriodTypeID, ts.InterimTypeID, ts.TimeSeriesDate,
	ts.CompanyFiscalYear, ts.EncoreFlag, ts.AutoCalcFlag, d.VoyFormTypeId,
	ts.ReportTypeID, ts.DocumentID,
	-- Document Stuff
	d.DAMDocumentId, d.DocumentDate, d.PublicationDateTime, d.FormTypeID,
	d.ExportFlag
from Timeseries ts
join Document d on ts.DocumentId = d.id
where ts.Id = @tsId
";

			using (SqlCommand cmd = new SqlCommand(preQuery_timeseriesComponent, conn)) {
				cmd.Parameters.Add(new SqlParameter("@tsId", SqlDbType.UniqueIdentifier) { Value = timeseriesId });

				using (SqlDataReader reader = cmd.ExecuteReader(CommandBehavior.SingleResult | CommandBehavior.SingleRow | CommandBehavior.SequentialAccess)) {
					while (reader.Read()) {
						SFTimeseriesDTO ts = new SFTimeseriesDTO();

						int c = 0;
						ts.PeriodLength = reader.GetInt32(c++);
						ts.PeriodType = reader.GetString(c++);
						ts.InterimType = reader.GetString(c++);
						ts.PeriodEndDate = reader.GetDateTime(c++);
						ts.CompanyFiscalYear = Convert.ToInt16(reader.GetDecimal(c++));
						ts.IsRecap = reader.GetBoolean(c++);
						ts.IsAutoCalc = reader.GetInt32(c++) == 1;
						ts.VoyagerFormType = reader.GetString(c++);
						ts.ReportType = reader.GetString(c++);
						ts.SFDocumentId = reader.GetGuid(c++);

						return ts;
					}
				}
			}

			return null;
		}

		private Dictionary<string, TimeseriesValueDTO> _GetTimeseriesSDBValues(SqlConnection conn, int templateMasterId, Guid timeseriesId) {
			string preQuery_timeseriesSDBValues = @"
select 
	-- Shared
	tsd.SDBItemId, tsd.SDBItemTypeId,
	-- Per ItemType
	tsd.Value, tsd.ExpressionId,
	tsd.Date,
	tsd.BookValueFlagId,
	tsd.EarningsFlagId,
	tsd.EarningsCodeId
from vw_SDBTimeSeriesDetail tsd
join SDBTemplateItem sdbti on tsd.sdbitemId = sdbti.SDBItemID
where tsd.TimeSeriesId = @tsId
	and sdbti.SDBTemplateMasterID = @templMasterId
";

			Dictionary<string, TimeseriesValueDTO> toRet = new Dictionary<string, TimeseriesValueDTO>();

			using (SqlCommand cmd = new SqlCommand(preQuery_timeseriesSDBValues, conn)) {
				cmd.Parameters.Add(new SqlParameter("@tsId", SqlDbType.UniqueIdentifier) { Value = timeseriesId });
				cmd.Parameters.Add(new SqlParameter("@templMasterId", SqlDbType.Int) { Value = templateMasterId });

				using (SqlDataReader reader = cmd.ExecuteReader(CommandBehavior.SingleResult | CommandBehavior.SequentialAccess)) {
					while (reader.Read()) {
						TimeseriesValueDTO val = new TimeseriesValueDTO();

						int c = 0;
						string sdbItem = reader.GetInt32(c++).ToString();
						string itemType = reader.GetString(c++).ToUpper();

						if (itemType == "E") { // Expression
							c = 2;
							decimal value = reader.GetDecimal(c++);
							int expressionId = reader.GetInt32(c++);

							val.Contents = value.ToString();
							val.ValueDetails = new ExpressionTimeseriesValueDetailDTO() { Id = expressionId };
						} else if (itemType == "T") { // Date Time
							c = 4;
							DateTime date = reader.GetDateTime(c++);

							val.Contents = date.ToString();
							val.ValueDetails = new DateTimeseriesValueDetailDTO() { Date = date };
						} else if (itemType == "B") { // Book Value
							c = 5;
							int flag = reader.GetInt32(c++);

							val.Contents = flag.ToString();
							val.ValueDetails = new LookupTimeseriesValueDetailDTO() { LookupName = "BookValue", Value = flag.ToString() };
						} else if (itemType == "F") { // Earnings Flag
							c = 6;
							int flag = reader.GetInt32(c++);

							val.Contents = flag.ToString();
							val.ValueDetails = new LookupTimeseriesValueDetailDTO() { LookupName = "EarningsFlag", Value = flag.ToString() };
						} else if (itemType == "G") { // Earnings Code
							c = 7;
							int flag = reader.GetInt32(c++);

							val.Contents = flag.ToString();
							val.ValueDetails = new LookupTimeseriesValueDetailDTO() { LookupName = "EarningsCode", Value = flag.ToString() };
						} else {
							// Uh oh! Unknown item type!
						}

						toRet.Add(sdbItem, val);
					}
				}
			}

			foreach (TimeseriesValueDTO val in toRet.Values) {
				if (val.ValueDetails is ExpressionTimeseriesValueDetailDTO) {
					_ExpandExpressionValueDetail(conn, val, StandardizationType.SDB);
				}
			}

			return toRet;
		}

		private Dictionary<string, TimeseriesValueDTO> _GetTimeseriesSTDValues(SqlConnection conn, string templateMasterId, Guid timeseriesId) {
			string preQuery_timeseriesSDBValues = @"
select 
	tsd.STDItemId, tsd.STDItemTypeId, tsd.Value, tsd.STDExpressionID,
	tsd.Date, tsd.BookValueFlagID, tsd.EarningsFlagID, tsd.EarningsCodeID 
from vw_STDTimeSeriesDetail tsd
join STDTemplateItem stdti on tsd.STDItemId = stdti.STDItemID
where tsd.TimeSeriesId =  @tsId
	and stdti.STDTemplateMasterCode = @templMasterId
";

			Dictionary<string, TimeseriesValueDTO> toRet = new Dictionary<string, TimeseriesValueDTO>();

			using (SqlCommand cmd = new SqlCommand(preQuery_timeseriesSDBValues, conn)) {
				cmd.Parameters.Add(new SqlParameter("@tsId", SqlDbType.UniqueIdentifier) { Value = timeseriesId });
				cmd.Parameters.Add(new SqlParameter("@templMasterId", SqlDbType.VarChar, 4) { Value = templateMasterId });

				using (SqlDataReader reader = cmd.ExecuteReader(CommandBehavior.SingleResult | CommandBehavior.SequentialAccess)) {
					while (reader.Read()) {
						TimeseriesValueDTO val = new TimeseriesValueDTO();

						int c = 0;
						string sdbItem = reader.GetInt32(c++).ToString();
						string itemType = reader.GetString(c++).ToUpper();

						if (itemType == "E") { // Expression
							c = 2;
							decimal value = reader.GetDecimal(c++);
							int expressionId = reader.GetInt32(c++);

							val.Contents = value.ToString();
							val.ValueDetails = new ExpressionTimeseriesValueDetailDTO() { Id = expressionId };
						} else if (itemType == "T") { // Date Time
							c = 4;
							DateTime date = reader.GetDateTime(c++);

							val.Contents = date.ToString();
							val.ValueDetails = new DateTimeseriesValueDetailDTO() { Date = date };
						} else if (itemType == "B") { // Book Value
							c = 5;
							int flag = reader.GetInt32(c++);

							val.Contents = flag.ToString();
							val.ValueDetails = new LookupTimeseriesValueDetailDTO() { LookupName = "BookValue", Value = flag.ToString() };
						} else if (itemType == "F") { // Earnings Flag
							c = 6;
							int flag = reader.GetInt32(c++);

							val.Contents = flag.ToString();
							val.ValueDetails = new LookupTimeseriesValueDetailDTO() { LookupName = "EarningsFlag", Value = flag.ToString() };
						} else if (itemType == "G") { // Earnings Code
							c = 7;
							int flag = reader.GetInt32(c++);

							val.Contents = flag.ToString();
							val.ValueDetails = new LookupTimeseriesValueDetailDTO() { LookupName = "EarningsCode", Value = flag.ToString() };
						} else {
							// Uh oh! Unknown item type!
						}

						toRet.Add(sdbItem, val);
					}
				}
			}

			foreach (TimeseriesValueDTO val in toRet.Values) {
				if (val.ValueDetails is ExpressionTimeseriesValueDetailDTO) {
					_ExpandExpressionValueDetail(conn, val, StandardizationType.STD);
				}
			}

			return toRet;
		}

		private void _ExpandExpressionValueDetail(SqlConnection conn, TimeseriesValueDTO value, StandardizationType dataType) {
			ExpressionTimeseriesValueDetailDTO expValue = value.ValueDetails as ExpressionTimeseriesValueDetailDTO;
			if (expValue == null) {
				return;
			}

			string preQuery_expressionExpansion = dataType == StandardizationType.SDB ? @"
;WITH CTE (rootExpressionId, expressionId) AS (
	SELECT e.id, e.id
	FROM Expression e
	WHERE e.id = @expressionId

	UNION ALL

	SELECT CTE.rootExpressionId, e.id
	FROM CTE
	JOIN Expression cExp on CTE.expressionId = cExp.id
	JOIN Expression e on cExp.LRef1 = e.ID OR cExp.LRef2 = e.ID
)
SELECT c.expressionId, op.[Description], e.Value1, e.LRef1, e.FRef1, e.Value2, e.LRef2, e.FRef2
FROM CTE c
JOIN Expression e on c.expressionId = e.ID
JOIN OperationType op on e.OperationTypeID = op.ID
" : @"WITH CTE (rootExpressionId, expressionId) AS (
	SELECT e.id, e.id
	FROM STDExpression e
	WHERE e.id = @expressionId

	UNION ALL

	SELECT CTE.rootExpressionId, e.id
	FROM CTE
	JOIN STDExpression cExp on CTE.expressionId = cExp.id
	JOIN STDExpression e on cExp.LRef1 = e.ID OR cExp.LRef2 = e.ID
)
SELECT c.expressionId, op.[Description], e.Value1, e.LRef1, e.FRef1, e.Value2, e.LRef2, e.FRef2
FROM CTE c
JOIN STDExpression e on c.expressionId = e.ID
JOIN OperationType op on e.OperationTypeID = op.ID
";

			Dictionary<int, ExpressionTimeseriesValueDetailDTO> nodes = new Dictionary<int, ExpressionTimeseriesValueDetailDTO>();
			HashSet<int> tableCells = new HashSet<int>();

			using (SqlCommand cmd = new SqlCommand(preQuery_expressionExpansion, conn)) {
				cmd.Parameters.Add(new SqlParameter("@expressionId", SqlDbType.Int) { Value = expValue.Id });

				using (SqlDataReader reader = cmd.ExecuteReader(CommandBehavior.SingleResult | CommandBehavior.SequentialAccess)) {
					while (reader.Read()) {
						ExpressionTimeseriesValueDetailDTO node = new ExpressionTimeseriesValueDetailDTO();

						int c = 0;
						int id = reader.GetInt32(c++);
						string op = reader.GetString(c++);

						node.Id = id;
						node.Operation = op;

						// Left node
						double? val = reader.GetNullable<double>(c++);
						int? lref = reader.GetNullable<int>(c++);
						int? fref = reader.GetNullable<int>(c++);

						if (val.HasValue) {
							node.LeftNode = new ValueExpressionNode() { Value = (decimal)val.Value };
						} else if (lref.HasValue) {
							ExpressionTimeseriesValueDetailDTO subnode = new ExpressionTimeseriesValueDetailDTO() { Id = lref.Value };
							node.LeftNode = new SubexpressionExpressionNode() { Expression = subnode };
						} else if (fref.HasValue) {
							node.LeftNode = new SFCellExpressionNode() { TableCellId = fref.Value };
							tableCells.Add(fref.Value);
						} else {
							// Uh oh! Unknown node type!
						}

						// Right Node
						val = reader.GetNullable<double>(c++);
						lref = reader.GetNullable<int>(c++);
						fref = reader.GetNullable<int>(c++);

						if (val.HasValue) {
							node.RightNode = new ValueExpressionNode() { Value = (decimal)val.Value };
						} else if (lref.HasValue) {
							ExpressionTimeseriesValueDetailDTO subnode = new ExpressionTimeseriesValueDetailDTO() { Id = lref.Value };
							node.RightNode = new SubexpressionExpressionNode() { Expression = subnode };
						} else if (fref.HasValue) {
							node.RightNode = new SFCellExpressionNode() { TableCellId = fref.Value };
							tableCells.Add(fref.Value);
						} else {
							// Uh oh! Unknown node type!
						}

						nodes.Add(id, node);
					}
				}
			}

			Dictionary<int, CellExpressionNode> cells = new Dictionary<int, CellExpressionNode>();
			foreach (int cellId in tableCells) {
				cells.Add(cellId, _GetTableCellDetails(conn, cellId));
			}

			foreach (ExpressionTimeseriesValueDetailDTO v in nodes.Values) {
				if (v.LeftNode is SubexpressionExpressionNode) {
					int expId = (v.LeftNode as SubexpressionExpressionNode).Expression.Id;
					v.LeftNode = new SubexpressionExpressionNode() { Expression = nodes[expId] };
				} else if (v.LeftNode is CellExpressionNode) {
					int tableCellId = (v.LeftNode as SFCellExpressionNode).TableCellId;
					v.LeftNode = cells[tableCellId];
				}

				if (v.RightNode is SubexpressionExpressionNode) {
					int expId = (v.RightNode as SubexpressionExpressionNode).Expression.Id;
					v.RightNode = new SubexpressionExpressionNode() { Expression = nodes[expId] };
				} else if (v.RightNode is CellExpressionNode) {
					int tableCellId = (v.RightNode as SFCellExpressionNode).TableCellId;
					v.RightNode = cells[tableCellId];
				}
			}

			value.ValueDetails = nodes[expValue.Id];
		}

		private CellExpressionNode _GetTableCellDetails(SqlConnection conn, int tableCellId) {
			string preQuery_cellExpansion = @"
SELECT DISTINCT tc.id, d.ID, d.DAMDocumentId, tc.Offset, tc.ValueNumeric, tc.Value, tc.Currency, sf.Value, tc.CompanyFinancialTermID, cft.[Description]
FROM tablecell tc
JOIN DimensionToCell dtc on tc.id = dtc.TableCellID
JOIN TableDimension td on dtc.TableDimensionID = td.ID
JOIN DocumentTable dt on td.DocumentTableID = dt.ID
JOIN Document d on dt.DocumentID = d.id
JOIN ScalingFactor sf on tc.ScalingFactorID = sf.ID
LEFT JOIN CompanyFinancialTerm cft on tc.CompanyFinancialTermID = cft.ID
WHERE tc.ID = @tcId
";

			using (SqlCommand cmd = new SqlCommand(preQuery_cellExpansion, conn)) {
				cmd.Parameters.Add(new SqlParameter("@tcId", SqlDbType.Int) { Value = tableCellId });

				using (SqlDataReader reader = cmd.ExecuteReader(CommandBehavior.SingleResult | CommandBehavior.SingleRow | CommandBehavior.SequentialAccess)) {
					while (reader.Read()) {
						int c = 0;

						int id = reader.GetInt32(c++);
						Guid docId = reader.GetGuid(c++);
						Guid damDocId = reader.GetGuid(c++);
						string off = reader.GetString(c++);
						decimal num = reader.GetDecimal(c++);
						string val = reader.GetString(c++);
						string curr = reader.GetNullableObj<string>(c++);
						double scaling = reader.GetDouble(c++);
						int? cftId = reader.GetNullable<int>(c++);
						string cftLabel = reader.GetString(c++);

						Tuple<int, FLYTOffset> flytOff = _parseSourcelink(off);

						SFCellExpressionNode node = new SFCellExpressionNode()
						{
							TableCellId = id,
							SFDocumentId = docId,
							DAMDocumentId = damDocId,
							DAMRootId = flytOff.Item1,
							Offset = flytOff.Item2,
							NumericValue = num,
							AsPresentedValue = val,
							Currency = curr,
							ScalingBase10 = Convert.ToInt32(Math.Log10(scaling)),
							CompanyFinancialTermId = cftId,
							CompanyFinancialTermLabel = cftLabel
						};
						return node;
					}
				}
			}

			return null;
		}

		private Tuple<int, FLYTOffset> _parseSourcelink(string flyt) {
			string[] comp = flyt.Split('|');

			int rootId = Int32.Parse(comp[2].Substring(1));

			if (comp[0][0] == 'p') {
				string[] bbox = comp[1].Substring(1).Split(',');
				PDFOffset off = new PDFOffset()
				{
					Page = Int32.Parse(comp[0].Substring(1)),
					Left = Int32.Parse(bbox[0]),
					Top = Int32.Parse(bbox[1]),
					Right = Int32.Parse(bbox[2]),
					Bottom = Int32.Parse(bbox[3])
				};

				return new Tuple<int, FLYTOffset>(rootId, off);
			} else if (comp[0][0] == 'o') {
				HTMLOffset off = new HTMLOffset()
				{
					Offset = Int32.Parse(comp[0].Substring(1)),
					Length = Int32.Parse(comp[1].Substring(1))
				};

				return new Tuple<int, FLYTOffset>(rootId, off);
			} else {
				// Uh oh! Unknown offset type!
				return null;
			}
		}
	}
}