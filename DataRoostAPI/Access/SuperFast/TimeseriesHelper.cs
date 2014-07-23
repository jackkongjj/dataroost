using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Web;
using CCS.Fundamentals.DataRoostAPI.Models;
using CCS.Fundamentals.DataRoostAPI.Models.TimeseriesValues;
using FactSet.Data.SqlClient;

namespace CCS.Fundamentals.DataRoostAPI.Access.SuperFast {
	public class TimeseriesHelper {
		private readonly string connectionString;

		public TimeseriesHelper(string connectionString) {
			this.connectionString = connectionString;
		}

		public TimeseriesDTO[] QuerySDBTimeseries(int iconum, TemplateIdentifier templateId) {
			string preQuery_timeseriesIdentification = @"
select distinct ts.id
from Timeseries ts
join Document d on ts.DocumentID = d.id
join DocumentSeries ds on d.DocumentSeriesID = ds.ID
join vw_SDBTimeSeriesDetail sdbd on ts.id = sdbd.TimeSeriesId
join SDBTemplateItem sdbti on sdbd.sdbitemId = sdbti.SDBItemID
where ds.CompanyID = @iconum
	and sdbti.SDBTemplateMasterID = @templMasterId
";

			TemplatesHelper th = new TemplatesHelper(connectionString, iconum, StandardizationType.SDB);
			int templMaster = th.GetTemplateMasterId(templateId);

			using (SqlConnection conn = new SqlConnection(connectionString)) {
				conn.Open();

				HashSet<Guid> timeseries = new HashSet<Guid>();

				using (SqlCommand cmd = new SqlCommand(preQuery_timeseriesIdentification, conn)) {
					cmd.Parameters.Add(new SqlParameter("@iconum", SqlDbType.Int) { Value = iconum });
					cmd.Parameters.Add(new SqlParameter("@templMasterId", SqlDbType.Int) { Value = templMaster });

					using (SqlDataReader reader = cmd.ExecuteReader(CommandBehavior.SingleResult | CommandBehavior.SequentialAccess)) {
						while (reader.Read()) {
							int c = 0;

							timeseries.Add(reader.GetGuid(c++));
						}
					}
				}

				return ConvertSDBTimeseries(conn, timeseries, null);
			}
		}

		public TimeseriesDTO[] GetSDBTemplatesTimeseries(int iconum, TemplateIdentifier templateId, string timeseriesId) {
			string preQuery_timeseriesFromId = @"
select distinct ts.id
from Timeseries ts
join Document d on ts.DocumentID = d.id
join DocumentSeries ds on d.DocumentSeriesID = ds.ID
join vw_SDBTimeSeriesDetail sdbd on ts.id = sdbd.TimeSeriesId
join SDBTemplateItem sdbti on sdbd.sdbitemId = sdbti.SDBItemID
join SDBItem sdb on sdbd.SDBItemId = sdb.ID
where ds.CompanyID = @iconum
	and sdbti.SDBTemplateMasterID = @templMasterId
	and d.id = @SFDocumentId
	and ts.CompanyFiscalYear = @FiscalYear
	and ts.TimeSeriesDate = @PeriodEndDate
	and ts.InterimTypeID = @InterimType
	and ts.AutoCalcFlag = @AutoCalcFlag
";
			TimeseriesIdentifier tsId = new TimeseriesIdentifier(timeseriesId);
			TemplatesHelper th = new TemplatesHelper(connectionString, iconum, StandardizationType.SDB);
			int templMaster = th.GetTemplateMasterId(templateId);

			using (SqlConnection conn = new SqlConnection(connectionString)) {
				conn.Open();

				HashSet<Guid> timeseries = new HashSet<Guid>();

				using (SqlCommand cmd = new SqlCommand(preQuery_timeseriesFromId, conn)) {
					cmd.Parameters.Add(new SqlParameter("@iconum", SqlDbType.Int) { Value = iconum });
					cmd.Parameters.Add(new SqlParameter("@templMasterId", SqlDbType.Int) { Value = templMaster });
					cmd.Parameters.Add(new SqlParameter("@SFDocumentId", SqlDbType.UniqueIdentifier) { Value = tsId.SFDocumentId });
					cmd.Parameters.Add(new SqlParameter("@FiscalYear", SqlDbType.Decimal) { Value = tsId.CompanyFiscalYear });
					cmd.Parameters.Add(new SqlParameter("@PeriodEndDate", SqlDbType.DateTime) { Value = tsId.PeriodEndDate });
					cmd.Parameters.Add(new SqlParameter("@InterimType", SqlDbType.Char, 2) { Value = tsId.InterimType });
					cmd.Parameters.Add(new SqlParameter("@AutoCalcFlag", SqlDbType.Int) { Value = (tsId.IsAutoCalc ? 1 : 0) });

					using (SqlDataReader reader = cmd.ExecuteReader(CommandBehavior.SingleResult | CommandBehavior.SequentialAccess)) {
						while (reader.Read()) {
							int c = 0;

							timeseries.Add(reader.GetGuid(c++));
						}
					}
				}

				return ConvertSDBTimeseries(conn, timeseries, templMaster);
			}
		}

		private TimeseriesDTO[] ConvertSDBTimeseries(SqlConnection conn, HashSet<Guid> timeseriesIds, int? templateMasterId) {
			List<TimeseriesDTO> timeseries = new List<TimeseriesDTO>();
			foreach (Guid timeseriesId in timeseriesIds) {
				TimeseriesDTO ts = _GetSDBTimeseriesDTO(conn, timeseriesId);

				if (templateMasterId.HasValue) {
					ts.Values = _GetTimeseriesSDBValues(conn, templateMasterId.Value, timeseriesId);
				}

				timeseries.Add(ts);
			}

			// Condense our PIT/Duration Timeseries into single instance
			TimeseriesDTO[] condensed = timeseries
				.GroupBy(x => new TimeseriesIdentifier(x).GetToken())
				.Select(x =>
				{
					TimeseriesDTO t = null;

					using (IEnumerator<TimeseriesDTO> e = x.GetEnumerator()) {
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

		private TimeseriesDTO _GetSDBTimeseriesDTO(SqlConnection conn, Guid timeseriesId) {
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
						TimeseriesDTO ts = new TimeseriesDTO();

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

		private Dictionary<int, TimeseriesValueDTO> _GetTimeseriesSDBValues(SqlConnection conn, int templateMasterId, Guid timeseriesId) {
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

			Dictionary<int, TimeseriesValueDTO> toRet = new Dictionary<int, TimeseriesValueDTO>();

			using (SqlCommand cmd = new SqlCommand(preQuery_timeseriesSDBValues, conn)) {
				cmd.Parameters.Add(new SqlParameter("@tsId", SqlDbType.UniqueIdentifier) { Value = timeseriesId });
				cmd.Parameters.Add(new SqlParameter("@templMasterId", SqlDbType.Int) { Value = templateMasterId });

				using (SqlDataReader reader = cmd.ExecuteReader(CommandBehavior.SingleResult | CommandBehavior.SequentialAccess)) {
					while (reader.Read()) {
						TimeseriesValueDTO val = new TimeseriesValueDTO();

						int c = 0;
						int sdbItem = reader.GetInt32(c++);
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
					_ExpandExpressionValueDetail(conn, val);
				}
			}

			return toRet;
		}

		private void _ExpandExpressionValueDetail(SqlConnection conn, TimeseriesValueDTO value) {
			ExpressionTimeseriesValueDetailDTO expValue = value.ValueDetails as ExpressionTimeseriesValueDetailDTO;
			if (expValue == null) {
				return;
			}

			string preQuery_expressionExpansion = @"
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
							node.LeftNode = new CellExpressionNode() { TableCellId = fref.Value };
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
							node.RightNode = new CellExpressionNode() { TableCellId = fref.Value };
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
					int tableCellId = (v.LeftNode as CellExpressionNode).TableCellId;
					v.LeftNode = cells[tableCellId];
				}

				if (v.RightNode is SubexpressionExpressionNode) {
					int expId = (v.RightNode as SubexpressionExpressionNode).Expression.Id;
					v.RightNode = new SubexpressionExpressionNode() { Expression = nodes[expId] };
				} else if (v.RightNode is CellExpressionNode) {
					int tableCellId = (v.RightNode as CellExpressionNode).TableCellId;
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

						CellExpressionNode node = new CellExpressionNode()
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