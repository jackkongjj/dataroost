using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using CCS.Fundamentals.DataRoostAPI.Helpers;
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

		public TimeseriesDTO[] QuerySDBTimeseries(int iconum, TemplateIdentifier templateId, TimeseriesIdentifier timeseriesId, StandardizationType dataType,string statementType , NameValueCollection queryFilter = null) {
			string preQuery_timeseriesIdentification = dataType == StandardizationType.SDB ? @"
select  distinct ts.id from supercore.timeslice ts with (nolock)
join supercore.documenttimeslice dts with (nolock) on dts.timesliceid = ts.id
join DocumentSeries ds on dts.DocumentSeriesID = ds.ID
left join Document d on d.DAMDocumentId = dts.DAMDocumentId and dts.DocumentSeriesID = d.DocumentSeriesID 
join supercore.StatementModelDetail smd with(nolock) on smd.TimeSliceID = ts.ID
join SuperCore.ModelMaster mm with (nolock) on mm.Id = smd.modelmasterid
join SuperCore.ModelScreenAssociation msa with (nolock) on msa.ModelMasterId = mm.Id and ts.IndustryCountryAssociationID = msa.IndustryCountryAssociationId
join Supercore.ScreenMaster sm WITH (NOLOCK) ON sm.Id = msa.ScreenMasterId and ts.ReportTypeID = sm.ReportTypeId
--join supercore.ScreenDetail sd on sm.id = sd.ScreenMasterId
where  ds.CompanyID = @iconum	and ts.InterimTypeID != '--' and collectiontypeid = 'V' and mm.StatementTypeID = @StatementTypeID
and sm.id = @ScreenMasterId

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
	UNION
	select Code from STDTemplateMaster where TemplateName = 'SF Full - Pension' AND  'A' = @reportTypeId
	AND 'N' = @updateTypeId
	AND 1 = @templateTypeId
)
select distinct ts.id, tmi.Code
from Timeseries ts
join document d on ts.DocumentID = d.ID
join DocumentSeries ds on d.DocumentSeriesID = ds.ID
join vw_STDTimeSeriesDetail stdd on ts.ID = stdd.TimeSeriesId
join STDTemplateItem stdti on stdd.STDItemId = stdti.STDItemID
join TemplateMasterID tmi on tmi.Code = stdti.STDTemplateMasterCode
where ds.CompanyID = @iconum and ts.InterimTypeID != '--' and d.exportflag = 1 ";

		
			bool requestedSpecificTimeSerie = (timeseriesId != null);
			string templateMasterId = string.Empty;

			if (requestedSpecificTimeSerie) {
					 preQuery_timeseriesIdentification += @" --and d.id = isnull(@SFDocumentId, d.id)
	and ts.CompanyFiscalYear = isnull(@FiscalYear, ts.CompanyFiscalYear)
	and ts.TimeSliceDate = isnull(@PeriodEndDate, ts.TimeSliceDate)
	and ts.InterimTypeID = isnull(@InterimType, ts.InterimTypeID)
	and ts.AutoCalcFlag = isnull(@AutoCalcFlag, ts.AutoCalcFlag)
	and ts.AccountTypeId = isnull(@AccountType, ts.AccountTypeId)";
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
                    cmd.Parameters.AddWithValue("@StatementTypeID",statementType);
                    cmd.Parameters.AddWithValue("@ScreenMasterId",templateId.TemplateId);
					if (requestedSpecificTimeSerie) {
						cmd.Parameters.Add(new SqlParameter("@SFDocumentId", SqlDbType.UniqueIdentifier) { Value = timeseriesId.SFDocumentId });
						cmd.Parameters.Add(new SqlParameter("@FiscalYear", SqlDbType.Decimal) { Value = timeseriesId.CompanyFiscalYear });
						cmd.Parameters.Add(new SqlParameter("@PeriodEndDate", SqlDbType.DateTime) { Value = timeseriesId.PeriodEndDate });
						cmd.Parameters.Add(new SqlParameter("@InterimType", SqlDbType.Char, 2) { Value = timeseriesId.InterimType });
						cmd.Parameters.Add(new SqlParameter("@AutoCalcFlag", SqlDbType.Int) { Value = (timeseriesId.IsAutoCalc ? 1 : 0) });
						cmd.Parameters.Add(new SqlParameter("@AccountType", SqlDbType.Char, 1) { Value = timeseriesId.AccountType });
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
							
							timeseries.Add(reader.GetGuid(0));
						
						}
					}
				}
                templateMasterId = templateId.TemplateId.ToString();

                if (dataType == StandardizationType.SDB)
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
select distinct 
	-- Timeseries Components
	ts.PeriodLength, ts.PeriodTypeID, ts.InterimTypeID, ts.TimeSliceDate,
	ts.CompanyFiscalYear, ts.EncoreFlag, ts.AutoCalcFlag, d.VoyFormTypeId,
	ts.ReportTypeID, d.ID,
	-- Document Stuff
	d.DAMDocumentId,
	d.PublicationDateTime,
	ts.CurrencyCode, ts.ScalingFactorID,
  d.DocumentDate, map.stdcode,
   ts.AccountTypeID,
	d.FormTypeID,	isnull(d.ExportFlag,0)
from SuperCore.TimeSlice ts
left join SuperCore.DocumentTimeSlice dts on ts.ID = dts.timesliceid
left join Document d on d.DAMDocumentId = dts.DamDocumentID and d.DocumentSeriesID = dts.DocumentSeriesID
left join SDBSTDTimeSeriesMapping map on map.ReportTypeID = ts.ReportTypeID and map.InterimTypeID = ts.InterimTypeID and map.AccountTypeID = ts.AccountTypeId
where  ts.CollectionTypeId = 'V' and ts.Id = @tsId
";

			using (SqlCommand cmd = new SqlCommand(preQuery_timeseriesComponent, conn)) {
				cmd.Parameters.Add(new SqlParameter("@tsId", SqlDbType.UniqueIdentifier) { Value = timeseriesId });

				using (SqlDataReader reader = cmd.ExecuteReader()) {
					while (reader.Read()) {
						SFTimeseriesDTO ts = new SFTimeseriesDTO();

					
						ts.PeriodLength = reader.GetInt32(0);
						ts.PeriodType = reader.GetString(1);
						ts.InterimType = reader.GetString(2);
						ts.PeriodEndDate = reader.GetDateTime(3);
						ts.CompanyFiscalYear = Convert.ToInt16(reader.GetDecimal(4));
						ts.IsRecap = reader.GetBoolean(5);
						ts.IsAutoCalc = reader.GetInt32(6) == 1;
						ts.VoyagerFormType = reader.GetStringSafe(7);
						ts.ReportType = reader.GetString(8);
						ts.SFDocumentId = reader.GetNullable<Guid>(9);
						ts.DamDocumentId = reader.GetNullable<Guid>(10);
						ts.PublicationDate = reader[11] == DBNull.Value ? DateTime.MinValue : reader.GetDateTime(11);
						ts.IsoCurrency = reader.GetStringSafe(12);
						ts.ScalingFactor = reader.GetStringSafe(13);
						ts.DocumentDate = reader[14] == DBNull.Value ? DateTime.MinValue : reader.GetDateTime(14);
						ts.StdTimeSeriesCode = reader.GetStringSafe(15);
						ts.AccountType = reader.GetStringSafe(16);
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
	tsd.SDBItemId, si.SDBItemTypeId,
	-- Per ItemType
	tsd.Value, tsd.ExpressionId
from SuperCore.SDBTimeSliceDetail tsd
join SuperCore.ScreenDetail sdbti on tsd.sdbitemId = sdbti.SDBItemID
join sdbitem si on si.id = tsd.sdbitemId 
where tsd.TimeSliceID = @tsId
	and sdbti.ScreenMasterId = @templMasterId
";

			Dictionary<string, TimeseriesValueDTO> toRet = new Dictionary<string, TimeseriesValueDTO>();

			using (SqlCommand cmd = new SqlCommand(preQuery_timeseriesSDBValues, conn)) {
				cmd.Parameters.Add(new SqlParameter("@tsId", SqlDbType.UniqueIdentifier) { Value = timeseriesId });
				cmd.Parameters.Add(new SqlParameter("@templMasterId", SqlDbType.Int) { Value = templateMasterId });

				using (SqlDataReader reader = cmd.ExecuteReader(CommandBehavior.SingleResult | CommandBehavior.SequentialAccess)) {
					while (reader.Read()) {
						TimeseriesValueDTO val = new TimeseriesValueDTO();

						
						string sdbItem = reader.GetInt32(0).ToString();
						string itemType = reader.GetString(1).ToUpper();
                        string value = reader.GetStringSafe(2);
                        string expression = reader.GetStringSafe(3);
						if (itemType == "E") { // Expression
							
							val.Contents = value.ToString();
                            val.ValueDetails = new ExpressionTimeseriesValueDetailDTO { Id = expression };
						} else if (itemType == "T") { // Date Time

                            DateTime date = DateTime.Parse(value);

							val.Contents = date.ToString();
							val.ValueDetails = new DateTimeseriesValueDetailDTO() { Date = date };
						} else if (itemType == "B") { // Book Value
							
                            int flag = int.Parse(value);

							val.Contents = flag.ToString();
							val.ValueDetails = new LookupTimeseriesValueDetailDTO() { LookupName = "BookValue", Value = flag.ToString() };
						} else if (itemType == "F") { // Earnings Flag
							
							int flag =int.Parse(value);

							val.Contents = flag.ToString();
							val.ValueDetails = new LookupTimeseriesValueDetailDTO() { LookupName = "EarningsFlag", Value = flag.ToString() };
						} else if (itemType == "G") { // Earnings Code

                            int flag = int.Parse(value);

							val.Contents = flag.ToString();
							val.ValueDetails = new LookupTimeseriesValueDetailDTO() { LookupName = "EarningsCode", Value = flag.ToString() };
						} else {
							// Uh oh! Unknown item type!
						}

						toRet.Add(sdbItem, val);
					}
				}
			}

            IEnumerable<string> expressionIds =  toRet.Values.Where(o => (o.ValueDetails as ExpressionTimeseriesValueDetailDTO) != null &&
            !string.IsNullOrEmpty((o.ValueDetails as ExpressionTimeseriesValueDetailDTO).Id)).Select((o => (o.ValueDetails as ExpressionTimeseriesValueDetailDTO).Id));


            ExprStoreClient expClient = new ExprStoreClient(ConfigurationManager.AppSettings["ExpressionStore"].ToString());
            List<ExprObjectTree> exprObjectTrees =  expClient.SearchExpressionById(expressionIds);

           

			foreach (TimeseriesValueDTO val in toRet.Values) {
                ExpressionTimeseriesValueDetailDTO expVal = val.ValueDetails as ExpressionTimeseriesValueDetailDTO;

                if (expVal != null) {
                    expVal.MathMl = exprObjectTrees.Where(o => o.ExpressionId == expVal.Id).OrderBy(o => o.Key).ToList();
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
							int ind = c++;
							int expressionId = reader.IsDBNull(ind) ? 0 : reader.GetInt32(ind);

							val.Contents = value.ToString();
							val.ValueDetails = expressionId == 0 ? ((TimeseriesValueDetailDTO)new TextTimeseriesValueDetailDTO()) : new ExpressionTimeseriesValueDetailDTO() { Id = expressionId.ToString() };
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
					//_ExpandExpressionValueDetail(conn, val, StandardizationType.STD);
				}
			}

			return toRet;
		}

        private void _ExpandExpressionValueDetail(SqlConnection conn, TimeseriesValueDTO value, StandardizationType dataType)
        {
            ExpressionTimeseriesValueDetailDTO expValue = value.ValueDetails as ExpressionTimeseriesValueDetailDTO;
            if (expValue == null && !string.IsNullOrEmpty(expValue.Id) )
            {
                return;
            }

            ExprStoreClient expClient = new ExprStoreClient(ConfigurationManager.AppSettings["ExpressionStore"].ToString());
            expValue.MathMl = expClient.SearchExpressionById(new List<string> { expValue.Id }); 
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
						decimal? num = reader.GetNullable<decimal>(c++);
						string val = reader.GetNullableObj<string>(c++);
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
			if (string.IsNullOrWhiteSpace(flyt)) { return new Tuple<int, FLYTOffset>(0, new PDFOffset()); }
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