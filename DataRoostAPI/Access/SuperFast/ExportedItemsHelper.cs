using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;

using DataRoostAPI.Common.Models;

using FactSet.Data.SqlClient;

using Fundamentals.Helpers.DataAccess;

using CCS.Fundamentals.DataRoostAPI.Helpers;

namespace CCS.Fundamentals.DataRoostAPI.Access.SuperFast {

	public class ExportedItemsHelper : SqlHelper {

		private readonly string _sfConnectionString;

		public ExportedItemsHelper(string sfConnectionString) {
			_sfConnectionString = sfConnectionString;
		}

		public ExportedItem[] GetExportedItems(StandardizationType standardizationType,
		                                       List<string> itemCodes,
		                                       DateTime startDate,
		                                       DateTime endDate,
																					 List<string> countries = null) {
			const string sdbQuery =
				@"SELECT tmp.CompanyID, tmp.DocumentDate, tmp.FormTypeID, tmp.PublicationDateTime, tmp.DAMDocumentId, fds.IsoCountry FROM
																(select DISTINCT ds.CompanyID, d.DocumentDate, d.FormTypeID, d.PublicationDateTime, d.DAMDocumentId 
																	from Document d (nolock)
																			join DocumentSeries ds (nolock)
																					on d.DocumentSeriesID = ds.ID
																			join ExportedDocumentLog edl (nolock)
																					on edl.DocumentID = d.DAMDocumentId
																					and edl.CompanyID = ds.CompanyID
																			join TimeSeries ts 
																					on d.Id = ts.DocumentId
																					and ts.EncoreFlag = 0
																					and ts.AutoCalcFlag = 0
																			join SDBTimeSeriesDetailSecurity sdbs (nolock)
																					on ts.ID = sdbs.TimeSeriesID
																			join SDBItem s (nolock)
																					on sdbs.SdbItemId = s.id
																	where EndTimeStamp between @startDate and @endDate
																		and @itemCodes is null
																	union
																	select DISTINCT ds.CompanyID, d.DocumentDate, d.FormTypeID, d.PublicationDateTime, d.DAMDocumentId
																	from Document d (nolock)
																			join DocumentSeries ds (nolock)
																					on d.DocumentSeriesID = ds.ID
																			join ExportedDocumentLog edl (nolock)
																					on edl.DocumentID = d.DAMDocumentId
																					and edl.CompanyID = ds.CompanyID
																			join TimeSeries ts 
																					on d.Id = ts.DocumentId
																					and ts.EncoreFlag = 0
																					and ts.AutoCalcFlag = 0
																			join vw_sdbtimeseriesdetail sdbs (nolock)
																					on ts.ID = sdbs.TimeSeriesID
																			join SDBItem s (nolock)
																					on sdbs.SdbItemId = s.id
																			join fnc_SplitString(@itemCodes, ',') as fnc
																					on s.SDBCode = CASE WHEN @itemCodes = '' THEN s.SDBCode ELSE fnc.token END
																	where EndTimeStamp between @startDate and @endDate) as tmp
																join FdsTriPpiMap fds on tmp.CompanyID = fds.iconum
																order by tmp.DocumentDate desc";

			const string stdQuery =
				@"SELECT tmp.CompanyID, tmp.DocumentDate, tmp.FormTypeID, tmp.PublicationDateTime, tmp.DAMDocumentId, fds.IsoCountry FROM
																(select DISTINCT ds.CompanyID, d.DocumentDate, d.FormTypeID, d.PublicationDateTime, d.DAMDocumentId
																	from Document d (nolock)
																			join DocumentSeries ds (nolock)
																					on d.DocumentSeriesID = ds.ID
																			join ExportedDocumentLog edl (nolock)
																					on edl.DocumentID = d.DAMDocumentId
																					and edl.CompanyID = ds.CompanyID
																			join TimeSeries ts 
																					on d.Id = ts.DocumentId
																					and ts.EncoreFlag = 0
																					and ts.AutoCalcFlag = 0
																			join STDTimeSeriesDetailSecurity stds (nolock)
																					on ts.ID = stds.TimeSeriesID
																			join STDItem s (nolock)
																					on stds.STDItemId = s.id
																	where EndTimeStamp between @startDate and @endDate
																		and @itemCodes is null
																union
																select DISTINCT ds.CompanyID, d.DocumentDate, d.FormTypeID, d.PublicationDateTime, d.DAMDocumentId
																	from Document d (nolock)
																			join DocumentSeries ds (nolock)
																					on d.DocumentSeriesID = ds.ID
																			join ExportedDocumentLog edl (nolock)
																					on edl.DocumentID = d.DAMDocumentId
																					and edl.CompanyID = ds.CompanyID
																			join TimeSeries ts 
																					on d.Id = ts.DocumentId
																					and ts.EncoreFlag = 0
																					and ts.AutoCalcFlag = 0
																			join vw_stdtimeseriesdetail stds (nolock)
																					on ts.ID = stds.TimeSeriesID
																			join STDItem s (nolock)
																					on stds.STDItemId = s.id
																			join fnc_SplitString(@itemCodes, ',') as fnc
																					on s.STDCode = fnc.token
																	where EndTimeStamp between @startDate and @endDate) as tmp
																join FdsTriPpiMap fds on tmp.CompanyID = fds.iconum
																order by tmp.CompanyID, tmp.DocumentDate, tmp.FormTypeID, tmp.PublicationDateTime";

			string query = stdQuery;
			if (standardizationType == StandardizationType.SDB) {
				query = sdbQuery;
			}

			string itemCodeString = string.Empty;
			if (itemCodes != null) {
				itemCodeString = string.Join(",", itemCodes);
			}

			IEnumerable<ExportedItem> items = ExecuteQuery(query,
			                                               new List<SqlParameter>
			                                               {
				                                               new SqlParameter("@startDate", startDate),
				                                               new SqlParameter("@endDate", endDate),
				                                               new SqlParameter("@itemCodes", itemCodeString),
			                                               },
			                                               reader => {
				                                               return new ExportedItem
				                                                      {
					                                                      Iconum = reader.GetInt32(0).ToString(),
					                                                      ReportDate = reader.GetDateTime(1),
					                                                      FormType = reader.GetStringSafe(2),
					                                                      PublicationDate = reader.GetDateTime(3),
					                                                      DocumentId = reader.GetGuid(4).ToString(),
																																Country = reader.GetStringSafe(5)
				                                                      };
			                                               });
			if (countries != null && countries.Count() > 0) {
				items = items.Where(i => i.Country.In(countries));
			}

			return items.ToArray();
		}

		public ExportedItem[] GetAllExportedShareItems(StandardizationType standardizationType,
																			 DateTime startDate,
																			 DateTime endDate,
																			 List<string> countries = null) {
			const string sdbQuery = @"select DISTINCT ds.CompanyID, d.DocumentDate, d.FormTypeID, d.PublicationDateTime, d.DAMDocumentId, fds.IsoCountry
																	from Document d (nolock)
																			join DocumentSeries ds (nolock)
																					on d.DocumentSeriesID = ds.ID
																			join FdsTriPpiMap fds (nolock)
																					on fds.iconum = ds.CompanyID
																			join ExportedDocumentLog edl (nolock)
																					on edl.DocumentID = d.DAMDocumentId
																					and edl.CompanyID = ds.CompanyID
																			join TimeSeries ts 
																					on d.Id = ts.DocumentId
																					and ts.EncoreFlag = 0
																					and ts.AutoCalcFlag = 0
																			join SDBTimeSeriesDetailSecurity sdbs (nolock)
																					on ts.ID = sdbs.TimeSeriesID
																			join SDBItem s (nolock)
																					on sdbs.SdbItemId = s.id
																	where EndTimeStamp between @startDate and @endDate
																	order by s.Id, d.id, ts.TimeSeriesDate desc";

			const string stdQuery = @"select DISTINCT ds.CompanyID, d.DocumentDate, d.FormTypeID, d.PublicationDateTime, d.DAMDocumentId, fds.IsoCountry
																	from Document d (nolock)
																			join DocumentSeries ds (nolock)
																					on d.DocumentSeriesID = ds.ID
																			join FdsTriPpiMap fds (nolock)
																					on fds.iconum = ds.CompanyID
																			join ExportedDocumentLog edl (nolock)
																					on edl.DocumentID = d.DAMDocumentId
																					and edl.CompanyID = ds.CompanyID
																			join TimeSeries ts 
																					on d.Id = ts.DocumentId
																					and ts.EncoreFlag = 0
																					and ts.AutoCalcFlag = 0
																			join STDTimeSeriesDetailSecurity stds (nolock)
																					on ts.ID = stds.TimeSeriesID
																			join STDItem s (nolock)
																					on stds.STDItemId = s.id
																	where EndTimeStamp between @startDate and @endDate
																	order by ds.CompanyID, d.DocumentDate, d.FormTypeID, d.PublicationDateTime";
			string query = stdQuery;
			if (standardizationType == StandardizationType.SDB) {
				query = sdbQuery;
			}

			IEnumerable<ExportedItem> items = ExecuteQuery(query,
										 new List<SqlParameter>
				             {
					             new SqlParameter("@startDate", startDate),
					             new SqlParameter("@endDate", endDate),
				             },
										 reader =>
										 {
											 return new ExportedItem
											 {
												 Iconum = reader.GetInt32(0).ToString(),
												 ReportDate = reader.GetDateTime(1),
												 FormType = reader.GetStringSafe(2),
												 PublicationDate = reader.GetDateTime(3),
												 DocumentId = reader.GetGuid(4).ToString(),
												 Country = reader.GetStringSafe(5)
											 };
										 });
			if (countries != null && countries.Count() > 0) {
				items = items.Where(i => i.Country.In(countries));
			}

			return items.ToArray();
		}

		protected override SqlConnection GetDatabaseConnection() {
			return new SqlConnection(_sfConnectionString);
		}

	}

}
