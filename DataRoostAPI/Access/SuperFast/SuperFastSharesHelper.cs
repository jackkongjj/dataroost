using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Data;
using System.Data.SqlClient;

using DataRoostAPI.Common.Models;

using FactSet.Data.SqlClient;

namespace CCS.Fundamentals.DataRoostAPI.Access.SuperFast {
	public class SuperFastSharesHelper {

		private readonly string _connectionString;

		public SuperFastSharesHelper(string connectionString) {
			_connectionString = connectionString;
		}

		public Dictionary<string, List<ShareClassDataItem>> GetLatestCompanyFPEShareData(int iconum, DateTime? reportDate, DateTime? since) {
			Dictionary<string, List<ShareClassDataItem>> perShareData = new Dictionary<string, List<ShareClassDataItem>>();

			const string queryNonDC = @"SELECT temp.Cusip, temp.Value, temp.Date, temp.ItemName, temp.STDCode FROM 
                                        (SELECT stds.SecurityID Cusip, stds.Value, std.ItemName, std.STDCode, ts.TimeSeriesDate Date, 
	                                        row_number() over (partition by stds.STDItemID, stds.SecurityID order by ts.TimeSeriesDate desc) as rank 
	                                        from STDTimeSeriesDetailSecurity stds (nolock)
																						join FdsTriPpiMap fds (nolock)
																							on fds.cusip = stds.SecurityID
																						join STDItem std (nolock)
																							on stds.STDItemId = std.ID
																							and std.SecurityFlag = 1
																						join STDTemplateItem t (nolock)
																							on t.STDItemID = std.ID
																							and t.STDTemplateMasterCode = 'PSIT'
																						join TimeSeries ts (nolock)
																							on stds.TimeSeriesID = ts.Id
																							and ts.AutoCalcFlag = 0
																							and ts.EncoreFlag = 0
																						join InterimType it (nolock)
																							on ts.InterimTypeID = it.ID
																						join Document d (nolock)
																							on ts.DocumentID = d.ID
																							and d.ExportFlag = 1 
                                          where fds.iconum = @iconum
																						and ts.TimeSeriesDate <= @searchDate AND (@since IS NULL OR ts.TimeSeriesDate >= @since)) temp
                                        where temp.rank = 1";


			const string queryDC = @"SELECT temp.Cusip, temp.Value, temp.Date, temp.ItemName, temp.STDCode FROM 
                                        (SELECT stds.SecurityID Cusip, stds.Value, std.ItemName, std.STDCode, ts.TimeSliceDate Date, 
	                                        row_number() over (partition by stds.STDItemID, stds.SecurityID order by ts.TimeSliceDate desc) as rank 
	                                        from STDTimeSliceDetailSecurity stds (nolock)
																						join FdsTriPpiMap fds (nolock)
																							on fds.cusip = stds.SecurityID
																						join STDItem std (nolock)
																							on stds.STDItemId = std.ID
																							and std.SecurityFlag = 1
																						join STDTemplateItem t (nolock)
																							on t.STDItemID = std.ID
																							and t.STDTemplateMasterCode = 'PSIT'
																						join TimeSlice ts (nolock)
																							on stds.TimeSliceID = ts.Id
																							and ts.AutoCalcFlag = 0
																							and ts.EncoreFlag = 0
																						join InterimType it (nolock)
																							on ts.InterimTypeID = it.ID
																						join Document d (nolock)
																							on ts.DocumentID = d.ID
																							and d.ExportFlag = 1 
                                          where fds.iconum = @iconum
																						and ts.TimeSliceDate <= @searchDate AND (@since IS NULL OR ts.TimeSliceDate >= @since)
																						) temp
                                        where temp.rank = 1";
			bool isIconumDC = IsIconumDC(iconum);

			string query = isIconumDC ? queryDC : queryNonDC;
			DateTime searchDate = DateTime.Now;
			if (reportDate != null) {
				searchDate = (DateTime)reportDate;
			}
			using (SqlConnection connection = new SqlConnection(_connectionString)) {
				connection.Open();

				using (var cmd = new SqlCommand(query, connection)) {
					cmd.Parameters.AddWithValue("@iconum", iconum);
					cmd.Parameters.AddWithValue("@searchDate", searchDate);
					if (since == null) {
						cmd.Parameters.AddWithValue("@since", DBNull.Value);
					}
					else {
						cmd.Parameters.AddWithValue("@since", since);
					}

					using (var reader = cmd.ExecuteReader()) {
						while (reader.Read()) {
							string cusip = reader.GetStringSafe(0);
							ShareClassDataItem item = new ShareClassNumericItem
							{
								Name = reader.GetStringSafe(3),
								ItemId = reader.GetStringSafe(4),
								Value = reader.GetDecimal(1),
								ReportDate = reader.GetDateTime(2),
							};
							if (!perShareData.ContainsKey(cusip)) {
								perShareData.Add(cusip, new List<ShareClassDataItem>());
							}
							List<ShareClassDataItem> dataItems = perShareData[cusip];
							dataItems.Add(item);
						}
					}
				}
			}

			return perShareData;
		}

		public Dictionary<int, Dictionary<string, List<ShareClassDataItem>>> GetLatestCompanyFPEShareData(List<int> iconums, DateTime? reportDate, DateTime? since) {

            const string createTableQuery = @"CREATE TABLE #CompanyIds ( iconum INT NOT NULL PRIMARY KEY )";

			const string query = @"
                SELECT temp.Cusip, temp.Value, temp.Date, temp.ItemName, temp.STDCode, temp.iconum 
	                FROM (
		                SELECT stds.SecurityID Cusip, stds.Value, std.ItemName, std.STDCode, ts.TimeSeriesDate Date, p.iconum iconum,
			                row_number() over (partition by stds.STDItemID, stds.SecurityID order by ts.TimeSeriesDate desc) as rank 
			                FROM STDTimeSeriesDetailSecurity stds (nolock)
				                join PpiIconumMap p (nolock) 
					                on p.cusip = stds.SecurityID
				                join STDItem std (nolock)
					                on stds.STDItemId = std.ID
					                and std.SecurityFlag = 1
				                join STDTemplateItem t (nolock)
					                on t.STDItemID = std.ID
					                and t.STDTemplateMasterCode = 'PSIT'
				                join TimeSeries ts (nolock)
					                on stds.TimeSeriesID = ts.Id
					                and ts.AutoCalcFlag = 0
					                and ts.EncoreFlag = 0
				                join Document d (nolock)
					                on ts.DocumentID = d.ID
				                join DocumentSeries ds (nolock)
					                on d.DocumentSeriesId = ds.Id 
				                join #CompanyIds i (nolock)
					                on i.iconum = ds.CompanyID
				                left join MigrateToTimeSlice mi (nolock)
					                on mi.Iconum = i.iconum
					                and mi.MigrationStatusID != 1					
			                WHERE ts.TimeSeriesDate <= @searchDate AND (@since IS NULL OR ts.TimeSeriesDate >= @since) and d.ExportFlag = 1
	                ) temp
	                WHERE temp.rank = 1
                UNION
                SELECT temp.Cusip, temp.Value, temp.Date, temp.ItemName, temp.STDCode, temp.iconum 
	                FROM (
		                SELECT stds.SecurityID Cusip, stds.Value, std.ItemName, std.STDCode, ts.TimeSliceDate Date, p.iconum iconum,
			                row_number() over (partition by stds.STDItemID, stds.SecurityID order by ts.TimeSliceDate desc) as rank 
			                FROM STDTimeSliceDetailSecurity stds (nolock)
				                join PpiIconumMap p (nolock)
					                on p.cusip = stds.SecurityID
				                join STDItem std (nolock)
					                on stds.STDItemId = std.ID
					                and std.SecurityFlag = 1
				                join STDTemplateItem t (nolock)
					                on t.STDItemID = std.ID
					                and t.STDTemplateMasterCode = 'PSIT'
				                join dbo.TimeSlice ts (nolock)
					                on stds.TimeSliceID = ts.Id
					                and ts.AutoCalcFlag = 0
					                and ts.EncoreFlag = 0
				                join Document d (nolock)
					                on ts.DocumentID = d.ID
				                join DocumentSeries ds (nolock)
					                on d.DocumentSeriesId = ds.Id 
				                join #CompanyIds i (nolock)
					                on i.iconum = ds.CompanyID
				                join MigrateToTimeSlice mi (nolock)
					                on mi.Iconum = i.iconum
					                and mi.MigrationStatusID = 1
			                WHERE ts.TimeSliceDate <= @searchDate AND (@since IS NULL OR ts.TimeSliceDate >= @since) and d.ExportFlag = 1
	                ) temp
	                WHERE temp.rank = 1
                ";

		
			DateTime searchDate = DateTime.Now;
			if (reportDate != null) {
				searchDate = (DateTime)reportDate;
			}

			Dictionary<int, Dictionary<string, List<ShareClassDataItem>>> companyShareData = new Dictionary<int, Dictionary<string, List<ShareClassDataItem>>>();
			DataTable table = new DataTable();
			table.Columns.Add("iconum", typeof(int));
			foreach (int iconum in iconums) {
				table.Rows.Add(iconum);
				companyShareData.Add(iconum, new Dictionary<string, List<ShareClassDataItem>>());
			}

			// Create Global Temp Table
			using (SqlConnection connection = new SqlConnection(_connectionString)) {
				connection.Open();

                // Create Temp table for Iconum upload
				using (SqlCommand cmd = new SqlCommand(createTableQuery, connection)) {
					cmd.ExecuteNonQuery();
				}

				// Bulk Upload all Iconums to Temp table
				using (SqlBulkCopy bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, null)) {
					bulkCopy.BatchSize = table.Rows.Count;
					bulkCopy.DestinationTableName = "#CompanyIds";
					bulkCopy.WriteToServer(table);
				}

                // Fetch FPE data
				using (SqlCommand cmd = new SqlCommand(query, connection)) {
                    cmd.CommandTimeout = 120;
				    cmd.Parameters.Add(new SqlParameter("@searchDate", SqlDbType.DateTime2) {Value = searchDate});
				    cmd.Parameters.Add(new SqlParameter("@since", SqlDbType.DateTime2)
				    {
				        Value = (since == null ? DBNull.Value : (object) since)
				    });

					using (SqlDataReader reader = cmd.ExecuteReader()) {
						while (reader.Read()) {
							string cusip = reader.GetStringSafe(0);
							int iconum = reader.GetInt32(5);
							Dictionary<string, List<ShareClassDataItem>> perShareData = companyShareData[iconum];
							ShareClassDataItem item = new ShareClassNumericItem
							{
								Name = reader.GetStringSafe(3),
								ItemId = reader.GetStringSafe(4),
								Value = reader.GetDecimal(1),
								ReportDate = reader.GetDateTime(2),
							};
							if (!perShareData.ContainsKey(cusip)) {
								perShareData.Add(cusip, new List<ShareClassDataItem>());
							}
							perShareData[cusip].Add(item);
						}
					}
				}
			}
			return companyShareData;
		}

		public List<ShareClassDataItem> GetLatestFPEShareData(string cusip, DateTime? reportDate) {
			List<ShareClassDataItem> perShareData = new List<ShareClassDataItem>();

			const string query = @"SELECT temp.Cusip, temp.Value, temp.Date, temp.ItemName, temp.STDCode FROM 
                                        (SELECT stds.SecurityID Cusip, stds.Value, std.ItemName, std.STDCode, ts.TimeSeriesDate Date, 
	                                        row_number() over (partition by stds.STDItemID order by ts.TimeSeriesDate desc) as rank 
	                                        from STDTimeSeriesDetailSecurity stds (nolock)
	                                        join STDItem std (nolock)
		                                        on stds.STDItemId = std.ID
		                                        and std.SecurityFlag = 1
											join STDTemplateItem t (nolock)
												on t.STDItemID = std.ID
												and t.STDTemplateMasterCode = 'PSIT'
	                                        join TimeSeries ts (nolock)
		                                        on stds.TimeSeriesID = ts.Id
		                                        and ts.AutoCalcFlag = 0
		                                        and ts.EncoreFlag = 0
	                                        join InterimType it (nolock)
		                                        on ts.InterimTypeID = it.ID
	                                        join Document d (nolock)
		                                        on ts.DocumentID = d.ID
		                                        and d.ExportFlag = 1 
                                          where stds.SecurityID = @cusip
												and ts.TimeSeriesDate <= @searchDate) temp
                                        where temp.rank = 1";

			DateTime searchDate = DateTime.Now;
			if (reportDate != null) {
				searchDate = (DateTime)reportDate;
			}
			using (SqlConnection connection = new SqlConnection(_connectionString)) {
				connection.Open();

				using (var cmd = new SqlCommand(query, connection)) {
					cmd.Parameters.AddWithValue("@cusip", cusip);
					cmd.Parameters.AddWithValue("@searchDate", searchDate);

					using (var reader = cmd.ExecuteReader()) {
						while (reader.Read()) {
							ShareClassDataItem item = new ShareClassNumericItem
							{
								Name = reader.GetStringSafe(3),
								ItemId = reader.GetStringSafe(4),
								Value = reader.GetDecimal(1),
								ReportDate = reader.GetDateTime(2),
							};
							perShareData.Add(item);
						}
					}
				}
			}

			return perShareData;
		}

		public Dictionary<string, List<ShareClassDataItem>> GetCurrentShareDataItems(int iconum) {
			string queryNonDC = @"SELECT s.ID, s.STDItemID, s.STDExpressionID, s.Value, i.STDCode, i.ItemName, s.Date, s.SecurityID
                                FROM vw_STDCompanyDetail s
                                    JOIN STDItem i ON i.ID = s.STDItemID
                                WHERE s.iconum = @iconum AND s.SecurityID IS NOT NULL";


			string queryDC = @"SELECT s.ID, s.STDItemID, null, case when  (s.MathML is not null and s.MathML != '<null/>') then s.Value  ELSE null end  ,
                                i.STDCode, i.ItemName, case s.mathml when '<null/>' then s.Value when null then s.Value else null end , s.SecurityID
                                FROM dbo.STDCompanyDetailElastic s
                                    JOIN STDItem i ON i.ID = s.STDItemID
                                WHERE s.iconum = @iconum AND s.SecurityID IS NOT NULL";

			bool isIconumDC = IsIconumDC(iconum);

			string query = isIconumDC ? queryDC : queryNonDC;

			Dictionary<string, List<ShareClassDataItem>> perShareData = new Dictionary<string, List<ShareClassDataItem>>();
			using (SqlConnection connection = new SqlConnection(_connectionString)) {
				connection.Open();

				using (var cmd = new SqlCommand(query, connection)) {
					cmd.Parameters.AddWithValue("@iconum", iconum);

					using (var reader = cmd.ExecuteReader()) {
						while (reader.Read()) {
							string cusip = reader.GetStringSafe(7);
							if (!perShareData.ContainsKey(cusip)) {
								perShareData.Add(cusip, new List<ShareClassDataItem>());
							}
							List<ShareClassDataItem> items = perShareData[cusip];
							string itemName = reader.GetStringSafe(5);
							string itemCode = reader.GetStringSafe(4);
							decimal? value = reader.GetNullable<decimal>(3);
							DateTime? reportDate = reader.GetNullable<DateTime>(6);

							ShareClassDataItem item = null;
							if (reportDate == null) {
								item = new ShareClassNumericItem
								{
									ItemId = itemCode,
									Name = itemName,
									Value = (decimal)value,
								};
							}

							if (value == null) {
								item = new ShareClassDateItem
								{
									ItemId = itemCode,
									Name = itemName,
									Value = (DateTime)reportDate,
								};
							}
							
							items.Add(item);
						}
					}
				}
			}

			return perShareData;
		}

		private bool IsIconumDC(int iconum) {
			bool result = false;
			const string query = @"if exists (SELECT * from dbo.MigrateToTimeSlice where MigrationStatusID = 1 and Iconum = @iconum)
														begin 
														 select convert(bit,1)
														end else 
														begin 
														 select convert(bit,0)
														end";
			using (SqlConnection connection = new SqlConnection(_connectionString)) {
				connection.Open();
				using (var cmd = new SqlCommand(query, connection)) {
					cmd.Parameters.AddWithValue("@iconum", iconum);

					using (var reader = cmd.ExecuteReader()) {
						if (reader.Read()) {
							result = reader.GetBoolean(0);
						}
					}
				}
			}
			return result;
		}
	}
}