using System;
using System.Collections.Generic;
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


		public Dictionary<int, Dictionary<string, List<ShareClassDataItem>>> GetAllFpeShareDataForStdCode(
				List<int> iconums, string stdCode, DateTime? reportDate, DateTime? since) {

			const string createTableQuery = @"CREATE TABLE #CompanyIds ( iconum INT NOT NULL PRIMARY KEY )";

			const string pantheonQuery = @"
select cusip,SecPermId,Value,Date,ItemName,STDCode,iconum,rank  from (
SELECT 
                        t2.Cusip, t2.SecPermId, t2.Value, t2.Date, t2.ItemName, t2.STDCode, t2.iconum, t2.rank
	                FROM (
		                SELECT 
                            stds.SecurityID Cusip, p.PermId SecPermId, stds.Value, std.ItemName, std.STDCode, ts.TimeSliceDate Date, i.iconum iconum,
			                row_number() over (partition by stds.STDItemID, p.PermId order by ts.TimeSliceDate desc, ts.ReportTypeID asc, ts.AutoCalcFlag ASC) as rank 
			            FROM #CompanyIds i (nolock)
							join DocumentSeries ds (nolock) on ds.CompanyID = i.iconum                            
							join supercore.documenttimeslice d (nolock) on d.DocumentSeriesID = ds.ID
							join Document d1 on d1.DAMDocumentId = d.DamDocumentID and d.DocumentSeriesID = d1.documentseriesid
							join supercore.TimeSlice ts (nolock) on ts.id = d.timesliceid and ts.EncoreFlag = 0
							join supercore.STDTimeSliceDetail stds (nolock) on stds.TimeSliceID = ts.Id
							join STDItem std (nolock) on stds.STDItemId = std.ID and std.SecurityFlag = 1
							join STDTemplateItem t (nolock) on t.STDItemID = std.ID and t.STDTemplateMasterCode = 'PSIT'
							join secmas_sym_cusip_alias p (nolock) on p.Cusip = stds.SecurityID
									WHERE stds.SecurityId is not null and std.STDCode =  @stdCode AND ts.TimeSliceDate <= @searchDate AND (@since IS NULL OR ts.TimeSliceDate >= @since) and d1.ExportFlag = 1
	                ) t2
                    --ORDER BY t2.SecPermId, t2.rank
)a order by a.SecPermId,a.rank
";

			// CQ 91901: Final reports (Annual & Interim) should have higher priority over Prelim reports

			var searchDate = DateTime.Now;
			if (reportDate != null) {
				searchDate = (DateTime)reportDate;
			}

			// Populate DataTable with all Iconums
			var companyShareData = new Dictionary<int, Dictionary<string, List<ShareClassDataItem>>>();
			var table = new DataTable();
			table.Columns.Add("iconum", typeof(int));
			foreach (int iconum in iconums) {
				table.Rows.Add(iconum);
				companyShareData.Add(iconum, new Dictionary<string, List<ShareClassDataItem>>());
			}

			// Create Global Temp Table
			using (var connection = new SqlConnection(_connectionString)) {
				connection.Open();

				// Create Temp table for Iconum upload
				using (var cmd = new SqlCommand(createTableQuery, connection)) {
					cmd.ExecuteNonQuery();
				}

				// Bulk Upload all Iconums to Temp table
				using (var bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, null)) {
					bulkCopy.BatchSize = table.Rows.Count;
					bulkCopy.DestinationTableName = "#CompanyIds";
					bulkCopy.WriteToServer(table);
				}

				// Fetch FPE data
				using (SqlCommand cmd = new SqlCommand(pantheonQuery, connection)) {
					cmd.CommandTimeout = 120;
					cmd.Parameters.Add(new SqlParameter("@stdCode", SqlDbType.Char, 5) { Value = stdCode });
					cmd.Parameters.Add(new SqlParameter("@searchDate", SqlDbType.DateTime2) { Value = searchDate });
					cmd.Parameters.Add(new SqlParameter("@since", SqlDbType.DateTime2)
					{
						Value = (since == null ? DBNull.Value : (object)since)
					});

					using (SqlDataReader reader = cmd.ExecuteReader()) {
						while (reader.Read()) {
							string cusip = reader.GetStringSafe(0);
							string secPermId = reader.GetStringSafe(1);
							int iconum = reader.GetInt32(6);

							if (!companyShareData.ContainsKey(iconum) || string.IsNullOrEmpty(secPermId)) {
								// This is possible if PpiIconumMap has same Cusip associated with two different Iconums and we 
								// received request for only one of them. Hence, ignore that extra iconum returned from above query
								continue;
							}
							Dictionary<string, List<ShareClassDataItem>> perShareData = companyShareData[iconum];
							if (!perShareData.ContainsKey(secPermId)) {
								perShareData.Add(secPermId, new List<ShareClassDataItem>());
							}

							decimal tmpValue;
							var nullableValue = decimal.TryParse(reader.GetStringSafe(2), out tmpValue) ? tmpValue : (decimal?)null;
							if (nullableValue.HasValue) {
								ShareClassDataItem item = new ShareClassNumericItem
								{
									Name = reader.GetStringSafe(4),
									ItemId = reader.GetStringSafe(5),
									Value = nullableValue.Value,
									ReportDate = reader.GetDateTime(3)
								};
								perShareData[secPermId].Add(item);
							}
						}
					}
				}
			}
			return companyShareData;
		}


		public Dictionary<int, Dictionary<string, List<ShareClassDataItem>>> GetLatestCompanyFPEShareData(List<int> iconums, DateTime? reportDate, DateTime? since) {

			const string createTableQuery = @"CREATE TABLE #CompanyIds ( iconum INT NOT NULL PRIMARY KEY )";

			// CQ 91901: Final reports (Annual & Interim) should have higher priority over Prelim reports
			const string query =
					@"SELECT 
                        t2.Cusip, t2.SecPermId, t2.Value, t2.Date, t2.ItemName, t2.STDCode, t2.iconum
	                FROM (
		                SELECT 
                            stds.SecurityID Cusip, p.PermId SecPermId, stds.Value, std.ItemName, std.STDCode, ts.TimeSliceDate Date, i.iconum iconum,
			                row_number() over (partition by stds.STDItemID, p.PermId order by ts.TimeSliceDate desc, ts.ReportTypeID asc, ts.AutoCalcFlag ASC) as rank 
			            FROM #CompanyIds i (nolock)
							join DocumentSeries ds (nolock) on ds.CompanyID = i.iconum
                            join SuperCore.MigrateToTemplates mts with (nolock) on mts.iconum = i.iconum and mts.MigrationstatusId = 1
							join supercore.documenttimeslice d (nolock) on d.DocumentSeriesID = ds.ID
							join Document d1 on d1.DAMDocumentId = d.DamDocumentID and d.DocumentSeriesID = d1.documentseriesid
							join supercore.TimeSlice ts (nolock) on ts.id = d.timesliceid and ts.EncoreFlag = 0
							join supercore.STDTimeSliceDetail stds (nolock) on stds.TimeSliceID = ts.Id
							join STDItem std (nolock) on stds.STDItemId = std.ID and std.SecurityFlag = 1
							join STDTemplateItem t (nolock) on t.STDItemID = std.ID and t.STDTemplateMasterCode = 'PSIT'
							join secmas_sym_cusip_alias p (nolock) on p.Cusip = stds.SecurityID
			            WHERE ts.TimeSliceDate <= @searchDate AND (@since IS NULL OR ts.TimeSliceDate >= @since) and d1.ExportFlag = 1
	                ) t2
	               WHERE t2.rank = 1 and t2.SecPermId IS NOT NULL                    
					";

			var searchDate = DateTime.Now;
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
					cmd.Parameters.Add(new SqlParameter("@searchDate", SqlDbType.DateTime2) { Value = searchDate });
					cmd.Parameters.Add(new SqlParameter("@since", SqlDbType.DateTime2)
					{
						Value = (since == null ? DBNull.Value : (object)since)
					});

					using (SqlDataReader reader = cmd.ExecuteReader()) {
						while (reader.Read()) {
							string cusip = reader.GetStringSafe(0);
							string secPermId = reader.GetStringSafe(1);
							int iconum = reader.GetInt32(6);
							if (!companyShareData.ContainsKey(iconum)) {
								// This is possible if PpiIconumMap has same Cusip associated with two different Iconums and we 
								// received request for only one of them. Hence, ignore that extra iconum returned from above query
								continue;
							}
							Dictionary<string, List<ShareClassDataItem>> perShareData = companyShareData[iconum];

							decimal tmpValue;
							var nullableValue = decimal.TryParse(reader.GetStringSafe(2), out tmpValue) ? tmpValue : (decimal?)null;
							if (nullableValue.HasValue) {
								ShareClassDataItem item = new ShareClassNumericItem
								{
									Name = reader.GetStringSafe(4),
									ItemId = reader.GetStringSafe(5),
									Value = nullableValue.Value,
									ReportDate = reader.GetDateTime(3)
								};
								if (!perShareData.ContainsKey(secPermId)) {
									perShareData.Add(secPermId, new List<ShareClassDataItem>());
								}
								perShareData[secPermId].Add(item);
							}

						}
					}
				}
			}
			return companyShareData;
		}

		public Dictionary<string, List<ShareClassDataItem>> GetCurrentShareDataItems(int iconum) {

			string queryPantheon = @"SELECT s.ID, s.STDItemID, '', s.Value, i.STDCode, i.ItemName, '', s.SecurityID, i.StdItemTypeId
                                FROM SuperCore.STDTimeSliceDetail s
                                    JOIN STDItem i ON i.ID = s.STDItemID
								join SuperCore.TimeSlice ts with (nolock) on s.TimeSliceID = ts.ID
								join SuperCore.DocumentTimeSlice dts with (nolock)  on dts.timesliceid = ts.id
								join dbo.documentseries ds on ds.id = dts.documentseriesid
                  WHERE ds.CompanyID =  @iconum AND s.SecurityID IS NOT NULL";


			string queryDC = @"SELECT s.ID, s.STDItemID, null STDExpressionID, case when  (s.MathML is not null and s.MathML != '<null/>') then s.Value  ELSE null end  Value,
                                i.STDCode, i.ItemName, case s.mathml when '<null/>' then s.Value when null then s.Value else null end , s.SecurityID, i.StdItemTypeId
                                FROM dbo.STDCompanyDetailElastic s
                                    JOIN STDItem i ON i.ID = s.STDItemID
                                WHERE s.iconum = @iconum AND s.SecurityID IS NOT NULL";

			bool isIconumPantheon = IsIconumPantheon(iconum);

			string query = isIconumPantheon ? queryPantheon : queryDC;

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
							string itemTypeId = reader.GetString(8);
							ShareClassDataItem item = null;

							if (itemTypeId == "E" || itemTypeId == "S") {
								// It's a decimal value
								decimal tmpValue;
								var value = decimal.TryParse(reader.GetStringSafe(3), out tmpValue) ? tmpValue : (decimal?)null;
								if (value.HasValue) {
									item = new ShareClassNumericItem
									{
										ItemId = itemCode,
										Name = itemName,
										Value = value.Value
									};
								}
							} else if (itemTypeId == "T") {
								// It's a date value
								DateTime? reportDate;
								DateTime tmpValue;

								reportDate = DateTime.TryParse(reader.GetStringSafe(3), out tmpValue) ? tmpValue : (DateTime?)null;


								if (reportDate.HasValue) {
									item = new ShareClassDateItem
									{
										ItemId = itemCode,
										Name = itemName,
										Value = reportDate.Value
									};
								}
							}

							if (item != null) {
								items.Add(item);
							}
						}
					}
				}
			}

			return perShareData;
		}

		private bool IsIconumPantheon(int iconum) {
			bool result = false;
			const string query = @"if exists (SELECT * from SuperCore.MigrateToTemplates where MigrationStatusID = 1 and Iconum = @iconum)
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