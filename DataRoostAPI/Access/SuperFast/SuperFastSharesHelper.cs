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

		public Dictionary<int, Dictionary<string, List<ShareClassDataItem>>> GetLatestCompanyFPEShareData(List<int> iconums, DateTime? reportDate, DateTime? since) {

            const string createTableQuery = @"CREATE TABLE #CompanyIds ( iconum INT NOT NULL PRIMARY KEY )";

			const string query = @"
                SELECT temp.Cusip, temp.Value, temp.Date, temp.ItemName, temp.STDCode, temp.iconum 
	                FROM (
		                SELECT stds.SecurityID Cusip, stds.Value, std.ItemName, std.STDCode, ts.TimeSeriesDate Date, p.iconum iconum,
			                row_number() over (partition by stds.STDItemID, stds.SecurityID order by ts.TimeSeriesDate desc, ts.AutoCalcFlag ASC) as rank 
			                FROM STDTimeSeriesDetailSecurity stds (nolock)
				                join PpiIconumMap p (nolock) 
					                on p.cusip = stds.SecurityID and p.Iconum > 0
				                join STDItem std (nolock)
					                on stds.STDItemId = std.ID
					                and std.SecurityFlag = 1
				                join STDTemplateItem t (nolock)
					                on t.STDItemID = std.ID
					                and t.STDTemplateMasterCode = 'PSIT'
				                join TimeSeries ts (nolock)
					                on stds.TimeSeriesID = ts.Id
					                and ts.EncoreFlag = 0
				                join Document d (nolock)
					                on ts.DocumentID = d.ID
				                join DocumentSeries ds (nolock)
					                on d.DocumentSeriesId = ds.Id 
				                join #CompanyIds i (nolock)
					                on i.iconum = ds.CompanyID
				                left join MigrateToTimeSlice mi (nolock)
					                on mi.Iconum = i.iconum
			                WHERE ts.TimeSeriesDate <= @searchDate AND (@since IS NULL OR ts.TimeSeriesDate >= @since) and d.ExportFlag = 1
                                and (mi.Iconum is NULL or mi.MigrationStatusID != 1)
	                ) temp
	                WHERE temp.rank = 1
                UNION
                SELECT temp.Cusip, temp.Value, temp.Date, temp.ItemName, temp.STDCode, temp.iconum 
	                FROM (
		                SELECT stds.SecurityID Cusip, stds.Value, std.ItemName, std.STDCode, ts.TimeSliceDate Date, p.iconum iconum,
			                row_number() over (partition by stds.STDItemID, stds.SecurityID order by ts.TimeSliceDate desc, ts.AutoCalcFlag ASC) as rank 
			                FROM STDTimeSliceDetailSecurity stds (nolock)
				                join PpiIconumMap p (nolock)
					                on p.cusip = stds.SecurityID and p.Iconum > 0
				                join STDItem std (nolock)
					                on stds.STDItemId = std.ID
					                and std.SecurityFlag = 1
				                join STDTemplateItem t (nolock)
					                on t.STDItemID = std.ID
					                and t.STDTemplateMasterCode = 'PSIT'
				                join dbo.TimeSlice ts (nolock)
					                on stds.TimeSliceID = ts.Id
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
						    if (!companyShareData.ContainsKey(iconum)) {
                                // This is possible if PpiIconumMap has same Cusip associated with two different Iconums and we 
                                // received request for only one of them. Hence, ignore that extra iconum returned from above query
						        continue;
						    }
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

		public Dictionary<string, List<ShareClassDataItem>> GetCurrentShareDataItems(int iconum) {

			string queryNonDC = @"SELECT s.ID, s.STDItemID, s.STDExpressionID, s.Value, i.STDCode, i.ItemName, s.Date, s.SecurityID, i.StdItemTypeId
                                FROM vw_STDCompanyDetail s
                                    JOIN STDItem i ON i.ID = s.STDItemID
                                WHERE s.iconum = @iconum AND s.SecurityID IS NOT NULL";


			string queryDC = @"SELECT s.ID, s.STDItemID, null STDExpressionID, case when  (s.MathML is not null and s.MathML != '<null/>') then s.Value  ELSE null end  Value,
                                i.STDCode, i.ItemName, case s.mathml when '<null/>' then s.Value when null then s.Value else null end , s.SecurityID, i.StdItemTypeId
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
						    string itemTypeId = reader.GetString(8);
                            ShareClassDataItem item = null;

                            if (itemTypeId == "E") {
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
                            }
						    else if (itemTypeId == "T") {
                                // It's a date value
                                DateTime? reportDate;
                                DateTime tmpValue;
                                if (isIconumDC) {
                                    reportDate = DateTime.TryParse(reader.GetStringSafe(3), out tmpValue) ? tmpValue : (DateTime?)null;
                                } else {
                                    reportDate = DateTime.TryParse(reader.GetStringSafe(6), out tmpValue) ? tmpValue : (DateTime?)null;
                                }

                                if (reportDate.HasValue) {
                                    item = new ShareClassDateItem
                                    {
                                        ItemId = itemCode,
                                        Name = itemName,
                                        Value = reportDate.Value,
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