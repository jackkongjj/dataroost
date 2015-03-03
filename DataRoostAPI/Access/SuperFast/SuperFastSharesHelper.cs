using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Data;
using System.Data.SqlClient;
using CCS.Fundamentals.DataRoostAPI.Models;
using CCS.Fundamentals.DataRoostAPI.Models.SuperFast;
using CCS.Fundamentals.DataRoostAPI.Models.TimeseriesValues;
using FactSet.Data.SqlClient;

namespace CCS.Fundamentals.DataRoostAPI.Access.SuperFast {
	public class SuperFastSharesHelper {

		private readonly string _connectionString;

		public SuperFastSharesHelper(string connectionString) {
			_connectionString = connectionString;
		}

		public List<ShareClassDataItem> GetLatestFPEShareData(string cusip) {
			List<ShareClassDataItem> perShareData = new List<ShareClassDataItem>();

			const string query = @"SELECT temp.Cusip, temp.Value, temp.Date, temp.ItemName, temp.STDCode FROM 
                                        (SELECT stds.SecurityID Cusip, stds.Value, std.ItemName, std.STDCode, ts.TimeSeriesDate Date, 
	                                        row_number() over (partition by stds.STDItemID order by ts.TimeSeriesDate desc) as rank 
	                                        from STDTimeSeriesDetailSecurity stds (nolock)
	                                        join STDItem std (nolock)
		                                        on stds.STDItemId = std.ID
		                                        and std.SecurityFlag = 1
																					join STDTemplateItem t
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
                                          where stds.SecurityID = @cusip) temp
                                        where temp.rank = 1";

			using (SqlConnection connection = new SqlConnection(_connectionString)) {
				connection.Open();

				using (var cmd = new SqlCommand(query, connection)) {
					cmd.Parameters.AddWithValue("@cusip", cusip);

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
	}
}