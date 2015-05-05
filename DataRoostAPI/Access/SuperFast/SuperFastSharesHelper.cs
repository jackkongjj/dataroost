﻿using System;
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

		public List<ShareClassDataItem> GetLatestFPEShareData(string cusip, DateTime? reportDate) {
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
			string query = @"SELECT s.ID, s.STDItemID, s.STDExpressionID, s.Value, i.STDCode, i.ItemName, s.Date, s.SecurityID
                                FROM vw_STDCompanyDetail s
                                    JOIN STDItem i ON i.ID = s.STDItemID
                                WHERE s.iconum = @iconum AND s.SecurityID IS NOT NULL";

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
	}
}