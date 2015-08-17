using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;

using CCS.Fundamentals.DataRoostAPI.Access.SuperFast;
using CCS.Fundamentals.DataRoostAPI.Access.Voyager;

using DataRoostAPI.Common.Models;

using FactSet.Data.SqlClient;

namespace CCS.Fundamentals.DataRoostAPI.Access.Company {

	public class CompanyHelper {

		private readonly string _damConnectionString;
		private readonly string _lionConnectionString;
		private readonly string _sfConnectionString;
		private readonly string _voyConnectionString;

		public CompanyHelper(string sfConnectionString,
		                     string voyConnectionString,
		                     string lionConnectionString,
		                     string damConnectionString) {
			_sfConnectionString = sfConnectionString;
			_voyConnectionString = voyConnectionString;
			_lionConnectionString = lionConnectionString;
			_damConnectionString = damConnectionString;
		}

		public CompanyDTO GetCompany(int iconum) {
			string query = @"SELECT f.Firm_Name, t.Descrip, c.name_long, c.name_short, c.iso_country
                                FROM FilerMst f
	                                LEFT JOIN FilerTypes t ON t.Code = f.Filer_Type
	                                LEFT JOIN Countries c ON c.iso_country = f.ISO_Country
                                WHERE Iconum = @iconum";

			CompanyDTO company = new CompanyDTO();
			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {
				using (SqlCommand cmd = new SqlCommand(query, conn)) {
					conn.Open();
					cmd.Parameters.AddWithValue("@iconum", iconum);

					using (SqlDataReader sdr = cmd.ExecuteReader()) {
						if (sdr.Read()) {
							company.Name = sdr.GetStringSafe(0);
							company.CompanyType = sdr.GetStringSafe(1);
							company.CountryId = sdr.GetStringSafe(4);
							company.Country = new CountryDTO
							                  {
								                  LongName = sdr.GetStringSafe(2),
								                  ShortName = sdr.GetStringSafe(3),
								                  Id = sdr.GetStringSafe(4),
								                  Iso3 = sdr.GetStringSafe(4)
							                  };
						}
					}
				}
			}

			company.ShareClasses = GetCompanyShareClasses(iconum);
			company.Iconum = iconum;
			company.EntitiyPermId = PermId.Iconum2PermId(iconum);
			company.Id = company.EntitiyPermId;
			company.RootPPI = GetRootPPI(iconum);

			return company;
		}

		public CompanyDTO GetCompany(string permId) {
			int iconum = PermId.PermId2Iconum(permId);
			return GetCompany(iconum);
		}

		private string GetRootPPI(int iconum) {
			string query = @"SELECT dbo.getppiforiconum(@iconum)";

			string ppi = null;

			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {
				using (SqlCommand cmd = new SqlCommand(query, conn)) {
					conn.Open();
					cmd.Parameters.AddWithValue("@iconum", iconum);

					using (SqlDataReader sdr = cmd.ExecuteReader()) {
						if (sdr.Read()) {
							ppi = sdr.GetStringSafe(0);
						}
					}
				}
			}

			if (ppi == "<FAILED!>") {
				// If failed to find PPI for iconum lookup PPI for parent company iconum
				query = @"SELECT dbo.getppiforiconum(Parent_ID) FROM FilerMst WHERE Iconum = @iconum";
				using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {
					using (SqlCommand cmd = new SqlCommand(query, conn)) {
						conn.Open();
						cmd.Parameters.AddWithValue("@iconum", iconum);

						using (SqlDataReader sdr = cmd.ExecuteReader()) {
							if (sdr.Read()) {
								ppi = sdr.GetStringSafe(0);
							}
						}
					}
				}

				if (ppi == "<FAILED!>") {
					ppi = null;
				}
			}

			return ppi;
		}

		private IEnumerable<ShareClassDTO> GetCompanyShareClasses(int iconum) {
			string query = @"SELECT s.Cusip,
                                    s.Iconum,
                                    s.Name,
                                    a.Description,
                                    e.Description,
                                    s.Inception_Date,
                                    s.Term_Date,
                                    s.Price,
                                    s.Shares_Out,
                                    s.Ticker,
                                    s.Sedol,
                                    s.ISIN,
                                    s.To_Cusip,
                                    s.Issue_Type,
                                    p.PPI,
                                    x.permid
                                FROM SecMas s
		                            LEFT JOIN IssueTypes i ON i.Code = s.Issue_Type
		                            LEFT JOIN SecMasExchanges e ON e.Exchange_Code = s.Exchange_Code
		                            LEFT JOIN AssetClasses a ON a.Code = i.Asset_Code
                                LEFT JOIN FdsTriPpiMap p ON p.CUSIP = s.Cusip
                                LEFT JOIN secmas_sym_cusip_alias x ON x.Cusip = s.Cusip
	                            WHERE s.Iconum = @iconum
                                    --AND RIGHT(p.PPI, 1) != '0'
                                    --AND s.term_date IS NULL
                                    --AND s.Cusip in (SELECT DISTINCT d.SecurityID FROM SDBTimeSeriesDetailSecurity d JOIN secmas s ON s.Cusip = d.SecurityID WHERE s.iconum = @iconum)";

			List<ShareClassDTO> shareClasses = new List<ShareClassDTO>();
			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {
				using (SqlCommand cmd = new SqlCommand(query, conn)) {
					conn.Open();
					cmd.Parameters.AddWithValue("@iconum", iconum);

					using (SqlDataReader sdr = cmd.ExecuteReader()) {
						while (sdr.Read()) {
							ShareClassDTO shareClass = new ShareClassDTO
							                           {
								                           Cusip = sdr.GetStringSafe(0),

								                           //Iconum = sdr.GetInt32(1),
								                           Name = sdr.GetStringSafe(2),
								                           AssetClass = sdr.GetStringSafe(3),
								                           ListedOn = sdr.GetStringSafe(4),
								                           InceptionDate = sdr.GetDateTime(5),
								                           TermDate = sdr.GetNullable<DateTime>(6),

								                           //CurrentPrice = sdr.GetDecimal(7),
								                           //CurrentSharesOutstanding = sdr.GetDecimal(8),
								                           TickerSymbol = sdr.GetStringSafe(9),
								                           Sedol = sdr.GetStringSafe(10),
								                           Isin = sdr.GetStringSafe(11),

								                           //ToCusip = sdr.GetStringSafe(12),
								                           IssueType = sdr.GetStringSafe(13),
								                           PPI = sdr.GetStringSafe(14),
								                           Id = sdr.GetStringSafe(15),
								                           PermId = sdr.GetStringSafe(15)
							                           };
							shareClasses.Add(shareClass);
						}
					}
				}
			}

			return shareClasses;
		}

		public IEnumerable<ShareClassDataDTO> GetCompanyShareClassData(int iconum, DateTime? reportDate) {
			List<ShareClassDataDTO> shareClassDataList = new List<ShareClassDataDTO>();
			IEnumerable<ShareClassDTO> shareClasses = GetCompanyShareClasses(iconum);

			EffortDTO effort = GetCompanyEffort(iconum);
			if (effort.Name == "superfast") {
				SuperFastSharesHelper superfastShares = new SuperFastSharesHelper(_sfConnectionString);
				Dictionary<string, List<ShareClassDataItem>> superfastSecurityItems =
					superfastShares.GetLatestCompanyFPEShareData(iconum, reportDate);

				foreach (ShareClassDTO shareClass in shareClasses) {
					List<ShareClassDataItem> securityItemList = new List<ShareClassDataItem>();
					if (shareClass.Cusip != null && superfastSecurityItems.ContainsKey(shareClass.Cusip)) {
						securityItemList = superfastSecurityItems[shareClass.Cusip];
					}
					ShareClassDataDTO shareClassData = new ShareClassDataDTO(shareClass, securityItemList);
					shareClassDataList.Add(shareClassData);
				}
			}
			else if (effort.Name == "voyager") {
				VoyagerSharesHelper voyagerShares = new VoyagerSharesHelper(_voyConnectionString, _sfConnectionString);
				Dictionary<string, List<ShareClassDataItem>> voyagerSecurityItems = voyagerShares.GetLatestFPEShareData(iconum,
				                                                                                                        reportDate);
				foreach (ShareClassDTO shareClass in shareClasses) {
					List<ShareClassDataItem> securityItemList = new List<ShareClassDataItem>();
					if (shareClass.PPI != null && voyagerSecurityItems.ContainsKey(shareClass.PPI)) {
						securityItemList = voyagerSecurityItems[shareClass.PPI];
					}
					ShareClassDataDTO shareClassData = new ShareClassDataDTO(shareClass, securityItemList);
					shareClassDataList.Add(shareClassData);
				}
			}

			return shareClassDataList;
		}

		// TODO: change this to point to shares instead of voyager and superfast
		public IEnumerable<ShareClassDataDTO> GetCurrentCompanyShareClassData(int iconum) {
			List<ShareClassDataDTO> shareClassDataList = new List<ShareClassDataDTO>();
			IEnumerable<ShareClassDTO> shareClasses = GetCompanyShareClasses(iconum);

			SuperFastSharesHelper superfastShares = new SuperFastSharesHelper(_sfConnectionString);
			VoyagerSharesHelper voyagerShares = new VoyagerSharesHelper(_voyConnectionString, _sfConnectionString);
			Dictionary<string, List<ShareClassDataItem>> voyagerSecurityItems = voyagerShares.GetCurrentShareDataItems(iconum);
			Dictionary<string, List<ShareClassDataItem>> superfastSecurityItems = superfastShares.GetCurrentShareDataItems(iconum);
			foreach (ShareClassDTO shareClass in shareClasses) {
				List<ShareClassDataItem> securityItemList = new List<ShareClassDataItem>();
				if (superfastSecurityItems.ContainsKey(shareClass.Cusip)) {
					securityItemList = superfastSecurityItems[shareClass.Cusip];
				}
				else if (shareClass.PPI != null && voyagerSecurityItems.ContainsKey(shareClass.PPI)) {
					securityItemList = voyagerSecurityItems[shareClass.PPI];
				}
				ShareClassDataDTO shareClassData = new ShareClassDataDTO(shareClass, securityItemList);
				shareClassDataList.Add(shareClassData);
			}

			return shareClassDataList;
		}

		public EffortDTO GetCompanyEffort(int iconum) {
			string query = @"select 'x'
												from dbo.CompanyLists cl (nolock)
													join dbo.CompanyListCompanies clc (nolock) on cl.id = clc.CompanyListId
												where cl.ShortName = 'SF_NewMarketWhiteList'
													AND iconum = @iconum
											 union
											 select 'x'
												from dbo.FdsTriPpiMap
												where iconum = @iconum AND IsAdr = 0
													AND IsoCountry IN ('US', 'ZW')";

			using (SqlConnection conn = new SqlConnection(_damConnectionString)) {
				using (SqlCommand cmd = new SqlCommand(query, conn)) {
					conn.Open();
					cmd.Parameters.AddWithValue("@iconum", iconum);

					using (SqlDataReader sdr = cmd.ExecuteReader()) {
						if (sdr.Read()) {
							EffortDTO superfastEffort = new EffortDTO();
							superfastEffort.Name = "superfast";
							return superfastEffort;
						}
					}
				}
			}
			EffortDTO voyagerEffort = new EffortDTO();
			voyagerEffort.Name = "voyager";
			return voyagerEffort;
		}

		public Dictionary<int, EffortDTO> GetCompaniesEfforts(List<int> companies) {
			Dictionary<int, EffortDTO> effortDictionary = new Dictionary<int, EffortDTO>();
			DataTable table = new DataTable();
			table.Columns.Add("iconum", typeof (int));
			foreach (int iconum in companies) {
				table.Rows.Add(iconum);
				effortDictionary.Add(iconum, new EffortDTO { Name = "voyager" });
			}

			string createTableQuery = @"IF OBJECT_ID('tempdb..##CompanyIds', 'U') IS NOT NULL DROP TABLE dbo.##CompanyIds;
                                    CREATE TABLE dbo.##CompanyIds (
	                                    iconum INT NOT NULL
                                    )";
			string query = @"select i.iconum
												from dbo.CompanyLists cl (nolock)
													join dbo.CompanyListCompanies clc (nolock) on cl.id = clc.CompanyListId
													join dbo.##CompanyIds i on i.iconum = clc.iconum
												where cl.ShortName = 'SF_NewMarketWhiteList'
											 union
											 select i.iconum
												from dbo.FdsTriPpiMap fds
													join dbo.##CompanyIds i on i.iconum = fds.iconum 
												where IsAdr = 0 AND IsoCountry IN ('US', 'ZW')";

			// Create Global Temp Table
			using (SqlConnection connection = new SqlConnection(_damConnectionString)) {
				connection.Open();
				using (SqlCommand cmd = new SqlCommand(createTableQuery, connection)) {
					cmd.ExecuteNonQuery();
				}

				// Upload all sedols to Temp table
				using (SqlBulkCopy bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, null)) {
					bulkCopy.BatchSize = table.Rows.Count;
					bulkCopy.DestinationTableName = "dbo.##CompanyIds";
					try {
						bulkCopy.WriteToServer(table);
					}
					catch (Exception ex) {
						// Debug.WriteLine(ex.StackTrace, ex.InnerException.Message);
						//_logger.Error("Error Bulk Uploading Sedols to Lion Temp Table.", ex);
					}
				}

				using (SqlCommand cmd = new SqlCommand(query, connection)) {
					using (SqlDataReader reader = cmd.ExecuteReader()) {
						while (reader.Read()) {
							int iconum = reader.GetInt32(0);
							EffortDTO superfastEffort = new EffortDTO { Name = "superfast" };
							effortDictionary[iconum] = superfastEffort;
						}
					}
				}
			}

			return effortDictionary;
		}

	}

}
