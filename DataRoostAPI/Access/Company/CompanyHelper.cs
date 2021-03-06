using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Net.Mail;
using CCS.Fundamentals.DataRoostAPI.Access.SuperFast;
using CCS.Fundamentals.DataRoostAPI.Access.Voyager;

using DataRoostAPI.Common.Exceptions;
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

		public object querySeq(String[] querys, String damid, String iconum) {
			foreach (String query in querys) {
				using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {
					using (SqlCommand cmd = new SqlCommand(query, conn)) {
						conn.Open();
						cmd.Parameters.AddWithValue("@damid", damid);
						cmd.Parameters.AddWithValue("@iconum", iconum);

						using (SqlDataReader sdr = cmd.ExecuteReader()) {
							if (sdr.Read()) {
								var ret = new
								{
									PPI = sdr.GetStringSafe(0),
									Iconum = sdr.GetInt32(1),
									Firm_Name = sdr.GetStringSafe(2),
									Profile = sdr.GetStringSafe(3),
									CompanyPriority = 0,
									Country_Name = sdr.GetStringSafe(4),
									DAMDocumentId = sdr.GetGuid(5),
									ReportType = sdr.GetStringSafe(6),
									ReportDate = sdr.GetDateTimeSafe(7)
								};
								return ret;
							}
						}
					}
				}
			}
			return null;
		}


		public object GetCompanyByDamID(String damid, String iconum) {
			string query = @"select d.PPI, f.Iconum, f.Firm_Name, ig.IndustryGroupCode, c.name_long, d.DAMDocumentId, r.Description,d.DocumentDate
from document as d
	join DocumentSeries as ds on d.DocumentSeriesID = ds.id
	left join FilerMst f on f.Iconum = ds.CompanyID
	left join FilerTypes t on t.Code = f.Filer_Type
	left join Countries c on c.iso_country = f.ISO_Country
	left join CompanyIndustry ci on ci.Iconum = f.Iconum
	left join IndustryDetail id on id.id = ci.IndustryDetailID
	left join IndustryGroup ig on ig.ID = id.IndustryGroupID
	left join ReportType r on r.id = d.reporttypeid
where DAMDocumentId =  @damid and f.Iconum=@iconum
	  ";
			string query1 = @"select d.PPI, f.Iconum, f.Firm_Name, ig.Description, c.name_long, d.DAMDocumentId, r.Description,d.DocumentDate
from document as d (nolock)
join DocumentSeries as ds (nolock) on d.DocumentSeriesID = ds.id
join FilerMst f (nolock) on f.Iconum = ds.CompanyID
join FilerTypes t (nolock) on t.Code = f.Filer_Type
join Countries c (nolock) on c.iso_country = f.ISO_Country
join CompanyIndustry ci (nolock) on ci.Iconum = f.Iconum
join IndustryDetail id (nolock) on id.id = ci.IndustryDetailID
join IndustryGroup ig (nolock) on ig.ID = id.IndustryGroupID
join ReportType r (nolock) on r.id = d.reporttypeid
where f.Iconum=@iconum and DAMDocumentId=@damid 
	  ";
			string query2 = @"select d.PPI, f.Iconum, f.Firm_Name, ig.Description, c.name_long, dts.DAMDocumentId, r.Description,d.DocumentDate 
from supercore.documenttimeslice dts (nolock)
join document as d (nolock) on d.DAMDocumentId = dts.DamDocumentID
join documentseries ds (nolock) on ds.id = dts.DocumentSeriesID
join FilerMst f (nolock) on f.Iconum = ds.CompanyID
join FilerTypes t (nolock) on t.Code = f.Filer_Type
join Countries c (nolock) on c.iso_country = f.ISO_Country
join Supercore.TimeSlice (nolock) ts on ts.id = dts.timesliceid
join IndustryCountryAssociation (nolock) ica on ica.id = ts.IndustryCountryAssociationID
join IndustryDetail id (nolock) on id.id = ica.IndustryDetailId
join IndustryGroup ig (nolock) on ig.ID = id.IndustryGroupID
join ReportType r (nolock) on r.id = d.reporttypeid
where dts.DamDocumentID=@damid and ds.CompanyID=@iconum
	  ";
			string query3 = @"select d.PPI, f.Iconum, f.Firm_Name, ig.Description, c.name_long, d.DAMDocumentId, r.Description,d.DocumentDate 
from Document d (nolock)
join documentseries ds (nolock) on ds.id = d.DocumentSeriesID
join FilerMst f (nolock) on f.Iconum = ds.CompanyID
join FilerTypes t (nolock) on t.Code = f.Filer_Type
join Countries c (nolock) on c.iso_country = f.ISO_Country
join companyindustry ci (nolock) on ci.Iconum = ds.CompanyID
join IndustryDetail id (nolock) on id.id = ci.IndustryDetailID
join IndustryGroup ig (nolock) on ig.ID = id.IndustryGroupID
join ReportType r (nolock) on r.id = d.reporttypeid
where ds.CompanyID=@iconum 
	  ";

			return querySeq(new String[] { query1, query2, query3 }, damid, iconum);
		}

		public CompanyDTO GetCompany(int iconum) {

			iconum = LookForStitchedIconum(iconum);

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

			const string domicileCountryQuery = @"SELECT c.name_long, c.name_short, c.iso_country
                FROM ppiiconummap p
	                LEFT JOIN Countries c ON c.iso_country = p.IsoCountry
                WHERE Iconum = @iconum
                ORDER BY isadr";

			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {
				using (SqlCommand cmd = new SqlCommand(domicileCountryQuery, conn)) {
					conn.Open();
					cmd.Parameters.AddWithValue("@iconum", iconum);

					using (SqlDataReader sdr = cmd.ExecuteReader()) {
						if (sdr.Read()) {
							company.DomicileCountryId = sdr.GetStringSafe(2);
							company.DomicileCountry = new CountryDTO
							{
								LongName = sdr.GetStringSafe(0),
								ShortName = sdr.GetStringSafe(1),
								Id = sdr.GetStringSafe(2),
								Iso3 = sdr.GetStringSafe(2)
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
			company.CollectionEffort = GetCompanyEffort(iconum);
			company.AbsolutePriority = GetAbsolutePriority(iconum);
			company.Priority = GetPriorityBucket(iconum);
			company.IsNationCode = GetIsNationCode(iconum);

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
			Dictionary<int, List<ShareClassDataDTO>> shareClassDictionary = GetCompanyShareClasses(new List<int> { iconum });
			if (shareClassDictionary.Count() > 0) {
				return shareClassDictionary.Values.First();
			}

			return null;
		}

		private Dictionary<int, List<ShareClassDataDTO>> GetCompanyShareClasses(List<int> iconums) {

			Dictionary<int, List<ShareClassDataDTO>> companyShareClasses = ShareClasses(iconums);
			//List<int> missingIconums = new List<int>();
			//foreach (int iconum in iconums) {
			//	if (!companyShareClasses.ContainsKey(iconum)) {
			//		missingIconums.Add(iconum);
			//	}
			//}

			//foreach (List<ShareClassDataDTO> shareClasses in companyShareClasses.Values) {
			//	IEnumerable<string> ppis = shareClasses.Where(s => s.PPI != null).Select(s => s.PPI).Distinct();
			//	IEnumerable<IGrouping<string, string>> groups = ppis.GroupBy(i => i.Substring(0, i.Length - 1));
			//	foreach (IGrouping<string, string> ppiGroup in groups) {
			//		if (ppiGroup.Count() > 1) {
			//			string rootPpi = ppiGroup.FirstOrDefault(i => i != null && i.EndsWith("0"));
			//			ShareClassDataDTO rootShareClass = shareClasses.FirstOrDefault(s => s.PPI == rootPpi);
			//			shareClasses.Remove(rootShareClass);
			//		}
			//	}
			//}

			//VoyagerSharesHelper voyagerShares = new VoyagerSharesHelper(_voyConnectionString, _sfConnectionString);
			//voyagerShares.PopulateTypeOfShare(companyShareClasses);

			return companyShareClasses;
		}

		private Dictionary<int, List<ShareClassDataDTO>> ShareClasses(List<int> iconums) {
			const string createTableQuery = @"CREATE TABLE #iconums ( iconum INT NOT NULL )";

			const string query = @"SELECT p.Cusip,
                                        p.Iconum,
                                        p.Name,
                                        a.Description,
                                        e.Description,
                                        s.Inception_Date,
                                        s.Term_Date,
                                        s.Price,
                                        s.Shares_Out,
                                        s.Ticker,
                                        p.Sedol,
                                        s.ISIN,
                                        s.To_Cusip,
                                        s.Issue_Type,
                                        p.PPI,
                                        x.permid,
                                        p.ShareType
                                    FROM PpiIconumMap p
									    LEFT JOIN #iconums ico ON ico.iconum = p.iconum
									    LEFT JOIN SecMas s ON p.CUSIP = s.Cusip
									    LEFT JOIN IssueTypes i ON i.Code = s.Issue_Type
									    LEFT JOIN SecMasExchanges e ON e.Exchange_Code = s.Exchange_Code
									    LEFT JOIN AssetClasses a ON a.Code = i.Asset_Code
									    LEFT JOIN secmas_sym_cusip_alias x ON x.Cusip = s.Cusip
								    WHERE ico.Iconum IS NOT NULL";

			Dictionary<int, List<ShareClassDataDTO>> companyShareClasses = new Dictionary<int, List<ShareClassDataDTO>>();
			DataTable table = new DataTable();
			table.Columns.Add("iconum", typeof(int));
			foreach (int iconum in iconums) {
				table.Rows.Add(iconum);
			}

			// Create Global Temp Table
			using (SqlConnection connection = new SqlConnection(_sfConnectionString)) {
				connection.Open();
				using (SqlCommand cmd = new SqlCommand(createTableQuery, connection)) {
					cmd.ExecuteNonQuery();
				}

				// Upload all iconums to Temp table
				using (SqlBulkCopy bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, null)) {
					bulkCopy.BatchSize = table.Rows.Count;
					bulkCopy.DestinationTableName = "#iconums";
					bulkCopy.WriteToServer(table);
				}

				using (SqlCommand cmd = new SqlCommand(query, connection)) {

					using (SqlDataReader sdr = cmd.ExecuteReader()) {
						while (sdr.Read()) {
							int iconum = sdr.GetInt32(1);
							ShareClassDataDTO shareClass = new ShareClassDataDTO
							{
								Cusip = sdr.GetStringSafe(0),

								Name = sdr.GetStringSafe(2),
								AssetClass = sdr.GetStringSafe(3),
								ListedOn = sdr.GetStringSafe(4),
								InceptionDate = sdr.GetNullable<DateTime>(5),
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
								PermId = sdr.GetStringSafe(15),
								TypeOfShare = sdr.GetStringSafe(16)
							};
							if (!companyShareClasses.ContainsKey(iconum)) {
								companyShareClasses.Add(iconum, new List<ShareClassDataDTO>());
							}
							companyShareClasses[iconum].Add(shareClass);
						}
					}
				}
			}

			return companyShareClasses;
		}

		private int LookForStitchedIconum(int iconum) {

			const string query = @"
SELECT h.iconum, m.iconum
FROM PpiIconumMapHistory h (nolock)
	JOIN PPiIconumMap m (nolock) ON h.PPI = m.PPI
WHERE h.iconum = @iconum AND m.iconum > 0
ORDER BY ChangeDate DESC";

			int newIconum = iconum;
			using (SqlConnection connection = new SqlConnection(_sfConnectionString)) {
				connection.Open();

				using (SqlCommand cmd = new SqlCommand(query, connection)) {
					cmd.Parameters.AddWithValue("@iconum", iconum);

					using (SqlDataReader sdr = cmd.ExecuteReader()) {
						while (sdr.Read()) {
							int oldIconum = sdr.GetInt32(0);
							newIconum = sdr.GetInt32(1);
						}
					}
				}
			}

			return newIconum;
		}

		public Dictionary<int, List<ShareClassDataDTO>> GetCompanyShareClassData(List<int> iconums, DateTime? reportDate, DateTime? since) {
			try {
				DateTime startTime = DateTime.Now;
				Dictionary<int, List<ShareClassDataDTO>> companyShareClassData = GetCompanyShareClasses(iconums);
				Dictionary<int, EffortDTO> companyEfforts = GetCompaniesEfforts(iconums);

				//Skipping Voyager check for US Companies, as those are already transitioned to Supercore
				List<int> voyagerIconums = companyEfforts.Where(x => x.Value.Name == EffortDTO.Voyager().Name ||
				!companyShareClassData.ContainsKey(x.Key) ||
				!companyShareClassData[x.Key].Any() ||
				!companyShareClassData[x.Key].First().PPI.StartsWith("C840")).Select(k => k.Key).ToList();

				List<int> superfastIconums = companyEfforts.Where(kvp => kvp.Value.Name == EffortDTO.SuperCore().Name).Select(kvp => kvp.Key).ToList();

				// Supercore data is SecPermId based
				if (superfastIconums.Count > 0) {
					SuperFastSharesHelper superfastShares = new SuperFastSharesHelper(_sfConnectionString);
					Dictionary<int, Dictionary<string, List<ShareClassDataItem>>> superfastShareData =
							superfastShares.GetLatestCompanyFPEShareData(superfastIconums, reportDate, since);
					foreach (KeyValuePair<int, Dictionary<string, List<ShareClassDataItem>>> keyValuePair in superfastShareData) {
						int iconum = keyValuePair.Key;
						Dictionary<string, List<ShareClassDataItem>> superfastSecurityItems = keyValuePair.Value;
						if (companyShareClassData.ContainsKey(iconum)) {
							List<ShareClassDataDTO> shareClassDataList = companyShareClassData[iconum];
							foreach (ShareClassDataDTO shareClass in shareClassDataList) {
								List<ShareClassDataItem> securityItemList = new List<ShareClassDataItem>();
								if (shareClass.PermId != null && superfastSecurityItems.ContainsKey(shareClass.PermId) && superfastSecurityItems[shareClass.PermId] != null) {
									securityItemList = superfastSecurityItems[shareClass.PermId];
								}
								shareClass.ShareClassData = securityItemList;
							}
						}
					}
				}

				// Voyager data is PPI based
				if (voyagerIconums.Count > 0) {
					VoyagerSharesHelper voyagerShares = new VoyagerSharesHelper(_voyConnectionString, _sfConnectionString);
					Dictionary<int, Dictionary<string, List<ShareClassDataItem>>> voyagerShareData =
							voyagerShares.GetLatestCompanyFPEShareData(voyagerIconums, reportDate, since);
					foreach (KeyValuePair<int, Dictionary<string, List<ShareClassDataItem>>> keyValuePair in voyagerShareData) {
						int iconum = keyValuePair.Key;
						Dictionary<string, List<ShareClassDataItem>> voyagerSecurityItems = keyValuePair.Value;
						if (companyShareClassData.ContainsKey(iconum)) {
							List<ShareClassDataDTO> shareClassDataList = companyShareClassData[iconum];
							foreach (ShareClassDataDTO shareClass in shareClassDataList) {
								if (shareClass.ShareClassData == null)
									shareClass.ShareClassData = new List<ShareClassDataItem>();
								if (shareClass.PPI != null && voyagerSecurityItems.ContainsKey(shareClass.PPI) && voyagerSecurityItems[shareClass.PPI].Any()) {
									var supercoreReportDate = shareClass.ShareClassData.Any() ? shareClass.ShareClassData.Max(x => x.ReportDate) : DateTime.MinValue;
									if (voyagerSecurityItems[shareClass.PPI].Max(x => x.ReportDate) > supercoreReportDate)
										shareClass.ShareClassData = voyagerSecurityItems[shareClass.PPI];
								}
							}
						}
					}
				}

				return companyShareClassData;
			} catch (Exception ex) {
				LogException(ex, string.Join(",", iconums));
				throw ex;
			}
		}

		/// <summary>
		/// Returns all Fiscal Period End std items for given iconums and std code  within provided date range
		/// </summary>
		/// <param name="iconums"></param>
		/// <param name="stdCode"></param>
		/// <param name="reportDate"></param>
		/// <param name="since"></param>
		/// <returns></returns>
		public Dictionary<int, List<ShareClassDataDTO>> GetAllShareClassData(List<int> iconums, string stdCode, DateTime? reportDate, DateTime? since) {
			try {
				Dictionary<int, List<ShareClassDataDTO>> companyShareClassData = GetCompanyShareClasses(iconums);
				Dictionary<int, EffortDTO> companyEfforts = GetCompaniesEfforts(iconums);

				List<int> voyagerIconums = companyEfforts.Keys.ToList();
				List<int> superfastIconums = companyEfforts.Where(kvp => kvp.Value.Name == EffortDTO.SuperCore().Name).Select(kvp => kvp.Key).ToList();
				HashSet<string> mergeChecker = new HashSet<string>();

				// Supercore data is SecPermId based
				if (superfastIconums.Count > 0) {
					SuperFastSharesHelper superfastShares = new SuperFastSharesHelper(_sfConnectionString);
					Dictionary<int, Dictionary<string, List<ShareClassDataItem>>> superfastShareData =
									superfastShares.GetAllFpeShareDataForStdCode(superfastIconums, stdCode, reportDate, since);
					foreach (KeyValuePair<int, Dictionary<string, List<ShareClassDataItem>>> keyValuePair in superfastShareData) {
						int iconum = keyValuePair.Key;
						Dictionary<string, List<ShareClassDataItem>> superfastSecurityItems = keyValuePair.Value;
						if (companyShareClassData.ContainsKey(iconum)) {
							List<ShareClassDataDTO> shareClassDataList = companyShareClassData[iconum];
							foreach (ShareClassDataDTO shareClass in shareClassDataList) {
								List<ShareClassDataItem> securityItemList = new List<ShareClassDataItem>();
								if (shareClass.PermId != null && superfastSecurityItems.ContainsKey(shareClass.PermId) && superfastSecurityItems[shareClass.PermId] != null) {
									foreach (var item in superfastSecurityItems[shareClass.PermId]) {
										var key = String.Format("{0}:{1}:{2}:{3}:{4}", iconum, shareClass.PermId ?? "", shareClass.PPI ?? "", item.ReportDate, item.ItemId);
										if (!mergeChecker.Contains(key)) {
											securityItemList.Add(item);
											mergeChecker.Add(key);
										}
									}
								}
								shareClass.ShareClassData = securityItemList;
							}
						}
					}
				}

				// Voyager data is PPI based
				if (voyagerIconums.Count > 0) {
					VoyagerSharesHelper voyagerShares = new VoyagerSharesHelper(_voyConnectionString, _sfConnectionString);
					Dictionary<int, Dictionary<string, List<ShareClassDataItem>>> voyagerShareData =
									voyagerShares.GetAllFpeShareDataForStdCode(voyagerIconums, stdCode, reportDate, since);
					foreach (KeyValuePair<int, Dictionary<string, List<ShareClassDataItem>>> keyValuePair in voyagerShareData) {
						int iconum = keyValuePair.Key;
						Dictionary<string, List<ShareClassDataItem>> voyagerSecurityItems = keyValuePair.Value;
						if (companyShareClassData.ContainsKey(iconum)) {
							List<ShareClassDataDTO> shareClassDataList = companyShareClassData[iconum];
							foreach (ShareClassDataDTO shareClass in shareClassDataList) {
								if (shareClass.ShareClassData == null)
									shareClass.ShareClassData = new List<ShareClassDataItem>();
								if (shareClass.PPI != null && voyagerSecurityItems.ContainsKey(shareClass.PPI) && voyagerSecurityItems[shareClass.PPI].Any()) {
									foreach (var item in voyagerSecurityItems[shareClass.PPI]) {
										var key = String.Format("{0}:{1}:{2}:{3}:{4}", iconum, shareClass.PermId, shareClass.PPI, item.ReportDate, item.ItemId);
										if (!mergeChecker.Contains(key)) {
											shareClass.ShareClassData.Add(item);
											mergeChecker.Add(key);
										}
									}
								}
							}
						}
					}
				}

				return companyShareClassData;
			} catch (Exception ex) {
				LogException(ex, string.Join(",", iconums));
				throw ex;
			}
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
				if (shareClass.Cusip != null && superfastSecurityItems.ContainsKey(shareClass.Cusip)) {
					securityItemList = superfastSecurityItems[shareClass.Cusip];
				} else if (shareClass.PPI != null && voyagerSecurityItems.ContainsKey(shareClass.PPI)) {
					securityItemList = voyagerSecurityItems[shareClass.PPI];
				}
				ShareClassDataDTO shareClassData = new ShareClassDataDTO(shareClass, securityItemList);
				shareClassDataList.Add(shareClassData);
			}

			return shareClassDataList;
		}

		public EffortDTO GetCompanyEffort(int iconum) {
			Dictionary<int, EffortDTO> effortDictionary = GetCompaniesEfforts(new List<int>() { iconum });
			if (!effortDictionary.ContainsKey(iconum)) {
				return null;
			}
			return effortDictionary[iconum];
		}

		public Dictionary<int, EffortDTO> GetCompaniesEfforts(List<int> companies) {

			Dictionary<int, EffortDTO> effortDictionary = new Dictionary<int, EffortDTO>();			
			foreach (int iconum in companies) {				
				effortDictionary.Add(iconum, EffortDTO.SuperCore());
			}
			return effortDictionary;
		}

		public Dictionary<int, CompanyPriority> GetCompanyPriority(List<int> iconums) {
			Dictionary<int, CompanyPriority> priorities = new Dictionary<int, CompanyPriority>();
			Dictionary<int, decimal?> absolutePriorities = GetAbsolutePriority(iconums);
			Dictionary<int, int?> priorityBuckets = GetPriorityBucket(iconums);
			foreach (int iconum in iconums) {
				decimal? absolutePriority = absolutePriorities[iconum];
				int? priorityBucket = priorityBuckets[iconum];
				priorities.Add(iconum, new CompanyPriority { AbsolutePriority = absolutePriority, Priority = priorityBucket });
			}
			return priorities;
		}

		public decimal? GetAbsolutePriority(int iconum) {
			Dictionary<int, decimal?> companyPriority = GetAbsolutePriority(new List<int> { iconum });
			if (!companyPriority.ContainsKey(iconum)) {
				throw new MissingIconumException(iconum);
			}
			return companyPriority[iconum];
		}

		public Dictionary<int, decimal?> GetAbsolutePriority(List<int> iconums) {
			Dictionary<int, decimal?> priorityDictionary = new Dictionary<int, decimal?>();
			DataTable table = new DataTable();
			table.Columns.Add("iconum", typeof(int));
			foreach (int iconum in iconums) {
				table.Rows.Add(iconum);
				priorityDictionary.Add(iconum, null);
			}

			const string createTableQuery = @"CREATE TABLE #iconums ( iconum INT NOT NULL )";
			const string query = @"SELECT p.iconum, p.priority
																FROM CompanyPriority p
																	JOIN #iconums i ON i.iconum = p.iconum";

			// Create Global Temp Table
			using (SqlConnection connection = new SqlConnection(_damConnectionString)) {
				connection.Open();
				using (SqlCommand cmd = new SqlCommand(createTableQuery, connection)) {
					cmd.ExecuteNonQuery();
				}

				// Upload all iconums to Temp table
				using (SqlBulkCopy bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, null)) {
					bulkCopy.BatchSize = table.Rows.Count;
					bulkCopy.DestinationTableName = "#iconums";
					bulkCopy.WriteToServer(table);
				}

				using (SqlCommand cmd = new SqlCommand(query, connection)) {
					using (SqlDataReader reader = cmd.ExecuteReader()) {
						while (reader.Read()) {
							int iconum = reader.GetInt32(0);
							double priority = reader.GetFloat(1);
							priorityDictionary[iconum] = decimal.Parse(priority.ToString());
						}
					}
				}
			}

			return priorityDictionary;
		}

		private bool GetIsNationCode(int iconum) {
			const string query = @"SELECT isnationcode FROM ppiiconummap WHERE iconum = @iconum";

			// Create Global Temp Table
			using (SqlConnection connection = new SqlConnection(_sfConnectionString)) {
				connection.Open();
				using (SqlCommand cmd = new SqlCommand(query, connection)) {
					cmd.Parameters.AddWithValue("@iconum", iconum);
					using (SqlDataReader reader = cmd.ExecuteReader()) {
						while (reader.Read()) {
							return reader.GetBoolean(0);
						}
					}
				}
			}

			return false;
		}


		public int? GetPriorityBucket(int iconum) {
			Dictionary<int, int?> companyPriority = GetPriorityBucket(new List<int> { iconum });
			if (!companyPriority.ContainsKey(iconum)) {
				throw new MissingIconumException(iconum);
			}
			return companyPriority[iconum];
		}

		public Dictionary<int, int?> GetPriorityBucket(List<int> iconums) {
			Dictionary<int, int?> priorityDictionary = new Dictionary<int, int?>();
			DataTable table = new DataTable();
			table.Columns.Add("iconum", typeof(int));
			foreach (int iconum in iconums) {
				table.Rows.Add(iconum);
				priorityDictionary.Add(iconum, null);
			}

			const string createTableQuery = @"CREATE TABLE #iconums ( iconum INT NOT NULL )";
			const string query = @"SELECT iconum, priority FROM
																(SELECT p.iconum, p.priority,
																		row_number() OVER (PARTITION BY p.iconum ORDER BY IsActive DESC, IsReleased DESC, IsAdr ASC, priority ASC) as rank
																	FROM FdsTriPpiMap p
																		JOIN #iconums i ON i.iconum = p.iconum
																	WHERE RIGHT(p.ppi, 1) = '0') tmp
																WHERE rank = 1";


			// Create Global Temp Table
			using (SqlConnection connection = new SqlConnection(_damConnectionString)) {
				connection.Open();
				using (SqlCommand cmd = new SqlCommand(createTableQuery, connection)) {
					cmd.ExecuteNonQuery();
				}

				// Upload all iconums to Temp table
				using (SqlBulkCopy bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, null)) {
					bulkCopy.BatchSize = table.Rows.Count;
					bulkCopy.DestinationTableName = "#iconums";
					bulkCopy.WriteToServer(table);
				}

				using (SqlCommand cmd = new SqlCommand(query, connection)) {
					using (SqlDataReader reader = cmd.ExecuteReader()) {
						while (reader.Read()) {
							int iconum = reader.GetInt32(0);
							Byte? priority = reader.GetNullable<Byte>(1);
							priorityDictionary[iconum] = priority;
						}
					}
				}
			}

			return priorityDictionary;
		}

		public string GetSecPermId(int iconum) {
			string query = @"select p.iconum, p.EntityPermId, p.PermId, p.Name, p.IsoCountry, t.Descrip from PpiIconumMap p
												left join filermst f on p.iconum = f.Iconum
												left join FilerTypes t on t.Code = f.Filer_Type
												where p.iconum = @iconum";

			string result = "";
			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {
				using (SqlCommand cmd = new SqlCommand(query, conn)) {
					conn.Open();
					cmd.Parameters.AddWithValue("@iconum", iconum);

					using (SqlDataReader sdr = cmd.ExecuteReader()) {
						if (sdr.Read()) {
							result = sdr.GetStringSafe(2);
						}
					}
				}
			}
			return result;
		}

		private void LogException(Exception ex, string exMeta = null) {
			var EnvStackTrace = String.Join("<br/>", Environment.StackTrace.Replace("\r", "").Split('\n').Where(x => x.Contains("CurrentShares")));
			if (!string.IsNullOrEmpty(exMeta))
				EnvStackTrace += "<br/><br/>ExceptionMeta: " + exMeta;
			SendEmail(ex.Message + "<br/>" + ex.StackTrace + "<br/><br/>" + EnvStackTrace);
		}

		private void SendEmail(string body) {

			var EnvStackTrace = String.Join("<br/>", Environment.StackTrace.Replace("\r", "").Split('\n').Where(x => x.Contains("CurrentShares")));
			SmtpClient mySMTP = new SmtpClient("mail.factset.com");
			MailAddress mailFrom = new MailAddress("CIE.CurrentShares.Support@factset.com", "CIE Current Shares Support");
			MailMessage message = new MailMessage();
			message.From = mailFrom;
			message.To.Add(new MailAddress("apatil@factset.com", "Abhijeet Patil"));
			message.Subject = "Current Shares Logger";
			message.DeliveryNotificationOptions = DeliveryNotificationOptions.OnFailure;
			message.Body = body;
			message.IsBodyHtml = true;
			mySMTP.Send(message);
		}
	}
}
