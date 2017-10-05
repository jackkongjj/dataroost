using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;

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

            using (SqlConnection conn = new SqlConnection(_sfConnectionString))
            {
                using (SqlCommand cmd = new SqlCommand(domicileCountryQuery, conn))
                {
                    conn.Open();
                    cmd.Parameters.AddWithValue("@iconum", iconum);

                    using (SqlDataReader sdr = cmd.ExecuteReader())
                    {
                        if (sdr.Read())
                        {
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
            List<int> missingIconums = new List<int>();
		    foreach (int iconum in iconums) {
		        if (!companyShareClasses.ContainsKey(iconum)) {
		            missingIconums.Add(iconum);
		        }
		    }

            foreach (List<ShareClassDataDTO> shareClasses in companyShareClasses.Values) {
				IEnumerable<string> ppis = shareClasses.Where(s => s.PPI != null).Select(s => s.PPI).Distinct();
				IEnumerable<IGrouping<string, string>> groups = ppis.GroupBy(i => i.Substring(0, i.Length - 1));
				foreach (IGrouping<string, string> ppiGroup in groups) {
					if (ppiGroup.Count() > 1) {
						string rootPpi = ppiGroup.FirstOrDefault(i => i != null && i.EndsWith("0"));
						ShareClassDataDTO rootShareClass = shareClasses.FirstOrDefault(s => s.PPI == rootPpi);
						shareClasses.Remove(rootShareClass);
					}
				}
			}

			VoyagerSharesHelper voyagerShares = new VoyagerSharesHelper(_voyConnectionString, _sfConnectionString);
			voyagerShares.PopulateTypeOfShare(companyShareClasses);

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
                                        x.permid
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
            foreach (int iconum in iconums)
            {
                table.Rows.Add(iconum);
            }

            // Create Global Temp Table
            using (SqlConnection connection = new SqlConnection(_sfConnectionString))
            {
                connection.Open();
                using (SqlCommand cmd = new SqlCommand(createTableQuery, connection))
                {
                    cmd.ExecuteNonQuery();
                }

                // Upload all iconums to Temp table
                using (SqlBulkCopy bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, null))
                {
                    bulkCopy.BatchSize = table.Rows.Count;
                    bulkCopy.DestinationTableName = "#iconums";
                    bulkCopy.WriteToServer(table);
                }

                using (SqlCommand cmd = new SqlCommand(query, connection))
                {

                    using (SqlDataReader sdr = cmd.ExecuteReader())
                    {
                        while (sdr.Read())
                        {
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
                                PermId = sdr.GetStringSafe(15)
                            };
                            if (!companyShareClasses.ContainsKey(iconum))
                            {
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
			DateTime startTime = DateTime.Now;
			Dictionary<int, List<ShareClassDataDTO>> companyShareClassData = GetCompanyShareClasses(iconums);
			Dictionary<int, EffortDTO> companyEfforts = GetCompaniesEfforts(iconums);
			TimeSpan shareClassAndEffortsDuration = DateTime.Now.Subtract(startTime);

			List<int> voyagerIconums = companyEfforts.Where(kvp => kvp.Value.Name == EffortDTO.Voyager().Name).Select(kvp => kvp.Key).ToList();
			List<int> superfastIconums = companyEfforts.Where(kvp => kvp.Value.Name == EffortDTO.SuperCore().Name).Select(kvp => kvp.Key).ToList();

			DateTime superfastStartTime = DateTime.Now;
			if (superfastIconums.Count > 0) {
				SuperFastSharesHelper superfastShares = new SuperFastSharesHelper(_sfConnectionString);
				Dictionary<int, Dictionary<string, List<ShareClassDataItem>>> superfastShareData =
					superfastShares.GetLatestCompanyFPEShareData(superfastIconums, reportDate, since);
				foreach (KeyValuePair<int, Dictionary<string, List<ShareClassDataItem>>>  keyValuePair in superfastShareData) {
					int iconum = keyValuePair.Key;
					Dictionary<string, List<ShareClassDataItem>> superfastSecurityItems = keyValuePair.Value;
					if (companyShareClassData.ContainsKey(iconum)) {
						List<ShareClassDataDTO> shareClassDataList = companyShareClassData[iconum];
						foreach (ShareClassDataDTO shareClass in shareClassDataList) {
							List<ShareClassDataItem> securityItemList = new List<ShareClassDataItem>();
							if (shareClass.Cusip != null && superfastSecurityItems.ContainsKey(shareClass.Cusip)) {
								securityItemList = superfastSecurityItems[shareClass.Cusip];
							}
							shareClass.ShareClassData = securityItemList;
						}
					}
				}
			}
			TimeSpan superfastDuration = DateTime.Now.Subtract(superfastStartTime);

			DateTime voyagerStartTime = DateTime.Now;
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
							List<ShareClassDataItem> securityItemList = new List<ShareClassDataItem>();
							if (shareClass.PPI != null && voyagerSecurityItems.ContainsKey(shareClass.PPI)) {
								securityItemList = voyagerSecurityItems[shareClass.PPI];
							}
							shareClass.ShareClassData = securityItemList;
						}
					}
				}
			}
			TimeSpan voyagerDuration = DateTime.Now.Subtract(voyagerStartTime);

			return companyShareClassData;
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
            DateTime startTime = DateTime.Now;
            Dictionary<int, List<ShareClassDataDTO>> companyShareClassData = GetCompanyShareClasses(iconums);
            Dictionary<int, EffortDTO> companyEfforts = GetCompaniesEfforts(iconums);

            List<int> voyagerIconums = companyEfforts.Where(kvp => kvp.Value.Name == EffortDTO.Voyager().Name).Select(kvp => kvp.Key).ToList();
            List<int> superfastIconums = companyEfforts.Where(kvp => kvp.Value.Name == EffortDTO.SuperCore().Name).Select(kvp => kvp.Key).ToList();

            DateTime superfastStartTime = DateTime.Now;
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
                            if (shareClass.Cusip != null && superfastSecurityItems.ContainsKey(shareClass.Cusip)) {
                                securityItemList = superfastSecurityItems[shareClass.Cusip];
                            }
                            shareClass.ShareClassData = securityItemList;
                        }
                    }
                }
            }
            TimeSpan superfastDuration = DateTime.Now.Subtract(superfastStartTime);

            DateTime voyagerStartTime = DateTime.Now;
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
                            List<ShareClassDataItem> securityItemList = new List<ShareClassDataItem>();
                            if (shareClass.PPI != null && voyagerSecurityItems.ContainsKey(shareClass.PPI)) {
                                securityItemList = voyagerSecurityItems[shareClass.PPI];
                            }
                            shareClass.ShareClassData = securityItemList;
                        }
                    }
                }
            }
            TimeSpan voyagerDuration = DateTime.Now.Subtract(voyagerStartTime);

            return companyShareClassData;
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
			Dictionary<int, EffortDTO> effortDictionary = GetCompaniesEfforts(new List<int>() { iconum });
			if (!effortDictionary.ContainsKey(iconum)) {
				return null;
			}
			return effortDictionary[iconum];
		}

        public Dictionary<int, EffortDTO> GetCompaniesEfforts(List<int> companies) {

            Dictionary<int, EffortDTO> effortDictionary = new Dictionary<int, EffortDTO>();
			DataTable table = new DataTable();
			table.Columns.Add("iconum", typeof (int));
			foreach (int iconum in companies) {
				table.Rows.Add(iconum);
				effortDictionary.Add(iconum, EffortDTO.Voyager());
			}

			const string createTableQuery = @"CREATE TABLE #iconums ( iconum INT NOT NULL )";
            const string query = @"
with ico as
(
       select c.iconum, IsoCountry 
       from #iconums c
       join fdstrippimap fds on fds.iconum = c.iconum
       where IsAdr = 0 and IsActive=1 --order by ClientAddDate, [Priority]
)      
select  fds.iconum
from fce.rules r
join ico fds on r.country = fds.IsoCountry
join fce.RulesToPath rtp on r.id = rtp.RuleId
join fce.Paths p on p.Id = rtp.PathId
join fce.PathTransitions pt on p.Id = pt.PathId
join WorkQueueTasks wqt on wqt.id = pt.taskid
left join fce.CompanyListRulesToPath clr on clr.RulesToPathId = rtp.Id
where wqt.name = 'Finantula' and clr.RulesToPathId is null
union
select matchList.iconum from
(
       select fds.iconum, rulestopathid, matchcount = count(distinct 1)
       from fce.rules r
       join ico fds on r.country = fds.IsoCountry
       join fce.RulesToPath rtp on r.id = rtp.RuleId
       join fce.Paths p on p.Id = rtp.PathId
       join fce.PathTransitions pt on p.Id = pt.PathId
       join WorkQueueTasks wqt on wqt.id = pt.taskid
       join fce.CompanyListRulesToPath clr on clr.RulesToPathId = rtp.Id
       join companylistcompanies clc on clc.companylistid = clr.companylistid and clc.iconum = fds.iconum
       where wqt.name = 'Finantula'
       group by clr.RulesToPathId, fds.iconum
) as matchList
join (
       select rulestopathid, matchcount = count(1)
       from fce.CompanyListRulesToPath
       group by rulestopathid
) as reqList on matchList.RulesToPathId = reqList.RulesToPathId and matchList.matchcount = reqList.matchcount";

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
                            EffortDTO superfastEffort = EffortDTO.SuperCore();
							effortDictionary[iconum] = superfastEffort;
						}
					}
				}
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

	    private bool GetIsNationCode(int iconum ) {
            const string query = @"SELECT isnationcode FROM ppiiconummap WHERE iconum = @iconum";

            // Create Global Temp Table
            using (SqlConnection connection = new SqlConnection(_sfConnectionString))
            {
                connection.Open();
                using (SqlCommand cmd = new SqlCommand(query, connection))
                {
					cmd.Parameters.AddWithValue("@iconum", iconum);
                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
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
	}

}
