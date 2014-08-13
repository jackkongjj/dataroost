using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Data;
using System.Data.SqlClient;
using FactSet.Data.SqlClient;
using CCS.Fundamentals.DataRoostAPI.Models;

namespace CCS.Fundamentals.DataRoostAPI.Access.Company {
	public class CompanyHelper {
		private readonly string _sfConnectionString;
		private readonly string _lionConnectionString;

		public CompanyHelper(string sfConnectionString, string lionConnectionString) {
			_sfConnectionString = sfConnectionString;
			_lionConnectionString = lionConnectionString;
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
							company.Country = new CountryDTO()
							{
								LongName = sdr.GetStringSafe(2),
								ShortName = sdr.GetStringSafe(3),
								Id = sdr.GetStringSafe(4),
								Iso3 = sdr.GetStringSafe(4),
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
			string query = @"SELECT PPI FROM FdsTriPpiMap WHERE iconum = @iconum AND RIGHT(PPI, 1) = '0'";

			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {
				using (SqlCommand cmd = new SqlCommand(query, conn)) {
					conn.Open();
					cmd.Parameters.AddWithValue("@iconum", iconum);

					using (SqlDataReader sdr = cmd.ExecuteReader()) {
						if (sdr.Read()) {
							return sdr.GetStringSafe(0);
						}
					}
				}
			}

			return string.Empty;
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
                                    p.PPI
                                    --x.PermID
                                FROM SecMas s
		                            LEFT JOIN IssueTypes i ON i.Code = s.Issue_Type
		                            LEFT JOIN SecMasExchanges e ON e.Exchange_Code = s.Exchange_Code
		                            LEFT JOIN AssetClasses a ON a.Code = i.Asset_Code
                                    LEFT JOIN FdsTriPpiMap p ON p.CUSIP = s.Cusip
                                    --LEFT JOIN SecMas_XRef x ON x.Cusip = s.Cusip
	                            WHERE s.Iconum = @iconum
                                    --AND RIGHT(p.PPI, 1) != '0'
                                    AND s.term_date IS NULL
                                    AND s.Cusip in (SELECT DISTINCT d.SecurityID FROM SDBTimeSeriesDetailSecurity d JOIN secmas s ON s.Cusip = d.SecurityID WHERE s.iconum = @iconum)";

			List<ShareClassDTO> shareClasses = new List<ShareClassDTO>();
			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {
				using (SqlCommand cmd = new SqlCommand(query, conn)) {
					conn.Open();
					cmd.Parameters.AddWithValue("@iconum", iconum);

					using (SqlDataReader sdr = cmd.ExecuteReader()) {
						while (sdr.Read()) {
							ShareClassDTO shareClass = new ShareClassDTO()
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
								//PermId = sdr.GetStringSafe(15),
							};
							shareClasses.Add(shareClass);
						}
					}
				}
			}

			return shareClasses;
		}
	}
}