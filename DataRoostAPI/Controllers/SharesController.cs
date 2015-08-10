using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;
using System.Configuration;
using System.Web.Http.Cors;

using CCS.Fundamentals.DataRoostAPI.Access;
using CCS.Fundamentals.DataRoostAPI.Access.Company;

using DataRoostAPI.Common.Models;

namespace CCS.Fundamentals.DataRoostAPI.Controllers {

	[RoutePrefix("api/v1/companies")]
	public class SharesController : ApiController {

		[Route("{CompanyId}/shares/latestFiscalPeriodEnd/")]
		[HttpGet]
		public ShareClassDataDTO[] GetLatestFiscalPeriodEndSharesData(string CompanyId, DateTime? reportDate = null) {
			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ConnectionString;
			string voyConnectionString = ConfigurationManager.ConnectionStrings["Voyager"].ConnectionString;
			string lionConnectionString = ConfigurationManager.ConnectionStrings["Lion"].ConnectionString;
			string damConnectionString = ConfigurationManager.ConnectionStrings["FFDAM"].ConnectionString;

			int iconum = PermId.PermId2Iconum(CompanyId);

			CompanyHelper helper = new CompanyHelper(sfConnectionString, voyConnectionString, lionConnectionString, damConnectionString);
			return helper.GetCompanyShareClassData(iconum, reportDate).ToArray();
		}

		[Route("shares/latestFiscalPeriodEnd/")]
		[HttpPost]
		public Dictionary<int, ShareClassDataDTO[]> GetLatestFiscalPeriodEndSharesData(List<string> companyIds) {
			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ConnectionString;
			string voyConnectionString = ConfigurationManager.ConnectionStrings["Voyager"].ConnectionString;
			string lionConnectionString = ConfigurationManager.ConnectionStrings["Lion"].ConnectionString;
			string damConnectionString = ConfigurationManager.ConnectionStrings["FFDAM"].ConnectionString;

			Dictionary<int, ShareClassDataDTO[]> companyData = new Dictionary<int, ShareClassDataDTO[]>();
			foreach (string companyId in companyIds) {
				int iconum = PermId.PermId2Iconum(companyId);

				CompanyHelper helper = new CompanyHelper(sfConnectionString,
				                                         voyConnectionString,
				                                         lionConnectionString,
				                                         damConnectionString);
				ShareClassDataDTO[] data = helper.GetCompanyShareClassData(iconum, null).ToArray();
				companyData.Add(iconum, data);
			}
			return companyData;
		}

		[Route("{CompanyId}/shares/currentShares/")]
		[HttpGet]
		public ShareClassDataDTO[] GetCurrentShareData(string CompanyId) {
			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ConnectionString;
			string voyConnectionString = ConfigurationManager.ConnectionStrings["Voyager"].ConnectionString;
			string lionConnectionString = ConfigurationManager.ConnectionStrings["Lion"].ConnectionString;
			string damConnectionString = ConfigurationManager.ConnectionStrings["FFDAM"].ConnectionString;

			int iconum = PermId.PermId2Iconum(CompanyId);

			CompanyHelper helper = new CompanyHelper(sfConnectionString, voyConnectionString, lionConnectionString, damConnectionString);
			return helper.GetCurrentCompanyShareClassData(iconum).ToArray();
		}
	}
}
