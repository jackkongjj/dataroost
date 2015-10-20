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
		public Dictionary<int, List<ShareClassDataDTO>> GetLatestFiscalPeriodEndSharesData(string CompanyId, DateTime? reportDate = null, DateTime? since = null) {
			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ConnectionString;
			string voyConnectionString = ConfigurationManager.ConnectionStrings["Voyager"].ConnectionString;
			string lionConnectionString = ConfigurationManager.ConnectionStrings["Lion"].ConnectionString;
			string damConnectionString = ConfigurationManager.ConnectionStrings["FFDAM"].ConnectionString;

			int iconum = PermId.PermId2Iconum(CompanyId);
			List<int> iconumList = new List<int> {iconum};

			CompanyHelper helper = new CompanyHelper(sfConnectionString, voyConnectionString, lionConnectionString, damConnectionString);
			return helper.GetCompanyShareClassData(iconumList, reportDate, since);
		}

		[Route("shares/latestFiscalPeriodEnd/")]
		[HttpPost]
		public Dictionary<int, List<ShareClassDataDTO>> GetLatestFiscalPeriodEndSharesData(List<string> companyIds, DateTime? reportDate = null, DateTime? since = null) {
			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ConnectionString;
			string voyConnectionString = ConfigurationManager.ConnectionStrings["Voyager"].ConnectionString;
			string lionConnectionString = ConfigurationManager.ConnectionStrings["Lion"].ConnectionString;
			string damConnectionString = ConfigurationManager.ConnectionStrings["FFDAM"].ConnectionString;

			List<int> iconums = companyIds.Select(companyId => PermId.PermId2Iconum(companyId)).ToList();
			iconums = iconums.Distinct().ToList();

			CompanyHelper helper = new CompanyHelper(sfConnectionString,
																				 voyConnectionString,
																				 lionConnectionString,
																				 damConnectionString);
			return helper.GetCompanyShareClassData(iconums, reportDate, since);
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
