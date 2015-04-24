using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;
using System.Configuration;
using System.Web.Http.Cors;

using CCS.Fundamentals.DataRoostAPI.Models;
using CCS.Fundamentals.DataRoostAPI.Access;
using CCS.Fundamentals.DataRoostAPI.Access.Company;

namespace CCS.Fundamentals.DataRoostAPI.Controllers {

	[EnableCors("*", "*", "*")]
	[RoutePrefix("api/v1/companies/{CompanyId}/shares")]
	public class SharesController : ApiController {

		[Route("latestFiscalPeriodEnd/")]
		[HttpGet]
		public ShareClassDataDTO[] GetLatestFiscalPeriodEndSharesData(string CompanyId, DateTime? reportDate = null) {
			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ConnectionString;
			string voyConnectionString = ConfigurationManager.ConnectionStrings["Voyager"].ConnectionString;
			string lionConnectionString = ConfigurationManager.ConnectionStrings["Lion"].ConnectionString;

			int iconum = PermId.PermId2Iconum(CompanyId);

			CompanyHelper helper = new CompanyHelper(sfConnectionString, voyConnectionString, lionConnectionString);
			return helper.GetCompanyShareClassData(iconum, reportDate).ToArray();
		}

		[Route("currentShares/")]
		[HttpGet]
		public ShareClassDataDTO[] GetCurrentShareData(string CompanyId) {
			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ConnectionString;
			string voyConnectionString = ConfigurationManager.ConnectionStrings["Voyager"].ConnectionString;
			string lionConnectionString = ConfigurationManager.ConnectionStrings["Lion"].ConnectionString;

			int iconum = PermId.PermId2Iconum(CompanyId);

			CompanyHelper helper = new CompanyHelper(sfConnectionString, voyConnectionString, lionConnectionString);
			return helper.GetCurrentCompanyShareClassData(iconum).ToArray();
		}
	}
}
