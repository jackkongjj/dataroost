using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;
using System.Configuration;
using CCS.Fundamentals.DataRoostAPI.Access;
using CCS.Fundamentals.DataRoostAPI.Access.Company;

using DataRoostAPI.Common.Models;

namespace CCS.Fundamentals.DataRoostAPI.Controllers {

	[RoutePrefix("api/v1")]
	public class CompanyController : ApiController {

		[Route("companies/")]
		[HttpGet]
		public CompanyDTO[] QueryCompanies() {
			throw new NotImplementedException();
		}

		[Route("companies/{CompanyId}")]
		[HttpGet]
		public CompanyDTO[] GetCompanies(string CompanyId) {
			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ConnectionString;
			string voyConnectionString = ConfigurationManager.ConnectionStrings["Voyager"].ConnectionString;
			string lionConnectionString = ConfigurationManager.ConnectionStrings["Lion"].ConnectionString;
			string damConnectionString = ConfigurationManager.ConnectionStrings["FFDAM"].ConnectionString;

			int iconum = PermId.PermId2Iconum(CompanyId);

			CompanyHelper helper = new CompanyHelper(sfConnectionString, voyConnectionString, lionConnectionString, damConnectionString);
			return new CompanyDTO[] { helper.GetCompany(iconum) };
		}

		[Route("companies/{CompanyId}/collectioneffort/")]
		[HttpGet]
		public EffortDTO GetCompanyCollectionEffort(string CompanyId) {
			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ConnectionString;
			string voyConnectionString = ConfigurationManager.ConnectionStrings["Voyager"].ConnectionString;
			string lionConnectionString = ConfigurationManager.ConnectionStrings["Lion"].ConnectionString;
			string damConnectionString = ConfigurationManager.ConnectionStrings["FFDAM"].ConnectionString;

			int iconum = PermId.PermId2Iconum(CompanyId);

			CompanyHelper helper = new CompanyHelper(sfConnectionString, voyConnectionString, lionConnectionString, damConnectionString);
			return helper.GetCompanyEffort(iconum);
		}

		[Route("companies/collectioneffort/")]
		[HttpPost]
		public Dictionary<int, EffortDTO> GetCollectionEffortForCompanies(List<string> companyIds) {
			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ConnectionString;
			string voyConnectionString = ConfigurationManager.ConnectionStrings["Voyager"].ConnectionString;
			string lionConnectionString = ConfigurationManager.ConnectionStrings["Lion"].ConnectionString;
			string damConnectionString = ConfigurationManager.ConnectionStrings["FFDAM"].ConnectionString;

			Dictionary<int, EffortDTO> companyEfforts = new Dictionary<int, EffortDTO>();
			foreach (string companyId in companyIds) {

				int iconum = PermId.PermId2Iconum(companyId);

				CompanyHelper helper = new CompanyHelper(sfConnectionString,
				                                         voyConnectionString,
				                                         lionConnectionString,
				                                         damConnectionString);
				companyEfforts.Add(iconum, helper.GetCompanyEffort(iconum));
			}

			return companyEfforts;
		}

		[Route("companies/{CompanyId}/efforts/")]
		[HttpGet]
		public EffortDTO[] QueryEfforts(string CompanyId) {
			return new EffortDTO[] { new EffortDTO() { Name = "voyager" }, new EffortDTO() { Name = "superfast" }, new EffortDTO() { Name = "sfvoy_join" } };
		}
	}
}
