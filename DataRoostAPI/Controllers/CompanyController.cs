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
			if (string.IsNullOrEmpty(CompanyId)) {
				return null;
			}

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
			if (string.IsNullOrEmpty(CompanyId)) {
				return null;
			}

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
			if (companyIds == null || companyIds.Count < 1) {
				return new Dictionary<int, EffortDTO>();
			}

			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ConnectionString;
			string voyConnectionString = ConfigurationManager.ConnectionStrings["Voyager"].ConnectionString;
			string lionConnectionString = ConfigurationManager.ConnectionStrings["Lion"].ConnectionString;
			string damConnectionString = ConfigurationManager.ConnectionStrings["FFDAM"].ConnectionString;

			List<int> iconumList = companyIds.Select(companyId => PermId.PermId2Iconum(companyId)).ToList();
			iconumList = iconumList.Distinct().ToList();

			CompanyHelper helper = new CompanyHelper(sfConnectionString,
				                                         voyConnectionString,
				                                         lionConnectionString,
				                                         damConnectionString);
			return helper.GetCompaniesEfforts(iconumList);
		}

		[Route("companies/{CompanyId}/companypriority/")]
		[HttpGet]
		public CompanyPriority GetPriorityForCompany(string CompanyId) {
			if (string.IsNullOrEmpty(CompanyId)) {
				return null;
			}

			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ConnectionString;
			string voyConnectionString = ConfigurationManager.ConnectionStrings["Voyager"].ConnectionString;
			string lionConnectionString = ConfigurationManager.ConnectionStrings["Lion"].ConnectionString;
			string damConnectionString = ConfigurationManager.ConnectionStrings["FFDAM"].ConnectionString;

			int iconum = PermId.PermId2Iconum(CompanyId);

			CompanyHelper helper = new CompanyHelper(sfConnectionString,
																								 voyConnectionString,
																								 lionConnectionString,
																								 damConnectionString);
			Dictionary<int, CompanyPriority> priorities = helper.GetCompanyPriority(new List<int> {iconum});
			if (priorities.ContainsKey(iconum)) {
				return priorities[iconum];
			}

			return null;
		}

		[Route("companies/companypriority/")]
		[HttpPost]
		public Dictionary<int, CompanyPriority> GetPriorityForCompanies(List<string> companyIds) {
			if (companyIds == null || companyIds.Count < 1) {
				return new Dictionary<int, CompanyPriority>();
			}

			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ConnectionString;
			string voyConnectionString = ConfigurationManager.ConnectionStrings["Voyager"].ConnectionString;
			string lionConnectionString = ConfigurationManager.ConnectionStrings["Lion"].ConnectionString;
			string damConnectionString = ConfigurationManager.ConnectionStrings["FFDAM"].ConnectionString;

			List<int> iconumList = companyIds.Select(companyId => PermId.PermId2Iconum(companyId)).ToList();
			iconumList = iconumList.Distinct().ToList();

			CompanyHelper helper = new CompanyHelper(sfConnectionString,
																								 voyConnectionString,
																								 lionConnectionString,
																								 damConnectionString);
			return helper.GetCompanyPriority(iconumList);
		}
			
		[Route("companies/{CompanyId}/efforts/")]
		[HttpGet]
		public EffortDTO[] QueryEfforts(string CompanyId) {
			if (string.IsNullOrEmpty(CompanyId)) {
				return null;
			}

			return new EffortDTO[] { EffortDTO.Voyager(), EffortDTO.SuperCore(), EffortDTO.Kpi(), EffortDTO.Segments(), new EffortDTO() { Name = "sfvoy_join" } };
		}
	}
}
