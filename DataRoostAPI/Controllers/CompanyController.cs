using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;
using System.Configuration;
using CCS.Fundamentals.DataRoostAPI.Models;
using CCS.Fundamentals.DataRoostAPI.Access;
using CCS.Fundamentals.DataRoostAPI.Access.Company;

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

			int iconum = PermId.PermId2Iconum(CompanyId);

			CompanyHelper helper = new CompanyHelper(sfConnectionString);
			return new CompanyDTO[] { helper.GetCompany(iconum) };
		}

		[Route("companies/{CompanyId}/efforts/")]
		[HttpGet]
		public EffortDTO[] QueryEfforts(string CompanyId) {
			throw new NotImplementedException();
		}
	}
}
