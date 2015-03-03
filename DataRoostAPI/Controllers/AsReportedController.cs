using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;

namespace CCS.Fundamentals.DataRoostAPI.Controllers {

	[RoutePrefix("api/v1/companies/{CompanyId}/efforts/asreported")]
	public class AsReportedController : ApiController {

		[Route("documents/{documentId}")]
		[HttpGet]
		public string[] GetDocument(string CompanyId, string documentId) {
			throw new NotImplementedException();
		}

		[Route("documents/")]
		[HttpGet]
		public string[] GetDocuments(string CompanyId, int? startYear = null, int? endYear = null, string reportType = null) {
			throw new NotImplementedException();
		}
	}
}
