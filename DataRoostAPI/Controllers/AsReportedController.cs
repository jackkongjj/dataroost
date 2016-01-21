using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;

using CCS.Fundamentals.DataRoostAPI.Access;
using CCS.Fundamentals.DataRoostAPI.Access.AsReported;

using DataRoostAPI.Common.Models.AsReported;

namespace CCS.Fundamentals.DataRoostAPI.Controllers {

	[RoutePrefix("api/v1/companies/{CompanyId}/efforts/asreported")]
	public class AsReportedController : ApiController {

		[Route("documents/{documentId}")]
		[HttpGet]
		public AsReportedDocument GetDocument(string CompanyId, string documentId) {
			int iconum = PermId.PermId2Iconum(CompanyId);

			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			DocumentHelper documentHelper = new DocumentHelper(sfConnectionString);
			return documentHelper.GetDocument(iconum, documentId);
		}

		[Route("documents/")]
		[HttpPost]
		public AsReportedDocument[] GetDocuments(string CompanyId, List<string> documentIds) {
			int iconum = PermId.PermId2Iconum(CompanyId);

			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			DocumentHelper documentHelper = new DocumentHelper(sfConnectionString);
			return documentHelper.GetDocuments(iconum, documentIds);
		}

		[Route("documents/")]
		[HttpGet]
		public AsReportedDocument[] GetDocuments(string CompanyId, int? startYear = null, int? endYear = null, string reportType = null) {
			int iconum = PermId.PermId2Iconum(CompanyId);

			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			DocumentHelper documentHelper = new DocumentHelper(sfConnectionString);

			DateTime startDate = new DateTime(1900, 1, 1);
			if (startYear != null) {
				startDate = new DateTime((int)startYear, 1, 1);
			}
			DateTime endDate = new DateTime(2100, 12, 31);
			if (endYear != null) {
				endDate = new DateTime((int)endYear, 12, 31);
			}

			return documentHelper.GetDocuments(iconum, startDate, endDate, reportType);
		}

		[Route("companyFinancialTerms/")]
		[HttpGet]
		public CompanyFinancialTerm[] GetCompanyFinancialTerms(string CompanyId) {
			int iconum = PermId.PermId2Iconum(CompanyId);

			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			CompanyFinancialTermsHelper companyFinancialTermsHelper = new CompanyFinancialTermsHelper(sfConnectionString);
			return companyFinancialTermsHelper.GetCompanyFinancialTerms(iconum);
		}
	}
}
