﻿using System;
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

		//TODO: Add IsSummary for timeslices and Derivation Meta for cells, add MTMW.
		[Route("templates/{TemplateName}/{DocumentId}")]
		[HttpGet]
		public AsReportedTemplate GetTemplate(string CompanyId, string TemplateName, Guid DocumentId) {
			int iconum = PermId.PermId2Iconum(CompanyId);
			if (TemplateName == null)
				return null;

			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
			return helper.GetTemplate(iconum, TemplateName, DocumentId);
		}

		[Route("templates/{TemplateName}/skeleton/")]
		[HttpGet]
		public AsReportedTemplateSkeleton GetTemplateSkeleton(string CompanyId, string TemplateName) {
			int iconum = PermId.PermId2Iconum(CompanyId);
			if (TemplateName == null)
				return null;

			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
			return helper.GetTemplateSkeleton(iconum, TemplateName);
		}

		//TODO: Add Derivation Meta for cells, add MTMW and like period validation indicators.
		[Route("staticHierarchy/{id}")]
		[HttpGet]
		public StaticHierarchy GetStaticHierarchy(string CompanyId, int id) {
			int iconum = PermId.PermId2Iconum(CompanyId);
			if (id == 0 || id == -1)
				return null;

			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
			return helper.GetStaticHierarchy(id);
		}

		//TODO: Add IsSummary
		[Route("timeSlice/{id}")]
		[HttpGet]
		public TimeSlice GetTimeSlice(string CompanyId, int id) {
			int iconum = PermId.PermId2Iconum(CompanyId);
			if (id == 0 || id == -1)
				return null;

			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
			return helper.GetTimeSlice(id);
		}
        [Route("cells/{id}/flipsign/")]
        [HttpGet]
        public TableCell FlipSign(string id)
        {
            string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
            AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
            return helper.GetCell(id);
        }

		public class test {
			public int TargetStaticHierarchyID { get; set; }
			public List<int> StitchingStaticHierarchyIDs { get; set; }
		}


		[Route("templates/{TemplateName}/stitch/{DocumentId}/")]
		[HttpPost]
		public StitchResult PostStitch(string CompanyId, string TemplateName, Guid DocumentId, test test) {
			int iconum = PermId.PermId2Iconum(CompanyId);

			if (test.TargetStaticHierarchyID == 0 || test.StitchingStaticHierarchyIDs.Count == 0 || test.StitchingStaticHierarchyIDs.Any(s => s == 0))
				return null;

			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
			return helper.StitchStaticHierarchies(test.TargetStaticHierarchyID, DocumentId, test.StitchingStaticHierarchyIDs, iconum);
		}
	}
}
