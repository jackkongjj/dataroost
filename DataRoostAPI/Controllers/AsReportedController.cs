using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;
using System.Net.Mail;
using CCS.Fundamentals.DataRoostAPI.Access;
using CCS.Fundamentals.DataRoostAPI.Access.AsReported;
using System.Diagnostics;
using DataRoostAPI.Common.Models.AsReported;
using System.Runtime.InteropServices;

namespace CCS.Fundamentals.DataRoostAPI.Controllers {
	[RoutePrefix("api/v1/companies/{CompanyId}/efforts/asreported")]
	public class AsReportedController : ApiController {

		[Route("documents/{documentId}")]
		[HttpGet]
		public AsReportedDocument GetDocument(string CompanyId, string documentId) {
			int iconum = PermId.PermId2Iconum(CompanyId);

			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			string damConnectionString = ConfigurationManager.ConnectionStrings["FFDAM"].ToString();
			DocumentHelper documentHelper = new DocumentHelper(sfConnectionString, damConnectionString);
			return documentHelper.GetDocument(iconum, documentId);
		}

		[Route("history/{documentId}")]
		[HttpGet]
		public AsReportedDocument[] GetDocuments(string CompanyId, string documentId) {
			int iconum = PermId.PermId2Iconum(CompanyId);

			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			string damConnectionString = ConfigurationManager.ConnectionStrings["FFDAM"].ToString();
			DocumentHelper documentHelper = new DocumentHelper(sfConnectionString, damConnectionString);
			return documentHelper.GetDocuments(iconum, documentId);
		}

		[Route("documents/")]
		[HttpGet]
		public AsReportedDocument[] GetDocuments(string CompanyId, int? startYear = null, int? endYear = null, string reportType = null) {
			int iconum = PermId.PermId2Iconum(CompanyId);

			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			string damConnectionString = ConfigurationManager.ConnectionStrings["FFDAM"].ToString();
			DocumentHelper documentHelper = new DocumentHelper(sfConnectionString, damConnectionString);

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

		[Route("staticHierarchy/{id}")]
		[HttpPut]
		public ScarResult EditHierarchyLabel(string CompanyId, int id, StringInput input) {
			if (input == null || string.IsNullOrEmpty(input.StringData))
				return null;
			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
			return helper.UpdateStaticHierarchyLabel(id, input.StringData);
		}

		[Route("staticHierarchy/{id}/dragdrop/{targetId}/{location}")]
		[HttpPut]
		public ScarResult EditHierarchyLabel(string CompanyId, int id, int targetId, string location) {
			if (string.IsNullOrEmpty(location))
				return null;
			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
			return helper.DragDropStaticHierarchyLabel(id, targetId, location.ToUpper());
		}

		[Route("staticHierarchy/{id}/group")]
		[HttpPut]
		public ScarResult GroupStatichHierarchy(string CompanyId, int id) {
			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
			return helper.UpdateStaticHierarchySeperator(id, true);
		}
		[Route("staticHierarchy/{id}/ungroup")]
		[HttpPut]
		public ScarResult UngroupStatichHierarchy(string CompanyId, int id) {
			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
			return helper.UpdateStaticHierarchySeperator(id, false);
		}

		[Route("staticHierarchy/{id}/header")]
		[HttpPost]
		public ScarResult AddHeaderStatichHierarchy(string CompanyId, int id) {
			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
			return helper.UpdateStaticHierarchyAddHeader(id);
		}

		[Route("staticHierarchy/{id}/header")]
		[HttpDelete]
		public ScarResult DeleteHeaderStatichHierarchyWithId(string CompanyId, ScarStringListInput input) {
			return DeleteHeaderStatichHierarchy(CompanyId, input);
		}

		[Route("staticHierarchy/header")]
		[HttpDelete]
		public ScarResult DeleteHeaderStatichHierarchy(string CompanyId, ScarStringListInput input) {
			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
			if (input == null || input.StaticHierarchyIDs.Count == 0 || input.StaticHierarchyIDs.Any(s => s == 0))
				return null;

			return helper.UpdateStaticHierarchyDeleteHeader(input.StringData, input.StaticHierarchyIDs);
		}

		[Route("staticHierarchy/{id}/parent")]
		[HttpPost]
		public ScarResult AddParentStatichHierarchy(string CompanyId, int id) {
			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
			return helper.UpdateStaticHierarchyAddParent(id);
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


		[Route("timeSlice/{id}/reporttype")]
		[HttpPut]
		public ScarResult	 PutTimeSlice(string CompanyId, int id, StringInput input) {
			if (input == null || string.IsNullOrEmpty(input.StringData))
				return null;
			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
			return helper.UpdateTimeSliceReportType(id, input.StringData);
		}

		[Route("timeSlice/{id}/interimtype")]
		[HttpPost]
		public ScarResult PostTimeSlice(string CompanyId, int id, StringInput input) {
			if (input == null || string.IsNullOrEmpty(input.StringData))
				return null;
			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
			return helper.CloneUpdateTimeSlice(id, input.StringData);
		}



		[Route("cells/{id}/flipsign/{DocumentId}/")]
		[HttpGet]
		public ScarResult FlipSign(string id, Guid DocumentId) {
			// there shouldn't be a getter for this. My Mistake. 
			string CompanyId = "36468";
			int iconum = PermId.PermId2Iconum(CompanyId);
			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
			return helper.FlipSign(id, DocumentId, iconum, 0);
		}

		[Route("cells/{id}/flipsign/{DocumentId}/")]
		[HttpPost]
		public ScarResult FlipSign(string id, Guid DocumentId, ScarInput input) {
			string CompanyId = "36468";
			int iconum = PermId.PermId2Iconum(CompanyId);
			if (input == null || input.TargetStaticHierarchyID == 0)
				return null;
			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
			return helper.FlipSign(id, DocumentId, iconum, input.TargetStaticHierarchyID);
		}

		[Route("cells/{id}/children/flipsign/{DocumentId}/")]
		[HttpPost]
		public ScarResult FlipChildren(string CompanyId, string id, Guid DocumentId, ScarInput input) {
			int iconum = PermId.PermId2Iconum(CompanyId);
			//if (input == null || input.TargetStaticHierarchyID == 0)
			//	return null;
			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
			return helper.FlipChildren(id, DocumentId, iconum, 0);
		}

		[Route("cells/{id}/historical/flipsign/{DocumentId}/")]
		[HttpPost]
		public ScarResult FlipHistorical(string CompanyId, string id, Guid DocumentId, ScarInput input) {
			int iconum = PermId.PermId2Iconum(CompanyId);
			//if (input == null || input.TargetStaticHierarchyID == 0)
			//	return null;
			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
			return helper.FlipHistorical(id, DocumentId, iconum, 0);
		}

		[Route("cells/{id}/addMTMW/{DocumentId}/")]
		[HttpGet]
		public TableCellResult AddMakeTheMathWorkNote(string id, Guid DocumentId) {
			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
			return helper.AddMakeTheMathWorkNote(id, DocumentId);
		}

		[Route("cells/{id}/addLikePeriod/{DocumentId}/")]
		[HttpGet]
		public TableCellResult AddLikePeriodValidationNote(string id, Guid DocumentId) {
			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
			return helper.AddLikePeriodValidationNote(id, DocumentId);
		}

		[Route("templates/{TemplateName}/timeslice/{DocumentId}/")]
		[HttpGet]
		public ScarResult GetTimeSliceByTemplate(string CompanyId, string TemplateName, Guid DocumentId) {
			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
			return helper.GetTimeSliceByTemplate(TemplateName, DocumentId);
		}

		[Route("templates/{TemplateName}/timeslice/review")]
		[HttpGet]
		public ScarResult GetTimeSliceReview(string CompanyId, string TemplateName) {
			int iconum = PermId.PermId2Iconum(CompanyId);
			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
			return helper.GetReviewTimeSlice(TemplateName, iconum);
		}

		[Route("templates/{TemplateName}/stitch/{DocumentId}/")]
		[HttpPost]
		public StitchResult PostStitch(string CompanyId, string TemplateName, Guid DocumentId, StitchInput stitchInput) {
			int iconum = PermId.PermId2Iconum(CompanyId);

			if (stitchInput == null || stitchInput.TargetStaticHierarchyID == 0 || stitchInput.StitchingStaticHierarchyIDs.Count == 0 || stitchInput.StitchingStaticHierarchyIDs.Any(s => s == 0))
				return null;

			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
			return helper.StitchStaticHierarchies(stitchInput.TargetStaticHierarchyID, DocumentId, stitchInput.StitchingStaticHierarchyIDs, iconum);
		}


		[Route("templates/{TemplateName}/unstitch/{DocumentId}/")]
		[HttpPost]
		public UnStitchResult PostUnStitch(string CompanyId, string TemplateName, Guid DocumentId, UnStitchInput unstitchInput) {
			int iconum = PermId.PermId2Iconum(CompanyId);

			if (unstitchInput == null || unstitchInput.TargetStaticHierarchyID == 0)
				return null;

			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
			return helper.UnstitchStaticHierarchy(unstitchInput.TargetStaticHierarchyID, DocumentId, iconum);
		}
		
		public class StitchInput {
			public int TargetStaticHierarchyID { get; set; }
			public List<int> StitchingStaticHierarchyIDs { get; set; }
		}

		public class UnStitchInput {
			public int TargetStaticHierarchyID { get; set; }
		}

		public class ScarInput {
			public int TargetStaticHierarchyID { get; set; }
			public List<int> StitchingStaticHierarchyIDs { get; set; }
		}

		public class ScarStringListInput {
			public string StringData { get; set; }
			public List<int> StaticHierarchyIDs { get; set; }
		}


		public class StringInput {
			public string StringData { get; set; }
		}
	}
}