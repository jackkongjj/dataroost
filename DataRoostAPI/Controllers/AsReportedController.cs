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
		[Route("templates/{TemplateName}/{DocumentId}/obselete")]
		[HttpGet]
		public AsReportedTemplate GetTemplateObselete(string CompanyId, string TemplateName, Guid DocumentId) {
			int iconum = PermId.PermId2Iconum(CompanyId);
			if (TemplateName == null)
				return null;

			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
			return helper.GetTemplate(iconum, TemplateName, DocumentId);
		}

		[Route("templates/{TemplateName}/{DocumentId}")]
		[HttpGet]
		public ScarResult GetTemplate(string CompanyId, string TemplateName, Guid DocumentId) {
			int iconum = PermId.PermId2Iconum(CompanyId);
			if (TemplateName == null)
				return null;

			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
			return helper.GetTemplateInScarResult(iconum, TemplateName, DocumentId);
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

		[Route("staticHierarchy/{id}/move")]
		[HttpPut]
		public ScarResult MoveStaticHierarchy(string CompanyId, int id, StringInput input) {
			if (input == null || string.IsNullOrEmpty(input.StringData))
				return null;
			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
			return helper.UpdateStaticHierarchyMove(id, input.StringData);
		}


		[Route("staticHierarchy/{id}/childrenexpanddown")]
		[HttpPut]
		public ScarResult SwitchChildrenOrientation(string CompanyId, int id) {
			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
			return helper.UpdateStaticHierarchySwitchChildrenOrientation(id);
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

		[Route("staticHierarchy/{id}/unittype")]
		[HttpPut]
		public ScarResult UpdateStatichHierarchyUnitType(string CompanyId, int id, StringInput input) {
			if (input == null || string.IsNullOrEmpty(input.StringData))
				return null;
			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
			return helper.UpdateStaticHierarchyUnitType(id, input.StringData);
		}
		[Route("staticHierarchy/{id}/meta")]
		[HttpPut]
		public ScarResult UpdateStatichHierarchyMeta(string CompanyId, int id, StringInput input) {
			if (input == null || string.IsNullOrEmpty(input.StringData))
				return null;
			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
			return helper.UpdateStaticHierarchyMeta(id, input.StringData);
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
		public ScarResult PutTimeSlice(string CompanyId, int id, StringInput input) {
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

		[Route("timeSlice/{id}/manualorgset")]
		[HttpPut]
		public ScarResult PutTimeSliceManualOrgSet(string CompanyId, int id, StringInput input) {
			if (input == null || string.IsNullOrEmpty(input.StringData))
				return null;
			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
			return helper.UpdateTimeSliceManualOrgSet(id, input.StringData);
		}


		[Route("cells/{id}/flipsign/{DocumentId}/")]
		[HttpPut]
		public ScarResult FlipSign(string CompanyId, string id, Guid DocumentId, ScarInput input) {
			int iconum = PermId.PermId2Iconum(CompanyId);
			int targetSH = 1;
			if (input != null && input.TargetStaticHierarchyID != 0)
				targetSH = input.TargetStaticHierarchyID;
			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
			return helper.FlipSign(id, DocumentId, iconum, targetSH);
		}

		[Route("cells/{id}/children/flipsign/{DocumentId}/")]
		[HttpPut]
		public ScarResult FlipChildren(string CompanyId, string id, Guid DocumentId, ScarInput input) {
			int iconum = PermId.PermId2Iconum(CompanyId);
			//if (input == null || input.TargetStaticHierarchyID == 0)
			//	return null;
			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
			return helper.FlipChildren(id, DocumentId, iconum, 0);
		}

		[Route("cells/{id}/historical/flipsign/{DocumentId}/")]
		[HttpPut]
		public ScarResult FlipHistorical(string CompanyId, string id, Guid DocumentId, ScarInput input) {
			int iconum = PermId.PermId2Iconum(CompanyId);
			//if (input == null || input.TargetStaticHierarchyID == 0)
			//	return null;
			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
			return helper.FlipHistorical(id, DocumentId, iconum, 0);
		}

		[Route("cells/{id}/children/historical/flipsign/{DocumentId}/")]
		[HttpPut]
		public ScarResult FlipChildrenHistorical(string CompanyId, string id, Guid DocumentId, ScarInput input) {
			int iconum = PermId.PermId2Iconum(CompanyId);
			//if (input == null || input.TargetStaticHierarchyID == 0)
			//	return null;
			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
			return helper.FlipChildrenHistorical(id, DocumentId, iconum, 0);
		}

		[Route("cells/{id}/addMTMW/{DocumentId}/")]
		[HttpGet]
		public TableCellResult AddMakeTheMathWorkNote(string id, Guid DocumentId) {
			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
			return helper.AddMakeTheMathWorkNote(id, DocumentId);
		}

		[Route("cells/{id}/addMTMW/{DocumentId}/")]
		[HttpPost]
		public TableCellResult AddMakeTheMathWorkNote(string id, Guid DocumentId, StringInput input) {
			if (input == null || string.IsNullOrEmpty(input.StringData))
				return AddMakeTheMathWorkNote(id, DocumentId);
			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
			return helper.AddMakeTheMathWorkNote(id, DocumentId, input.StringData);
		}

		[Route("cells/{id}/addLikePeriod/{DocumentId}/")]
		[HttpGet]
		public TableCellResult AddLikePeriodValidationNote(string id, Guid DocumentId) {
			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
			return helper.AddLikePeriodValidationNote(id, DocumentId);
		}

		[Route("cells/{id}/addLikePeriod/{DocumentId}/")]
		[HttpPost]
		public TableCellResult AddLikePeriodValidationNote(string id, Guid DocumentId, StringInput input) {
			if (input == null || string.IsNullOrEmpty(input.StringData))
				return AddLikePeriodValidationNote(id, DocumentId);
			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
			return helper.AddLikePeriodValidationNote(id, DocumentId, input.StringData);
		}

		[Route("cells/{id}")]
		[HttpGet]
		public ScarResult GetTableCell(string id) {
			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
			return helper.GetTableCell(id);
		}

		[Route("cells/{id}/meta/numericvalue")]
		[HttpPut]
		public ScarResult UpdateTableCellMetaNumericValue(string id, StringInput input) {
			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
			string newValue = "";
			if (input != null && !string.IsNullOrEmpty(input.StringData)) {
				newValue = input.StringData;
			}
			if (!string.IsNullOrWhiteSpace(newValue)) {
				return null;
			}

			return helper.UpdateTableCellMetaNumericValue(id, newValue);
		}
		[Route("cells/{id}/meta/scalingfactor")]
		[HttpPut]
		public ScarResult UpdateTableCellMetaScalingFactor(string id, StringInput input) {
			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
			string newValue = "";
			if (input != null && !string.IsNullOrEmpty(input.StringData)) {
				newValue = input.StringData;
			}
			if (!string.IsNullOrWhiteSpace(newValue)) {
				return null;
			}

			return helper.UpdateTableCellMetaScalingFactor(id, newValue);
		}
		[Route("cells/{id}/meta/perioddate")]
		[HttpPut]
		public ScarResult UpdateTableCellMetaPeriodDate(string id, StringInput input) {
			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
			string newValue = "";
			if (input != null && !string.IsNullOrEmpty(input.StringData)) {
				newValue = input.StringData;
			}
			if (!string.IsNullOrWhiteSpace(newValue)) {
				return null;
			}

			return helper.UpdateTableCellMetaPeriodDate(id, newValue);
		}
		[Route("cells/{id}/meta/periodtype")]
		[HttpPut]
		public ScarResult UpdateTableCellMetaPeriodType(string id, StringInput input) {
			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
			string newValue = "";
			if (input != null && !string.IsNullOrEmpty(input.StringData)) {
				newValue = input.StringData;
			}
			if (!string.IsNullOrWhiteSpace(newValue)) {
				return null;
			}

			return helper.UpdateTableCellMetaPeriodType(id, newValue);
		}
		[Route("cells/{id}/meta/periodlength")]
		[HttpPut]
		public ScarResult UpdateTableCellMetaPeriodLength(string id, StringInput input) {
			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
			string newValue = "";
			if (input != null && !string.IsNullOrEmpty(input.StringData)) {
				newValue = input.StringData;
			}
			if (!string.IsNullOrWhiteSpace(newValue)) {
				return null;
			}

			return helper.UpdateTableCellMetaPeriodLength(id, newValue);
		}
		[Route("cells/{id}/meta/currency")]
		[HttpPut]
		public ScarResult UpdateTableCellMetaCurrency(string id, StringInput input) {
			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
			string newValue = "";
			if (input != null && !string.IsNullOrEmpty(input.StringData)) {
				newValue = input.StringData;
			}
			if (!string.IsNullOrWhiteSpace(newValue)) {
				return null;
			}

			return helper.UpdateTableCellMetaCurrency(id, newValue);
		}
		[Route("cells/{id}/row/cusip")]
		[HttpPut]
		public ScarResult UpdateTableRowMetaCusip(string id, StringInput input) {
			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
			string newValue = "";
			if (input != null && !string.IsNullOrEmpty(input.StringData)) {
				newValue = input.StringData;
			}
			if (!string.IsNullOrWhiteSpace(newValue)) {
				return null;
			}

			return helper.UpdateTableRowMetaCusip(id, newValue);
		}
		[Route("cells/{id}/row/pit")]
		[HttpPut]
		public ScarResult UpdateTableRowMetaPit(string id) {
			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);

			return helper.UpdateTableRowMetaPit(id, "");
		}
		[Route("cells/{id}/row/scalingfactor")]
		[HttpPut]
		public ScarResult UpdateTableRowMetaScalingFactor(string id, StringInput input) {
			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
			string newValue = "";
			if (input != null && !string.IsNullOrEmpty(input.StringData)) {
				newValue = input.StringData;
			}
			if (!string.IsNullOrWhiteSpace(newValue)) {
				return null;
			}

			return helper.UpdateTableRowMetaScalingFactor(id, newValue);
		}
		[Route("cells/{id}/column/perioddate")]
		[HttpPut]
		public ScarResult UpdateTableColumnMetaPeriodDate(string id, StringInput input) {
			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
			string newValue = "";
			if (input != null && !string.IsNullOrEmpty(input.StringData)) {
				newValue = input.StringData;
			}
			if (!string.IsNullOrWhiteSpace(newValue)) {
				return null;
			}

			return helper.UpdateTableColumnMetaPeriodDate(id, newValue);
		}
		[Route("cells/{id}/column/columnheader")]
		[HttpPut]
		public ScarResult UpdateTableColumnMetaColumnHeader(string id, StringInput input) {
			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
			string newValue = "";
			if (input != null && !string.IsNullOrEmpty(input.StringData)) {
				newValue = input.StringData;
			}
			if (!string.IsNullOrWhiteSpace(newValue)) {
				return null;
			}

			return helper.UpdateTableColumnMetaColumnHeader(id, newValue);
		}
		[Route("cells/{id}/column/periodtype")]
		[HttpPut]
		public ScarResult UpdateTableColumnMetaPeriodType(string id, StringInput input) {
			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
			string newValue = "";
			if (input != null && !string.IsNullOrEmpty(input.StringData)) {
				newValue = input.StringData;
			}
			if (!string.IsNullOrWhiteSpace(newValue)) {
				return null;
			}

			return helper.UpdateTableColumnMetaPeriodType(id, newValue);
		}
		[Route("cells/{id}/column/periodlength")]
		[HttpPut]
		public ScarResult UpdateTableColumnMetaPeriodLength(string id, StringInput input) {
			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
			string newValue = "";
			if (input != null && !string.IsNullOrEmpty(input.StringData)) {
				newValue = input.StringData;
			}
			if (!string.IsNullOrWhiteSpace(newValue)) {
				return null;
			}

			return helper.UpdateTableColumnMetaPeriodLength(id, newValue);
		}

		[Route("cells/{id}/column/currency")]
		[HttpPut]
		public ScarResult UpdateTableColumnMetaCurrencyCode(string id, StringInput input) {
			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
			string newValue = "";
			if (input != null && !string.IsNullOrEmpty(input.StringData)) {
				newValue = input.StringData;
			}
			if (!string.IsNullOrWhiteSpace(newValue)) {
				return null;
			}

			return helper.UpdateTableColumnMetaCurrencyCode(id, newValue);
		}
		[Route("cells/{id}/column/interimtype")]
		[HttpPut]
		public ScarResult UpdateTableColumnMetaInterimType(string id, StringInput input) {
			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
			string newValue = "";
			if (input != null && !string.IsNullOrEmpty(input.StringData)) {
				newValue = input.StringData;
			}
			if (!string.IsNullOrWhiteSpace(newValue)) {
				return null;
			}

			return helper.UpdateTableColumnMetaInterimType(id, newValue);
		}
		[Route("tdp/{id}")]
		[HttpPost]
		public ScarResult UpdateTDP(string id, StringInput input) {
			ScarResult result = new ScarResult();
			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
			string newValue = "";
			if (input != null && !string.IsNullOrEmpty(input.StringData)) {
				newValue = input.StringData;
			}
			newValue = JsonToSQL.Json_UpdateTDPExample;
			if (string.IsNullOrWhiteSpace(newValue)) {
				result.Message = "bad input";
			} else {
				try {
					 result = helper.UpdateTDP(newValue);
				} catch (Exception ex) {
					result.Message += ex.Message;
				}
			}
			return result;
			//return helper.UpdateTableColumnMetaInterimType(id, newValue);
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
		[Route("templates/{TemplateName}/timeslice/{id}/issummary")]
		[HttpPut]
		public ScarResult PutTimeSliceIsSummary(string CompanyId, string TemplateName, int id) {
			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
			return helper.UpdateTimeSliceIsSummary(id, TemplateName);
		}

		[Route("templates/{TemplateName}/timeslice/{id}/periodnote")]
		[HttpPut]
		public ScarResult PutTimeSlicePeriodNote(string CompanyId, string TemplateName, int id, StringInput input) {
			string periodNote = null;
			if (input != null && !string.IsNullOrEmpty(input.StringData))
				periodNote = input.StringData;
			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
			return helper.UpdateTimeSlicePeriodNote(id, periodNote);
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

	[RoutePrefix("api/v1/companies/{CompanyId}/efforts/zerominute")]
	public class ZeroMinuteUpdateController : ApiController {
		private bool IsZeroMinuteUpdate() {
			bool isZeroMinuteUpdate = false;
			string zerominutekey = ConfigurationManager.AppSettings["IsZeroMinuteUpdate"];
			if (!string.IsNullOrEmpty(zerominutekey)) {
				isZeroMinuteUpdate = Convert.ToBoolean(zerominutekey);
			}
			return true;
			return isZeroMinuteUpdate;
		}

		public Guid GetSfDocumentId(string CompanyId, string documentId) {
			var document = GetDocument(CompanyId, documentId);
			Guid sfDocumentId = new Guid("00000000-0000-0000-0000-000000000000");
			if (document == null || document.SuperFastDocumentId == null) {
				// probably due to incorrect CompanyId. 
				// Just inquiry the DocumentTable
			} else {
				sfDocumentId = new Guid(document.SuperFastDocumentId);
			}
			return sfDocumentId;
		}
		public AsReportedDocument GetDocument(string CompanyId, string documentId) {
			int iconum = PermId.PermId2Iconum(CompanyId);

			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			string damConnectionString = ConfigurationManager.ConnectionStrings["FFDAM"].ToString();
			DocumentHelper documentHelper = new DocumentHelper(sfConnectionString, damConnectionString);
			var document = documentHelper.GetDocument(iconum, documentId);
			return document;
		}
	
		public class StringInput {
			public string StringData { get; set; }
		}

		[Route("documents/{damdocumentId}")]
		[HttpPut]
		public bool ExecuteZeroMinuteUpdate(string CompanyId, Guid damdocumentId, StringInput input) {
			string startReason = "-";
			if (input != null && !string.IsNullOrEmpty(input.StringData)) {
				startReason = input.StringData;
			}
			DateTime startTime = DateTime.UtcNow;

			var sfDocument = GetDocument(CompanyId, damdocumentId.ToString());
			Guid SfDocumentId = new Guid(sfDocument.SuperFastDocumentId); // SFDocumentID
			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);


			//Need to get SFDocumentID at least for creating timeslices in DoInterimType
			//FFDocumentHistory.GetSuperFastDocumentID(DAMDocumentId, Iconum).Value

			// InterimType
			// RedStar
			// Set IncomeOrientation
			// MTMW & LPV
			// ARd validation
			ScarResult result = null;
			result = new ScarResult();
			Tuple<bool, string> returnValue = new Tuple<bool,string>(false, "");
			bool runOnce = true;
			string isoCountry = helper.GetDocumentIsoCountry(SfDocumentId);
			bool isUsDocument = (!string.IsNullOrEmpty(isoCountry) && isoCountry.ToUpper() == "US");
			string ExceptionSource = "Exception at Unknown: ";
			if (IsZeroMinuteUpdate() && isUsDocument) {
				try {
					while (runOnce) {
						ExceptionSource = "Exception at DoInterimTypeAndCurrency: ";
						returnValue = DoInterimTypeAndCurrency(CompanyId, damdocumentId).ReturnValue;

						if (!returnValue.Item1) {
							break;
						}
						ExceptionSource = "Exception at DoRedStarSlotting: ";
						returnValue = DoRedStarSlotting(CompanyId, damdocumentId).ReturnValue;
						if (!returnValue.Item1) {
							break;
						}
						ExceptionSource = "Exception at DoSetIncomeOrientation: ";
						DoSetIncomeOrientation(CompanyId, damdocumentId);

						ExceptionSource = "Exception at DoMTMWAndLPVValidation: ";
						returnValue = DoMTMWAndLPVValidation(CompanyId, damdocumentId).ReturnValue;

						if (!returnValue.Item1) {
							//System.Text.StringBuilder sb = new System.Text.StringBuilder();

							//string Ids = mtmwRet.cells.Select(x => x.ID.ToString()).Aggregate((a, b) => a + "," + b);
							//returnValue = new Tuple<bool, string>(false, "mtmwlpvfailed: " + Ids);
							break;
						}
						ExceptionSource = "Exception at DoARDValidation: ";
						returnValue = DoARDValidation(CompanyId, damdocumentId).ReturnValue;
						if (!returnValue.Item1) {
							break;
						}
						runOnce = false;
					}
				} catch (Exception ex) {
					returnValue = new Tuple<bool, string>(false, ExceptionSource + returnValue.Item2 + ex.Message + new string(ex.StackTrace.Take(1000).ToArray()));
				}
				try {
					helper.LogError(damdocumentId, startReason, startTime, CompanyId, returnValue.Item1, returnValue.Item2);
				} catch { }
				return returnValue.Item1;
				//I think that the plan is for SFAutoStitchingAgent to return success if we succeeded in Zero Minute
				//and failure if we don't so we probably just have to return true;

			}
			return false;
		}

		[Route("documents/{damdocumentId}/ard")]
		[HttpPut]
		public ScarResult DoARDValidation(string CompanyId, Guid damdocumentId) {
			var sfDocument = GetDocument(CompanyId, damdocumentId.ToString());
			Guid SfDocumentId = new Guid(sfDocument.SuperFastDocumentId); // SFDocumentID

			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
			ScarResult result = new ScarResult();
			result.ReturnValue = helper.ARDValidation(SfDocumentId);
			return result;
		}
		[Route("documents/{damdocumentId}/redstarslotting")]
		[HttpPut]
		public ScarResult DoRedStarSlotting(string CompanyId, Guid damdocumentId) {
			var sfDocument = GetDocument(CompanyId, damdocumentId.ToString());
			Guid SfDocumentId = new Guid(sfDocument.SuperFastDocumentId); // SFDocumentID

			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
			ScarResult result = new ScarResult();

			result.ReturnValue = helper.UpdateRedStarSlotting(SfDocumentId);
			return result;
		}
		[Route("documents/{damdocumentId}/setincome")]
		[HttpPut]
		public void DoSetIncomeOrientation(string CompanyId, Guid damdocumentId) {
			var sfDocument = GetDocument(CompanyId, damdocumentId.ToString());
			Guid SfDocumentId = new Guid(sfDocument.SuperFastDocumentId); // SFDocumentID
			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
			helper.SetIncomeOrientation(SfDocumentId);
		}
		[Route("documents/{damdocumentId}/validatetables")]
		[HttpPut]
		public ScarResult DoInterimTypeAndCurrency(string CompanyId, Guid damdocumentId) {
			var sfDocument = GetDocument(CompanyId, damdocumentId.ToString());
			Guid SfDocumentId = new Guid(sfDocument.SuperFastDocumentId); // SFDocumentID
			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			int iconum = PermId.PermId2Iconum(CompanyId);
			AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
			var result = new ScarResult();
			var errorMessage = helper.CheckParsedTableInterimTypeAndCurrency(SfDocumentId, iconum);
			result.ReturnValue = new Tuple<bool, string>(string.IsNullOrEmpty(errorMessage), errorMessage);
			return result;
		}

		private List<string> LPVMetaTypes = new List<string>() { "NI", "RV", "TA", "TL", "LE", "PS", "PE", "CC" };

		[Route("documents/{damdocumentId}/mtmwandlpv2")]
		[HttpPut]
		public MTMWLPVReturn DoMTMWAndLPVValidation2(string CompanyId, Guid damdocumentId) {
			var sfDocument = GetDocument(CompanyId, damdocumentId.ToString());
			Guid SfDocumentId = new Guid(sfDocument.SuperFastDocumentId); // SFDocumentID
			int iconum = PermId.PermId2Iconum(CompanyId);

			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			List<AsReportedTemplate> templates = new List<AsReportedTemplate>();

			AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
			foreach (string TemplateName in helper.GetAllTemplates(sfConnectionString, iconum).Where(t => t == "IS" || t == "BS" || t == "CF"))
				templates.Add(helper.GetTemplate(iconum, TemplateName, SfDocumentId));

			//IEnumerable<StaticHierarchy> shs = templates.SelectMany(t => t.StaticHierarchies.Where(sh => sh.Cells.Any(c => c.LikePeriodValidationFlag || c.MTMWValidationFlag)));
			IEnumerable<SCARAPITableCell> cells = templates.SelectMany(t => t.StaticHierarchies.SelectMany(sh => sh.Cells.Where(c => ((c.MTMWValidationFlag && sh.StaticHierarchyMetaType != "SD") || 
					(c.LikePeriodValidationFlag
					&& ((sh.UnitTypeId == 2 || sh.UnitTypeId == 1 || LPVMetaTypes.Contains(sh.StaticHierarchyMetaType) && sh.StaticHierarchyMetaType != "SD"))
					&& templates.First(te => te.TimeSlices.Any(ts => ts.Cells.Contains(c))).TimeSlices.First(ti => ti.Cells.Contains(c)).DamDocumentId == damdocumentId
					)
				))));

			//if (templates.Any(t => t.StaticHierarchies.Any(sh => sh.Cells.Any(c => c.LikePeriodValidationFlag || c.MTMWValidationFlag)))) {
			if (cells.Count() > 0) {
				return new MTMWLPVReturn() { success = false, cells = cells.ToList() };
			}

			return new MTMWLPVReturn() { success = true };
		}


		[Route("documents/{damdocumentId}/mtmwandlpv")]
		[HttpPut]
		public ScarResult DoMTMWAndLPVValidation(string CompanyId, Guid damdocumentId) {
			var sfDocument = GetDocument(CompanyId, damdocumentId.ToString());
			Guid SfDocumentId = new Guid(sfDocument.SuperFastDocumentId); // SFDocumentID
			int iconum = PermId.PermId2Iconum(CompanyId);

			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			List<AsReportedTemplate> templates = new List<AsReportedTemplate>();
			var result = new ScarResult();
			System.Text.StringBuilder errorMessageBuilder = new System.Text.StringBuilder();
			bool isSuccess = true;
			AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
			try {
				foreach (string TemplateName in helper.GetAllTemplates(sfConnectionString, iconum).Where(t => t == "IS" || t == "BS" || t == "CF")) {
					templates = new List<AsReportedTemplate>();
					templates.Add(helper.GetTemplate(iconum, TemplateName, SfDocumentId));

					IEnumerable<SCARAPITableCell> mtmwcells = templates.SelectMany(t => t.StaticHierarchies
						.SelectMany(sh => sh.Cells.Where(c => c.MTMWValidationFlag && sh.StaticHierarchyMetaType != "SD")));
					int mtmwcount = mtmwcells.Count();
					if (mtmwcells.Count() > 0) {
						errorMessageBuilder.Append(TemplateName + ": MTMW: ");
						foreach (var cell in mtmwcells) {
							var timeslice = templates.First().TimeSlices.FirstOrDefault(x => x.Cells.Contains(cell));
							errorMessageBuilder.Append(string.Format("{0}({1},{2}) ", cell.DisplayValue.HasValue ? cell.DisplayValue.Value.ToString("0.##") : "-", timeslice.TimeSlicePeriodEndDate.ToString("yyyy-MM-dd"), timeslice.ReportType));
						}
						isSuccess = false;
					}

					IEnumerable<SCARAPITableCell> lpvcells = templates.SelectMany(t => t.StaticHierarchies.Where(sh => (sh.UnitTypeId == 2 || sh.UnitTypeId == 1 || LPVMetaTypes.Contains(sh.StaticHierarchyMetaType)) && sh.StaticHierarchyMetaType != "SD")
						.SelectMany(sh => sh.Cells.Where(c => c.LikePeriodValidationFlag
						&& templates.First().TimeSlices.First(ti => ti.Cells.Contains(c)).DamDocumentId == damdocumentId)));
					int lpvcount = lpvcells.Count();
					if (lpvcells.Count() > 0) {
						errorMessageBuilder.Append(TemplateName + ": LPV: ");
						foreach (var cell in lpvcells) {
							var timeslice = templates.First().TimeSlices.FirstOrDefault(x => x.Cells.Contains(cell));
							errorMessageBuilder.Append(string.Format("{0}({1},{2}) ", cell.DisplayValue.HasValue ? cell.DisplayValue.Value.ToString("0.##") : "-", timeslice.TimeSlicePeriodEndDate.ToString("yyyy-MM-dd"), timeslice.ReportType));
						}
						isSuccess = false;
					}

				}
			} catch (System.Data.SqlClient.SqlException ex) {
				if (ex.ErrorCode == 1205) {
					// victim of deadlock
					errorMessageBuilder = new System.Text.StringBuilder("Multiple MTMw Errors Encountered");
					isSuccess = false;
				} else {
					isSuccess = false;
					errorMessageBuilder = new System.Text.StringBuilder("Multiple MTMW Errors Encountered.");
				}
			} catch (Exception ex) {
				isSuccess = false;
				errorMessageBuilder = new System.Text.StringBuilder("Multiple MTMW Errors Encountered");
			}

			result.ReturnValue = new Tuple<bool, string>(isSuccess, errorMessageBuilder.ToString());
			return result;
		}

		public class MTMWLPVReturn {
			public bool success { get; set; }
			public List<SCARAPITableCell> cells { get; set; }
		}


		[Route("documents/{damdocumentId}/mtmw")]
		[HttpGet]
		public bool DoMTMWValidation(string CompanyId, Guid damdocumentId) {
			var sfDocument = GetDocument(CompanyId, damdocumentId.ToString());
			Guid SfDocumentId = new Guid(sfDocument.SuperFastDocumentId);
			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
			var result = helper.GetMtmwTableCells(0, SfDocumentId);
			return result;
		}
		[Route("documents/{damdocumentId}/lpv")]
		[HttpPut]
		public ScarResult DoLPVValidation(string CompanyId, Guid damdocumentId) {
			var sfDocument = GetDocument(CompanyId, damdocumentId.ToString());
			Guid SfDocumentId = new Guid(sfDocument.SuperFastDocumentId);
			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
			var result = helper.GetLpvTableCells(0, SfDocumentId);
			return result;
		}
		[Route("documents/{damdocumentId}/export")]
		[HttpPut]
		public ScarResult DoExport(string CompanyId, Guid damdocumentId) {
			var sfDocument = GetDocument(CompanyId, damdocumentId.ToString());
			Guid SfDocumentId = new Guid(sfDocument.SuperFastDocumentId);

			return new ScarResult();
		}


	}
}