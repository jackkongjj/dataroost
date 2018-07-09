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
using System.Net.NetworkInformation;

namespace CCS.Fundamentals.DataRoostAPI.Controllers {
	[RoutePrefix("api/v1/companies/{CompanyId}/efforts/asreported")]
	public class AsReportedController : ApiController {
		public static void SendEmail(string subject, string emailBody) {
			try {
				SmtpClient mySMTP = new SmtpClient("mail.factset.com");
				MailAddress mailFrom = new MailAddress("myself@factset.com", "IMA DataRoost");
				MailMessage message = new MailMessage();
				message.From = mailFrom;
				var ljiang = new MailAddress("ljiang@factset.com", "Lun Jiang");
				var leo = new MailAddress("lchang@factset.com", "Lun Jiang");
				message.To.Add(ljiang);
				//message.To.Add(leo);
				message.Subject = subject + " from " + Environment.MachineName;
				message.DeliveryNotificationOptions = DeliveryNotificationOptions.OnFailure;
				message.Body = emailBody;
				message.IsBodyHtml = true;
				mySMTP.Send(message);
			} catch { }
		}

		private void LogError(Exception ex, string extra = "") {
			string msg = ex.Message + ex.StackTrace;
			if (ex.InnerException != null)
				msg += "INNER EXCEPTION" + ex.InnerException.Message + ex.InnerException.StackTrace;
			SendEmail("DataRoost Exception", msg + extra);
		}

		[Route("documents/{documentId}")]
		[HttpGet]
		public AsReportedDocument GetDocument(string CompanyId, string documentId) {
			try {
				int iconum = PermId.PermId2Iconum(CompanyId);

				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
				string damConnectionString = ConfigurationManager.ConnectionStrings["FFDAM"].ToString();
				DocumentHelper documentHelper = new DocumentHelper(sfConnectionString, damConnectionString);
				return documentHelper.GetDocument(iconum, documentId);
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}

		[Route("documents/SplitAdjustmentDate/{documentId}")]
		[HttpPut]
		public ScarResult UpdateSplitAdjustmentDate(string CompanyId, Guid documentId, StringInput input) {
			try {
				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
				int iconum = PermId.PermId2Iconum(CompanyId);
				string damConnectionString = ConfigurationManager.ConnectionStrings["FFDAM"].ToString();
				DocumentHelper helper = new DocumentHelper(sfConnectionString, damConnectionString);

				var errorMessage = helper.UpdateSplitAdjustmentDate(documentId.ToString(), input.StringData, iconum);

				var result = new ScarResult();
				Dictionary<string, string> returnValue = new Dictionary<string, string>();
				returnValue["Success"] = string.IsNullOrEmpty(errorMessage) ? "T" : "F";
				returnValue["Message"] = errorMessage;
				result.ReturnValue = returnValue;
				return result;
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}

		[Route("history/{documentId}")]
		[HttpGet]
		public AsReportedDocument[] GetDocuments(string CompanyId, string documentId) {
			try {
				int iconum = PermId.PermId2Iconum(CompanyId);

				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
				string damConnectionString = ConfigurationManager.ConnectionStrings["FFDAM"].ToString();
				DocumentHelper documentHelper = new DocumentHelper(sfConnectionString, damConnectionString);
				return documentHelper.GetDocuments(iconum, documentId);
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}

		[Route("documents/")]
		[HttpGet]
		public AsReportedDocument[] GetDocuments(string CompanyId, int? startYear = null, int? endYear = null, string reportType = null) {
			try {
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
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}

		[Route("companyFinancialTerms/")]
		[HttpGet]
		public CompanyFinancialTerm[] GetCompanyFinancialTerms(string CompanyId) {
			try {
				int iconum = PermId.PermId2Iconum(CompanyId);

				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
				CompanyFinancialTermsHelper companyFinancialTermsHelper = new CompanyFinancialTermsHelper(sfConnectionString);
				return companyFinancialTermsHelper.GetCompanyFinancialTerms(iconum);
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}

		//TODO: Add IsSummary for timeslices and Derivation Meta for cells, add MTMW.
		[Route("templates/{TemplateName}/{DocumentId}/obselete")]
		[HttpGet]
		public AsReportedTemplate GetTemplateObselete(string CompanyId, string TemplateName, Guid DocumentId) {
			try {
				int iconum = PermId.PermId2Iconum(CompanyId);
				if (TemplateName == null)
					return null;

				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
				AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
				return helper.GetTemplate(iconum, TemplateName, DocumentId);
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}

		public static double PingTimeAverage(string host, int echoNum) {
			long totalTime = 0;
			int timeout = 120;
			Ping pingSender = new Ping();

			for (int i = 0; i < echoNum; i++) {
				PingReply reply = pingSender.Send(host, timeout);
				if (reply.Status == IPStatus.Success) {
					totalTime += reply.RoundtripTime;
				}
			}
			return totalTime / echoNum;
		}

		public static string PingMessage() {
			string result = "PingTime: ";
			try {
				string hostname = "ffdamsql-staging.prod.factset.com";
				string searchString = "Data Source=tcp:";
				var connectString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
				var startindex = connectString.IndexOf(searchString);
				if (startindex <= 0) {
					searchString = "Data Source=";
					startindex = connectString.IndexOf(searchString);
				}
				startindex += searchString.Length;
				hostname = connectString.Substring(startindex);
				var endIndex = hostname.IndexOf(";");
				hostname = hostname.Substring(0, endIndex);
				result += string.Format("{0} ms ", PingTimeAverage(hostname, 4));
			} catch {
				result += "error";
			}
			return result;
		}

		[Route("templates/{TemplateName}/{DocumentId}")]
		[HttpGet]
		public ScarResult GetTemplate(string CompanyId, string TemplateName, Guid DocumentId) {
			try {
				int iconum = PermId.PermId2Iconum(CompanyId);
				if (TemplateName == null)
					return null;

				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
				AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
				return helper.GetTemplateInScarResult(iconum, TemplateName, DocumentId);
			} catch (Exception ex) {
				LogError(ex, string.Format(PingMessage() + "CompanyId:{0}, TemplateName: {1}, DocumentId: {2}", CompanyId, TemplateName, DocumentId));
				return null;
			}
		}

		[Route("templates/{TemplateName}/{DocumentId}")]
		[HttpPost]
		public ScarResult PostTemplate(string CompanyId, string TemplateName, Guid DocumentId, StringInput data) {
			try {

				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
				AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
				return helper.CreateStaticHierarchyForTemplate(0, TemplateName, DocumentId);
			} catch (Exception ex) {
				LogError(ex, string.Format("CompanyId:{0}, TemplateName: {1}, DocumentId: {2}", CompanyId, TemplateName, DocumentId));
				return null;
			}
		}

		[Route("templates/{TemplateName}/skeleton/")]
		[HttpGet]
		public AsReportedTemplateSkeleton GetTemplateSkeleton(string CompanyId, string TemplateName) {
			try {
				int iconum = PermId.PermId2Iconum(CompanyId);
				if (TemplateName == null)
					return null;

				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
				AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
				return helper.GetTemplateSkeleton(iconum, TemplateName);
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}

		[Route("templates/{TemplateName}/{DocumentId}/copyhierarchy")]
		[HttpPost]
		public ScarResult CopyDocumentHierarchy(string CompanyId, string TemplateName, Guid DocumentId, StringInput input) {
			try {
				int iconum = PermId.PermId2Iconum(CompanyId);
				int tabletypeID;
				if (input == null || string.IsNullOrEmpty(input.StringData) || !int.TryParse(input.StringData, out tabletypeID))
					return null;
				if (tabletypeID <= 0)
					return null;
				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
				AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
				return helper.CopyDocumentHierarchy(iconum, tabletypeID, DocumentId);
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}
		//TODO: Add Derivation Meta for cells, add MTMW and like period validation indicators.
		[Route("staticHierarchy/{id}")]
		[HttpGet]
		public StaticHierarchy GetStaticHierarchy(string CompanyId, int id) {
			try {
				int iconum = PermId.PermId2Iconum(CompanyId);
				if (id == 0 || id == -1)
					return null;

				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
				AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
				return helper.GetStaticHierarchy(id);
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}

		[Route("staticHierarchy/{id}")]
		[HttpPut]
		public ScarResult EditHierarchyLabel(string CompanyId, int id, StringInput input) {
			try {
				if (input == null || string.IsNullOrEmpty(input.StringData))
					return null;
				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
				AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
				return helper.UpdateStaticHierarchyLabel(id, input.StringData);
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}

		[Route("staticHierarchy/cusip/{id}")]
		[HttpPut]
		public ScarResult UpdateHierarchyCusip(string CompanyId, int id, StringInput input) {
			try {
				if (input == null || string.IsNullOrEmpty(input.StringData))
					return null;
				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
				AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
				return helper.UpdateStaticHierarchyCusip(id, input.StringData);
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}

		[Route("staticHierarchy/{id}/move")]
		[HttpPut]
		public ScarResult MoveStaticHierarchy(string CompanyId, int id, StringInput input) {
			try {
				if (input == null || string.IsNullOrEmpty(input.StringData))
					return null;
				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
				AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
				return helper.UpdateStaticHierarchyMove(id, input.StringData);
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}


		[Route("staticHierarchy/{id}/childrenexpanddown")]
		[HttpPut]
		public ScarResult SwitchChildrenOrientation(string CompanyId, int id) {
			try {
				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
				AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
				return helper.UpdateStaticHierarchySwitchChildrenOrientation(id);
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}

		[Route("staticHierarchy/{id}/dragdrop/{targetId}/{location}")]
		[HttpPut]
		public ScarResult DragDropStaticHierarchyLabel(string CompanyId, int id, int targetId, string location) {
			try {
				if (string.IsNullOrEmpty(location))
					return null;
				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
				AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
				return helper.DragDropStaticHierarchyLabel(id, targetId, location.ToUpper());
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}

		[Route("staticHierarchy/{id}/dragdrop/header/{location}")]
		[HttpPut]
		public ScarResult DragDropStaticHierarchyLabelByString(string CompanyId, int id, string location, StringDictionary dict) {
			try {
				if (string.IsNullOrEmpty(location))
					return null;
				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
				AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
				int tableTypeid;
				int.TryParse(dict.StringData["TableTypeId"], out tableTypeid);
				return helper.DragDropStaticHierarchyLabelByString(tableTypeid, dict.StringData["DraggedLabel"], dict.StringData["TargetLabel"], location.ToUpper());
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}


		[Route("staticHierarchy/{id}/unittype")]
		[HttpPut]
		public ScarResult UpdateStatichHierarchyUnitType(string CompanyId, int id, StringInput input) {
			try {
				if (input == null || string.IsNullOrEmpty(input.StringData))
					return null;
				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
				AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
				return helper.UpdateStaticHierarchyUnitType(id, input.StringData);
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}
		[Route("staticHierarchy/{id}/meta")]
		[HttpPut]
		public ScarResult UpdateStatichHierarchyMeta(string CompanyId, int id, StringInput input) {
			try {
				if (input == null || string.IsNullOrEmpty(input.StringData))
					return null;
				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
				AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
				return helper.UpdateStaticHierarchyMeta(id, input.StringData);
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}
		[Route("staticHierarchy/{id}/group")]
		[HttpPut]
		public ScarResult GroupStatichHierarchy(string CompanyId, int id) {
			try {
				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
				AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
				return helper.UpdateStaticHierarchySeperator(id, true);
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}
		[Route("staticHierarchy/{id}/ungroup")]
		[HttpPut]
		public ScarResult UngroupStatichHierarchy(string CompanyId, int id) {
			try {
				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
				AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
				return helper.UpdateStaticHierarchySeperator(id, false);
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}
		[Route("staticHierarchy/{id}/header")]
		[HttpPut]
		public ScarResult EditHierarchyHeaderLabel(string CompanyId, int id, StringInput input) {
			try {
				if (input == null || string.IsNullOrEmpty(input.StringData))
					return null;
				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
				AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
				return helper.UpdateStaticHierarchyHeaderLabel(id, input.StringData);
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}
		[Route("staticHierarchy/{id}/header")]
		[HttpPost]
		public ScarResult AddHeaderStatichHierarchy(string CompanyId, int id) {
			try {
				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
				AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
				return helper.UpdateStaticHierarchyAddHeader(id);
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}

		[Route("staticHierarchy/delete")]
		[HttpDelete]
		public ScarResult DeleteStaticHierarchyWithId(string CompanyId, ScarStringListInput input) {
			try {
				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
				AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
				if (input == null || input.StaticHierarchyIDs.Count == 0 || input.StaticHierarchyIDs.Any(s => s == 0))
					return null;

				return helper.DeleteStaticHierarchy(input.StaticHierarchyIDs);
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}

		[Route("staticHierarchy/cleanup")]
		[HttpDelete]
		public ScarResult CleanupStaticHierarchyWithId(string CompanyId, ScarStringListInput input) {
			try {
				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
				AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
				if (input == null || input.StaticHierarchyIDs.Count == 0 || input.StaticHierarchyIDs.Any(s => s == 0))
					return null;

				return helper.CleanupStaticHierarchy(input.StaticHierarchyIDs);
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}

		[Route("staticHierarchy/{id}/header")]
		[HttpDelete]
		public ScarResult DeleteHeaderStaticHierarchyWithId(string CompanyId, ScarStringListInput input) {
			try {
				return DeleteHeaderStaticHierarchy(CompanyId, input);
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}

		[Route("staticHierarchy/header")]
		[HttpDelete]
		public ScarResult DeleteHeaderStaticHierarchy(string CompanyId, ScarStringListInput input) {
			try {
				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
				AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
				if (input == null || input.StaticHierarchyIDs.Count == 0 || input.StaticHierarchyIDs.Any(s => s == 0))
					return null;

				return helper.UpdateStaticHierarchyDeleteHeader(input.StringData, input.StaticHierarchyIDs);
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}

		[Route("staticHierarchy/{id}/parent")]
		[HttpPost]
		public ScarResult AddParentStatichHierarchy(string CompanyId, int id) {
			try {
				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
				AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
				return helper.UpdateStaticHierarchyAddParent(id);
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}

		[Route("staticHierarchy/parent")]
		[HttpDelete]
		public ScarResult DeleteParentStaticHierarchy(string CompanyId, ScarStringListInput input) {
			try {
				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
				AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
				if (input == null || input.StaticHierarchyIDs.Count == 0 || input.StaticHierarchyIDs.Any(s => s == 0))
					return null;

				return helper.UpdateStaticHierarchyDeleteParent(input.StringData, input.StaticHierarchyIDs);
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}

		[Route("staticHierarchy/{id}/danglingheader")]
		[HttpPost]
		public ScarResult ConvertDanglingHeader(string CompanyId, int id, StringInput input) {
			try {
				int iconum = PermId.PermId2Iconum(CompanyId);
				if (input == null || string.IsNullOrEmpty(input.StringData))
					return null;
				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
				AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
				return helper.UpdateStaticHierarchyConvertDanglingHeader(id, input.StringData);
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}


		//TODO: Add IsSummary
		[Route("timeSlice/{id}")]
		[HttpGet]
		public TimeSlice GetTimeSlice(string CompanyId, int id) {
			try {
				int iconum = PermId.PermId2Iconum(CompanyId);
				if (id == 0 || id == -1)
					return null;

				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
				AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
				return helper.GetTimeSlice(id);
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}

		[Route("timeSlice/{id}")]
		[HttpPost]
		public ScarResult CreateTimeSlice(string CompanyId, int id, StringInput input) {
			try {
				int iconum = PermId.PermId2Iconum(CompanyId);
				if (input == null || string.IsNullOrEmpty(input.StringData))
					return null;
				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
				AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
				return helper.CreateTimeSlice(input.StringData);
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}


		[Route("timeSlice/{id}/reporttype")]
		[HttpPut]
		public ScarResult PutTimeSlice(string CompanyId, int id, StringInput input) {
			try {
				if (input == null || string.IsNullOrEmpty(input.StringData))
					return null;
				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
				AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
				return helper.UpdateTimeSliceReportType(id, input.StringData);
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}

		[Route("timeSlice/{id}/interimtype")]
		[HttpPost]
		public ScarResult PostTimeSlice(string CompanyId, int id, StringInput input) {
			try {
				if (input == null || string.IsNullOrEmpty(input.StringData))
					return null;
				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
				AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
				return helper.CloneUpdateTimeSlice(id, input.StringData);
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}

		[Route("timeSlice/{id}/manualorgset")]
		[HttpPut]
		public ScarResult PutTimeSliceManualOrgSet(string CompanyId, int id, StringInput input) {
			try {
				if (input == null || string.IsNullOrEmpty(input.StringData))
					return null;
				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
				AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
				return helper.UpdateTimeSliceManualOrgSet(id, input.StringData);
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}


		[Route("cells/{id}/flipsign/{DocumentId}/")]
		[HttpPut]
		public ScarResult FlipSign(string CompanyId, string id, Guid DocumentId, ScarInput input) {
			try {
				int iconum = PermId.PermId2Iconum(CompanyId);
				int targetSH = 1;
				if (input != null && input.TargetStaticHierarchyID != 0)
					targetSH = input.TargetStaticHierarchyID;
				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
				AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
				return helper.FlipSign(id, DocumentId, iconum, targetSH);
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}

		[Route("cells/{id}/children/flipsign/{DocumentId}/")]
		[HttpPut]
		public ScarResult FlipChildren(string CompanyId, string id, Guid DocumentId, ScarInput input) {
			try {
				int iconum = PermId.PermId2Iconum(CompanyId);
				//if (input == null || input.TargetStaticHierarchyID == 0)
				//	return null;
				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
				AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
				return helper.FlipChildren(id, DocumentId, iconum, 0);
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}

		[Route("cells/{id}/historical/flipsign/{DocumentId}/")]
		[HttpPut]
		public ScarResult FlipHistorical(string CompanyId, string id, Guid DocumentId, ScarInput input) {
			try {
				int iconum = PermId.PermId2Iconum(CompanyId);
				//if (input == null || input.TargetStaticHierarchyID == 0)
				//	return null;
				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
				AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
				return helper.FlipHistorical(id, DocumentId, iconum, 0);
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}

		[Route("cells/{id}/children/historical/flipsign/{DocumentId}/")]
		[HttpPut]
		public ScarResult FlipChildrenHistorical(string CompanyId, string id, Guid DocumentId, ScarInput input) {
			try {
				int iconum = PermId.PermId2Iconum(CompanyId);
				//if (input == null || input.TargetStaticHierarchyID == 0)
				//	return null;
				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
				AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
				return helper.FlipChildrenHistorical(id, DocumentId, iconum, 0);
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}

		[Route("cells/{id}/swapvalue")]
		[HttpPut]
		public ScarResult SwapValue(string id, StringInput input) {
			try {
				if (input == null || string.IsNullOrEmpty(input.StringData)) {
					var ret = new ScarResult();
					ret.ReturnValue["Success"] = "F";
					return ret;
				}
				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
				AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
				return helper.SwapValue(id, input.StringData);
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}

		[Route("cells/{id}/addMTMW/{DocumentId}/")]
		[HttpGet]
		public TableCellResult AddMakeTheMathWorkNote(string id, Guid DocumentId) {
			try {
				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
				AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
				return helper.AddMakeTheMathWorkNote(id, DocumentId);
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}

		[Route("cells/{id}/addMTMW/{DocumentId}/")]
		[HttpPost]
		public TableCellResult AddMakeTheMathWorkNote(string id, Guid DocumentId, StringInput input) {
			try {
				if (input == null || string.IsNullOrEmpty(input.StringData))
					return AddMakeTheMathWorkNote(id, DocumentId);
				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
				AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
				return helper.AddMakeTheMathWorkNote(id, DocumentId, input.StringData);
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}

		[Route("cells/{id}/addLikePeriod/{DocumentId}/")]
		[HttpGet]
		public TableCellResult AddLikePeriodValidationNote(string id, Guid DocumentId) {
			try {
				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
				AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
				return helper.AddLikePeriodValidationNote(id, DocumentId);
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}

		[Route("cells/{id}/addLikePeriod/{DocumentId}/")]
		[HttpPost]
		public TableCellResult AddLikePeriodValidationNote(string id, Guid DocumentId, StringInput input) {
			try {
				if (input == null || string.IsNullOrEmpty(input.StringData))
					return AddLikePeriodValidationNote(id, DocumentId);
				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
				AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
				return helper.AddLikePeriodValidationNote(id, DocumentId, input.StringData);
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}

		[Route("cells/{id}")]
		[HttpGet]
		public ScarResult GetTableCell(string id) {
			try {
				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
				AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
				return helper.GetTableCell(id);
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}

		[Route("cells/{id}/meta/numericvalue")]
		[HttpPut]
		public ScarResult UpdateTableCellMetaNumericValue(string id, StringInput input) {
			try {
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
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}
		[Route("cells/{id}/meta/scalingfactor")]
		[HttpPut]
		public ScarResult UpdateTableCellMetaScalingFactor(string id, StringInput input) {
			try {
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
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}
		[Route("cells/{id}/meta/perioddate")]
		[HttpPut]
		public ScarResult UpdateTableCellMetaPeriodDate(string id, StringInput input) {
			try {
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
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}
		[Route("cells/{id}/meta/periodtype")]
		[HttpPut]
		public ScarResult UpdateTableCellMetaPeriodType(string id, StringInput input) {
			try {
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
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}
		[Route("cells/{id}/meta/periodlength")]
		[HttpPut]
		public ScarResult UpdateTableCellMetaPeriodLength(string id, StringInput input) {
			try {
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
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}
		[Route("cells/{id}/meta/currency")]
		[HttpPut]
		public ScarResult UpdateTableCellMetaCurrency(string id, StringInput input) {
			try {
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
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}
		[Route("cells/{id}/row/cusip")]
		[HttpPut]
		public ScarResult UpdateTableRowMetaCusip(string id, StringInput input) {
			try {
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
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}
		[Route("cells/{id}/row/pit")]
		[HttpPut]
		public ScarResult UpdateTableRowMetaPit(string id) {
			try {
				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
				AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);

				return helper.UpdateTableRowMetaPit(id, "");
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}
		[Route("cells/{id}/row/scalingfactor")]
		[HttpPut]
		public ScarResult UpdateTableRowMetaScalingFactor(string id, StringInput input) {
			try {
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
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}
		[Route("cells/{id}/column/perioddate")]
		[HttpPut]
		public ScarResult UpdateTableColumnMetaPeriodDate(string id, StringInput input) {
			try {
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
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}
		[Route("cells/{id}/column/columnheader")]
		[HttpPut]
		public ScarResult UpdateTableColumnMetaColumnHeader(string id, StringInput input) {
			try {
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
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}
		[Route("cells/{id}/column/periodtype")]
		[HttpPut]
		public ScarResult UpdateTableColumnMetaPeriodType(string id, StringInput input) {
			try {
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
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}
		[Route("cells/{id}/column/periodlength")]
		[HttpPut]
		public ScarResult UpdateTableColumnMetaPeriodLength(string id, StringInput input) {
			try {
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
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}

		[Route("cells/{id}/column/currency")]
		[HttpPut]
		public ScarResult UpdateTableColumnMetaCurrencyCode(string id, StringInput input) {
			try {
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
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}
		[Route("cells/{id}/column/interimtype")]
		[HttpPut]
		public ScarResult UpdateTableColumnMetaInterimType(string id, StringInput input) {
			try {
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
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}
		[Route("tdp/{TemplateName}/{DocumentId}")]
		[HttpGet]
		public ScarResult GetTDP(string CompanyId, string TemplateName, Guid DocumentId) {
			try {
				int iconum = PermId.PermId2Iconum(CompanyId);
				if (TemplateName == null)
					return null;

				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
				AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
				return helper.GetTemplateInScarResult(iconum, TemplateName, DocumentId);
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}
		[Route("tdp/{id}")]
		[HttpPost]
		public ScarResult UpdateTDP(string id, StringInput input) {
			try {
				ScarResult result = new ScarResult();
				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
				AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
				string newValue = "";
				if (input != null && !string.IsNullOrEmpty(input.StringData)) {
					newValue = input.StringData;
				}
				//newValue = JsonToSQL.Json_UpdateTDPExample;
				if (string.IsNullOrWhiteSpace(newValue)) {
					result.Message = "Missing Json Input";
				} else {
					try {
						result = helper.UpdateTDPByDocumentTableID(id, newValue);
					} catch (Exception ex) {
						result.Message += ex.Message;
					}
				}
				return result;
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}

		[Route("tdp/{id}/deleterowcolumn")]
		[HttpPost]
		public ScarResult DeleteRowColumnTDP(string id, StringInput input) {
			try {
				ScarResult result = new ScarResult();
				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
				AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
				string newValue = "";
				if (input != null && !string.IsNullOrEmpty(input.StringData)) {
					newValue = input.StringData;
				}
				//newValue = JsonToSQL.Json_UpdateTDPExample;
				if (string.IsNullOrWhiteSpace(newValue)) {
					result.Message = "Missing Json Input";
				} else {
					try {
						result = helper.DeleteRowColumnTDPByDocumentTableID(id, newValue);
					} catch (Exception ex) {
						result.Message += ex.Message;
					}
				}
				return result;
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}

		[Route("tdp/{id}")]
		[HttpDelete]
		public ScarResult DeleteTDP(string id) {
			try {
				ScarResult result = new ScarResult();
				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
				AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
				string newValue = "";

				result = helper.DeleteDocumentTableID(id);
				return result;
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}
		[Route("documenttimeslicetablecell/{id}")]
		[HttpPut]
		public ScarResult UpdateDocumentTimeSliceTableCell(string id, StringInput input) {
			try {
				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
				AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
				string newValue = "";
				if (input != null && !string.IsNullOrEmpty(input.StringData)) {
					newValue = input.StringData;
				}
				if (!string.IsNullOrWhiteSpace(newValue)) {
					return null;
				}

				return helper.UpdateDocumentTimeSliceTableCell(id, newValue);
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}

		[Route("documenttimeslicetablecell/{id}")]
		[HttpPost]
		public ScarResult CopyDocumentTimeSliceTableCell(string id, StringInput input) {
			try {
				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
				AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
				string newValue = "";
				if (input != null && !string.IsNullOrEmpty(input.StringData)) {
					newValue = input.StringData;
				}
				if (!string.IsNullOrWhiteSpace(newValue)) {
					return null;
				}

				return helper.CopyDocumentTimeSliceTableCell(id, newValue);
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}

		[Route("documenttimeslicetablecell/{id}")]
		[HttpDelete]
		public ScarResult DeleteDocumentTimeSliceTableCell(string id) {
			try {
				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
				AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
				return helper.DeleteDocumentTimeSliceTableCell(id, "");
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}
		[Route("templates/{TemplateName}/timeslice/{DocumentId}/")]
		[HttpGet]
		public ScarResult GetTimeSliceByTemplate(string CompanyId, string TemplateName, Guid DocumentId) {
			try {
				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
				AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
				return helper.GetTimeSliceByTemplate(TemplateName, DocumentId);
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}

		[Route("templates/{TemplateName}/timeslice/review")]
		[HttpGet]
		public ScarResult GetTimeSliceReview(string CompanyId, string TemplateName) {
			try {
				int iconum = PermId.PermId2Iconum(CompanyId);
				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
				AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
				return helper.GetReviewTimeSlice(TemplateName, iconum);
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}
		[Route("templates/{TemplateName}/timeslice/{id}/issummary")]
		[HttpPut]
		public ScarResult PutTimeSliceIsSummary(string CompanyId, string TemplateName, int id) {
			try {
				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
				AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
				return helper.UpdateTimeSliceIsSummary(id, TemplateName);
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}

		[Route("templates/{TemplateName}/timeslice/{id}/periodnote")]
		[HttpPut]
		public ScarResult PutTimeSlicePeriodNote(string CompanyId, string TemplateName, int id, StringInput input) {
			try {
				string periodNote = null;
				if (input != null && !string.IsNullOrEmpty(input.StringData))
					periodNote = input.StringData;
				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
				AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);

				return helper.UpdateTimeSlicePeriodNote(id, periodNote);
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}

		[Route("templates/{TemplateName}/stitch/{DocumentId}/")]
		[HttpPost]
		public StitchResult PostStitch(string CompanyId, string TemplateName, Guid DocumentId, StitchInput stitchInput) {
			try {
				int iconum = PermId.PermId2Iconum(CompanyId);

				if (stitchInput == null || stitchInput.TargetStaticHierarchyID == 0 || stitchInput.StitchingStaticHierarchyIDs.Count == 0 || stitchInput.StitchingStaticHierarchyIDs.Any(s => s == 0))
					return null;

				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
				AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
				return helper.StitchStaticHierarchies(stitchInput.TargetStaticHierarchyID, DocumentId, stitchInput.StitchingStaticHierarchyIDs, iconum);
			} catch (Exception ex) {
				LogError(ex, string.Format(PingMessage() + "CompanyId:{0}, TemplateName: {1}, DocumentId: {2}", CompanyId, TemplateName, DocumentId));
				LogError(ex, string.Format(PingMessage() + "CompanyId:{0}, TemplateName: {1}, DocumentId: {2}, TargetStaticHierarchyID: {3}, StitchingIDs {4}", CompanyId, TemplateName, DocumentId, stitchInput.TargetStaticHierarchyID, string.Join("|", stitchInput.StitchingStaticHierarchyIDs)));
				return null;
			}
		}


		[Route("templates/{TemplateName}/unstitch/{DocumentId}/")]
		[HttpPost]
		public UnStitchResult PostUnStitch(string CompanyId, string TemplateName, Guid DocumentId, UnStitchInput unstitchInput) {
			try {
				int iconum = PermId.PermId2Iconum(CompanyId);

				if (unstitchInput == null || unstitchInput.TargetStaticHierarchyID == 0)
					return null;

				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
				AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
				return helper.UnstitchStaticHierarchy(unstitchInput.TargetStaticHierarchyID, DocumentId, iconum, unstitchInput.DocumentTimeSliceIDs);
			} catch (Exception ex) {
				LogError(ex, string.Format(PingMessage() + "CompanyId:{0}, TemplateName: {1}, DocumentId: {2}", CompanyId, TemplateName, DocumentId));
				LogError(ex, string.Format(PingMessage() + "CompanyId:{0}, TemplateName: {1}, DocumentId: {2}, TargetStaticHierarchyID: {3}, StitchingIDs {4}", CompanyId, TemplateName, DocumentId, unstitchInput.TargetStaticHierarchyID, string.Join("|", unstitchInput.DocumentTimeSliceIDs)));
				return null;
			}
		}

		public class StitchInput {
			public int TargetStaticHierarchyID { get; set; }
			public List<int> StitchingStaticHierarchyIDs { get; set; }
		}

		public class UnStitchInput {
			public int TargetStaticHierarchyID { get; set; }
			public List<int> DocumentTimeSliceIDs { get; set; }
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
		public class StringDictionary {
			public Dictionary<string, string> StringData { get; set; }
		}
	}

	[RoutePrefix("api/v1/companies/{CompanyId}/efforts/zerominute")]
	public class ZeroMinuteUpdateController : ApiController {
		private void LogError(Exception ex) {
		}
		private bool IsZeroMinuteUpdate() {
			try {
				bool isZeroMinuteUpdate = false;
				string zerominutekey = ConfigurationManager.AppSettings["IsZeroMinuteUpdate"];
				if (!string.IsNullOrEmpty(zerominutekey)) {
					isZeroMinuteUpdate = Convert.ToBoolean(zerominutekey);
				}
				return true;
				return isZeroMinuteUpdate;
			} catch (Exception ex) {
				LogError(ex);
				return true;
			}
		}

		public Guid GetSfDocumentId(string CompanyId, string documentId) {
			try {
				var document = GetDocument(CompanyId, documentId);
				Guid sfDocumentId = new Guid("00000000-0000-0000-0000-000000000000");
				if (document == null || document.SuperFastDocumentId == null) {
					// probably due to incorrect CompanyId. 
					// Just inquiry the DocumentTable
				} else {
					sfDocumentId = new Guid(document.SuperFastDocumentId);
				}
				return sfDocumentId;
			} catch (Exception ex) {
				LogError(ex);
				return new Guid("00000000-0000-0000-0000-000000000000");
			}
		}
		public AsReportedDocument GetDocument(string CompanyId, string documentId) {
			try {
				int iconum = PermId.PermId2Iconum(CompanyId);

				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
				string damConnectionString = ConfigurationManager.ConnectionStrings["FFDAM"].ToString();
				DocumentHelper documentHelper = new DocumentHelper(sfConnectionString, damConnectionString);
				var document = documentHelper.GetDocument(iconum, documentId);
				return document;
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}

		public class StringInput {
			public string StringData { get; set; }
		}


		[Route("documents/{damdocumentId}")]
		[HttpPut]
		public bool ExecuteZeroMinuteUpdate(string CompanyId, Guid damdocumentId, StringInput input) {
			try {
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
				Dictionary<string, string> returnValue = new Dictionary<string, string>();
				returnValue["Success"] = "F";
				bool runOnce = true;
				string isoCountry = helper.GetDocumentIsoCountry(SfDocumentId);
				bool isUsDocument = (!string.IsNullOrEmpty(isoCountry) && isoCountry.ToUpper() == "US");
				string ExceptionSource = "Exception at Unknown: ";
				if (IsZeroMinuteUpdate() && isUsDocument) {
					try {
						while (runOnce) {
							ExceptionSource = "Exception at DoInterimTypeAndCurrency: ";
							returnValue = DoInterimTypeAndCurrency(CompanyId, damdocumentId).ReturnValue;

							if (!(returnValue["Success"] == "T")) {
								break;
							}
							ExceptionSource = "Exception at DoRedStarSlotting: ";
							var RedStarReturnValue = returnValue = DoRedStarSlotting(CompanyId, damdocumentId).ReturnValue;
							if (!(returnValue["Success"] == "T")) {
								if (returnValue["Message"].StartsWith("Exception")) { // if it's real exception, not redstar warning
									break;
								}
							}
							ExceptionSource = "Exception at DoSetIncomeOrientation: ";
							DoSetIncomeOrientation(CompanyId, damdocumentId);

							ExceptionSource = "Exception at DoMTMWAndLPVValidation: ";
							returnValue = DoMTMWAndLPVValidation(CompanyId, damdocumentId).ReturnValue;

							if (!(returnValue["Success"] == "T")) {
								//System.Text.StringBuilder sb = new System.Text.StringBuilder();

								//string Ids = mtmwRet.cells.Select(x => x.ID.ToString()).Aggregate((a, b) => a + "," + b);
								//returnValue = new Tuple<bool, string>(false, "mtmwlpvfailed: " + Ids);
								break;
							}
							if (!(RedStarReturnValue["Success"] == "T")) {
								ExceptionSource = "Exception at DoRedStarSlotting: ";
								returnValue = RedStarReturnValue;
								break;
							}
							ExceptionSource = "Exception at DoARDValidation: ";
							returnValue = DoARDValidation(CompanyId, damdocumentId).ReturnValue;
							if (!(returnValue["Success"] == "T")) {
								break;
							}
							runOnce = false;
						}
					} catch (Exception ex) {
						returnValue["Success"] = "F";
						returnValue["Message"] = ExceptionSource + returnValue["Message"] + ex.Message + new string(ex.StackTrace.Take(1000).ToArray());
					}
					try {
						helper.LogError(damdocumentId, startReason, startTime, CompanyId, (returnValue["Success"] == "T"), returnValue["Message"]);
					} catch { }
					return (returnValue["Success"] == "T");
					//I think that the plan is for SFAutoStitchingAgent to return success if we succeeded in Zero Minute
					//and failure if we don't so we probably just have to return true;

				}
				return false;
			} catch (Exception ex) {
				LogError(ex);
				return false;
			}
		}

		[Route("documents/{damdocumentId}/ard")]
		[HttpPut]
		public ScarResult DoARDValidation(string CompanyId, Guid damdocumentId) {
			try {
				var sfDocument = GetDocument(CompanyId, damdocumentId.ToString());
				Guid SfDocumentId = new Guid(sfDocument.SuperFastDocumentId); // SFDocumentID

				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
				AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
				ScarResult result = new ScarResult();
				result.ReturnValue = helper.ARDValidation(SfDocumentId);
				return result;
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}
		[Route("documents/{damdocumentId}/redstarslotting")]
		[HttpPut]
		public ScarResult DoRedStarSlotting(string CompanyId, Guid damdocumentId) {
			try {
				var sfDocument = GetDocument(CompanyId, damdocumentId.ToString());
				Guid SfDocumentId = new Guid(sfDocument.SuperFastDocumentId); // SFDocumentID

				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
				AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
				ScarResult result = new ScarResult();

				result.ReturnValue = helper.UpdateRedStarSlotting(SfDocumentId);
				return result;
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}
		[Route("documents/{damdocumentId}/setincome")]
		[HttpPut]
		public void DoSetIncomeOrientation(string CompanyId, Guid damdocumentId) {
			try {
				var sfDocument = GetDocument(CompanyId, damdocumentId.ToString());
				Guid SfDocumentId = new Guid(sfDocument.SuperFastDocumentId); // SFDocumentID
				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
				AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
				helper.SetIncomeOrientation(SfDocumentId);
			} catch (Exception ex) {
				LogError(ex);
				return;
			}
		}
		[Route("documents/{damdocumentId}/validatetables")]
		[HttpPut]
		public ScarResult DoInterimTypeAndCurrency(string CompanyId, Guid damdocumentId) {
			try {
				var sfDocument = GetDocument(CompanyId, damdocumentId.ToString());
				Guid SfDocumentId = new Guid(sfDocument.SuperFastDocumentId); // SFDocumentID
				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
				int iconum = PermId.PermId2Iconum(CompanyId);
				AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
				var result = new ScarResult();
				var errorMessage = helper.CheckParsedTableInterimTypeAndCurrency(SfDocumentId, iconum, "Full");
				Dictionary<string, string> returnValue = new Dictionary<string, string>();
				returnValue["Success"] = string.IsNullOrEmpty(errorMessage) ? "T" : "F";
				returnValue["Message"] = errorMessage;
				result.ReturnValue = returnValue;
				return result;
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}

		}
		[Route("documents/{damdocumentId}/validatetables/{ContentType}")]
		[HttpPut]
		public ScarResult DoInterimTypeAndCurrency(string CompanyId, Guid damdocumentId, string ContentType) {
			try {
				var sfDocument = GetDocument(CompanyId, damdocumentId.ToString());
				Guid SfDocumentId = new Guid(sfDocument.SuperFastDocumentId); // SFDocumentID
				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
				int iconum = PermId.PermId2Iconum(CompanyId);
				AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
				var result = new ScarResult();
				var errorMessage = helper.CheckParsedTableInterimTypeAndCurrency(SfDocumentId, iconum, ContentType);
				Dictionary<string, string> returnValue = new Dictionary<string, string>();
				returnValue["Success"] = string.IsNullOrEmpty(errorMessage) ? "T" : "F";
				returnValue["Message"] = errorMessage;
				result.ReturnValue = returnValue;
				return result;
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}
				
		private List<string> LPVMetaTypes = new List<string>() { "NI", "RV", "TA", "TL", "LE", "PS", "PE", "CC" };

		[Route("documents/{damdocumentId}/mtmwandlpv2")]
		[HttpPut]
		public MTMWLPVReturn DoMTMWAndLPVValidation2(string CompanyId, Guid damdocumentId) {
			try {
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
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}


		[Route("documents/{damdocumentId}/mtmwandlpv")]
		[HttpPut]
		public ScarResult DoMTMWAndLPVValidation(string CompanyId, Guid damdocumentId) {
			try {
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
							.SelectMany(sh => sh.Cells.Where(c => c.MTMWValidationFlag && sh.StaticHierarchyMetaType != "SD" && sh.StaticHierarchyMetaType != "FN")));
						int mtmwcount = mtmwcells.Count();
						if (mtmwcells.Count() > 0) {
							errorMessageBuilder.Append(TemplateName + ": MTMW: ");
							foreach (var cell in mtmwcells) {
								var timeslice = templates.First().TimeSlices.FirstOrDefault(x => x.Cells.Contains(cell));
								errorMessageBuilder.Append(string.Format("{0}({1},{2}) ", cell.DisplayValue.HasValue ? cell.DisplayValue.Value.ToString("0.##") : "-", timeslice.TimeSlicePeriodEndDate.ToString("yyyy-MM-dd"), timeslice.ReportType));
							}
							isSuccess = false;
						}

						IEnumerable<SCARAPITableCell> lpvcells = templates.SelectMany(t => t.StaticHierarchies.Where(sh => (sh.UnitTypeId == 2 || sh.UnitTypeId == 1 || LPVMetaTypes.Contains(sh.StaticHierarchyMetaType)) && sh.StaticHierarchyMetaType != "SD" && sh.StaticHierarchyMetaType != "FN")
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
				Dictionary<string, string> returnValue = new Dictionary<string, string>();
				returnValue["Success"] = isSuccess ? "T" : "F";
				returnValue["Message"] = errorMessageBuilder.ToString();
				result.ReturnValue = returnValue;
				return result;
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}

		public class MTMWLPVReturn {
			public bool success { get; set; }
			public List<SCARAPITableCell> cells { get; set; }
		}


		[Route("documents/{damdocumentId}/mtmw")]
		[HttpGet]
		public bool DoMTMWValidation(string CompanyId, Guid damdocumentId) {
			try {
				var sfDocument = GetDocument(CompanyId, damdocumentId.ToString());
				Guid SfDocumentId = new Guid(sfDocument.SuperFastDocumentId);
				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
				AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
				var result = helper.GetMtmwTableCells(0, SfDocumentId);
				return result;
			} catch (Exception ex) {
				LogError(ex);
				return false;
			}
		}
		[Route("documents/{damdocumentId}/lpv")]
		[HttpPut]
		public ScarResult DoLPVValidation(string CompanyId, Guid damdocumentId) {
			try {
				var sfDocument = GetDocument(CompanyId, damdocumentId.ToString());
				Guid SfDocumentId = new Guid(sfDocument.SuperFastDocumentId);
				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
				AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
				var result = helper.GetLpvTableCells(0, SfDocumentId);
				return result;
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}
		[Route("documents/{damdocumentId}/export")]
		[HttpPut]
		public ScarResult DoExport(string CompanyId, Guid damdocumentId) {
			try {
				var sfDocument = GetDocument(CompanyId, damdocumentId.ToString());
				Guid SfDocumentId = new Guid(sfDocument.SuperFastDocumentId);

				return new ScarResult();
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}


	}
}