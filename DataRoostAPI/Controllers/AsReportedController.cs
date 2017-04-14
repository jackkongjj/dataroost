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
	public static class PerformanceInfo1 {
		[DllImport("psapi.dll", SetLastError = true)]
		[return: MarshalAs(UnmanagedType.Bool)]
		public static extern bool GetPerformanceInfo([Out] out PerformanceInformation PerformanceInformation, [In] int Size);

		[StructLayout(LayoutKind.Sequential)]
		public struct PerformanceInformation {
			public int Size;
			public IntPtr CommitTotal;
			public IntPtr CommitLimit;
			public IntPtr CommitPeak;
			public IntPtr PhysicalTotal;
			public IntPtr PhysicalAvailable;
			public IntPtr SystemCache;
			public IntPtr KernelTotal;
			public IntPtr KernelPaged;
			public IntPtr KernelNonPaged;
			public IntPtr PageSize;
			public int HandlesCount;
			public int ProcessCount;
			public int ThreadCount;
		}
		public static Int64 GetTotalMemory() {
			PerformanceInformation pi = new PerformanceInformation();
			if (GetPerformanceInfo(out pi, Marshal.SizeOf(pi))) {
				return Convert.ToInt64((pi.PhysicalTotal.ToInt64() * pi.PageSize.ToInt64()));
			} else {
				return -1;
			}
		}
		public static Int64 GetAvaiableTotalMemory() {
			PerformanceInformation pi = new PerformanceInformation();

			if (GetPerformanceInfo(out pi, Marshal.SizeOf(pi))) {
				return Convert.ToInt64((pi.PhysicalAvailable.ToInt64() * pi.PageSize.ToInt64()));
			} else {
				return -1;
			}
		}
		static PerformanceCounter CpuCounterTotal = new PerformanceCounter("Processor",
"% Processor Time", "_Total");
		static PerformanceCounter CpuCounter0 = new PerformanceCounter("Processor",
"% Processor Time", "0");
		static PerformanceCounter CpuCounter1 = new PerformanceCounter("Processor",
"% Processor Time", "1");
		static PerformanceCounter CpuCounter2 = new PerformanceCounter("Processor",
"% Processor Time", "2");
		static PerformanceCounter CpuCounter3 = new PerformanceCounter("Processor",
"% Processor Time", "3");
		static PerformanceCounter CpuCounter4 = new PerformanceCounter("Processor",
"% Processor Time", "4");
		static PerformanceCounter CpuCounter5 = new PerformanceCounter("Processor",
"% Processor Time", "5");
		static PerformanceCounter CpuCounter6 = new PerformanceCounter("Processor",
"% Processor Time", "6");
		static PerformanceCounter CpuCounter7 = new PerformanceCounter("Processor",
"% Processor Time", "7");
		static PerformanceCounter CpuCounter8 = new PerformanceCounter("Processor",
"% Processor Time", "8");
		static string ProcessName = null;
		public static string GetPerformanceData() {
			try {
				string reading = "";
				PerformanceCounter WorkingSetPrivateMemoryCounter = new PerformanceCounter("Process",
				"Working Set - Private",
				GetNameToUseForMemory(Process.GetCurrentProcess()));

				float cpuPercentTotal = -1.0f;
				float cpuPercent0 = -1.0f;
				float cpuPercent1 = -1.0f;
				float cpuPercent2 = -1.0f;
				float cpuPercent3 = -1.0f;
				float cpuPercent4 = -1.0f;
				float cpuPercent5 = -1.0f;
				float cpuPercent6 = -1.0f;
				float cpuPercent7 = -1.0f;
				float cpuPercent8 = -1.0f;
				try {
					cpuPercentTotal = CpuCounterTotal.NextValue();
					cpuPercent0 = CpuCounter0.NextValue();
					cpuPercent1 = CpuCounter1.NextValue();
					cpuPercent2 = CpuCounter2.NextValue();
					cpuPercent3 = CpuCounter3.NextValue();
					cpuPercent4 = CpuCounter4.NextValue();
					cpuPercent5 = CpuCounter5.NextValue();
					cpuPercent6 = CpuCounter6.NextValue();
					cpuPercent7 = CpuCounter7.NextValue();
					cpuPercent8 = CpuCounter8.NextValue();
				} catch { }
				float usemem = WorkingSetPrivateMemoryCounter.NextValue();
				long tmem = PerformanceInfo1.GetTotalMemory();
				long ava = PerformanceInfo1.GetAvaiableTotalMemory();
				if (tmem == 0) {
					return " MEM Usage: UnFetchable";
				} else {
					reading += "<BR> CPU_total Percentage: " + cpuPercentTotal + " % <BR> \r\n";
					reading += "CPU_0 Percentage: " + cpuPercent0 + " % <BR> \r\n";
					reading += "CPU_1 Percentage: " + cpuPercent1 + " % <BR> \r\n";
					reading += "CPU_2 Percentage: " + cpuPercent2 + " % <BR> \r\n";
					reading += "CPU_3 Percentage: " + cpuPercent3 + " % <BR> \r\n";
					reading += "CPU_4 Percentage: " + cpuPercent4 + " % <BR> \r\n";
					reading += "CPU_5 Percentage: " + cpuPercent5 + " % <BR> \r\n";
					reading += "CPU_6 Percentage: " + cpuPercent6 + " % <BR> \r\n";
					reading += "CPU_7 Percentage: " + cpuPercent7 + " % <BR> \r\n";
					reading += "CPU_8 Percentage: " + cpuPercent8 + " % <BR> \r\n";
					reading += " MEM Avaiable: " + ava / 1048576 + "(MB) <BR> \r\n";
					reading += "MEM Size: " + tmem / 1048576 + "(MB) <BR> ";
					reading += "Process MEM Usage: " + (usemem / tmem).ToString("P") + " <BR>";
					reading += "System MEM Usage: " + (1 - ava * 1.0 / tmem).ToString("P") + "<BR> \r\n <BR> \r\n";
					return reading;
				}
			} catch (Exception ex) {

				return " MEM Usage: Get Execption";
			}
		}

		public static string GetNameToUseForMemory(Process proc) {
			if (!string.IsNullOrEmpty(ProcessName))
				return ProcessName;
			var nameToUseForMemory = String.Empty;
			var category = new PerformanceCounterCategory("Process");
			var instanceNames = category.GetInstanceNames().Where(x => x.Contains(proc.ProcessName));
			foreach (var instanceName in instanceNames) {
				using (var performanceCounter = new PerformanceCounter("Process", "ID Process", instanceName, true)) {
					if (performanceCounter.RawValue != proc.Id)
						continue;
					nameToUseForMemory = instanceName;
					break;
				}
			}
			ProcessName = nameToUseForMemory;
			return nameToUseForMemory;
		}

		public static void SendEmail(string subject, string emailBody) {
			try {
				SmtpClient mySMTP = new SmtpClient("mail.factset.com");
				MailAddress mailFrom = new MailAddress("myself@factset.com", "IMA DataRoost");
				MailMessage message = new MailMessage();
				message.From = mailFrom;
				message.To.Add(new MailAddress("ljiang@factset.com", "Lun Jiang"));
				message.Subject = subject;
				message.DeliveryNotificationOptions = DeliveryNotificationOptions.OnFailure;
				message.Body = emailBody;
				message.IsBodyHtml = true;
				mySMTP.Send(message);
			} catch { }
		}
	}

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
			//PerformanceInfo1.SendEmail("DataRoost Performance Enter", PerformanceInfo1.GetPerformanceData());
			int iconum = PermId.PermId2Iconum(CompanyId);
			if (TemplateName == null)
				return null;

			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
			//PerformanceInfo1.SendEmail("DataRoost Performance Created Helper", PerformanceInfo1.GetPerformanceData());
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

        [Route("cells/{id}/flipsign/{DocumentId}/")]
        [HttpGet]
        public ScarResult FlipSign(string id, Guid DocumentId)
        {
            string CompanyId = "36468";
            int iconum = PermId.PermId2Iconum(CompanyId);
            string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
            AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
            return helper.FlipSign(id, DocumentId, iconum, 0);
        }

        [Route("cells/{id}/flipsign/{DocumentId}/")]
        [HttpPost]
        public ScarResult FlipSign(string id, Guid DocumentId, ScarInput input)
        {
            string CompanyId = "36468";
            int iconum = PermId.PermId2Iconum(CompanyId);
            if (input == null || input.TargetStaticHierarchyID == 0)
                return null;
            string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
            AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
            return helper.FlipSign(id, DocumentId, iconum, input.TargetStaticHierarchyID);
        }

        [Route("cells/{id}/addMTMW/{DocumentId}/")]
        [HttpGet]
        public TableCellResult AddMakeTheMathWorkNote(string id, Guid DocumentId)
        {
            string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
            AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
            return helper.AddMakeTheMathWorkNote(id, DocumentId);
        }

        [Route("cells/{id}/addLikePeriod/{DocumentId}/")]
        [HttpGet]
        public TableCellResult AddLikePeriodValidationNote(string id, Guid DocumentId)
        {
            string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
            AsReportedTemplateHelper helper = new AsReportedTemplateHelper(sfConnectionString);
            return helper.AddLikePeriodValidationNote(id, DocumentId);
        }



		public class StitchInput {
			public int TargetStaticHierarchyID { get; set; }
			public List<int> StitchingStaticHierarchyIDs { get; set; }
		}

		public class UnStitchInput {
			public int TargetStaticHierarchyID { get; set; }
		}

        public class ScarInput
        {
            public int TargetStaticHierarchyID { get; set; }
            public List<int> StitchingStaticHierarchyIDs { get; set; }
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
	}
}