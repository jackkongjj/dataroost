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
using LogPerformance;
using CCS.Fundamentals.DataRoostAPI.CommLogger;

namespace CCS.Fundamentals.DataRoostAPI.Controllers {
	[CommunicationLogger]
	[RoutePrefix("api/v1/visualstitching")]
	public class VisualStitchingController : ApiController {
		public static void SendEmail(string subject, string emailBody) {
			try {
				SmtpClient mySMTP = new SmtpClient("mail.factset.com");
				MailAddress mailFrom = new MailAddress("myself@factset.com", "IMA DataRoost");
				MailMessage message = new MailMessage();
				message.From = mailFrom;
				var ljiang = new MailAddress("ljiang@factset.com", "Leo Jiang");
				var leo = new MailAddress("lchang@factset.com", "Leo");
				var adam = new MailAddress("apitzer@factset.com", "Adam Pitzer");
				var vsaxena = new MailAddress("vsaxena@factset.com", "Vaibhav Saxena");
				var rohan = new MailAddress("rthankachan@factset.com", "Rohan Jacob");
				message.To.Add(ljiang);
				message.To.Add(vsaxena);
				message.To.Add(adam);
				message.To.Add(leo);
				message.To.Add(rohan);
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

		[Route("json/id/{id}")]
		[HttpGet]
		public HttpResponseMessage GetDocument(int id) {
			try {

				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
				string damConnectionString = ConfigurationManager.ConnectionStrings["FFDAM"].ToString();
				var vsHelper = new VisualStitchingHelper(sfConnectionString);
				var json = vsHelper.GetJson(id);
                return new HttpResponseMessage()
                {
                    Content = new StringContent(json, System.Text.Encoding.UTF8, "application/json")
                };
            } catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}

        [Route("json/{hashkey}")]
        [HttpGet]
        public HttpResponseMessage GetDocumentByHashkey(string hashkey)
        {
            try
            {

                string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
                string damConnectionString = ConfigurationManager.ConnectionStrings["FFDAM"].ToString();
                var vsHelper = new VisualStitchingHelper(sfConnectionString);
                var json = vsHelper.GetJsonByHash(hashkey);
                return new HttpResponseMessage()
                {
                    Content = new StringContent(json, System.Text.Encoding.UTF8, "application/json")
                };
            }
            catch (Exception ex)
            {
                LogError(ex);
                return null;
            }
        }

        [Route("json/{hashkey}")]
        [HttpPost]
        public HttpResponseMessage SetDocumentByHashkey(string hashkey, Newtonsoft.Json.Linq.JObject value)
        {
            try
            {

                string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
                string damConnectionString = ConfigurationManager.ConnectionStrings["FFDAM"].ToString();
                var vsHelper = new VisualStitchingHelper(sfConnectionString);
                var json = vsHelper.SetJsonByHash(hashkey, Newtonsoft.Json.JsonConvert.SerializeObject(value));
                return new HttpResponseMessage()
                {
                    Content = new StringContent(json.ToString(), System.Text.Encoding.UTF8, "application/json")
                };
            }
            catch (Exception ex)
            {
                LogError(ex);
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
}