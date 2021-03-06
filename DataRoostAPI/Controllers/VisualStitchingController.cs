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
				var ljiang = new MailAddress("ljiang@factset.com", "Lun Jiang");
				var rohan = new MailAddress("rthankachan@factset.com", "Rohan Jacob");
				message.To.Add(ljiang);
				message.Subject = subject + " from " + Environment.MachineName;
				message.DeliveryNotificationOptions = DeliveryNotificationOptions.OnFailure;
				message.Body = emailBody;
				message.IsBodyHtml = true;
				mySMTP.Send(message);
			} catch { }
		}

        public static void SendEmailToAnalysts(string subject, string emailBody)
        {
            try
            {
                SmtpClient mySMTP = new SmtpClient("mail.factset.com");
                MailAddress mailFrom = new MailAddress("service@factset.com", "IMA DataRoost");
                MailMessage message = new MailMessage();
                message.From = mailFrom;
                var ljiang = new MailAddress("ljiang@factset.com", "Lun Jiang");
                var santhosh = new MailAddress("skuthuru@factset.com", "Santhosh Kuthuru");
                var prapolu = new MailAddress("prapolu@factset.com", "Prakash Rapolu");
                message.To.Add(ljiang);
                message.To.Add(santhosh);
                message.To.Add(prapolu);
                message.Subject = subject + " from " + Environment.MachineName;
                message.DeliveryNotificationOptions = DeliveryNotificationOptions.OnFailure;
                message.Body = emailBody;
                message.IsBodyHtml = true;
                mySMTP.Send(message);
            }
            catch { }
        }

        private void LogError(Exception ex, string extra = "") {
			string msg = ex.Message + ex.StackTrace;
			if (ex.InnerException != null)
				msg += "INNER EXCEPTION" + ex.InnerException.Message + ex.InnerException.StackTrace;
			SendEmail("DataRoost Exception", msg + extra);
		}
        private void LogErrorAutoCluster(Exception ex, string extra = "")
        {
            string msg = ex.Message + ex.StackTrace;
            if (ex.InnerException != null)
                msg += "INNER EXCEPTION" + ex.InnerException.Message + ex.InnerException.StackTrace;
            SendEmailToAnalysts("Auto-Clustering Failure", msg + extra);
        }

        [Route("json/id/{id}")]
		[HttpGet]
		public HttpResponseMessage GetDocument(int id) {
			try {

				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDoc-SCAR"].ToString();
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
		public HttpResponseMessage GetDocumentByHashkey(string hashkey) {
			try {

				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
				string damConnectionString = ConfigurationManager.ConnectionStrings["FFDAM"].ToString();
				var vsHelper = new VisualStitchingHelper(sfConnectionString);
				var json = vsHelper.GetJsonByHash(hashkey);
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
		[HttpPost]
		public HttpResponseMessage SetDocumentByHashkey(string hashkey, Newtonsoft.Json.Linq.JObject value) {
			try {

				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
				string damConnectionString = ConfigurationManager.ConnectionStrings["FFDAM"].ToString();
				var vsHelper = new VisualStitchingHelper(sfConnectionString);
				var json = vsHelper.SetJsonByHash(hashkey, Newtonsoft.Json.JsonConvert.SerializeObject(value));
				return new HttpResponseMessage()
				{
					Content = new StringContent(json.ToString(), System.Text.Encoding.UTF8, "application/json")
				};
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}

		[Route("datatree/{DamDocumentId}")]
		[HttpGet]
		public HttpResponseMessage GetDataTree(Guid DamDocumentId) {
			return GetDataTreeFileNo(DamDocumentId, 0);
		}

		[Route("datatree/{DamDocumentId}/{FileNo}")]
		[HttpGet]
		public HttpResponseMessage GetDataTreeFileNo(Guid DamDocumentId, int FileNo) {
			try {

				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDoc-SCAR"].ToString();
				sfConnectionString = @"Application Name=DataRoost;Data Source=ffdocumenthistory-prestage-rds-sqlserver-se-standalone.prod.factset.com;Initial Catalog=FFDocumentHistory;User ID=ffdocumenthistory_admin_dev;Password=1tpIDJLT;MultipleActiveResultSets=True;";

				var vsHelper = new VisualStitchingHelper(sfConnectionString);
				var json = vsHelper.GetDataTree();
				return new HttpResponseMessage()
				{
					Content = new StringContent(json, System.Text.Encoding.UTF8, "application/json")
				};
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}
		[Route("datatree/v2/{DamDocumentId}/{FileNo}")]
		[HttpGet]
		public HttpResponseMessage GetDataTreeFileNoV2(Guid DamDocumentId, int FileNo) {
			try {

				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDoc-SCAR"].ToString();
				sfConnectionString = @"Application Name=DataRoost;Data Source=ffdocumenthistory-prestage-rds-sqlserver-se-standalone.prod.factset.com;Initial Catalog=FFDocumentHistory;User ID=ffdocumenthistory_admin_dev;Password=1tpIDJLT;MultipleActiveResultSets=True;";

                var vsHelper = new VisualStitchingHelper(sfConnectionString);
                var json = vsHelper.GetPostGresDataTree();
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
        [Route("datatree/v3/{DamDocumentId}/{FileNo}")]
        [HttpGet]
        public HttpResponseMessage GetDataTreeFileNoV3(Guid DamDocumentId, int FileNo)
        {
            try
            {

                string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDoc-SCAR"].ToString();
                sfConnectionString = @"Application Name=DataRoost;Data Source=ffdocumenthistory-prestage-rds-sqlserver-se-standalone.prod.factset.com;Initial Catalog=FFDocumentHistory;User ID=ffdocumenthistory_admin_dev;Password=1tpIDJLT;MultipleActiveResultSets=True;";

                var vsHelper = new VisualStitchingHelper(sfConnectionString);
                var json = vsHelper.GetPostGresDataTree3();
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

        [Route("datatree/v3/profile/")]
        [HttpGet]
        public HttpResponseMessage GetDataTreeProfile()
        {
            try
            {

                string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDoc-SCAR"].ToString();
                sfConnectionString = @"Application Name=DataRoost;Data Source=ffdocumenthistory-prestage-rds-sqlserver-se-standalone.prod.factset.com;Initial Catalog=FFDocumentHistory;User ID=ffdocumenthistory_admin_dev;Password=1tpIDJLT;MultipleActiveResultSets=True;";

                var vsHelper = new VisualStitchingHelper(sfConnectionString);
                var json = vsHelper.GetPostGresDataTreeProfile();
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

        [Route("datatree/v3/profile/{name}")]
        [HttpPost]
        public HttpResponseMessage SaveDataTreeProfile(String name, StringInput input)
        {
            try
            {

                if (input == null)
                    return null;

                string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDoc-SCAR"].ToString();
                sfConnectionString = @"Application Name=DataRoost;Data Source=ffdocumenthistory-prestage-rds-sqlserver-se-standalone.prod.factset.com;Initial Catalog=FFDocumentHistory;User ID=ffdocumenthistory_admin_dev;Password=1tpIDJLT;MultipleActiveResultSets=True;";
                String jsonstr = input.StringData;
                var vsHelper = new VisualStitchingHelper(sfConnectionString);
                var json = vsHelper.SetPostGresDataTreeProfile(name, jsonstr);
                var ret = new Dictionary<string, string>();
                ret["data"] = json;
                return new HttpResponseMessage()
                {
                    Content = new StringContent(Newtonsoft.Json.JsonConvert.SerializeObject(ret), System.Text.Encoding.UTF8, "application/json")
                };
            }
            catch (Exception ex)
            {
                LogError(ex);
                return null;
            }
        }

        [Route("name-tree-api/offsets/{DamDocumentId}/{fileid}")]
        [HttpGet]
        public HttpResponseMessage GetDocumentOffsets(Guid DamDocumentId, int fileid)
        {
            try
            {

                string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDoc-SCAR"].ToString();
                sfConnectionString = @"Application Name=DataRoost;Data Source=ffdocumenthistory-prestage-rds-sqlserver-se-standalone.prod.factset.com;Initial Catalog=FFDocumentHistory;User ID=ffdocumenthistory_admin_dev;Password=1tpIDJLT;MultipleActiveResultSets=True;";

                var vsHelper = new VisualStitchingHelper(sfConnectionString);
                var json = vsHelper.GetDocumentOffsets(DamDocumentId.ToString(), fileid);
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

        [Route("name-tree-api/normtables/")]
        [HttpGet]
        public HttpResponseMessage GetDocumentTrees()
        {
            try
            {

                string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDoc-SCAR"].ToString();
                sfConnectionString = @"Application Name=DataRoost;Data Source=ffdocumenthistory-prestage-rds-sqlserver-se-standalone.prod.factset.com;Initial Catalog=FFDocumentHistory;User ID=ffdocumenthistory_admin_dev;Password=1tpIDJLT;MultipleActiveResultSets=True;";

                var vsHelper = new VisualStitchingHelper(sfConnectionString);
                var json = vsHelper.GetNameTreesNormTable();
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

        [Route("name-tree-api/trees/{DamDocumentId}/{fileid}")]
        [HttpGet]
        public HttpResponseMessage GetDocumentTrees(Guid DamDocumentId, int fileid)
        {
            try
            {

                string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDoc-SCAR"].ToString();
                sfConnectionString = @"Application Name=DataRoost;Data Source=ffdocumenthistory-prestage-rds-sqlserver-se-standalone.prod.factset.com;Initial Catalog=FFDocumentHistory;User ID=ffdocumenthistory_admin_dev;Password=1tpIDJLT;MultipleActiveResultSets=True;";

                var vsHelper = new VisualStitchingHelper(sfConnectionString);
                var json = vsHelper.GetNameTreesTableByDamid(DamDocumentId.ToString(), fileid);
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

        [Route("name-tree-api/clustertrees/{istest}")]
        [HttpPost]
        public HttpResponseMessage SaveClusterTree(Boolean istest, StringInput input)
        {
            try
            {

                if (input == null)
                    return null;

                string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDoc-SCAR"].ToString();
                sfConnectionString = @"Application Name=DataRoost;Data Source=ffdocumenthistory-prestage-rds-sqlserver-se-standalone.prod.factset.com;Initial Catalog=FFDocumentHistory;User ID=ffdocumenthistory_admin_dev;Password=1tpIDJLT;MultipleActiveResultSets=True;";
                String jsonstr = input.StringData;
                var vsHelper = new VisualStitchingHelper(sfConnectionString);
                var json = vsHelper.SaveClusterTree(jsonstr, istest);
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

        [Route("name-tree-api/rebuildtest")]
        [HttpGet]
        public String rebuildtest()
        {
            try
            {

                string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDoc-SCAR"].ToString();
                sfConnectionString = @"Application Name=DataRoost;Data Source=ffdocumenthistory-prestage-rds-sqlserver-se-standalone.prod.factset.com;Initial Catalog=FFDocumentHistory;User ID=ffdocumenthistory_admin_dev;Password=1tpIDJLT;MultipleActiveResultSets=True;";

                var vsHelper = new VisualStitchingHelper(sfConnectionString);
                var result = vsHelper.rebuildtest();
                return result;
            }
            catch (Exception ex)
            {
                LogError(ex);
                return null;
            }
        }

        [Route("name-tree-api/clustertrees/{DamDocumentId}/{iconum}/{istest}")]
        [HttpGet]
        public HttpResponseMessage GetClusterTreesWithIconum(Guid DamDocumentId, int iconum, Boolean istest)
        {
            try
            {

                string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDoc-SCAR"].ToString();
                sfConnectionString = @"Application Name=DataRoost;Data Source=ffdocumenthistory-prestage-rds-sqlserver-se-standalone.prod.factset.com;Initial Catalog=FFDocumentHistory;User ID=ffdocumenthistory_admin_dev;Password=1tpIDJLT;MultipleActiveResultSets=True;";

                var vsHelper = new VisualStitchingHelper(sfConnectionString);
                var json = vsHelper.GetPostGresClusterNameTreeTableNodeWithIconum(DamDocumentId.ToString(), iconum, istest);
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

        [Route("name-tree-api/clustertrees/{DamDocumentId}")]
        [HttpGet]
        public HttpResponseMessage GetClusterTrees(Guid DamDocumentId)
        {
            try
            {

                string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDoc-SCAR"].ToString();
                sfConnectionString = @"Application Name=DataRoost;Data Source=ffdocumenthistory-prestage-rds-sqlserver-se-standalone.prod.factset.com;Initial Catalog=FFDocumentHistory;User ID=ffdocumenthistory_admin_dev;Password=1tpIDJLT;MultipleActiveResultSets=True;";

                var vsHelper = new VisualStitchingHelper(sfConnectionString);
                var json = vsHelper.GetPostGresClusterNameTreeTableNode(DamDocumentId.ToString());
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

        [Route("name-tree-api/{DamDocumentId}/{iconum}/{tableid}/{fileid}")]
        [HttpPost]
        public String UpdateTableName(Guid DamDocumentId, int iconum, int tableid, int fileid, StringDictionary input)
        {
            try
            {

                string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDoc-SCAR"].ToString();
                sfConnectionString = @"Application Name=DataRoost;Data Source=ffdocumenthistory-prestage-rds-sqlserver-se-standalone.prod.factset.com;Initial Catalog=FFDocumentHistory;User ID=ffdocumenthistory_admin_dev;Password=1tpIDJLT;MultipleActiveResultSets=True;";

                var vsHelper = new VisualStitchingHelper(sfConnectionString);
                if (input.StringData.ContainsKey("iscorrect"))
                {
                    //bool iscorrect = Boolean.Parse(input.StringData["iscorrect"]);
                    //bool iscarboncorrect = Boolean.Parse(input.StringData["iscarboncorrect"]);
                    if (input.StringData["iscorrect"] == null)
                    {
                        input.StringData["iscorrect"] = "null";
                    }
                    if (input.StringData["iscarboncorrect"] == null)
                    {
                        input.StringData["iscarboncorrect"] = "null";
                    }
                    return vsHelper.UpdateTableTitleCorrect(DamDocumentId.ToString(), iconum, tableid, fileid, input.StringData["iscorrect"], input.StringData["iscarboncorrect"]);
                }
                else if (input.StringData.ContainsKey("normtitleid"))
                {
                    try
                    {
                        int newid = Int32.Parse(input.StringData["normtitleid"]);
                        return vsHelper.UpdateTableNormTableId(DamDocumentId.ToString(), iconum, tableid, fileid, newid);
                    }
                    catch (Exception ex)
                    {
                        return vsHelper.UpdateTableNormTableId(DamDocumentId.ToString(), iconum, tableid, fileid, null);
                    }
                }
                else
                {
                    return vsHelper.UpdateTableTitleComments(DamDocumentId.ToString(), iconum, tableid, fileid, input.StringData["comments"]);
                }
            }
            catch (Exception ex)
            {
                LogError(ex);
                return null;
            }
        }

        [Route("name-tree-api/{DamDocumentId}")]
        [HttpGet]
        public HttpResponseMessage GetNameTree(Guid DamDocumentId)
        {
            try
            {

                string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDoc-SCAR"].ToString();
                sfConnectionString = @"Application Name=DataRoost;Data Source=ffdocumenthistory-prestage-rds-sqlserver-se-standalone.prod.factset.com;Initial Catalog=FFDocumentHistory;User ID=ffdocumenthistory_admin_dev;Password=1tpIDJLT;MultipleActiveResultSets=True;";

                var vsHelper = new VisualStitchingHelper(sfConnectionString);
                var json = vsHelper.GetNameTree(DamDocumentId);
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

        //[Route("datatree/{DamDocumentId}/debug")]
        //[HttpGet]
        //public HttpResponseMessage GetDataTreeFake(Guid DamDocumentId)
        //{
        //    try
        //    {

        //        string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
        //        var vsHelper = new VisualStitchingHelper(sfConnectionString);
        //        var json = vsHelper.GetDataTreeFake(DamDocumentId);
        //        return new HttpResponseMessage()
        //        {
        //            Content = new StringContent(json, System.Text.Encoding.UTF8, "application/json")
        //        };
        //    }
        //    catch (Exception ex)
        //    {
        //        LogError(ex);
        //        return null;
        //    }
        //}
        //[Route("kpi/{DamDocumentId}/debug")]
  //      [Route("kpi/debug")]
  //      [HttpGet]
  //      public string PostKpi()
  //      {
  //          try
  //          {
		//		var vsHelper = new VisualStitchingHelper(sfConnectionString);
		//		var json = vsHelper.GetPostGresDataTree();
		//		return new HttpResponseMessage()
		//		{
		//			Content = new StringContent(json, System.Text.Encoding.UTF8, "application/json")
		//		};
		//	} catch (Exception ex) {
		//		LogError(ex);
		//		return null;
		//	}
		//}

		//[Route("datatree/{DamDocumentId}/debug")]
		//[HttpGet]
		//public HttpResponseMessage GetDataTreeFake(Guid DamDocumentId)
		//{
		//    try
		//    {

		//        string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
		//        var vsHelper = new VisualStitchingHelper(sfConnectionString);
		//        var json = vsHelper.GetDataTreeFake(DamDocumentId);
		//        return new HttpResponseMessage()
		//        {
		//            Content = new StringContent(json, System.Text.Encoding.UTF8, "application/json")
		//        };
		//    }
		//    catch (Exception ex)
		//    {
		//        LogError(ex);
		//        return null;
		//    }
		//}
		//[Route("kpi/{DamDocumentId}/debug")]
		[Route("kpi/debug")]
		[HttpGet]
		public string PostKpi() {
			try {

				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDoc-SCAR"].ToString();
				var vsHelper = new VisualStitchingHelper(sfConnectionString);
				Guid DamDocumentId = new Guid();
				var json = vsHelper.InsertKpiFake(DamDocumentId);
				return json;
			} catch (Exception ex) {
				LogError(ex);
				return "false";
			}
		}

		[Route("gdb/debug")]
		[HttpGet]
		public string PostGdbFake() {
			try {

				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDoc-SCAR"].ToString();
				var vsHelper = new VisualStitchingHelper(sfConnectionString);
				Guid DamDocumentId = new Guid();
				var json = vsHelper.InsertGdbFake(DamDocumentId);
				return json;
			} catch (Exception ex) {
				LogError(ex);
				return "false";
			}
		}

		[Route("gdb/{DamDocumentId}/{FileNo}")]
		[HttpGet]
		public string PostGdb(Guid DamDocumentId, int FileNo) {
			try {

				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDoc-SCAR"].ToString();
				var vsHelper = new VisualStitchingHelper(sfConnectionString);
				var json = vsHelper.InsertGdb(DamDocumentId, FileNo);
				return json;
			} catch (Exception ex) {
				LogError(ex);
				return "false";
			}
		}
		[Route("gdb/backfill/on")]
		[HttpGet]
		public string PostGdbCommitBackfillOn() {
			try {

				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDoc-SCAR-backfill"].ToString();
				sfConnectionString = @"Application Name=DataRoostBackfill;Data Source=ffdocumenthistory-prestage-rds-sqlserver-se-standalone.prod.factset.com;Initial Catalog=FFDocumentHistory;User ID=ffdocumenthistory_admin_dev;Password=1tpIDJLT;MultipleActiveResultSets=True;";
				var vsHelper = new VisualStitchingHelper(sfConnectionString);
				var json = vsHelper.GdbBackfillOn();
				return json;
			} catch (Exception ex) {
				LogError(ex);
				return "false";
			}
		}
		[Route("gdb/backfill/off")]
		[HttpGet]
		public string PostGdbCommitBackfillOff() {
			try {

				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDoc-SCAR-backfill"].ToString();
				sfConnectionString = @"Application Name=DataRoostBackfill;Data Source=ffdocumenthistory-prestage-rds-sqlserver-se-standalone.prod.factset.com;Initial Catalog=FFDocumentHistory;User ID=ffdocumenthistory_admin_dev;Password=1tpIDJLT;MultipleActiveResultSets=True;";
				var vsHelper = new VisualStitchingHelper(sfConnectionString);
				var json = vsHelper.GdbBackfillOff();
				return json;
			} catch (Exception ex) {
				LogError(ex);
				return "false";
			}
		}
		[Route("gdb/backfill")]
		[HttpGet]
		public string PostGdbCommitBackfill() {
			try {

				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDoc-SCAR-backfill"].ToString();
				var vsHelper = new VisualStitchingHelper(sfConnectionString);
				var json = vsHelper.GdbBackfill();
				return json;
			} catch (Exception ex) {
				LogError(ex);
				return "false";
			}
		}
		[Route("gdb/backfill/retry")]
		[HttpGet]
		public string PostGdbCommitBackfillRetry() {
			try {

				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDoc-SCAR-backfill"].ToString();
				var vsHelper = new VisualStitchingHelper(sfConnectionString);
				var json = vsHelper.GdbBackfill(1, true);
				return json;
			} catch (Exception ex) {
				LogError(ex);
				return "false";
			}
		}

		[Route("gdb/backfill/{maxThread}")]
		[HttpGet]
		public string PostGdbCommitBackfillThread(int maxThread) {
			try {

				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDoc-SCAR-backfill"].ToString();
				var vsHelper = new VisualStitchingHelper(sfConnectionString);
				var json = vsHelper.GdbBackfill(maxThread);
				return json;
			} catch (Exception ex) {
				LogError(ex);
				return "false";
			}
		}
		[Route("gdb/{DamDocumentId}/{FileNo}/commit")]
		[HttpGet]
		public string PostGdbCommit(Guid DamDocumentId, int FileNo) {
			try {
				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDoc-SCAR"].ToString();
				var vsHelper = new VisualStitchingHelper(sfConnectionString);
				var json = vsHelper.InsertGdbCommit(DamDocumentId, FileNo);
				return json;
			} catch (Exception ex) {
				LogError(ex);
				return "false";
			}
		}

		//    [Route("datatreetest/{DamDocumentId}/{FileNo}")]
		//[HttpGet]
		//public HttpResponseMessage GetDataTreeFileNoTest(Guid DamDocumentId, int FileNo) {
		//	try {

		//		string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
		//		var vsHelper = new VisualStitchingHelper(sfConnectionString);
		//		var json = vsHelper.GetDataTreeTest(DamDocumentId, FileNo);
		//		return new HttpResponseMessage()
		//		{
		//			Content = new StringContent(json, System.Text.Encoding.UTF8, "application/json")
		//		};
		//	} catch (Exception ex) {
		//		LogError(ex);
		//		return null;
		//	}
		//}
		[Route("nametree/{segment}")]
		[HttpGet]
		public HttpResponseMessage GetNameTree(string segment) {
			try {

				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
				var vsHelper = new VisualStitchingHelper(sfConnectionString);
				var json = vsHelper.GetTreeViewJS(segment);
				return new HttpResponseMessage()
				{
					Content = new StringContent(json, System.Text.Encoding.UTF8, "application/json")
				};
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}
		[Route("gdb/{sdbCode}")]
		[HttpGet]
		public HttpResponseMessage GetSDBCode(string sdbCode) {
			try {
				long sdb;
				if (!long.TryParse(sdbCode, out sdb)) {
					throw new Exception("bad SDBCode");
				}
				//long sdb = long.Parse(sdbCode);
				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDoc-SCAR"].ToString();
				var vsHelper = new VisualStitchingHelper(sfConnectionString);
				var json = vsHelper.GetGDBCode(sdb);
				return new HttpResponseMessage()
				{
					Content = new StringContent(Newtonsoft.Json.JsonConvert.SerializeObject(json), System.Text.Encoding.UTF8, "application/json")
				};
			} catch (Exception ex) {
				LogError(ex);
				return new HttpResponseMessage()
				{
					Content = new StringContent("", System.Text.Encoding.UTF8, "application/json")
				};
			}
		}
		[Route("gdb/{sdbCode}/grid")]
		[HttpGet]
		public HttpResponseMessage GetSDBCodeGrid(string sdbCode) {
			try {
				long sdb;
				if (!long.TryParse(sdbCode, out sdb)) {
					throw new Exception("bad SDBCode");
				}
				//long sdb = long.Parse(sdbCode);
				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDoc-SCAR"].ToString();
				var vsHelper = new VisualStitchingHelper(sfConnectionString);
				var json = vsHelper.GetGDBCodeGrid(sdb);
				return new HttpResponseMessage()
				{
					Content = new StringContent(Newtonsoft.Json.JsonConvert.SerializeObject(json), System.Text.Encoding.UTF8, "application/json")
				};
			} catch (Exception ex) {
				LogError(ex);
				return new HttpResponseMessage()
				{
					Content = new StringContent(ex.Message.ToString(), System.Text.Encoding.UTF8, "application/json")
				};
			}
		}
		[Route("gdbcode/{sdbCode}/{iconum}")]
		[HttpGet]
		public HttpResponseMessage GetGDBCodeGridForIconum(string sdbCode, int iconum) {
			try {
				long sdb;
				if (!long.TryParse(sdbCode, out sdb)) {
					throw new Exception("bad SDBCode");
				}
				//long sdb = long.Parse(sdbCode);
				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDoc-SCAR"].ToString();
				var vsHelper = new VisualStitchingHelper(sfConnectionString);
				var json = vsHelper.GetGDBCodeGridForIconum(sdb, iconum);
				return new HttpResponseMessage()
				{
					Content = new StringContent(Newtonsoft.Json.JsonConvert.SerializeObject(json), System.Text.Encoding.UTF8, "application/json")
				};
			} catch (Exception ex) {
				LogError(ex);
				return new HttpResponseMessage()
				{
					Content = new StringContent(ex.Message.ToString(), System.Text.Encoding.UTF8, "application/json")
				};
			}
		}

		[Route("gdbcode/{sdbCode}/{iconum}/{docId}")]
		[HttpGet]
		public HttpResponseMessage GetGDBCodeGridForIconum2(string sdbCode, int iconum, Guid? docID) {
			try {
				long sdb;
				if (!long.TryParse(sdbCode, out sdb)) {
					throw new Exception("bad SDBCode");
				}
				//long sdb = long.Parse(sdbCode);
				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDoc-SCAR"].ToString();
				var vsHelper = new VisualStitchingHelper(sfConnectionString);
				var json = vsHelper.GetGDBCodeGridForIconum(sdb, iconum, docID);
				return new HttpResponseMessage()
				{
					Content = new StringContent(Newtonsoft.Json.JsonConvert.SerializeObject(json), System.Text.Encoding.UTF8, "application/json")
				};
			} catch (Exception ex) {
				LogError(ex);
				return new HttpResponseMessage()
				{
					Content = new StringContent(ex.Message.ToString(), System.Text.Encoding.UTF8, "application/json")
				};
			}
		}


		[Route("gdbcount/{sdbCode}")]
		[HttpGet]
		public HttpResponseMessage GetGDBCountForIconum2(string sdbCode) {
			return GetGDBCountForIconum(sdbCode, null);
		}

		[Route("gdbcount/{sdbCode}/{iconum}")]
		[HttpGet]
		public HttpResponseMessage GetGDBCountForIconum(string sdbCode, int? iconum) {
			try {
				long sdb;
				if (!long.TryParse(sdbCode, out sdb)) {
					throw new Exception("bad SDBCode");
				}
				//long sdb = long.Parse(sdbCode);
				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDoc-SCAR"].ToString();
				var vsHelper = new VisualStitchingHelper(sfConnectionString);
				var json = vsHelper.GetGDBCountForIconum(sdb, iconum);
				return new HttpResponseMessage()
				{
					Content = new StringContent(Newtonsoft.Json.JsonConvert.SerializeObject(json), System.Text.Encoding.UTF8, "application/json")
				};
			} catch (Exception ex) {
				LogError(ex);
				return new HttpResponseMessage()
				{
					Content = new StringContent(ex.Message.ToString(), System.Text.Encoding.UTF8, "application/json")
				};
			}
		}
        [Route("cluster/debug/{level}")]
        [HttpGet]
        public int ClusterDebugLevel(int level)
        {
            try
            {
                string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDoc-SCAR"].ToString();
                var vsHelper = new VisualStitchingHelper(sfConnectionString);
                var result = vsHelper.DebugLevel(level);
                return result;
            }
            catch (Exception ex)
            {
                LogError(ex);
                return -1;
            }
        }
        [Route("cluster/extend/{iconum}")]
        [HttpGet]
        public HttpResponseMessage ClusterTreeExtendPut(int iconum)
        {
            try
            {
                if (iconum <= 0)
                    throw new NotImplementedException();

                string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDoc-SCAR"].ToString();
                var vsHelper = new VisualStitchingHelper(sfConnectionString);
                var json = vsHelper.ExtendClusterByIconum(1, iconum);
                return new HttpResponseMessage()
                {
                    Content = new StringContent(Newtonsoft.Json.JsonConvert.SerializeObject(json), System.Text.Encoding.UTF8, "application/json")
                };
            }
            catch (Exception ex)
            {
                LogError(ex);
                return new HttpResponseMessage()
                {
                    Content = new StringContent(ex.Message.ToString(), System.Text.Encoding.UTF8, "application/json")
                };
            }
        }


        [Route("cluster/extend/{iconum}/{docId}")]
        [HttpGet]
        public HttpResponseMessage ClusterTreeExtendPut2(int iconum, Guid docId)
        {
            try
            {
                if (iconum <= 0)
                    throw new NotImplementedException();

                string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDoc-SCAR"].ToString();
                var vsHelper = new VisualStitchingHelper(sfConnectionString);
                int contentSetId = vsHelper.IconumToContentSet(iconum);
                var success = vsHelper.ExtendClusterByDocument(contentSetId, iconum, docId);
                if (success)
                {
                    return new HttpResponseMessage()
                    {
                        Content = new StringContent(Newtonsoft.Json.JsonConvert.SerializeObject(success), System.Text.Encoding.UTF8, "application/json")
                    };
                }
                else
                {
                    return new HttpResponseMessage(HttpStatusCode.Accepted)
                    {
                    };
                }
            }
            catch (Exception ex)
            {
                VisualStitchingHelper.WriteLogToDatabase(VisualStitchingHelper.PGConnectionString(), docId, iconum, -1, -1, -1, ex.ToString());
                LogErrorAutoCluster(ex);
                return new HttpResponseMessage(HttpStatusCode.Accepted)
                {
                    Content = new StringContent(ex.Message.ToString(), System.Text.Encoding.UTF8, "application/json")
                };
            }
        }

        [Route("cluster/extend/{iconum}/{docId}/{tableid}")]
        [HttpGet]
        public HttpResponseMessage ClusterTreeExtendDocIdTableId(int iconum, Guid docId, int tableid)
        {
            try
            {
                if (iconum <= 0)
                    throw new NotImplementedException();

                string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDoc-SCAR"].ToString();
                var vsHelper = new VisualStitchingHelper(sfConnectionString);
                int contentSetId = vsHelper.IconumToContentSet(iconum);
                var json = vsHelper.ExtendClusterByDocument(contentSetId, iconum, docId, tableid);
                return new HttpResponseMessage()
                {
                    Content = new StringContent(Newtonsoft.Json.JsonConvert.SerializeObject(json), System.Text.Encoding.UTF8, "application/json")
                };
            }
            catch (Exception ex)
            {
                VisualStitchingHelper.WriteLogToDatabase(VisualStitchingHelper.PGConnectionString(), docId, iconum, tableid, -1, -1, ex.ToString());
                LogErrorAutoCluster(ex);
                return new HttpResponseMessage()
                {
                    Content = new StringContent(ex.Message.ToString(), System.Text.Encoding.UTF8, "application/json")
                };
            }
        }

        [Route("cluster/extend/{contentSetId}/{iconum}/{docId}/{tableid}")]
        [HttpGet]
        public HttpResponseMessage ClusterTreeExtendIndustryDocIdTableId(int contentSetId, int iconum, Guid docId, int tableid)
        {
            try
            {
                if (iconum <= 0)
                    throw new NotImplementedException();

                string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDoc-SCAR"].ToString();
                var vsHelper = new VisualStitchingHelper(sfConnectionString);
                var json = vsHelper.ExtendClusterByDocument(contentSetId, iconum, docId, tableid);
                return new HttpResponseMessage()
                {
                    Content = new StringContent(Newtonsoft.Json.JsonConvert.SerializeObject(json), System.Text.Encoding.UTF8, "application/json")
                };
            }
            catch (Exception ex)
            {
                VisualStitchingHelper.WriteLogToDatabase(VisualStitchingHelper.PGConnectionString(), docId, iconum, tableid, -1, -1, ex.ToString());
                LogErrorAutoCluster(ex);
                return new HttpResponseMessage()
                {
                    Content = new StringContent(ex.Message.ToString(), System.Text.Encoding.UTF8, "application/json")
                };
            }
        }

        #region DEV
        [Route("cluster/extend/{iconum}/dev")]
        [HttpGet]
        public HttpResponseMessage ClusterTreeExtendIconumDev(int iconum)
        {
            try
            {
                if (iconum <= 0)
                    throw new NotImplementedException();

                string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDoc-SCAR"].ToString();
                var vsHelper = new VisualStitchingHelper(sfConnectionString, VisualStitchingHelper.PGDevConnectionString());
                var json = vsHelper.ExtendClusterByIconumDev(iconum);
                return new HttpResponseMessage()
                {
                    Content = new StringContent(Newtonsoft.Json.JsonConvert.SerializeObject(json), System.Text.Encoding.UTF8, "application/json")
                };
            }
            catch (Exception ex)
            {
                LogError(ex);
                return new HttpResponseMessage()
                {
                    Content = new StringContent(ex.Message.ToString(), System.Text.Encoding.UTF8, "application/json")
                };
            }
        }

        [Route("cluster/extend/{iconum}/{docId}/dev")]
        [HttpGet]
        public HttpResponseMessage ClusterTreeExtendIconumDocIdDev(int iconum, Guid docId)
        {
            try
            {
                if (iconum <= 0)
                    throw new NotImplementedException();

                string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDoc-SCAR"].ToString();
                var vsHelper = new VisualStitchingHelper(sfConnectionString, VisualStitchingHelper.PGDevConnectionString());
                var success = vsHelper.ExtendClusterByDocumentDev(iconum, docId);
                if (success)
                {
                    return new HttpResponseMessage()
                    {
                        Content = new StringContent(Newtonsoft.Json.JsonConvert.SerializeObject(success), System.Text.Encoding.UTF8, "application/json")
                    };
                }
                else
                {
                    return new HttpResponseMessage(HttpStatusCode.Accepted)
                    {
                        Content = new StringContent(Newtonsoft.Json.JsonConvert.SerializeObject(success), System.Text.Encoding.UTF8, "application/json")
                    };
                }
            }
            catch (Exception ex)
            {
                LogError(ex);
                return new HttpResponseMessage(HttpStatusCode.Accepted)
                {
                    Content = new StringContent(ex.Message.ToString(), System.Text.Encoding.UTF8, "application/json")
                };
            }
        }
        [Route("cluster/extend/{iconum}/{docId}/{tableid}/dev")]
        [HttpGet]
        public HttpResponseMessage ClusterTreeExtendIconumDocIdTableIdDev(int iconum, Guid docId, int tableid)
        {
            try
            {
                if (iconum <= 0)
                    throw new NotImplementedException();

                string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDoc-SCAR"].ToString();
                var vsHelper = new VisualStitchingHelper(sfConnectionString, VisualStitchingHelper.PGDevConnectionString());
                var json = vsHelper.ExtendClusterByDocumentDev(iconum, docId, tableid);
                return new HttpResponseMessage()
                {
                    Content = new StringContent(Newtonsoft.Json.JsonConvert.SerializeObject(json), System.Text.Encoding.UTF8, "application/json")
                };
            }
            catch (Exception ex)
            {
                LogError(ex);
                return new HttpResponseMessage()
                {
                    Content = new StringContent(ex.Message.ToString(), System.Text.Encoding.UTF8, "application/json")
                };
            }
        }
        #endregion  
        [Route("cluster/errorlog")]
        [HttpGet]
        public List<VisualStitching.Common.Models.ClusterError> GetClusterErrorLog()
        {
            try
            {

                string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDoc-SCAR"].ToString();
                var vsHelper = new VisualStitchingHelper(sfConnectionString);
                var json = vsHelper.ReadLogFromDatabase();
                return json;
            }
            catch (Exception ex)
            {
                LogError(ex);
            }
            return new List<VisualStitching.Common.Models.ClusterError>();
        }
        public class StitchInput {
			public int TargetStaticHierarchyID { get; set; }
			public List<int> StitchingStaticHierarchyIDs { get; set; }
		}

        public class UnStitchInput
        {
            public int TargetStaticHierarchyID { get; set; }
            public List<int> DocumentTimeSliceIDs { get; set; }
        }

        public class ScarInput
        {
            public int TargetStaticHierarchyID { get; set; }
            public List<int> StitchingStaticHierarchyIDs { get; set; }
        }

        public class ScarStringListInput
        {
            public string StringData { get; set; }
            public List<int> StaticHierarchyIDs { get; set; }
        }


        public class StringInput
        {
            public string StringData { get; set; }
        }
        public class StringDictionary
        {
            public Dictionary<string, string> StringData { get; set; }
        }
    }
}