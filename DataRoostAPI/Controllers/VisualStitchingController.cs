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
				//message.To.Add(vsaxena);
				//message.To.Add(adam);
				//message.To.Add(leo);
				//message.To.Add(rohan);
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
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}
		[Route("datatree/v3/{DamDocumentId}/{FileNo}")]
		[HttpGet]
		public HttpResponseMessage GetDataTreeFileNoV3(Guid DamDocumentId, int FileNo) {
			try {

				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDoc-SCAR"].ToString();
				sfConnectionString = @"Application Name=DataRoost;Data Source=ffdocumenthistory-prestage-rds-sqlserver-se-standalone.prod.factset.com;Initial Catalog=FFDocumentHistory;User ID=ffdocumenthistory_admin_dev;Password=1tpIDJLT;MultipleActiveResultSets=True;";

				var vsHelper = new VisualStitchingHelper(sfConnectionString);
				var json = vsHelper.GetPostGresDataTree3();
				return new HttpResponseMessage()
				{
					Content = new StringContent(json, System.Text.Encoding.UTF8, "application/json")
				};
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}

		[Route("datatree/v3/profile/")]
		[HttpGet]
		public HttpResponseMessage GetDataTreeProfile() {
			try {

				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDoc-SCAR"].ToString();
				sfConnectionString = @"Application Name=DataRoost;Data Source=ffdocumenthistory-prestage-rds-sqlserver-se-standalone.prod.factset.com;Initial Catalog=FFDocumentHistory;User ID=ffdocumenthistory_admin_dev;Password=1tpIDJLT;MultipleActiveResultSets=True;";

				var vsHelper = new VisualStitchingHelper(sfConnectionString);
				var json = vsHelper.GetPostGresDataTreeProfile();
				return new HttpResponseMessage()
				{
					Content = new StringContent(json, System.Text.Encoding.UTF8, "application/json")
				};
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}

		[Route("datatree/v3/profile/{name}")]
		[HttpPost]
		public HttpResponseMessage SaveDataTreeProfile(String name, StringInput input) {
			try {

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
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}

		[Route("name-tree-api/offsets/{DamDocumentId}/{fileid}")]
		[HttpGet]
		public HttpResponseMessage GetDocumentOffsets(Guid DamDocumentId, int fileid) {
			try {

				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDoc-SCAR"].ToString();
				sfConnectionString = @"Application Name=DataRoost;Data Source=ffdocumenthistory-prestage-rds-sqlserver-se-standalone.prod.factset.com;Initial Catalog=FFDocumentHistory;User ID=ffdocumenthistory_admin_dev;Password=1tpIDJLT;MultipleActiveResultSets=True;";

				var vsHelper = new VisualStitchingHelper(sfConnectionString);
				var json = vsHelper.GetDocumentOffsets(DamDocumentId.ToString(), fileid);
				return new HttpResponseMessage()
				{
					Content = new StringContent(json, System.Text.Encoding.UTF8, "application/json")
				};
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}

		[Route("name-tree-api/normtables/")]
		[HttpGet]
		public HttpResponseMessage GetDocumentTrees() {
			try {

				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDoc-SCAR"].ToString();
				sfConnectionString = @"Application Name=DataRoost;Data Source=ffdocumenthistory-prestage-rds-sqlserver-se-standalone.prod.factset.com;Initial Catalog=FFDocumentHistory;User ID=ffdocumenthistory_admin_dev;Password=1tpIDJLT;MultipleActiveResultSets=True;";

				var vsHelper = new VisualStitchingHelper(sfConnectionString);
				var json = vsHelper.GetNameTreesNormTable();
				return new HttpResponseMessage()
				{
					Content = new StringContent(json, System.Text.Encoding.UTF8, "application/json")
				};
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}

		[Route("name-tree-api/trees/{DamDocumentId}/{fileid}")]
		[HttpGet]
		public HttpResponseMessage GetDocumentTrees(Guid DamDocumentId, int fileid) {
			try {

				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDoc-SCAR"].ToString();
				sfConnectionString = @"Application Name=DataRoost;Data Source=ffdocumenthistory-prestage-rds-sqlserver-se-standalone.prod.factset.com;Initial Catalog=FFDocumentHistory;User ID=ffdocumenthistory_admin_dev;Password=1tpIDJLT;MultipleActiveResultSets=True;";

				var vsHelper = new VisualStitchingHelper(sfConnectionString);
				var json = vsHelper.GetNameTreesTableByDamid(DamDocumentId.ToString(), fileid);
				return new HttpResponseMessage()
				{
					Content = new StringContent(json, System.Text.Encoding.UTF8, "application/json")
				};
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}

		[Route("name-tree-api/{DamDocumentId}/{iconum}/{tableid}/{fileid}")]
		[HttpPost]
		public String UpdateTableName(Guid DamDocumentId, int iconum, int tableid, int fileid, StringDictionary input) {
			try {

				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDoc-SCAR"].ToString();
				sfConnectionString = @"Application Name=DataRoost;Data Source=ffdocumenthistory-prestage-rds-sqlserver-se-standalone.prod.factset.com;Initial Catalog=FFDocumentHistory;User ID=ffdocumenthistory_admin_dev;Password=1tpIDJLT;MultipleActiveResultSets=True;";

				var vsHelper = new VisualStitchingHelper(sfConnectionString);
				if (input.StringData.ContainsKey("iscorrect")) {
					bool iscorrect = Boolean.Parse(input.StringData["iscorrect"]);
					bool iscarboncorrect = Boolean.Parse(input.StringData["iscarboncorrect"]);
					return vsHelper.UpdateTableTitleCorrect(DamDocumentId.ToString(), iconum, tableid, fileid, iscorrect, iscarboncorrect);
				} 
				else if (input.StringData.ContainsKey("normtitleid")) {
					try {
						int newid = Int32.Parse(input.StringData["normtitleid"]);
						return vsHelper.UpdateTableNormTableId(DamDocumentId.ToString(), iconum, tableid, fileid, newid);
					} catch (Exception ex) {
						return vsHelper.UpdateTableNormTableId(DamDocumentId.ToString(), iconum, tableid, fileid, null);
					}
				} else {
					return vsHelper.UpdateTableTitleComments(DamDocumentId.ToString(), iconum, tableid, fileid, input.StringData["comments"]);
				}
			} catch (Exception ex) {
				LogError(ex);
				return null;
			}
		}

		[Route("name-tree-api/{DamDocumentId}")]
		[HttpGet]
		public HttpResponseMessage GetNameTree(Guid DamDocumentId) {
			try {

				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDoc-SCAR"].ToString();
				sfConnectionString = @"Application Name=DataRoost;Data Source=ffdocumenthistory-prestage-rds-sqlserver-se-standalone.prod.factset.com;Initial Catalog=FFDocumentHistory;User ID=ffdocumenthistory_admin_dev;Password=1tpIDJLT;MultipleActiveResultSets=True;";

				var vsHelper = new VisualStitchingHelper(sfConnectionString);
				var json = vsHelper.GetNameTree(DamDocumentId);
				return new HttpResponseMessage()
				{
					Content = new StringContent(json, System.Text.Encoding.UTF8, "application/json")
				};
			} catch (Exception ex) {
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