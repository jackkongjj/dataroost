using System;
using System.IO;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using Npgsql;
using NpgsqlTypes;
using DataRoostAPI.Common.Models.AsReported;
using System.Net;
using Newtonsoft.Json;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FactSet.Data.SqlClient;
using System.Text.RegularExpressions;
using CCS.Fundamentals.DataRoostAPI.Helpers;
using System.Net.Mail;

namespace CCS.Fundamentals.DataRoostAPI.Access.AsReported {
	public class VisualStitchingHelper {
        public static void SendEmail(string subject, string emailBody)
        {
            try
            {
                SmtpClient mySMTP = new SmtpClient("mail.factset.com");
                MailAddress mailFrom = new MailAddress("myself@factset.com", "IMA DataRoost VisualStitching");
                MailMessage message = new MailMessage();
                message.From = mailFrom;
                var ljiang = new MailAddress("ljiang@factset.com", "Lun Jiang");
                message.To.Add(ljiang);
                message.Subject = subject + " from " + Environment.MachineName;
                message.DeliveryNotificationOptions = DeliveryNotificationOptions.OnFailure;
                message.Body = emailBody;
                message.IsBodyHtml = true;
                mySMTP.Send(message);
            }
            catch { }
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
        private StringBuilder _levelOneLogger = new StringBuilder();
        private StringBuilder _levelTwoLogger = new StringBuilder();
        private StringBuilder _failureLogger = new StringBuilder();
        private bool _autoclusteringfailure = false;
        private readonly string _sfConnectionString;
        private readonly string _pgConnectionString;
        private string _environment = "STAGING";
        static int DebugLogLevel = 0;
		static VisualStitchingHelper() {
            DebugLogLevel = 0;
		}

        public VisualStitchingHelper(string sfConnectionString)
        {
            this._sfConnectionString = sfConnectionString;
            this._pgConnectionString = PGConnectionString();
        }

        public VisualStitchingHelper(string sfConnectionString, string pgConnectionString) {
			this._sfConnectionString = sfConnectionString;
            this._pgConnectionString = pgConnectionString;
		}

        public int DebugLevel(int newLevel)
        {
            DebugLogLevel = newLevel;
            return DebugLogLevel;
        }

		private string factsetIOconnString = "Host=ip-172-31-81-210.manager.factset.io;Port=32791;Username=uyQKYrcTSrnnqB;Password=NoCLf_xBeXiB0UXZjhZUNg7Zx8;Database=di8UFb70sJdA5e;sslmode=Require;Trust Server Certificate=true;";
		private string connString = "Host=ffautomation-dev-postgres.c8vzac0v5wdo.us-east-1.rds.amazonaws.com;Port=5432;Username=ffautomation_writer_user;Password=qyp0nMeA;Database=postgres;"; // sslmode=Require;Trust Server Certificate=true;

		public string GetJson(int id) {
			System.Text.StringBuilder sb = new System.Text.StringBuilder();
			using (
					var conn = new NpgsqlConnection(connString)) {
				conn.Open();
				// Retrieve all rows
				using (var cmd = new NpgsqlCommand("SELECT value FROM json where id = @id LIMIT 1", conn)) {
					cmd.Parameters.AddWithValue("id", id);
					using (var reader = cmd.ExecuteReader()) {
						while (reader.Read())
							sb.Append(reader.GetString(0));
					}
				}
			}
			return sb.ToString();
		}
		public string GetJsonByHash(string hashkey) {
			System.Text.StringBuilder sb = new System.Text.StringBuilder();
			using (
					var conn = new NpgsqlConnection(connString)) {
				conn.Open();
				// Retrieve all rows
				using (var cmd = new NpgsqlCommand("SELECT value FROM json where hashkey = @hashkey LIMIT 1", conn)) {
					cmd.Parameters.AddWithValue("@hashkey", hashkey);
					using (var reader = cmd.ExecuteReader()) {
						while (reader.Read())
							sb.Append(reader.GetString(0));
					}
				}
			}
			return sb.ToString();
		}
		public int SetJsonByHash(string hashkey, string value) {
			string query = @"
UPDATE json SET value=@value WHERE hashkey=@hashkey;
INSERT INTO json (value, hashkey)
       SELECT @value, @hashkey
       WHERE NOT EXISTS (SELECT 1 FROM json WHERE  hashkey=@hashkey);

SELECT coalesce(id, -1) FROM json where hashkey = @hashkey LIMIT 1;

";
			int result = 0;
			try {
				using (
						var conn = new NpgsqlConnection(connString)) {
					conn.Open();
					// Retrieve all rows
					using (var cmd = new NpgsqlCommand(query, conn)) {
						cmd.Parameters.AddWithValue("@hashkey", hashkey);
						cmd.Parameters.AddWithValue("@value", value);
						using (var reader = cmd.ExecuteReader()) {
							while (reader.Read())
								result = reader.GetInt32(0);
						}
					}
				}
			} catch (Exception ex) {
				result = -1;
			}
			return result;
		}
		public class TintInfo {

			[JsonProperty("tables")]
			public List<Table> Tables { get; set; }
			[JsonProperty("timeslices")]
			public List<TimeSlice> TimeSlices { get; set; }
		}
		public class Table {
			[JsonProperty("id")]
			public int Id { get; set; }
			[JsonProperty("type")]
			public string Type { get; set; }
			[JsonProperty("xbrlTableTitle")]
			public string XbrlTableTitle { get; set; }
			[JsonProperty("unit")]
			public string Unit { get; set; }
			[JsonProperty("currency")]
			public string Currency { get; set; }
			[JsonProperty("cells")]
			public List<Cell> Cells { get; set; }
			[JsonProperty("rows")]
			public List<Row> Rows { get; set; }
			[JsonProperty("cols")]
			public List<Column> Columns { get; set; }
			[JsonProperty("values")]
			public List<Value> Values { get; set; }
		}
		public class TimeSlice {
			public int FakeID { get; set; }
			[JsonProperty("CompanyFiscalYear")]
			public int CompanyFiscalYear { get; set; }
			[JsonProperty("PeriodType")]
			public string PeriodType { get; set; }
			[JsonProperty("PeriodTypeId")]
			public string PeriodTypeId { get; set; }
			[JsonProperty("Duration")]
			public int Duration { get; set; }
			[JsonProperty("TimeSlicePeriodEndDate")]
			public DateTime TimeSlicePeriodEndDate { get; set; }
			[JsonProperty("ReportingPeriodEndDate")]
			public DateTime ReportingPeriodEndDate { get; set; }
			[JsonProperty("ReportType")]
			public string ReportType { get; set; }
			[JsonProperty("IsRecap")]
			public bool IsRecap { get; set; }
			[JsonProperty("Offsets")]
			public List<string> Offsets { get; set; }
		}
		public class Cell {
			[JsonProperty("rowId")]
			public int rowId { get; set; }
			[JsonProperty("columnId")]
			public int columnId { get; set; }
			[JsonProperty("offset")]
			public string offset { get; set; }

		}
		public class Row {
			[JsonProperty("rowId")]
			public int Id { get; set; }
			[JsonProperty("label")]
			public string Label { get; set; }
			[JsonProperty("labelHierarhcy")]
			public List<string> LabelHierarchy { get; set; }

		}
		public class Column {
			[JsonProperty("columnId")]
			public int Id { get; set; }
			[JsonProperty("columnHeader")]
			public string columnHeader { get; set; }

		}
		public class Value {
			[JsonProperty("offset")]
			public string Offset { get; set; }
			[JsonProperty("value")]
			public string OriginalValue { get; set; }
			[JsonProperty("numericValue")]
			public string NumericValue { get; set; }
			[JsonProperty("scaling")]
			public string Scaling { get; set; }
			[JsonProperty("date")]
			public string Date { get; set; }
			[JsonProperty("unit")]
			public string Unit { get; set; }
			[JsonProperty("xbrlTag")]
			public string XbrlTag { get; set; }

		}
		public class Node {
			[JsonProperty("id")]
			public int Id { get; set; }
			[JsonProperty("title")]
			public string Title { get; set; }
			[JsonIgnore]
			public int? ParentId { get; set; }

			[JsonProperty("nodes")]
			public List<Node> Nodes { get; set; }
			[JsonIgnore]
			public List<Tuple<string, string, string, Guid, string>> DocumentTuples { get; set; }
			[JsonProperty("documents")]
			public List<string> Documents { get; set; }
			[JsonProperty("comment")]
			public string Comment { get; set; }
			[JsonIgnore]
			public string Childrentitle { get; set; }
		}

		public class ClusterNameTreeNode {
			[JsonProperty("documentid")]
			public string documentid { get; set; }
			[JsonProperty("hiearachyid")]
			public int Hiearachyid { get; set; }
			[JsonProperty("iconum")]
			public int iconum { get; set; }
			[JsonProperty("id")]
			public long id { get; set; }
			[JsonProperty("industry")]
			public string Industry { get; set; }
			[JsonProperty("nodes")]
			public List<ClusterNameTreeNode> Nodes { get; set; }
			[JsonProperty("normtableid")]
			public int Normtableid { get; set; }
			[JsonProperty("normtitle")]
			public string NormtableTitle { get; set; }
			[JsonProperty("numericvalue")]
			public string numericvalue { get; set; }
			[JsonProperty("offset")]
			public string offset { get; set; }
			[JsonProperty("parentid")]
			public int? ParentID { get; set; }
			[JsonProperty("presentationid")]
			public int Presentationid { get; set; }
			[JsonProperty("role")]
			public string Role { get; set; }
			[JsonProperty("title")]
			public string Title { get; set; }
			[JsonProperty("value")]
			public string value { get; set; }
		}

		public class TableOffSetNode {
			[JsonProperty("documentid")]
			public string DocumentID { get; set; }
			[JsonProperty("title")]
			public string Title { get; set; }
			[JsonProperty("tableid")]
			public int TableID { get; set; }
			[JsonProperty("fileid")]
			public int FileID { get; set; }
			[JsonProperty("offset")]
			public string offset { get; set; }
			[JsonProperty("comments")]
			public string comments { get; set; }
			[JsonProperty("iscorrect")]
			public bool iscorrect { get; set; }
			[JsonProperty("iscarboncorrect")]
			public bool iscarboncorrect { get; set; }
			[JsonProperty("normtitle")]
			public string normtitle { get; set; }
			[JsonProperty("normtitleid")]
			public int? normtitleid { get; set; }
		}

		public class NameTreeTableNode {
			[JsonProperty("id")]
			public long id { get; set; }
			[JsonProperty("documentid")]
			public string DocumentID { get; set; }
			[JsonProperty("title")]
			public string Title { get; set; }
			[JsonProperty("tableid")]
			public int TableID { get; set; }
			[JsonProperty("fileid")]
			public int FileID { get; set; }
			[JsonProperty("indent")]
			public int indent { get; set; }
			[JsonProperty("iconum")]
			public int iconum { get; set; }
			[JsonProperty("isheader")]
			public Boolean isheader { get; set; }
			[JsonProperty("normtitle")]
			public string NormTitle { get; set; }
			[JsonProperty("cleanedrowlabel")]
			public string CleanedRowLabel { get; set; }
			[JsonProperty("offset")]
			public string offset { get; set; }
			[JsonProperty("adjustedrowid")]
			public int adjustedrowid { get; set; }
			[JsonProperty("nodes")]
			public List<NameTreeTableNode> Nodes { get; set; }
			[JsonProperty("info")]
			public TableOffSetNode info { get; set; }

			public String toString() {
				return "id:" + this.id + " title:" + this.Title + " tableid:" + this.TableID + " fileid:" + this.FileID + " indent:" + this.indent + " NormTitle:" + this.NormTitle + " offset:" + this.offset;
			}
		}


		public class NormTable {
			[JsonProperty("normtitleid")]
			public int normtitleid { get; set; }
			[JsonProperty("normtitle")]
			public string normtitle { get; set; }
		}

		public class Profile {
			[JsonProperty("name")]
			public string Name { get; set; }
			[JsonProperty("json")]
			public string Json { get; set; }
		}

		public class NameTreeNode {
			[JsonProperty("id")]
			public long Id { get; set; }
			[JsonProperty("clustered_id")]
			public long? ClusteredId { get; set; }
			[JsonProperty("as_reported_label")]
			public string AsReportedNodeLabel { get; set; }
			[JsonProperty("as_reported_value")]
			public string AsReportedValue { get; set; }
			[JsonProperty("as_reported_numeric_value")]
			public decimal? AsReportedNumericValue { get; set; }
			[JsonProperty("clustered_label")]
			public string ClusteredNodeLabel { get; set; }
			[JsonProperty("clustered_parent_id")]
			public long? ClusteredParentId { get; set; }

			[JsonIgnore]
			public List<NameTreeNode> Nodes { get; set; }
			[JsonProperty("hash_id")]
			public string HashId { get; set; }
			[JsonProperty("offset")]
			public string Offset { get; set; }
			[JsonProperty("document_id")]
			public Guid DocumentID { get; set; }
			[JsonProperty("iconum")]
			public int? Iconum { get; set; }
			[JsonProperty("cleaned_row_label")]
			public string CleanedRowLabel { get; set; }
			[JsonProperty("cleaned_column_label")]
			public string CleanedColumnLabel { get; set; }
			[JsonProperty("final_label")]
			public string FinalLabel { get; set; }
			[JsonProperty("context")]
			public string Context { get; set; }
			[JsonProperty("cell_date")]
			public DateTime? CellDate { get; set; }
			[JsonProperty("period_length")]
			public int? PeriodLength { get; set; }
			[JsonProperty("period_type")]
			public string PeriodType { get; set; }
			[JsonProperty("interim_type")]
			public string InterimType { get; set; }
			[JsonProperty("scaling")]
			public string Scaling { get; set; }
			[JsonProperty("currency")]
			public string Currency { get; set; }
			[JsonProperty("numeric_value")]
			public string NumericValue { get; set; }
			[JsonProperty("norm_table_id")]
			public int? NormTableId { get; set; }
			[JsonProperty("norm_table_description")]
			public string NormTableDescription { get; set; }
			[JsonProperty("raw_row_id")]
			public int? RawRowId { get; set; }
			[JsonProperty("adjusted_row_id")]
			public int? AdjustedRowId { get; set; }
			[JsonProperty("raw_col_id")]
			public int? RawColId { get; set; }
			[JsonProperty("raw_table_id")]
			public int? RawTableId { get; set; }
		}

		public class ReactNode {
			[JsonProperty("id")]
			public int Id { get; set; }
			[JsonProperty("title")]
			public string Title { get; set; }
			[JsonProperty("children")]
			public List<ReactNode> Nodes { get; set; }
		}


		private string GetTintFile(string url) {
			HttpWebRequest request = (HttpWebRequest)WebRequest.Create(url);
			request.ContentType = "application/json";
			request.Timeout = 120000;
			request.Method = "GET";
			HttpWebResponse response = null;
			try {
				response = (HttpWebResponse)request.GetResponse();
			} catch {
				throw new FileNotFoundException("call failed");
			}
			string outputresult = null;
			if (response.StatusCode == HttpStatusCode.OK) {
				using (var streamReader = new StreamReader(response.GetResponseStream())) {
					outputresult = streamReader.ReadToEnd();
				}
			} else if (response.StatusCode == HttpStatusCode.Accepted || response.StatusCode == HttpStatusCode.ServiceUnavailable) {
				throw new Exception("call failed");
			} else {
				throw new FileNotFoundException("call failed");
			}
			return outputresult;

		}

		private List<ReactNode> GetAngularTreeTest(TintInfo result) {
			List<ReactNode> nodes = new List<ReactNode>();
			string[] big3Table = { "BS", "IS", "CF" };
			foreach (var table in result.Tables) {
				if (!big3Table.Contains(table.Type.ToUpper()))
					continue;
				ReactNode t = new ReactNode();
				nodes.Add(t);
				t.Id = table.Id;
				t.Title = table.Type;
				t.Nodes = new List<ReactNode>();
				Stack<ReactNode> stack = new Stack<ReactNode>();
				stack.Push(t);

				foreach (var row in table.Rows) {
					int i = 0;
					foreach (var labelAtlevel in row.LabelHierarchy) {
						i++;
						if (stack.Count <= i) {
							break;
						}
						if (stack.ElementAt(stack.Count - i - 1).Title != labelAtlevel) {
							while (stack.Count > i && stack.Count > 1) {
								stack.Pop();
							}
						}
					}
					var lastRoot = stack.Peek();
					var endLabel = row.LabelHierarchy.Last();
					i = 0;
					int j = 0; // insert
					foreach (var labelAtlevel in row.LabelHierarchy) {
						i++;
						if (stack.Count > i) {
							continue;
						}
						if (stack.Peek().Title != labelAtlevel && labelAtlevel != endLabel) {
							ReactNode r = new ReactNode();
							r.Id = -1;
							r.Title = labelAtlevel;
							r.Nodes = new List<ReactNode>();
							lastRoot.Nodes.Add(r);
							lastRoot = r;
							stack.Push(r);
						} else {
							ReactNode r = new ReactNode();
							r.Id = row.Id;
							r.Title = endLabel;
							r.Nodes = new List<ReactNode>();
							lastRoot.Nodes.Add(r);
						}
					}

				}
			}
			return nodes;
		}

		public string GetDataTree() {

			int tries = 1;
			List<Node> nodes = new List<Node>();

			while (tries > 0) {
				try {
					nodes = GetAngularTree();
					tries = 0;
				} catch (Exception ex) {
					if (--tries > 0) {
						System.Threading.Thread.Sleep(1000);
					} else {
						return JsonConvert.SerializeObject(new List<Node>());
					}

				}
			}
			return JsonConvert.SerializeObject(nodes);
		}
		public string GetPostGresDataTree() {

			int tries = 1;
			List<Node> nodes = new List<Node>();

			while (tries > 0) {
				try {
					nodes = GetAngularTreePostGres();
					tries = 0;
				} catch (Exception ex) {
					if (--tries > 0) {
						System.Threading.Thread.Sleep(1000);
					} else {
						return JsonConvert.SerializeObject(new List<Node>());
					}

				}
			}
			return JsonConvert.SerializeObject(nodes);
		}

		public string GetPostGresDataTree3() {

			int tries = 1;
			List<Node> nodes = new List<Node>();

			while (tries > 0) {
				try {
					nodes = GetAngularTreePostGres3();
					tries = 0;
				} catch (Exception ex) {
					if (--tries > 0) {
						System.Threading.Thread.Sleep(1000);
					} else {
						return JsonConvert.SerializeObject(new List<Node>());
					}

				}
			}
			return JsonConvert.SerializeObject(nodes);
		}

		public string GetNameTreesTableByDamid(String damid, int fileid) {
			int tries = 1;
			List<NameTreeTableNode> nodes = new List<NameTreeTableNode>();

			while (tries > 0) {
				try {
					nodes = GetPostGresNameTreeTableNode(damid, fileid);
					tries = 0;
				} catch (Exception ex) {
					if (--tries > 0) {
						System.Threading.Thread.Sleep(1000);
					} else {
						return JsonConvert.SerializeObject(new List<TableOffSetNode>());
					}

				}
			}
			return JsonConvert.SerializeObject(nodes);
		}

		public string GetNameTreesNormTable() {
			int tries = 1;
			List<NormTable> nodes = new List<NormTable>();
			while (tries > 0) {
				try {
					nodes = GetPostGresNormTable();
					tries = 0;
				} catch (Exception ex) {
					if (--tries > 0) {
						System.Threading.Thread.Sleep(1000);
					} else {
						return JsonConvert.SerializeObject(new List<NormTable>());
					}

				}
			}
			return JsonConvert.SerializeObject(nodes);
		}

		public string GetDocumentOffsets(String damid, int fileid) {
			int tries = 1;
			List<TableOffSetNode> nodes = new List<TableOffSetNode>();

			while (tries > 0) {
				try {
					nodes = GetPostGresDocumentOffsets(damid, fileid);
					tries = 0;
				} catch (Exception ex) {
					if (--tries > 0) {
						System.Threading.Thread.Sleep(1000);
					} else {
						return JsonConvert.SerializeObject(new List<TableOffSetNode>());
					}

				}
			}
			return JsonConvert.SerializeObject(nodes);
		}

		public string UpdateTableNormTableId(String damid, int iconum, int tableid, int fileid, int? newid) {
			String idvalue = "null";
			if (newid.HasValue)
				idvalue = "" + newid.Value;

			String query = @"UPDATE html_table_identification SET norm_table_id={4} WHERE document_id='{0}' and iconum={1} and table_id={2} and file_id={3} ";
			try {
				using (var conn = new NpgsqlConnection(PGConnectionString())) {
					using (var cmd = new NpgsqlCommand(string.Format(query, damid, iconum, tableid, fileid, idvalue), conn)) {
						conn.Open();
						using (var sdr = cmd.ExecuteReader()) {
						}
					}
				}
			} catch (Exception ex) {
				return "Fail";
			}

			return "Success";
		}

		public string UpdateTableTitleComments(String damid, int iconum, int tableid, int fileid, String newtitle) {
			String query = @"
                                do $$
                                begin
                                    IF exists (select  from html_table_identification  WHERE document_id='{0}' and iconum={1} and table_id={2} and file_id={3} ) then 
	                                    UPDATE html_table_identification SET comments='{4}' WHERE document_id='{0}' and iconum={1} and table_id={2} and file_id={3};
	                                else 
	                                    insert into html_table_identification (document_id, iconum, table_id, file_id, comments) values ('{0}', {1}, {2}, {3}, '{4}'); 
		                            end if;
		                        end
		                        $$
                            ";
			try {
				using (var conn = new NpgsqlConnection(PGConnectionString())) {
					using (var cmd = new NpgsqlCommand(string.Format(query, damid, iconum, tableid, fileid, newtitle), conn)) {
						conn.Open();
						using (var sdr = cmd.ExecuteReader()) {
						}
					}
				}
			} catch (Exception ex) {
				return "Fail";
			}

			return "Success";
		}

		public string UpdateTableTitleCorrect(String damid, int iconum, int tableid, int fileid, string iscorrect, string iscarboncorrect) {
			String query = @"UPDATE html_table_identification SET is_correct={4},is_carbon_hier_correct={5} WHERE document_id='{0}' and iconum={1} and table_id={2} and file_id={3} ";
			try {
				using (var conn = new NpgsqlConnection(PGConnectionString())) {
					using (var cmd = new NpgsqlCommand(string.Format(query, damid, iconum, tableid, fileid, iscorrect, iscarboncorrect), conn)) {
						conn.Open();
						using (var sdr = cmd.ExecuteReader()) {
						}
					}
				}
			} catch (Exception ex) {
				return "Fail";
			}

			return "Success";
		}


		public string SetPostGresDataTreeProfile(String name, String jsonstr) {
			String query = @"UPDATE cluster_name_tree_profile SET json='{1}' WHERE name='{0}';
		INSERT INTO cluster_name_tree_profile (name, json)
       SELECT '{0}', '{1}'
       WHERE NOT EXISTS (SELECT 1 FROM cluster_name_tree_profile WHERE name='{0}');";
			String qq = string.Format(query, name, jsonstr);
			try {
				using (var conn = new NpgsqlConnection(PGConnectionString())) {
					using (var cmd = new NpgsqlCommand(string.Format(query, name, jsonstr), conn)) {
						conn.Open();
						using (var sdr = cmd.ExecuteReader()) {
						}
					}
				}
			} catch (Exception ex) {
				return "Fail";
			}

			return "Success";
		}

		public void getDamlist(ClusterNameTreeNode node, HashSet<string> set) {
			if (node.documentid != null) {
				set.Add(node.documentid);
			}
			foreach (ClusterNameTreeNode snode in node.Nodes) {
				getDamlist(snode, set);
			}
		}

		public void removeMapping(List<long> flatids, Boolean istest) {
			String query = @"
			delete from cluster_mapping where norm_name_tree_flat_id in ({0});
			";
			if (istest)
				query = @"
				delete from cluster_mapping_test where norm_name_tree_flat_id in ({0});
			";

			try {
				using (var conn = new NpgsqlConnection(PGConnectionString())) {
					using (var cmd = new NpgsqlCommand(string.Format(query, string.Join(",", flatids)), conn)) {
						conn.Open();
						using (var sdr = cmd.ExecuteReader()) {
						}
					}
				}
			} catch (Exception ex) {
			}

		}

		public Dictionary<long, int> getOldMapping(List<String> docids, Boolean istest, int presenetationid) {
			String query = @"
			select cm.* from cluster_mapping as cm
				join norm_name_tree_flat as f 
					on cm.norm_name_tree_flat_id = f.id	 and f.col_id = 1
			        and f.document_id  in ({0}) 
			    join cluster_hierarchy as ch on cm.cluster_hierarchy_id = ch.id
				and ch.cluster_presentation_id = {1}
			order by document_id,cluster_hierarchy_id, table_id
		";
			if (istest) {
				query = @"
			select cm.* from cluster_mapping_test as cm
				join norm_name_tree_flat as f 
					on cm.norm_name_tree_flat_id = f.id	 and f.col_id = 1
			        and f.document_id  in ({0}) 
			    join cluster_hierarchy_test as ch on cm.cluster_hierarchy_id = ch.id
				and ch.cluster_presentation_id = {1}
			order by document_id,cluster_hierarchy_id, table_id
		";
			}
			String allids = string.Join(",", docids.Select(x => string.Format("'{0}'", x)).ToList());
			Dictionary<long, int> dic = new Dictionary<long, int>();
			try {
				using (var conn = new NpgsqlConnection(PGConnectionString())) {
					using (var cmd = new NpgsqlCommand(string.Format(query, allids, presenetationid), conn)) {
						conn.Open();
						using (var sdr = cmd.ExecuteReader()) {
							while (sdr.Read()) {
								dic[(long)sdr.GetInt32(1)] = sdr.GetInt32(0);
							}

						}
					}
				}
			} catch (Exception ex) {
			}
			return dic;
		}

		public void getNodeMapping(ClusterNameTreeNode pnode, ClusterNameTreeNode node, Dictionary<long, ClusterNameTreeNode> dic, Dictionary<int, ClusterNameTreeNode> map) {
			if (node.Role == "item") {
				dic[node.id] = node;
				if (pnode != null) {
					node.Hiearachyid = pnode.Hiearachyid;
				}
			} else {
				if (node.Hiearachyid != 0)
					map[node.Hiearachyid] = node;
				foreach (ClusterNameTreeNode snode in node.Nodes) {
					getNodeMapping(node, snode, dic, map);
				}
			}
		}

		public void createnewnode(Dictionary<long, ClusterNameTreeNode> newmap, Boolean istest) {
			for (var i = 0; i < newmap.Keys.Count; i++) {
				ClusterNameTreeNode node = newmap[newmap.Keys.ElementAt(i)];
				if (node.Hiearachyid == 0 && node.Role != "item") {
					node.Hiearachyid = create_cluster_hierarchy(node, istest);
				}
			}
		}

		public Boolean isNodeExist(ClusterNameTreeNode node, Boolean istest) {
			String query = "select id from cluster_hierarchy where id={0}";
			if (istest)
				query = "select id from cluster_hierarchy_test where id={0}";
			try {
				using (var conn = new NpgsqlConnection(PGConnectionString())) {
					using (var cmd = new NpgsqlCommand(string.Format(query, node.Hiearachyid), conn)) {
						conn.Open();
						using (var sdr = cmd.ExecuteReader()) {
							while (sdr.Read()) {
								int id = sdr.GetInt32(0);
								return true;
							}
						}
					}
				}
			} catch (Exception ex) {

			}
			return false;
		}

		public void checkwholetreenode(ClusterNameTreeNode rootnode, ClusterNameTreeNode pnode, List<ClusterNameTreeNode> Nodes, Boolean istest) {
			for (int i = 0; i < Nodes.Count; i++) {
				ClusterNameTreeNode node = Nodes.ElementAt(i);
				node.Presentationid = rootnode.Presentationid;
				node.Industry = rootnode.Industry;
				node.Normtableid = rootnode.Normtableid;

				if (node.Role != "item") {
					if (node.Hiearachyid == 0 || !isNodeExist(node, istest)) {
						if (pnode == null)
							node.ParentID = null;
						else
							node.ParentID = pnode.Hiearachyid;

						node.Hiearachyid = create_cluster_hierarchy(node, istest);
					}
					checkwholetreenode(rootnode, node, node.Nodes, istest);
				} else {
					if (node.Hiearachyid == 0) {
						node.Hiearachyid = pnode.Hiearachyid;
					}
				}
			}
		}

		private int create_cluster_hierarchy(ClusterNameTreeNode node, Boolean istest) {
			string query = @"insert into cluster_hierarchy (cluster_presentation_id, description, display_order, parent_id, isheader) VALUES ({0},'{1}',{2},{3},{4});
				SELECT currval(pg_get_serial_sequence('cluster_hierarchy','id'));
			";
			if (istest)
				query = @"insert into cluster_hierarchy_test (cluster_presentation_id, description, display_order, parent_id, isheader) VALUES ({0},'{1}',{2},{3},{4});
				SELECT currval(pg_get_serial_sequence('cluster_hierarchy_test','id'));
			";
			//try {
			string pid = "null";
			if (node.ParentID.HasValue)
				pid = "" + node.ParentID.Value;

			using (var conn = new NpgsqlConnection(PGConnectionString())) {
				using (var cmd = new NpgsqlCommand(string.Format(query, node.Presentationid, node.Title.Replace("'", "''"), -1, pid, node.Role == "header"), conn)) {
					conn.Open();
					//cmd.ExecuteNonQuery();
					using (var sdr = cmd.ExecuteReader()) {
						while (sdr.Read()) {
							int newid = sdr.GetInt32(0);
							return newid;
						}
					}
					//return (int)cmd.ExecuteScalar();
				}
			}
			//} catch (Exception ex) {
			//	}
			return -1; // shouldn't happen
		}

		public void createnewmapping(Dictionary<long, ClusterNameTreeNode> newmap, Dictionary<long, int> oldmap, Boolean istest) {
			for (var i = 0; i < newmap.Keys.Count; i++) {
				ClusterNameTreeNode node = newmap[newmap.Keys.ElementAt(i)];
				if (!oldmap.ContainsKey(node.id) || node.Hiearachyid != oldmap[node.id]) {
					String sql = @"UPDATE {2} SET cluster_hierarchy_id={0} WHERE norm_name_tree_flat_id={1};
					INSERT INTO {2} (cluster_hierarchy_id, norm_name_tree_flat_id)
					SELECT {0}, {1}
						WHERE NOT EXISTS (SELECT 1 FROM {2} WHERE norm_name_tree_flat_id={1});";
					String mappingtable = "cluster_mapping";
					if (istest)
						mappingtable = "cluster_mapping_test";

					try {
						using (var conn = new NpgsqlConnection(PGConnectionString())) {
							using (var cmd = new NpgsqlCommand(string.Format(sql, node.Hiearachyid, node.id, mappingtable, mappingtable), conn)) {
								conn.Open();
								var sdr = cmd.ExecuteReader();
							}
						}
					} catch (Exception ex) {

					}
				}
			}
		}

		public List<int> getOldhierarchyids(int Presentationid, Boolean istest) {
			string query = "select id from cluster_hierarchy where cluster_presentation_id = {0}";
			if (istest)
				query = "select id from cluster_hierarchy_test where cluster_presentation_id = {0}";
			List<int> list = new List<int>();
			try {
				using (var conn = new NpgsqlConnection(PGConnectionString())) {
					using (var cmd = new NpgsqlCommand(string.Format(query, Presentationid), conn)) {
						conn.Open();
						using (var sdr = cmd.ExecuteReader()) {
							while (sdr.Read()) {
								list.Add(sdr.GetInt32(0));
							}
						}
					}
				}
			} catch (Exception ex) {

			}
			return list;

		}

		public void removehierarchy(List<int> oldhierarchyids, Dictionary<int, ClusterNameTreeNode> newhierarchymap, Boolean istest) {
			List<int> list = new List<int>();
			foreach (int hierarchyid in oldhierarchyids) {
				if (!newhierarchymap.ContainsKey(hierarchyid)) // need to remove
					list.Add(hierarchyid);
			}
			if (list.Count == 0)
				return;

			String query = @"update cluster_hierarchy set display_order = -2 where id in ({0});";

			if (istest)
				query = @"update cluster_hierarchy_test set display_order = -2 where id in ({0});";

			//try {
			using (var conn = new NpgsqlConnection(PGConnectionString())) {
				using (var cmd = new NpgsqlCommand(string.Format(query, string.Join(",", list), string.Join(",", list)), conn)) {
					conn.Open();
					using (var sdr = cmd.ExecuteReader()) {
					}
				}
			}
			//} catch (Exception ex) {
			//	Console.WriteLine("Ex");
			//}
		}

		public int getNormtableID(String title) {
			String query = "select id from norm_table where label='{0}'";

			using (var conn = new NpgsqlConnection(PGConnectionString())) {
				using (var cmd = new NpgsqlCommand(string.Format(query, title), conn)) {
					conn.Open();
					using (var sdr = cmd.ExecuteReader()) {
						while (sdr.Read()) {
							return sdr.GetInt32(0);
						}
					}
				}
			}
			return 9999;
		}

		public int genPresentation(String ind, int industry_id, int normtableid, Boolean istest) {

			string qry1 = @"
				select id from cluster_presentation where norm_table_id={0} and industry_id = {2}";
			if (istest)
				qry1 = @"select id from cluster_presentation_test where norm_table_id={0} and industry = '{1}'";


			String query = @"insert into cluster_presentation (norm_table_id, industry_id) values({0},{2}); 
                       SELECT currval(pg_get_serial_sequence('cluster_presentation', 'id'));";
			if (istest)
				query = @"insert into cluster_presentation_test (norm_table_id, industry) values({0},'{1}'); 
                       SELECT currval(pg_get_serial_sequence('cluster_presentation_test', 'id'));";
			int id = 9999;
			//Boolean needcreate = true;
			string industry = char.ToUpper(ind[0]) + ind.Substring(1);
			using (var conn = new NpgsqlConnection(PGConnectionString())) {
				try {
					using (var cmd = new NpgsqlCommand(string.Format(qry1, normtableid, industry, industry_id), conn)) {
						conn.Open();
						using (var sdr = cmd.ExecuteReader()) {
							while (sdr.Read()) {
								id = sdr.GetInt32(0);
								//needcreate = false;
								break;
							}
						}
					}
				} catch (Exception ex) {
					using (var cmd1 = new NpgsqlCommand(string.Format(query, normtableid, industry), conn)) {
						//conn.Open();
						using (var sdr = cmd1.ExecuteReader()) {
							while (sdr.Read()) {
								id = sdr.GetInt32(0);
								break;
							}
						}
					}
				}
				/*
				String query2 = @"insert into cluster_presentation_test (norm_table_id, industry) values({0},'{1}')";
				if (istest)
					query2 = @"insert into cluster_presentation_test (norm_table_id, industry) values({0},'{1}')";

				if (needcreate) {
					using (var cmd2 = new NpgsqlCommand(string.Format(query2, normtableid, industry), conn)) {
						//conn.Open();
						using (var sdr = cmd2.ExecuteReader()) {
						}
					}
				}
				*/

			}
			return id;

		}
		//


		public int createPresentation(ClusterNameTreeNode node, Boolean istest) {
			string industry = getIndustryByDamid(node.documentid, node.iconum);
			int industry_id = getIndustryID(industry);
			string normtitle = node.NormtableTitle;
			int normtableid = getNormtableID(normtitle);
			int newid = genPresentation(industry, industry_id, normtableid, istest);
			node.Industry = industry;
			node.Normtableid = normtableid;
			return newid;
		}

		public int getIndustryID(String label) {
			String query = "select id from industry where lower(label) = '{0}'";
			using (var conn = new NpgsqlConnection(PGConnectionString())) {
				using (var cmd = new NpgsqlCommand(string.Format(query, label.ToLower()), conn)) {
					conn.Open();
					using (var sdr = cmd.ExecuteReader()) {
						while (sdr.Read()) {
							return sdr.GetInt32(0);
						}
					}
				}
			}
			return -1;
		}


		public void updatehierarchy(int Presentationid, int Hiearachyid, string title, int order, int? pid, Boolean isheader, Boolean istest) {
			String query = "update cluster_hierarchy set description='{0}', display_order={1}, parent_id={2}, isheader={3} where id={4}";
			if (istest)
				query = "update cluster_hierarchy_test set description='{0}', display_order={1}, parent_id={2}, isheader={3} where id={4}";

			string parentid = "null";
			if (pid.HasValue)
				parentid = "" + pid;
			//try {
			using (var conn = new NpgsqlConnection(PGConnectionString())) {
				using (var cmd = new NpgsqlCommand(string.Format(query, title.Replace("'", "''"), order, parentid, isheader, Hiearachyid), conn)) {
					conn.Open();
					using (var sdr = cmd.ExecuteReader()) {
					}
				}
			}
			//} catch (Exception ex) {
			//		Console.WriteLine("XX");
			//}

		}

		public void updatewholehierarchy(ClusterNameTreeNode root, Boolean istest) {
			Dictionary<int, int> map = new Dictionary<int, int>();
			Queue<ClusterNameTreeNode> q = new Queue<ClusterNameTreeNode>();
			q.Enqueue(root);
			int order = 0;
			while (q.Count > 0) {
				ClusterNameTreeNode n = q.Dequeue();
				if (n.id != -2) {
					// update n
					if (n.Role != "item") {
						if (map.ContainsKey(n.Hiearachyid)) {
							updatehierarchy(root.Presentationid, n.Hiearachyid, n.Title, order, map[n.Hiearachyid], n.Role == "header", istest);
						} else {
							updatehierarchy(root.Presentationid, n.Hiearachyid, n.Title, order, null, n.Role == "header", istest);
						}
						order++;
					}

				}
				for (int i = 0; i < n.Nodes.Count; i++) {
					ClusterNameTreeNode snode = n.Nodes.ElementAt(i);
					q.Enqueue(snode);
					if (n != root)
						map[snode.Hiearachyid] = n.Hiearachyid;
				}
			}

		}

		public string SaveClusterTree(String jsonstr, Boolean istest) {
			ClusterNameTreeNode node = JsonConvert.DeserializeObject<ClusterNameTreeNode>(jsonstr);
			Boolean isNew = false;
			if (node.Presentationid == 0) {
				node.Presentationid = createPresentation(node, istest); // need to test
				isNew = true;
			}

			checkwholetreenode(node, null, node.Nodes, istest);

			HashSet<string> set = new HashSet<string>();
			getDamlist(node, set);
			List<String> docids = set.ToList();
			List<long> removeflatids = new List<long>();
			Dictionary<long, int> oldmap = getOldMapping(docids, istest, node.Presentationid).OrderBy(t => t.Key).ToDictionary(p => p.Key, q => q.Value);
			Dictionary<long, ClusterNameTreeNode> newmap = new Dictionary<long, ClusterNameTreeNode>();
			Dictionary<int, ClusterNameTreeNode> newhierarchymap = new Dictionary<int, ClusterNameTreeNode>();
			getNodeMapping(null, node, newmap, newhierarchymap);
			newmap = newmap.OrderBy(t => t.Key).ToDictionary(p => p.Key, q => q.Value);
			List<int> oldhierarchyids = getOldhierarchyids(node.Presentationid, istest);
			for (int i = 0; i < oldmap.Keys.Count; i++) {
				long key = oldmap.Keys.ElementAt(i);
				if (!newmap.ContainsKey(key)) {
					removeflatids.Add(key);
				}
			}

			if (removeflatids.Count > 0) {
				removeMapping(removeflatids, istest);
			}

			if (!isNew)
				createnewnode(newmap, istest);
			createnewmapping(newmap, oldmap, istest);
			//removehierarchy(oldhierarchyids, newhierarchymap, istest);
			updatewholehierarchy(node, istest);

			// 0 check if need to create new presentation
			// 1 loop through Node to see if need to create new hierarchy
			// 2 get damids from node
			// 3 get old mappings from the damids
			// 4 get new mapping from node
			// 5 remove mapping in old but not in new
			// 6 loop new mapping and see if need to create new hierarchy
			// 7 insert or update the mapping based on new mappinge
			// 8 compare new and old hierarchy to see if need to delete some in DB
			///==========================
			// 9 base on the tree to update cluster_hierarchy
			List<ClusterNameTreeNode> rootnodes = GetPostGresClusterNameTreeNodeWithIconum(node.documentid, node.iconum, node.Industry, istest);
			foreach (ClusterNameTreeNode n in rootnodes) {
				if (n.Presentationid == node.Presentationid) {
					Dictionary<int, ClusterNameTreeNode> hiermap = new Dictionary<int, ClusterNameTreeNode>();
					Dictionary<long, ClusterNameTreeNode> flatidmap = new Dictionary<long, ClusterNameTreeNode>();
					getNodeMapping(null, n, flatidmap, hiermap);
					foreach (String damid in docids) {
						populateClusterNameTree(damid, hiermap, istest);
					}
					//appendItemNodes(n, newhierarchymap);
					return JsonConvert.SerializeObject(n);
				}
			}
			return "[]";
		}

		public void appendItemNodes(ClusterNameTreeNode node, Dictionary<int, ClusterNameTreeNode> map) {
			if (node.Role != "item") {
				if (node.Hiearachyid != 0) {
					if (map.ContainsKey(node.Hiearachyid)) {
						ClusterNameTreeNode n = map[node.Hiearachyid];
						for (int i = n.Nodes.Count - 1; i >= 0; i--) {
							ClusterNameTreeNode subnode = n.Nodes.ElementAt(i);
							if (subnode.Role == "item" && !node.Nodes.Any(t => t.Role == "item" && t.id == node.id)) {
								node.Nodes.Insert(0, subnode);
							}
						}

					}
				}

				for (int i = 0; i < node.Nodes.Count; i++) {
					appendItemNodes(node.Nodes.ElementAt(i), map);
				}
			}
		}


		public string GetPostGresDataTreeProfile() {

			int tries = 1;
			List<Profile> profiles = new List<Profile>();

			while (tries > 0) {
				try {
					profiles = GetProfilePostGres();
					tries = 0;
				} catch (Exception ex) {
					if (--tries > 0) {
						System.Threading.Thread.Sleep(1000);
					} else {
						return JsonConvert.SerializeObject(new List<Node>());
					}

				}
			}
			return JsonConvert.SerializeObject(profiles);
		}

		public static string DeepCleanString(string str) {
			string result = str.ToLower();

			string[] remlabelWords = result.Split(' ');
			for (int i = 0; i < remlabelWords.Count(); i++) {
				var ii = new Regex(@"ii$");
				var ies = new Regex(@"ies$");
				var es = new Regex(@"(s|x|ch|sh)es$");
				var s = new Regex(@"([a-z]){3,}s$");
				var leases = new Regex(@"leases$");
				var less = new Regex(@"less");
				if (remlabelWords[i] == "radii") {
					remlabelWords[i] = "radius";
				} else if (ii.IsMatch(remlabelWords[i])) {
					remlabelWords[i] = Regex.Replace(remlabelWords[i], @"ii$", "us");
				} else if (leases.IsMatch(remlabelWords[i])) {
					remlabelWords[i] = Regex.Replace(remlabelWords[i], @"leases$", "lease");
				} else if (less.IsMatch(remlabelWords[i])) {
				} else if (ies.IsMatch(remlabelWords[i])) {
					remlabelWords[i] = Regex.Replace(remlabelWords[i], @"ies$", "y");
				} else if (es.IsMatch(remlabelWords[i])) {
					remlabelWords[i] = Regex.Replace(remlabelWords[i], @"es$", "");
				} else if (s.IsMatch(remlabelWords[i])) {
					remlabelWords[i] = Regex.Replace(remlabelWords[i], @"s$", "");
				}
				//spelling.Text = remlabelWords[i];
				//spelling.SpellCheck();
				//if (!oSpell.TestWord(remlabelWords[i]))
				//{
				//    remlabelWords[i] = "";
				//}
			}
			result = string.Join(" ", remlabelWords.Where(x => x.Length >= 1 && !string.IsNullOrWhiteSpace(x)).ToList());// && !_stops.ContainsKey(c.ToLower())));

			return result;

		}
		private List<Node> GetAngularTree() {
			const string query = @"
SELECT  [Id]
      ,[Label]
  FROM [ffdocumenthistory].[dbo].[GDBClusters_1203] order by id
			";
			List<Node> allNodes = new List<Node>();
			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {
				using (SqlCommand cmd = new SqlCommand(query, conn)) {
					conn.Open();

					using (SqlDataReader sdr = cmd.ExecuteReader()) {
						while (sdr.Read()) {
							var n = new Node();
							n.Id = (int)sdr.GetInt64(0);
							n.Title = sdr.GetString(1);
							n.Nodes = new List<Node>();
							allNodes.Add(n);
						}
					}
				}
			}
			List<Node> nodes = new List<Node>();
			string[] big3Table = { "BS", "IS", "CF" };
			bool first = false;
			if (true) {
				Node t = new Node();
				nodes.Add(t);
				t.Id = 0;
				t.Title = "AVG-BS";
				t.Nodes = new List<Node>();
				Stack<Node> stack = new Stack<Node>();
				stack.Push(t);

				foreach (var row in allNodes) {
					int i = 0;
					var cleanedRowTitle = DeepCleanString(row.Title);
					var labelHierarchy = cleanedRowTitle.Replace("[", "").Split(new char[] { ']' }, StringSplitOptions.RemoveEmptyEntries);
					foreach (var labelAtlevel in labelHierarchy) {
						i++;
						if (stack.Count <= i) {
							break;
						}
						if (stack.ElementAt(stack.Count - i - 1).Title != labelAtlevel) {
							while (stack.Count > i && stack.Count > 1) {
								stack.Pop();
							}
						}
					}
					var lastRoot = stack.Peek();
					var endLabel = labelHierarchy.Last();
					i = 0;
					int j = 0;
					foreach (var labelAtlevel in labelHierarchy) {
						i++;
						if (stack.Count > i) {// count to the last common level. 
							continue;
						}
						if (stack.Peek().Title != labelAtlevel && labelAtlevel != endLabel) {
							var currentRoot = stack.Peek();
							bool found = false;
							foreach (var m in currentRoot.Nodes) {
								if (m.Title == labelAtlevel) {
									lastRoot = m;
									stack.Push(m);
									found = true;
									break;
								}
							}
							if (!found) {
								Node r = new Node();
								r.Id = -1;
								r.Title = labelAtlevel;
								r.Nodes = new List<Node>();
								lastRoot.Nodes.Add(r);
								lastRoot = r;
								stack.Push(r);
							}
						} else {
							Node r = new Node();
							r.Id = row.Id;
							r.Title = endLabel;
							r.Nodes = new List<Node>();
							lastRoot.Nodes.Add(r);
							lastRoot = r;
							stack.Push(r);
						}
					}

				}
			}
			foreach (var n in nodes) {
				nodeDocuments(n);
			}
			List<Node> newTree = new List<Node>();
			foreach (var n in nodes.First().Nodes) {
				newTree.Add(n);
			}
			return newTree;
		}


		public static string PGConnectionString() {
#if DEBUG
            //return "Host=dsnametree.cluster-c8vzac0v5wdo.us-east-1.rds.amazonaws.com;Port=5432;Username=nametreedata_admin_user;Password=UEmtE39C;Database=nametreedata;sslmode=Require;Trust Server Certificate=true;";
            return "Host=dsnametree.cluster-cbpqurvkowt4.us-east-1.rds.amazonaws.com;Port=5432;Username=nametreedata_admin_user;Password=skAQGPAs;Database=nametreedata;sslmode=Require;Trust Server Certificate=true;";
#else
            return "Host=dsnametree.cluster-cbpqurvkowt4.us-east-1.rds.amazonaws.com;Port=5432;Username=nametreedata_admin_user;Password=skAQGPAs;Database=nametreedata;sslmode=Require;Trust Server Certificate=true;";
#endif
        }
        public static string PGDevConnectionString()
        {
#if DEBUG
            //return "Host=nametreedata.cluster-c85crloosogt.us-east-1.rds.amazonaws.com;Port=5432;Username=nametreedata_admin_user;Password=J51YjIfF;Database=nametreedata;sslmode=Require;Trust Server Certificate=true;"; //This is Produciton
            return "Host=dsnametree.cluster-cizhlzyxlrwg.us-east-1.rds.amazonaws.com;Port=5432;Username=nametreedata_admin_user;Password=PaeQKA74;Database=nametreedata;sslmode=Require;Trust Server Certificate=true;";
#else
            return "Host=dsnametree.cluster-cizhlzyxlrwg.us-east-1.rds.amazonaws.com;Port=5432;Username=nametreedata_admin_user;Password=PaeQKA74;Database=nametreedata;sslmode=Require;Trust Server Certificate=true;";  
#endif
        }
        private List<Profile> GetProfilePostGres() {
			const string query = @"
				SELECT name,json FROM cluster_name_tree_profile
			";
			List<Profile> allProfiles = new List<Profile>();
			using (var conn = new NpgsqlConnection(PGConnectionString())) {
				using (var cmd = new NpgsqlCommand(query, conn)) {
					conn.Open();

					using (var sdr = cmd.ExecuteReader()) {
						while (sdr.Read()) {
							var n = new Profile();
							n.Name = sdr.GetString(0);
							n.Json = sdr.GetString(1);
							allProfiles.Add(n);
						}
					}
				}
			}
			return allProfiles;
		}

		private NameTreeTableNode genNameTreeTableNode(NpgsqlDataReader sdr) {
			string norm_title = "N/A";
			if (sdr.GetStringSafe(6).Length > 1) {
				norm_title = sdr.GetStringSafe(6);
			}

			int nodeindent = 0;
			int rowid = 0;
			if (!sdr.IsDBNull(4)) {
				nodeindent = sdr.GetInt32(4);
			}
			if (!sdr.IsDBNull(10)) {
				rowid = sdr.GetInt32(10);
			}
			NameTreeTableNode node = new NameTreeTableNode
			{
				DocumentID = sdr.GetGuid(0).ToString(),
				iconum = sdr.GetInt32(1),
				Title = sdr.GetStringSafe(2),
				FileID = sdr.GetInt32(3),
				indent = nodeindent,
				TableID = sdr.GetInt32(5),
				NormTitle = norm_title,
				isheader = sdr.GetBoolean(7),
				CleanedRowLabel = sdr.GetStringSafe(8),// use this to find parent
				offset = sdr.GetStringSafe(9),
				adjustedrowid = rowid,
				Nodes = new List<NameTreeTableNode>(),
				id = sdr.GetInt64(11)
			};
			return node;
		}
		public NameTreeTableNode genRootNameTreeTableNode(NameTreeTableNode node) {
			return new NameTreeTableNode()
			{
				DocumentID = node.DocumentID,
				iconum = node.iconum,
				Title = "" + node.TableID,
				FileID = node.FileID,
				indent = -1,
				NormTitle = node.NormTitle,
				isheader = true,
				CleanedRowLabel = node.NormTitle,
				Nodes = new List<NameTreeTableNode>(),
				TableID = node.TableID,
				adjustedrowid = node.adjustedrowid,
				id = node.id * -1
			};
		}

		private List<NameTreeTableNode> GetPostGresNameTreeTableNode(String damid, int fileid) {
			const string query = @"
				select document_id,iconum, final_label, file_id,indent,table_id, '',is_total,cleaned_row_label,item_offset,adjusted_row_id,id 
         from norm_name_tree_flat where document_id = '{0}' 
              and col_id = 1 and file_id={1}
order by norm_table_title, table_id, indent,adjusted_row_id
			";

			List<NameTreeTableNode> treenodes = new List<NameTreeTableNode>();
			Dictionary<int, NameTreeTableNode> tableidmap = new Dictionary<int, NameTreeTableNode>();
			using (var conn = new NpgsqlConnection(PGConnectionString())) {
				using (var cmd = new NpgsqlCommand(string.Format(query, damid, fileid), conn)) {
					conn.Open();

					int tries = 1;
					int num = 0;
					while (tries > 0) {
						num++;
						try {
							List<int> tableIDList = new List<int>();
							NameTreeTableNode rootnode = null;// same table_id's root
							using (var sdr = cmd.ExecuteReader()) {
								while (sdr.Read()) {
									NameTreeTableNode node = genNameTreeTableNode(sdr);
									if (!tableidmap.ContainsKey(node.TableID)) {
										rootnode = genRootNameTreeTableNode(node);
										tableidmap[node.TableID] = rootnode;
										treenodes.Add(rootnode);
									} else {
										rootnode = tableidmap[node.TableID];
									}

									if (node.indent == 0) {
										rootnode.Nodes.Add(node);
									} else {
										try {
											NameTreeTableNode pnode = findParentByRowID(rootnode, node);
											pnode.Nodes.Add(node);
										} catch (Exception ex) {
											Console.WriteLine(num);
										}
									}
								}
							}
							tries = 0;
						} catch (Exception ex) {
							if (--tries > 0) {
								System.Threading.Thread.Sleep(1000);
							}
						}
					}
				}
			}

			return populateNameTree(treenodes);
		}

		public List<NameTreeTableNode> populateNameTree(List<NameTreeTableNode> treenodes) {
			List<TableOffSetNode> infolist = new List<TableOffSetNode>();
			if (treenodes.Count > 0) {
				infolist = GetPostGresDocumentOffsets(treenodes.ElementAt(0).DocumentID, treenodes.ElementAt(0).FileID);
			}

			Dictionary<string, NameTreeTableNode> map = new Dictionary<string, NameTreeTableNode>();
			List<NameTreeTableNode> list = new List<NameTreeTableNode>();
			NameTreeTableNode root = null;
			foreach (NameTreeTableNode node in treenodes) {

				try {
					if (infolist.First(t => t.TableID == node.TableID).normtitle != null) {
						node.NormTitle = infolist.First(t => t.TableID == node.TableID).normtitle;
						//node.NormTitle = norm_title;
					}
				} catch (Exception ex) {
					Console.WriteLine("");
				}

				String norm_title = node.NormTitle;

				if (!map.ContainsKey(norm_title)) {
					root = new NameTreeTableNode()
					{
						DocumentID = node.DocumentID,
						iconum = node.iconum,
						Title = node.NormTitle,
						FileID = node.FileID,
						indent = -2,
						NormTitle = node.NormTitle,
						isheader = true,
						CleanedRowLabel = node.NormTitle,
						Nodes = new List<NameTreeTableNode>(),
						TableID = node.TableID,
						id = node.TableID * 1000 + node.adjustedrowid
					};
					map[norm_title] = root;
					list.Add(root);
				}
				root = map[norm_title];
				if (infolist.Any(t => t.TableID == node.TableID)) {
					node.info = infolist.First(t => t.TableID == node.TableID);
					node.Title = node.info.Title + "(" + node.TableID + ")";
					node.offset = node.info.offset;
				}
				root.Nodes.Add(node);
			}
			return list;
		}

		public NameTreeTableNode findParentByRowID(NameTreeTableNode root, NameTreeTableNode node) {
			int rowid = node.adjustedrowid;
			int indent = node.indent;
			int preindent = indent - 1;

			while (preindent >= 0) {
				List<NameTreeTableNode> list = getSameIndentNode(root, preindent);
				for (int i = 0; i < list.Count; i++) {
					NameTreeTableNode rnode = list.ElementAt(i);
					if (rnode.adjustedrowid > rowid)
						return rnode;
				}
				if (list.Count > 0)
					return list.ElementAt(0);
				preindent--;
			}
			return null;
		}

		public List<NameTreeTableNode> getSameIndentNode(NameTreeTableNode root, int indent) {
			List<NameTreeTableNode> list = new List<NameTreeTableNode>();
			if (root.indent == indent) {
				list.Add(root);
			} else {
				for (int i = 0; i < root.Nodes.Count; i++) {
					list.AddRange(getSameIndentNode(root.Nodes.ElementAt(i), indent));
				}
			}
			return list;
		}
		public static int getDistance(String str1, String str2, int m, int n) {
			int[,] dp = new int[m + 1, n + 1];
			for (int i = 0; i <= m; i++) {
				for (int j = 0; j <= n; j++) {
					if (i == 0)
						dp[i, j] = j;
					else if (j == 0)
						dp[i, j] = i;
					else if (str1.ElementAt(i - 1) == str2.ElementAt(j - 1))
						dp[i, j] = dp[i - 1, j - 1];
					else
						dp[i, j] = 1 + Math.Min(dp[i, j - 1], Math.Min(dp[i - 1, j], dp[i - 1, j - 1]));
				}
			}
			return dp[m, n];
		}
		//by diatance
		public NameTreeTableNode findParent(Dictionary<int, List<NameTreeTableNode>> indentmap, NameTreeTableNode node) {
			List<NameTreeTableNode> list = indentmap[node.indent - 1];
			NameTreeTableNode ret = null;
			int min = Int32.MaxValue;
			for (int i = 0; i < list.Count; i++) {
				int cmin = getDistance(node.CleanedRowLabel, list.ElementAt(i).CleanedRowLabel, node.CleanedRowLabel.Length, list.ElementAt(i).CleanedRowLabel.Length);
				if (cmin < min) {
					min = cmin;
					ret = list.ElementAt(i);
				}
			}
			return ret;
		}


		private List<NormTable> GetPostGresNormTable() {
			const string query = @"select id, label from norm_table";
			List<NormTable> nodes = new List<NormTable>();
			using (var conn = new NpgsqlConnection(PGConnectionString())) {
				using (var cmd = new NpgsqlCommand(query, conn)) {
					conn.Open();

					using (var sdr = cmd.ExecuteReader()) {
						while (sdr.Read()) {

							nodes.Add(new NormTable()
							{
								normtitleid = sdr.GetInt32(0),
								normtitle = sdr.GetStringSafe(1)
							});
						}
					}
				}
			}
			return nodes;
		}

		public String getIndustryByDamid(String damid, int iconum = 0) {
			if (iconum == 0)
				return "bank";
			else {
				string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDoc-SCAR"].ConnectionString;
				string voyConnectionString = ConfigurationManager.ConnectionStrings["Voyager"].ConnectionString;
				string lionConnectionString = ConfigurationManager.ConnectionStrings["Lion"].ConnectionString;
				string damConnectionString = ConfigurationManager.ConnectionStrings["FFDAM"].ConnectionString;
				CCS.Fundamentals.DataRoostAPI.Access.Company.CompanyHelper helper = new CCS.Fundamentals.DataRoostAPI.Access.Company.CompanyHelper(sfConnectionString, voyConnectionString, lionConnectionString, damConnectionString);
				dynamic obj = helper.GetCompanyByDamID(damid, "" + iconum);
				String ret = obj.Profile;
				return ret.ToLower();
			}
		}

		private ClusterNameTreeNode genClusterNameTreeNode(NpgsqlDataReader sdr) {
			int norm_table_id = sdr.GetInt32(0);
			string norm_table = sdr.GetStringSafe(1);
			String industry = sdr.GetStringSafe(2); ;
			int cluster_hierarchy_id = sdr.GetInt32(3);
			int cluster_presentation_id = sdr.GetInt32(4);
			string title = sdr.GetStringSafe(5);
			int displayorder = sdr.GetInt32(6);

			int? parent_id = null;
			if (!sdr.IsDBNull(7)) {
				parent_id = sdr.GetInt32(7);
			}

			String role = sdr.GetBoolean(8) ? "header" : "node";
			ClusterNameTreeNode node = new ClusterNameTreeNode
			{
				Normtableid = norm_table_id,
				NormtableTitle = norm_table,
				Industry = industry,
				Hiearachyid = cluster_hierarchy_id,
				Presentationid = cluster_presentation_id,
				Title = title,
				ParentID = parent_id,
				Role = role,
				Nodes = new List<ClusterNameTreeNode>(),
				id = -1
			};
			return node;
		}

		public ClusterNameTreeNode genRootClusterNameTreeNode(ClusterNameTreeNode node) {
			return new ClusterNameTreeNode()
			{
				Normtableid = node.Normtableid,
				NormtableTitle = node.NormtableTitle,
				Industry = node.Industry,
				Hiearachyid = 0,
				Presentationid = node.Presentationid,
				Title = node.NormtableTitle,
				Role = "header",
				Nodes = new List<ClusterNameTreeNode>(),
				id = -2
			};
		}

		public string rebuildtest() {
			String query = @"
			drop table IF EXISTS cluster_mapping_test;
			drop table IF EXISTS cluster_hierarchy_test;
			drop table IF EXISTS cluster_presentation_test;

			CREATE TABLE cluster_presentation_test AS SELECT * FROM cluster_presentation;
			ALTER TABLE cluster_presentation_test ADD CONSTRAINT cluster_presentation_test_pkey PRIMARY KEY(id);
			ALTER TABLE cluster_presentation_test ADD CONSTRAINT cluster_presentation_norm_table_id_fkey FOREIGN KEY (norm_table_id) REFERENCES norm_table(id);
			ALTER TABLE cluster_presentation_test ALTER COLUMN id Add GENERATED ALWAYS AS IDENTITY;
			select setval('cluster_presentation_test_id_seq', (select max(id) from cluster_presentation_test), true);
									 
			CREATE TABLE cluster_hierarchy_test AS SELECT * FROM cluster_hierarchy;
			ALTER TABLE cluster_hierarchy_test ADD CONSTRAINT cluster_hierarchy_test_pkey PRIMARY KEY(id);
			ALTER TABLE cluster_hierarchy_test ADD CONSTRAINT cluster_hierarchy_test_cluster_presentation_test_id_fkey FOREIGN KEY (cluster_presentation_id) REFERENCES cluster_presentation_test(id);
			ALTER TABLE cluster_hierarchy_test ADD CONSTRAINT cluster_hierarchy_test_parent_id_fkey FOREIGN KEY (parent_id) REFERENCES cluster_hierarchy_test(id);
			ALTER TABLE cluster_hierarchy_test ALTER COLUMN id Add GENERATED ALWAYS AS IDENTITY;

			select setval('cluster_hierarchy_test_id_seq', (select max(id) from cluster_hierarchy_test), true);
			CREATE TABLE cluster_mapping_test AS SELECT * FROM cluster_mapping;
			ALTER TABLE cluster_mapping_test ADD CONSTRAINT cluster_mapping_cluster_hierarchy_test_id_fkey FOREIGN KEY (cluster_hierarchy_id) REFERENCES cluster_hierarchy_test(id);
			ALTER TABLE cluster_mapping_test ADD CONSTRAINT cluster_mapping_norm_name_tree_flat_id_fkey_fkey FOREIGN KEY (norm_name_tree_flat_id) REFERENCES norm_name_tree_flat(id);
			";


			try {
				using (var conn = new NpgsqlConnection(PGConnectionString())) {
					using (var cmd = new NpgsqlCommand(query, conn)) {
						conn.Open();
						using (var sdr = cmd.ExecuteReader()) {

						}
					}
				}
			} catch (Exception ex) {
				return "Fail:" + ex.Message;
			}
			return "Success";
		}


		public List<ClusterNameTreeNode> GetPostGresClusterNameTreeNodeWithIconum(String damid, int iconum, String industry, Boolean istest = false) {
			String damindustry = industry;
			if (industry == null)
				damindustry = getIndustryByDamid(damid, iconum);
			else
				damindustry = damindustry.ToLower();

			string query = @"
				select cp.norm_table_id, nt.label, i.label, ch.* from cluster_hierarchy as ch
				join cluster_presentation as cp on cluster_presentation_id = cp.id
				join norm_table as nt on cp.norm_table_id = nt.id
				join industry as i on cp.industry_id = i.id
					where lower(i.label)='{0}' and display_order >= 0
				order by norm_table_id, display_order
			";
			if (istest) {
				query = @"
				select cp.norm_table_id, nt.label, cp.Industry, ch.* from cluster_hierarchy_test as ch
				join cluster_presentation_test as cp on cluster_presentation_id = cp.id
				join norm_table as nt on cp.norm_table_id = nt.id
					where lower(cp.Industry)='{0}' and display_order >= 0
				order by norm_table_id, display_order
				";
			}

			List<ClusterNameTreeNode> treenodes = new List<ClusterNameTreeNode>();
			Dictionary<int, ClusterNameTreeNode> normtableidmap = new Dictionary<int, ClusterNameTreeNode>();
			Dictionary<int, ClusterNameTreeNode> clusteridmap = new Dictionary<int, ClusterNameTreeNode>();
			using (var conn = new NpgsqlConnection(PGConnectionString())) {
				using (var cmd = new NpgsqlCommand(string.Format(query, damindustry), conn)) {
					conn.Open();

					int tries = 1;
					int num = 0;
					while (tries > 0) {
						num++;
						try {
							List<int> tableIDList = new List<int>();
							ClusterNameTreeNode rootnode = null;// same table_id's root
							using (var sdr = cmd.ExecuteReader()) {
								while (sdr.Read()) {
									ClusterNameTreeNode node = genClusterNameTreeNode(sdr);
									if (!normtableidmap.ContainsKey(node.Normtableid)) {
										rootnode = genRootClusterNameTreeNode(node);
										normtableidmap[node.Normtableid] = rootnode;
										treenodes.Add(rootnode);
									} else {
										rootnode = normtableidmap[node.Normtableid];
									}

									clusteridmap[node.Hiearachyid] = node;
									if (!node.ParentID.HasValue) {
										rootnode.Nodes.Add(node);
									} else {
										if (clusteridmap.ContainsKey(node.ParentID.Value)) {
											ClusterNameTreeNode pnode = clusteridmap[node.ParentID.Value];
											pnode.Nodes.Add(node);
										} else {
											Console.WriteLine("");
										}

									}
								}
							}
							tries = 0;
						} catch (Exception ex) {
							if (--tries > 0) {
								System.Threading.Thread.Sleep(1000);
							}
						}
					}
				}
			}
			try {
				if (damid != null)
					populateClusterNameTree(damid, clusteridmap, istest);
			} catch (Exception ex) {
				Console.WriteLine(ex.Message);
			}
			return treenodes;
		}


		public string GetPostGresClusterNameTreeTableNodeWithIconum(String damid, int iconum, Boolean istest = false) {
			return JsonConvert.SerializeObject(GetPostGresClusterNameTreeNodeWithIconum(damid, iconum, null, istest));
		}

		public string GetPostGresClusterNameTreeTableNode(String damid) {
			String damindustry = getIndustryByDamid(damid);
			const string query = @"
				select cp.norm_table_id, nt.label, cp.Industry, ch.* from cluster_hierarchy as ch
				join cluster_presentation as cp on cluster_presentation_id = cp.id
				join norm_table as nt on cp.norm_table_id = nt.id
					where lower(cp.Industry)='{0}'
				order by norm_table_id, display_order
			";

			List<ClusterNameTreeNode> treenodes = new List<ClusterNameTreeNode>();
			Dictionary<int, ClusterNameTreeNode> normtableidmap = new Dictionary<int, ClusterNameTreeNode>();
			Dictionary<int, ClusterNameTreeNode> clusteridmap = new Dictionary<int, ClusterNameTreeNode>();
			using (var conn = new NpgsqlConnection(PGConnectionString())) {
				using (var cmd = new NpgsqlCommand(string.Format(query, damindustry), conn)) {
					conn.Open();

					int tries = 1;
					int num = 0;
					while (tries > 0) {
						num++;
						try {
							List<int> tableIDList = new List<int>();
							ClusterNameTreeNode rootnode = null;// same table_id's root
							using (var sdr = cmd.ExecuteReader()) {
								while (sdr.Read()) {
									ClusterNameTreeNode node = genClusterNameTreeNode(sdr);
									if (!normtableidmap.ContainsKey(node.Normtableid)) {
										rootnode = genRootClusterNameTreeNode(node);
										normtableidmap[node.Normtableid] = rootnode;
										treenodes.Add(rootnode);
									} else {
										rootnode = normtableidmap[node.Normtableid];
									}

									clusteridmap[node.Hiearachyid] = node;
									if (!node.ParentID.HasValue) {
										rootnode.Nodes.Add(node);
									} else {
										if (clusteridmap.ContainsKey(node.ParentID.Value)) {
											ClusterNameTreeNode pnode = clusteridmap[node.ParentID.Value];
											pnode.Nodes.Add(node);
										} else {
											Console.WriteLine("");
										}

									}
								}
							}
							tries = 0;
						} catch (Exception ex) {
							if (--tries > 0) {
								System.Threading.Thread.Sleep(1000);
							}
						}
					}
				}
			}
			populateClusterNameTree(damid, clusteridmap);
			return JsonConvert.SerializeObject(treenodes);
		}


		private void populateClusterNameTree(string damid, Dictionary<int, ClusterNameTreeNode> clusteridmap, bool isTest = false) {
			string query = @"
				select distinct cluster_hierarchy_id, norm_name_tree_flat_id, id, document_id, iconum, raw_row_label, raw_column_label, 
       cleaned_row_label,cleaned_column_label,raw_table_title, norm_table_title, xbrl_tag, item_offset, value, numeric_value, f.table_id
from cluster_mapping as cm
				join norm_name_tree_flat as f 
					on cm.norm_name_tree_flat_id = f.id	and f.col_id = 1 and f.document_id = '{0}' and item_offset like '%|r0'
			  order by cluster_hierarchy_id, f.table_id";

			if (isTest) {
				query = @"
				select distinct cluster_hierarchy_id, norm_name_tree_flat_id, id, document_id, iconum, raw_row_label, raw_column_label, 
       cleaned_row_label,cleaned_column_label,raw_table_title, norm_table_title, xbrl_tag, item_offset, value, numeric_value , f.table_id
from cluster_mapping_test as cm
				join norm_name_tree_flat as f 
					on cm.norm_name_tree_flat_id = f.id	and f.col_id = 1 and f.document_id = '{0}' and item_offset like '%|r0'
			  order by cluster_hierarchy_id, f.table_id";
			}

			using (var conn = new NpgsqlConnection(PGConnectionString())) {
				using (var cmd = new NpgsqlCommand(string.Format(query, damid.ToString()), conn)) {
					conn.Open();

					using (var sdr = cmd.ExecuteReader()) {
						while (sdr.Read()) {
							genClusterMappedNameTreeNode(sdr, clusteridmap);
						}
					}
				}
			}
		}

		private void genClusterMappedNameTreeNode(NpgsqlDataReader sdr, Dictionary<int, ClusterNameTreeNode> clusteridmap) {
			int cluster_hierarchy_id = sdr.GetInt32(0);
			if (!clusteridmap.ContainsKey(cluster_hierarchy_id)) {
				Console.WriteLine(cluster_hierarchy_id);
				return;
			}
			ClusterNameTreeNode pnode = clusteridmap[cluster_hierarchy_id];
			int norm_table_id = pnode.Normtableid;
			string norm_table = pnode.NormtableTitle;
			String industry = pnode.Industry;
			int cluster_presentation_id = pnode.Presentationid;

			string title = sdr.GetStringSafe(7);
			int? parent_id = null;
			String numericvalue = "";
			if (!sdr.IsDBNull(14)) {
				numericvalue = sdr.GetDecimal(14).ToString();
			}

			ClusterNameTreeNode node = new ClusterNameTreeNode
			{
				Normtableid = norm_table_id,
				NormtableTitle = norm_table,
				Industry = industry,
				Hiearachyid = cluster_hierarchy_id,
				Presentationid = cluster_presentation_id,
				Title = title,
				ParentID = parent_id,
				Role = "item",
				Nodes = new List<ClusterNameTreeNode>(),
				documentid = sdr.GetGuid(3).ToString(),
				iconum = sdr.GetInt32(4),
				id = sdr.GetInt64(2),
				offset = sdr.GetStringSafe(12),
				value = sdr.GetStringSafe(13),
				numericvalue = numericvalue
			};
			/*
			foreach (ClusterNameTreeNode n in pnode.Nodes) {
				if (n.Role == "item")
					return;
			}
			*/
			pnode.Nodes.Insert(0, node);
		}

		private List<TableOffSetNode> GetPostGresDocumentOffsets(String damid, int fileid) {
			const string query = @"
				select ht.table_id, ht.file_id, ht.title, ht.comments, ht.is_correct,ht.is_carbon_hier_correct, nt.label , nt.id
            from html_table_identification ht 
       left join norm_table nt on ht.norm_table_id = nt.id    
        where document_id = '{0}' and file_id = {1} order by table_id
			";
			List<TableOffSetNode> nodes = new List<TableOffSetNode>();
			using (var conn = new NpgsqlConnection(PGConnectionString())) {
				using (var cmd = new NpgsqlCommand(string.Format(query, damid, fileid), conn)) {
					conn.Open();

					List<int> tableIDList = new List<int>();
					using (var sdr = cmd.ExecuteReader()) {
						while (sdr.Read()) {
							int tableid = sdr.GetInt32(0);
							tableIDList.Add(tableid);
							string title = sdr.GetStringSafe(2);
							string comment = sdr.GetStringSafe(3);
							string normtitle = null;
							bool iscorrect = false;
							bool iscarboncorrect = false;
							int? normtitleid = null;
							if (!sdr.IsDBNull(4)) {
								iscorrect = sdr.GetBoolean(4);
							}
							if (!sdr.IsDBNull(5)) {
								iscarboncorrect = sdr.GetBoolean(5);
							}
							if (!sdr.IsDBNull(6)) {
								normtitle = sdr.GetStringSafe(6);
							}
							if (!sdr.IsDBNull(7)) {
								normtitleid = sdr.GetInt32(7);
							}
							TableOffSetNode node = new TableOffSetNode
							{
								TableID = tableid,
								Title = title,
								FileID = fileid,
								DocumentID = damid,
								comments = comment,
								iscorrect = iscorrect,
								iscarboncorrect = iscarboncorrect,
								normtitle = normtitle,
								normtitleid = normtitleid
							};
							nodes.Add(node);
						}
					}

					GetPostGresDocumentOffsetByTableIDList(nodes, tableIDList, damid);
				}
			}
			return nodes;
		}

		private void GetPostGresDocumentOffsetByTableIDList(List<TableOffSetNode> nodes, List<int> tableIDList, String damid) {
			const string query = @"
				select item_offset, table_id from
				(select item_offset, table_id,
				rank() over (partition by table_id order by substring(item_offset,2, Position('|' in item_offset)-2)::integer)
				from norm_name_tree_flat where document_id = '{0}'
				and  table_id = any(array[{1}]) and item_offset ilike 'o%|%' order by table_id) ranked_offsets
				where rank = 1";
			using (var conn = new NpgsqlConnection(PGConnectionString())) {
				using (var cmd = new NpgsqlCommand(string.Format(query, damid.ToString(), string.Join(",", tableIDList.ToArray())), conn)) {
					conn.Open();

					using (var sdr = cmd.ExecuteReader()) {
						while (sdr.Read()) {
							string offset = sdr.GetString(0);
							int tableid = sdr.GetInt32(1);

							int index = nodes.FindIndex(c => c.TableID == tableid);
							nodes[index].offset = offset;
						}
					}
				}
			}
		}

		private List<Node> GetAngularTreePostGres() {
			const string query = @"
SELECT  Id,Label, 0
  FROM cluster_name_tree  order by id
			";
			List<Node> allNodes = new List<Node>();

			using (var conn = new NpgsqlConnection(PGConnectionString())) {
				using (var cmd = new NpgsqlCommand(query, conn)) {
					conn.Open();

					using (var sdr = cmd.ExecuteReader()) {
						while (sdr.Read()) {
							var n = new Node();
							n.Id = (int)sdr.GetInt64(0);
							n.Title = sdr.GetString(1);
							n.ParentId = sdr.GetInt32(2); // count, NOT parentid
							n.Nodes = new List<Node>();
							allNodes.Add(n);
						}
					}
				}
			}
			List<Node> nodes = new List<Node>();
			string[] big3Table = { "BS", "IS", "CF" };
			bool first = false;
			if (true) {
				Node t = new Node();
				nodes.Add(t);
				t.Id = 0;
				t.Title = "AVG-BS";
				t.Nodes = new List<Node>();
				Stack<Node> stack = new Stack<Node>();
				stack.Push(t);

				foreach (var row in allNodes) {
					int i = 0;
					var cleanedRowTitle = DeepCleanString(row.Title);
					var labelHierarchy = cleanedRowTitle.Replace("[", "").Split(new char[] { ']' }, StringSplitOptions.RemoveEmptyEntries);
					if (labelHierarchy.Length == 0)
						continue;
					foreach (var labelAtlevel in labelHierarchy) {
						i++;
						if (stack.Count <= i) {
							break;
						}
						if (stack.ElementAt(stack.Count - i - 1).Title != labelAtlevel) {
							while (stack.Count > i && stack.Count > 1) {
								stack.Pop();
							}
						}
					}
					var lastRoot = stack.Peek();
					var endLabel = labelHierarchy.Last();
					i = 0;
					int j = 0;
					foreach (var labelAtlevel in labelHierarchy) {
						i++;
						if (stack.Count > i) {// count to the last common level. 
							continue;
						}
						if (stack.Peek().Title != labelAtlevel && labelAtlevel != endLabel) {
							var currentRoot = stack.Peek();
							bool found = false;
							foreach (var m in currentRoot.Nodes) {
								if (m.Title == labelAtlevel) {
									lastRoot = m;
									stack.Push(m);
									found = true;
									break;
								}
							}
							if (!found) {
								Node r = new Node();
								r.Id = -1;
								r.Title = labelAtlevel;
								r.ParentId = row.ParentId;
								r.Nodes = new List<Node>();
								lastRoot.Nodes.Add(r);
								lastRoot = r;
								stack.Push(r);
							}
						} else {
							Node r = new Node();
							r.Id = row.Id;
							r.Title = endLabel;
							r.ParentId = row.ParentId;
							r.Nodes = new List<Node>();
							lastRoot.Nodes.Add(r);
							lastRoot = r;
							stack.Push(r);
						}
					}

				}
			}
			//return nodes;
			foreach (var n in nodes) {
				PGnodeDocuments2(n);
			}
			List<Node> newTree = new List<Node>();
			Node unknown = new Node();
			unknown.Id = 0;
			unknown.Title = "Unknown";
			unknown.Nodes = new List<Node>();
			foreach (var n in nodes.First().Nodes) {
				if (n.Title.StartsWith("total asset") || n.Title.StartsWith("total liability and shareholder equity") || n.Title.StartsWith("total asset")) {
					newTree.Add(n);
				} else {
					unknown.Nodes.Add(n);
				}
			}
			newTree.Add(unknown);
			return newTree;
		}

		private List<Node> GetAngularTreePostGres3() {
			const string query = @"
SELECT  Id,Label, iconum_count, comment
  FROM popular_name_tree_new where iconum_count > 1 order by id
			";
			List<Node> allNodes = new List<Node>();

			using (var conn = new NpgsqlConnection(PGConnectionString())) {
				using (var cmd = new NpgsqlCommand(query, conn)) {
					conn.Open();

					using (var sdr = cmd.ExecuteReader()) {
						while (sdr.Read()) {
							var n = new Node();
							n.Id = (int)sdr.GetInt64(0);
							n.Title = sdr.GetString(1);
							n.ParentId = sdr.GetInt32(2); // count, NOT parentid
							n.Comment = sdr.GetStringSafe(3);
							n.Nodes = new List<Node>();
							allNodes.Add(n);
						}
					}
				}
			}
			List<Node> nodes = new List<Node>();
			string[] big3Table = { "BS", "IS", "CF" };
			bool first = false;
			if (true) {
				Node t = new Node();
				nodes.Add(t);
				t.Id = 0;
				t.Title = "AVG-BS";
				t.Nodes = new List<Node>();
				Stack<Node> stack = new Stack<Node>();
				stack.Push(t);

				foreach (var row in allNodes) {
					int i = 0;
					var cleanedRowTitle = row.Title;
					var labelHierarchy = cleanedRowTitle.Replace("[", "").Split(new char[] { ']' }, StringSplitOptions.RemoveEmptyEntries);
					if (labelHierarchy.Length == 0)
						continue;
					foreach (var labelAtlevel in labelHierarchy) {
						i++;
						if (stack.Count <= i) {
							break;
						}
						if (stack.ElementAt(stack.Count - i - 1).Title != labelAtlevel) {
							while (stack.Count > i && stack.Count > 1) {
								stack.Pop();
							}
						}
					}
					var lastRoot = stack.Peek();
					var endLabel = labelHierarchy.Last();
					i = 0;
					int j = 0;
					foreach (var labelAtlevel in labelHierarchy) {
						i++;
						if (stack.Count > i) {// count to the last common level. 
							continue;
						}
						if (stack.Peek().Title != labelAtlevel && labelAtlevel != endLabel) {
							var currentRoot = stack.Peek();
							bool found = false;
							foreach (var m in currentRoot.Nodes) {
								if (m.Title == labelAtlevel) {
									lastRoot = m;
									stack.Push(m);
									found = true;
									break;
								}
							}
							if (!found) {
								Node r = new Node();
								r.Id = -1;
								r.Title = labelAtlevel;
								r.ParentId = row.ParentId;
								r.Comment = row.Comment;
								r.Nodes = new List<Node>();
								lastRoot.Nodes.Add(r);
								lastRoot = r;
								stack.Push(r);
							}
						} else {
							Node r = new Node();
							r.Id = row.Id;
							r.Title = endLabel;
							r.ParentId = row.ParentId;
							r.Comment = row.Comment;
							r.Nodes = new List<Node>();
							lastRoot.Nodes.Add(r);
							lastRoot = r;
							stack.Push(r);
						}
					}

				}
			}
			foreach (var n in nodes) {
				PGnodeDocuments2(n);
			}
			List<Node> newTree = new List<Node>();
			Node unknown = new Node();
			unknown.Id = 0;
			unknown.Title = "Unknown";
			unknown.Nodes = new List<Node>();
			foreach (var n in nodes.First().Nodes) {
				if (n.Title.StartsWith("total asset") || n.Title.StartsWith("total liability and shareholder equity") || n.Title.StartsWith("total assets") || n.Title.StartsWith("total liabilities and")) {
					newTree.Add(n);
				} else if (n.Title.StartsWith("asset") || n.Title.StartsWith("liability and shareholder equity") || n.Title.StartsWith("total asset")) {
					newTree.Add(n);
				} else {
					unknown.Nodes.Add(n);
				}
			}
			newTree.Add(unknown);
			return newTree;
		}

		public string GetNameTree(Guid DamDocumentId) {

			int tries = 1;
			List<NameTreeNode> nodes = new List<NameTreeNode>();

			while (tries > 0) {
				try {
					nodes = GetNameTreePostGres(DamDocumentId);
					tries = 0;
				} catch (Exception ex) {
					if (--tries > 0) {
						System.Threading.Thread.Sleep(1000);
					} else {
						return JsonConvert.SerializeObject(new List<NameTreeNode>());
					}

				}
			}
			return JsonConvert.SerializeObject(nodes);
		}

		private List<NameTreeNode> GetNameTreePostGres(Guid DamDocumentId) {
			const string query = @"
 select tc.id, c.Label AS stdlabel, c.Id AS stdCode, tcf.raw_row_label, tcf.cleaned_row_label,
	 tcf.value, tcf.numeric_value, tc.item_offset as offset, tc.hash_id, tc.document_id, tcf.Iconum 
	 ,tcf.cleaned_row_label, tcf.cleaned_column_label, array_to_string(tcf.context, ','), tcf.cell_date, tcf.period_length, tcf.period_type
	 ,tcf.interim_type, tcf.scaling, tcf.currency, tcf.numeric_value, t.id, t.label, tc.final_label, tcf.row_id, tcf.adjusted_row_id, tcf.col_id, tcf.table_id
    from norm_name_tree tc
    join norm_table t
        on tc.norm_table_id = t.id
    join norm_name_tree_flat tcf
        on tc.Id = tcf.Id
    left join cluster_name_tree c
        on c.Id = tc.Cluster_id
where coalesce(TRIM(tc.item_offset), '') <> ''
    and tc.document_id = '{0}'
	order by t.id, tc.id
			";
			List<NameTreeNode> allNodes = new List<NameTreeNode>();

			using (var conn = new NpgsqlConnection(PGConnectionString())) {
				using (var cmd = new NpgsqlCommand(string.Format(query, DamDocumentId.ToString()), conn)) {
					conn.Open();

					using (var sdr = cmd.ExecuteReader()) {
						while (sdr.Read()) {
							var n = new NameTreeNode();
							// 0th = id per value
							// 5th = as reported value
							// 10th = Iconum
							// 11th = CLeaned row label
							// 16th = peiord type
							// 17 = interim type
							n.Id = sdr.GetInt64(0);
							n.ClusteredNodeLabel = sdr.GetStringSafe(1);
							n.ClusteredId = sdr.GetNullable<long>(2);
							n.AsReportedNodeLabel = sdr.GetStringSafe(3);
							n.AsReportedValue = sdr.GetStringSafe(5);
							n.AsReportedNumericValue = sdr.GetNullable<decimal>(6);
							n.Offset = sdr.GetStringSafe(7);
							n.HashId = sdr.GetStringSafe(8);
							n.DocumentID = sdr.GetGuid(9);
							n.Iconum = sdr.GetNullable<int>(10);
							n.CleanedRowLabel = sdr.GetStringSafe(11);
							n.CleanedColumnLabel = sdr.GetStringSafe(12);
							n.Context = sdr.GetStringSafe(13);
							n.CellDate = sdr.GetNullable<DateTime>(14);
							n.PeriodLength = sdr.GetNullable<int>(15);
							n.PeriodType = sdr.GetStringSafe(16);
							n.InterimType = sdr.GetStringSafe(17);
							n.Scaling = sdr.GetStringSafe(18);
							n.Currency = sdr.GetStringSafe(19);
							var numeric = sdr.GetNullable<decimal>(20);
							if (numeric == null) {
								n.NumericValue = "";

							} else {
								n.NumericValue = numeric.Value.ToString();

							}
							n.NormTableId = sdr.GetNullable<int>(21);
							n.NormTableDescription = sdr.GetStringSafe(22);
							n.FinalLabel = sdr.GetStringSafe(23);
							n.RawRowId = sdr.GetNullable<int>(24);
							n.AdjustedRowId = sdr.GetNullable<int>(25);
							n.RawColId = sdr.GetNullable<int>(26);
							n.RawTableId = sdr.GetNullable<int>(27);
							n.Nodes = new List<NameTreeNode>();
							allNodes.Add(n);
						}
					}
				}
			}
			foreach (var row in allNodes) {
				if (!string.IsNullOrWhiteSpace(row.ClusteredNodeLabel)) {
					var hiearchy = fn.Hierarchy(row.ClusteredNodeLabel);
					var parentLabel = fn.RevCdr(hiearchy) + fn.Unbox(fn.RevCar(hiearchy));
					var parent = allNodes.FirstOrDefault(x => x.ClusteredNodeLabel == parentLabel && x.NormTableId == row.NormTableId);
					if (parent != null) {
						row.ClusteredParentId = parent.ClusteredId;
					}
				}
			}
			return allNodes;
		}

		private Dictionary<long, List<Tuple<Guid, string, string>>> documentCluster = new Dictionary<long, List<Tuple<Guid, string, string>>>();
		private Dictionary<long, List<Tuple<Guid, string, string, string>>> pgdocumentCluster;
		private Dictionary<long, List<Tuple<Guid, string>>> pgdocumentCluster2;

		private Dictionary<long, List<Tuple<Guid, string, string>>> initDocumentCluster() {
			documentCluster = new Dictionary<long, List<Tuple<Guid, string, string>>>();
			try {
				const string query = @"
SELECT distinct code.GDBClusterID , item.DocumentId, item.label, item.value
  FROM [ffdocumenthistory].[dbo].[GDBCodes_1203] code
  JOIN [ffdocumenthistory].[dbo].[GDBTaggedItems_1203] item on code.id = item.GDBTableId
 where code.GDBClusterID is not null
 order by GDBClusterID
			";

				using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {
					using (SqlCommand cmd = new SqlCommand(query, conn)) {
						conn.Open();
						using (SqlDataReader sdr = cmd.ExecuteReader()) {
							while (sdr.Read()) {
								var id = sdr.GetInt64(0);
								if (!documentCluster.ContainsKey(id)) {
									documentCluster.Add(id, new List<Tuple<Guid, string, string>>());
								}
								var g = sdr.GetGuid(1);
								var h = sdr.GetStringSafe(2);
								var i = sdr.GetStringSafe(3);
								documentCluster[id].Add(new Tuple<Guid, string, string>(g, h, i));
							}
						}
					}
				}
			} catch {
			}
			return documentCluster;
		}

		private Dictionary<long, List<Tuple<Guid, string, string, string>>> initPgDocumentCluster() {
			pgdocumentCluster = new Dictionary<long, List<Tuple<Guid, string, string, string>>>();
			try {
				const string query = @"
 SELECT DISTINCT t.cluster_id_new,
    t.document_id,
    f.raw_row_label,
    f.value,
    p.item_code, t.item_offset 
   FROM norm_name_tree t
     JOIN norm_name_tree_flat f ON t.id = f.id
     LEFT JOIN prod_data p ON f.document_id = p.document_id AND  f.item_offset::text = p.item_offset::text    AND f.iconum = p.iconum
  WHERE t.cluster_id_new IS NOT NULL
  union
   SELECT DISTINCT t.cluster_id_new,
    t.document_id,
    f.raw_row_label,
    f.value,
    p.item_code, t.item_offset 
   FROM norm_name_tree t
     JOIN norm_name_tree_flat f ON t.id = f.id
     LEFT JOIN prod_data p ON f.document_id = p.document_id    AND f.iconum = p.iconum
  WHERE t.cluster_id_new IS NOT NULL and p.item_offset = '';
			";
				//select cluster_id, document_id, raw_row_label, value, item_code from vw_clusters_documents_sdb
				// order by cluster_id
				using (var conn = new NpgsqlConnection(PGConnectionString())) {
					using (var cmd = new NpgsqlCommand(query, conn)) {
						cmd.CommandTimeout = 600;
						conn.Open();
						using (var sdr = cmd.ExecuteReader()) {
							while (sdr.Read()) {
								var id = sdr.GetInt64(0);
								if (!pgdocumentCluster.ContainsKey(id)) {
									pgdocumentCluster.Add(id, new List<Tuple<Guid, string, string, string>>());
								}
								var g = sdr.GetGuid(1);
								var h = sdr.GetStringSafe(2);
								var i = sdr.GetStringSafe(3);
								i = sdr.GetStringSafe(5);
								var j = sdr.GetStringSafe(4);
								pgdocumentCluster[id].Add(new Tuple<Guid, string, string, string>(g, h, i, j));
							}
						}
					}
				}
			} catch (Exception ex) {
				var s = ex.Message;
			}
			return pgdocumentCluster;
		}
		private Dictionary<long, List<Tuple<Guid, string>>> initPgDocumentCluster2() {
			pgdocumentCluster2 = new Dictionary<long, List<Tuple<Guid, string>>>();
			try {
				const string query = @"
  SELECT DISTINCT t.cluster_id_new,
    t.document_id
   FROM norm_name_tree t
	 where t.cluster_id_new is not null
			";

				using (var conn = new NpgsqlConnection(PGConnectionString())) {
					using (var cmd = new NpgsqlCommand(query, conn)) {
						cmd.CommandTimeout = 600;
						conn.Open();
						using (var sdr = cmd.ExecuteReader()) {
							while (sdr.Read()) {
								var id = sdr.GetInt64(0);
								if (!pgdocumentCluster2.ContainsKey(id)) {
									pgdocumentCluster2.Add(id, new List<Tuple<Guid, string>>());
								}
								var g = sdr.GetGuid(1);
								var h = "";

								pgdocumentCluster2[id].Add(new Tuple<Guid, string>(g, h));
							}
						}
					}
				}
			} catch (Exception ex) {
				var s = ex.Message;
			}
			return pgdocumentCluster2;
		}
		private Dictionary<Guid, List<string>> tickerCluster = new Dictionary<Guid, List<string>>();
		private Dictionary<Guid, List<string>> pgtickerCluster;
		private Dictionary<Guid, List<string>> initTickerCluster() {
			tickerCluster = new Dictionary<Guid, List<string>>();
			try {
				const string query = @"
SELECT distinct   item.DocumentId, min(f.Firm_Name), min(f.BestTicker) 
  FROM [ffdocumenthistory].[dbo].[GDBCodes_1203] code
  JOIN [ffdocumenthistory].[dbo].[GDBTaggedItems_1203] item on code.id = item.GDBTableId
  JOIN Document d on d.DAMDocumentId = item.DocumentId
  JOIN DocumentSeries ds on d.DocumentSeriesID = ds.ID
  JOIN FilerMst f on f.Iconum = ds.CompanyID
 where code.GDBClusterID is not null
 group by item.DocumentId
			";

				using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {
					using (SqlCommand cmd = new SqlCommand(query, conn)) {
						cmd.CommandTimeout = 600;
						conn.Open();
						using (SqlDataReader sdr = cmd.ExecuteReader()) {
							while (sdr.Read()) {
								var id = sdr.GetGuid(0);
								if (!tickerCluster.ContainsKey(id)) {
									tickerCluster.Add(id, new List<string>());
								}
								var g = sdr.GetStringSafe(1);
								var h = sdr.GetStringSafe(2);
								tickerCluster[id].Add(g);
								tickerCluster[id].Add(h);
							}
						}
					}
				}
			} catch {
			}
			return tickerCluster;
		}

		private Dictionary<Guid, List<string>> initPgTickerCluster() {
			pgtickerCluster = new Dictionary<Guid, List<string>>();
			try {
				const string query = @"
select * from vw_documents_iconums
			";

				using (var conn = new NpgsqlConnection(PGConnectionString())) {
					using (var cmd = new NpgsqlCommand(query, conn)) {
						conn.Open();
						using (var sdr = cmd.ExecuteReader()) {
							while (sdr.Read()) {
								var id = sdr.GetGuid(0);
								if (!tickerCluster.ContainsKey(id)) {
									pgtickerCluster.Add(id, new List<string>());
								}
								var g = sdr.GetInt32(1).ToString();
								pgtickerCluster[id].Add(g);
							}
						}
					}
				}
			} catch {
			}
			return pgtickerCluster;
		}
		private Node PGNodeDocument(Node n) {
			if (n.ParentId.HasValue) {
				//n.Title += string.Format(" ({0})", n.ParentId);
			}
			foreach (var child in n.Nodes) {
				PGNodeDocument(child);
			}
			return n;
		}
		private Node PGnodeDocuments2(Node n, string hiearchy = "") {
			try {
				if (n.Documents == null) {
					n.Documents = new List<string>();
				}
				if (n.DocumentTuples == null) {
					n.DocumentTuples = new List<Tuple<string, string, string, Guid, string>>();
				}
				if (pgdocumentCluster == null) {
					initPgDocumentCluster();
				}
				if (pgdocumentCluster2 == null) {
					initPgDocumentCluster2();
				}
				if (pgtickerCluster == null) {
					initPgTickerCluster();

				}
				var nTitle = n.Title;
				if (string.IsNullOrWhiteSpace(n.Childrentitle)) {
					n.Childrentitle = "";
				}
				n.Childrentitle += nTitle;
				if (pgdocumentCluster.ContainsKey(n.Id)) { // n.id is ClusterID

					foreach (var d in pgdocumentCluster[n.Id]) {
						var doc = new List<string>();

						string z = "";
						string y = "";
						//doc.Add(d.ToString());
						if (pgtickerCluster.ContainsKey(d.Item1)) // d.Item1 is DocID
                    {
							//foreach(var t in tickerCluster[d.Item1])
							//{
							//    doc.Add(t);

							//}
							z = pgtickerCluster[d.Item1][0]; // iconum
							//y = pgtickerCluster[d.Item1][1]; // was ticker


						}
						//doc.Add(d.Item2);
						//n.Documents.Add(doc);
						// Item4 - SDB, Item2 = raw label, Item3 = value, Item1 = DocID
						var sdb = "x";
						if (!string.IsNullOrWhiteSpace(d.Item4)) {
							sdb = "SDB: " + d.Item4;
						}
						Tuple<string, string, string, Guid, string> tuple = new Tuple<string, string, string, Guid, string>(sdb, d.Item2, d.Item3 ?? "NA", d.Item1, "Iconum: " + z);
						n.DocumentTuples.Add(tuple);

					}
					foreach (var dt in n.DocumentTuples.OrderBy(x => x.Item1)) {
						n.Documents.Add(dt.ToString());
					}
					n.Documents.Add(string.Format("{0}[{1}]", "", n.Title));
					if (pgdocumentCluster.ContainsKey(n.Id)) {
						var set = new HashSet<Guid>();
						foreach (var g in pgdocumentCluster2[n.Id]) {
							set.Add(g.Item1);
						}
						n.Title += string.Format(" ({0})", set.Count);
					}
				} else if (pgdocumentCluster2.ContainsKey(n.Id)) {
					foreach (var d in pgdocumentCluster2[n.Id]) {
						var doc = new List<string>();

						string z = "";
						string y = "";
						//doc.Add(d.ToString());
						if (pgtickerCluster.ContainsKey(d.Item1)) // d.Item1 is DocID
                    {
							//foreach(var t in tickerCluster[d.Item1])
							//{
							//    doc.Add(t);

							//}
							z = pgtickerCluster[d.Item1][0]; // iconum
							//y = pgtickerCluster[d.Item1][1]; // was ticker


						}
						//doc.Add(d.Item2);
						//n.Documents.Add(doc);
						Tuple<string, string, string, Guid, string> tuple = new Tuple<string, string, string, Guid, string>("y ", d.Item2, "Missing", d.Item1, "Iconum: " + z);
						n.DocumentTuples.Add(tuple);

					}
					foreach (var dt in n.DocumentTuples.OrderBy(x => x.Item1)) {
						n.Documents.Add(dt.ToString());
					}
					n.Documents.Add(string.Format("{0}[{1}]", "", n.Title));
					var set = new HashSet<Guid>();
					foreach (var g in pgdocumentCluster2[n.Id]) {
						set.Add(g.Item1);
					}
					n.Title += string.Format(" ({0})", set.Count);
				} else {
					//n.Title += string.Format(" (Cid: {0})", n.Id);
				}
				foreach (var child in n.Nodes) {
					PGnodeDocuments2(child, string.Format("{0}[{1}]", hiearchy, nTitle));
				}
				//foreach (var child in n.Nodes)
				//{
				//    n.Childrentitle += childrenTitle(child);
				//}
				return n;
			} catch (Exception ex) {
				Console.WriteLine("");
			}
			return null;
		}
		private Node nodeDocuments(Node n, string hiearchy = "") {
			if (n.Documents == null) {
				n.Documents = new List<string>();
			}
			if (n.DocumentTuples == null) {
				n.DocumentTuples = new List<Tuple<string, string, string, Guid, string>>();
			}
			if (documentCluster == null || documentCluster.Count < 1) {
				initDocumentCluster();
				initTickerCluster();
			}
			var nTitle = n.Title;
			if (string.IsNullOrWhiteSpace(n.Childrentitle)) {
				n.Childrentitle = "";
			}
			n.Childrentitle += nTitle;
			if (documentCluster.ContainsKey(n.Id)) {

				foreach (var d in documentCluster[n.Id]) {
					var doc = new List<string>();

					string z = "";
					string y = "";
					//doc.Add(d.ToString());
					if (tickerCluster.ContainsKey(d.Item1)) {
						//foreach(var t in tickerCluster[d.Item1])
						//{
						//    doc.Add(t);

						//}
						z = tickerCluster[d.Item1][0];
						y = tickerCluster[d.Item1][1];


					}
					//doc.Add(d.Item2);
					//n.Documents.Add(doc);
					Tuple<string, string, string, Guid, string> tuple = new Tuple<string, string, string, Guid, string>(y, d.Item2, d.Item3, d.Item1, z);
					n.DocumentTuples.Add(tuple);

				}
				foreach (var dt in n.DocumentTuples.OrderBy(x => x.Item1)) {
					n.Documents.Add(dt.ToString());
				}
				n.Documents.Add(string.Format("{0}[{1}]", "", n.Title));
				var set = new HashSet<Guid>();
				foreach (var g in documentCluster[n.Id]) {
					set.Add(g.Item1);
				}
				n.Title += string.Format(" ({0})", set.Count);

			} else {
				n.Title += string.Format(" (Cid: {0})", n.Id);
			}
			foreach (var child in n.Nodes) {
				nodeDocuments(child, string.Format("{0}[{1}]", hiearchy, nTitle));
			}
			//foreach (var child in n.Nodes)
			//{
			//    n.Childrentitle += childrenTitle(child);
			//}
			return n;
		}

		public string childrenTitle(Node n, string hiearchy = "") {
			string s = "";
			foreach (var child in n.Nodes) {
				//n.Childrentitle += childrenTitle(child);
				s += n.Childrentitle;
			}
			s += n.Title;
			return s;
		}

		public string GetSegmentTree(string treeName) {
			const string query = @"
;WITH CteTables
AS
(
    SELECT p.ID, p.DisplayName, p.ParentID
    FROM NameTree (nolock) AS p
   WHERE DisplayName = @treeName and parentid is null
    
	UNION ALL
    
	SELECT child.ID, child.DisplayName,  child.ParentID
    FROM NameTree (nolock) AS child
	INNER JOIN CteTables as p
		ON child.ParentID = p.id and child.ParentID != child.ID  
)
 
SELECT ID, DisplayName, ParentID
FROM CteTables order by parentid
			";
			try {
				List<Node> allNodes = new List<Node>();
				using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {
					using (SqlCommand cmd = new SqlCommand(query, conn)) {
						conn.Open();
						cmd.Parameters.AddWithValue("@treeName", treeName);

						using (SqlDataReader sdr = cmd.ExecuteReader()) {
							while (sdr.Read()) {
								Node n = new Node();
								n.Id = sdr.GetInt16(0);
								n.Title = sdr.GetString(1);
								n.ParentId = sdr.IsDBNull(2) ? -1 : sdr.GetInt16(2);
								n.Nodes = new List<Node>();
								allNodes.Add(n);
							}
						}
					}
				}
				List<Node> nodes = new List<Node>();
				var rootnodes = allNodes.Where(x => x.ParentId == null || x.ParentId.Value < 0);
				nodes.AddRange(rootnodes);
				foreach (var n in allNodes) {
					if (nodes.Contains(n)) continue;
					var parentNode = allNodes.FirstOrDefault(x => x.Id == n.ParentId.Value);
					if (parentNode != null) {
						if (parentNode.Nodes == null) parentNode.Nodes = new List<Node>();
						parentNode.Nodes.Add(n);
					}
				}

				return JsonConvert.SerializeObject(nodes);

			} catch (Exception ex) {
				List<Node> errorNodes = new List<Node>();
				Node errorNode = new Node() { Id = 0, Title = "Error", Nodes = new List<Node>() };
				errorNodes.Add(errorNode);
				return JsonConvert.SerializeObject(errorNodes);
			}

		}
		public class TreeViewJSNode {
			[JsonProperty("id")]
			public int Id { get; set; }
			[JsonProperty("label")]
			public string Title { get; set; }
			[JsonIgnore]
			public int? ParentId { get; set; }

			[JsonProperty("children")]
			public List<TreeViewJSNode> Nodes { get; set; }
		}
		public string GetTreeViewJS(string treeName) {
			const string query = @"
;WITH CteTables
AS
(
    SELECT p.ID, p.DisplayName, p.ParentID
    FROM NameTree (nolock) AS p
   WHERE DisplayName = @treeName and parentid is null
    
	UNION ALL
    
	SELECT child.ID, child.DisplayName,  child.ParentID
    FROM NameTree (nolock) AS child
	INNER JOIN CteTables as p
		ON child.ParentID = p.id and child.ParentID != child.ID  
)
 
SELECT ID, DisplayName, ParentID
FROM CteTables order by parentid
			";
			try {
				List<TreeViewJSNode> allNodes = new List<TreeViewJSNode>();
				using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {
					using (SqlCommand cmd = new SqlCommand(query, conn)) {
						conn.Open();
						cmd.Parameters.AddWithValue("@treeName", treeName);

						using (SqlDataReader sdr = cmd.ExecuteReader()) {
							while (sdr.Read()) {
								var n = new TreeViewJSNode();
								n.Id = sdr.GetInt16(0);
								n.Title = sdr.GetString(1);
								n.ParentId = sdr.IsDBNull(2) ? -1 : sdr.GetInt16(2);
								n.Nodes = new List<TreeViewJSNode>();
								allNodes.Add(n);
							}
						}
					}
				}
				List<TreeViewJSNode> nodes = new List<TreeViewJSNode>();
				var rootnodes = allNodes.Where(x => x.ParentId == null || x.ParentId.Value < 0);
				nodes.AddRange(rootnodes);
				foreach (var n in allNodes) {
					if (nodes.Contains(n)) continue;
					var parentNode = allNodes.FirstOrDefault(x => x.Id == n.ParentId.Value);
					if (parentNode != null) {
						if (parentNode.Nodes == null) parentNode.Nodes = new List<TreeViewJSNode>();
						parentNode.Nodes.Add(n);
					}
				}

				return JsonConvert.SerializeObject(nodes);

			} catch (Exception ex) {
				List<TreeViewJSNode> errorNodes = new List<TreeViewJSNode>();
				TreeViewJSNode errorNode = new TreeViewJSNode() { Id = 0, Title = "Error", Nodes = new List<TreeViewJSNode>() };
				errorNodes.Add(errorNode);
				return JsonConvert.SerializeObject(errorNodes);
			}

		}

		public class CollectedValue {
			[JsonProperty("ItemCode")]
			public string ItemCode { get; set; }
			[JsonProperty("ItemName")]
			public string ItemName { get; set; }
			[JsonProperty("SourceLinkID")]
			public string SourceLinkID { get; set; }
			[JsonProperty("DataSource")]
			public string DataSource { get; set; }
		}

		private string GetURL(string url) {
			return GetTintFile(url);
		}

		static bool _gdbOnOff = false;
		public string GdbBackfillOff() {
			if (_gdbOnOff) {
				_gdbOnOff = !_gdbOnOff;
			}
			return _gdbOnOff.ToString();
		}
		public string GdbBackfillOn() {
			if (!_gdbOnOff) {
				_gdbOnOff = !_gdbOnOff;
				while (_gdbOnOff) {
					GdbBackfill(1, false, 1800);
				}
				return "";
			} else {
				return _gdbOnOff.ToString();
			}
		}

		public string GdbBackfill(int maxThread = 10, bool retry = false, int tries = 100) {
			StringBuilder sb = new StringBuilder();
			string sql = @"
            Select top 1 documentid, fileid, iconum from GDBBackfill where isStart = 0 and isEnd =0;
";
			string sql_retry = @"
            Select top 1 documentid, fileid, iconum from GDBBackfill where isStart = 1 and isEnd =0;
";
			string update_start_sql = @"
            update GDBBackfill set isStart = 1 where DocumentID = @DocumentID
";
			string update_end_sql = @"
            update GDBBackfill set isEnd = 1 where DocumentID = @DocumentID
";
			if (retry) {
				sql = sql_retry;
			}
			var threadList = new List<Task>();
			var guidList = new List<Guid>();
			List<string> messages = new List<string>();
			for (int i = 0; i < maxThread; i++) {
				using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {
					conn.Open();
					Guid docID = new Guid();

					int fileId = 0;
					int iconum = 0;
					using (SqlCommand cmd = new SqlCommand(sql, conn)) {
						using (SqlDataReader sdr = cmd.ExecuteReader()) {
							while (sdr.Read()) {
								docID = sdr.GetGuid(0);
								fileId = sdr.GetInt32(1);
								iconum = sdr.GetInt32(2);

							}
							guidList.Add(docID);
						}
					}
					using (SqlCommand cmd = new SqlCommand(update_start_sql, conn)) {
						cmd.Parameters.AddWithValue("@DocumentID", docID);
						using (SqlDataReader sdr = cmd.ExecuteReader()) {
							while (sdr.Read()) {
							}
						}
					}
					threadList.Add(Task.Run(() => InsertGdbCommitKVP(docID, fileId, tries, iconum)).ContinueWith(u => messages.Add(u.Result)));
				}
			}
			foreach (var t in threadList) {
				t.Wait();
			}
			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {
				conn.Open();
				foreach (var g in guidList) {
					if (!messages.Contains(g.ToString())) {
						continue;
					}
					using (SqlCommand cmd = new SqlCommand(update_end_sql, conn)) {
						cmd.Parameters.AddWithValue("@DocumentID", g);
						using (SqlDataReader sdr = cmd.ExecuteReader()) {
							while (sdr.Read()) {
							}
						}
					}
					sb.Append(g.ToString() + ",");
				}
			}
			foreach (var v in messages) {
				sb.Append(v + "*");
			}
			return sb.ToString();
		}
		private string InsertGdbCommitKVP(Guid guid, int i, int tries = 100, int iconum = 0) {
			var r = InsertGdbCommit(guid, i, tries, iconum);
			if (r == "true") {
				return guid.ToString();
			} else {
				return r;
			}
		}
		public string InsertGdbFake(Guid DamDocumentID) {
			return InsertGdb(new Guid("978dfe58-c4a2-e311-9b0b-1cc1de2561d4"), 92);
		}
		public string InsertGdbCommit(Guid DamDocumentID, int fileId, int tries = 100, int iconum = 0) {
			StringBuilder psb = new StringBuilder();
			psb.AppendLine("StartCommit. " + DamDocumentID.ToString() + " " + DateTime.UtcNow.ToString());
			string strResult = "";

			try {
				psb.AppendLine("Ln794." + DateTime.UtcNow.ToString());
				strResult = InsertGdb(DamDocumentID, fileId, "COMMIT TRAN;", tries, iconum);
				psb.AppendLine("Ln796." + DateTime.UtcNow.ToString());
				if (strResult.Length < 20) {
					return strResult;
				} else {
					psb.AppendLine("Ln803." + DateTime.UtcNow.ToString());
					using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {

						using (SqlCommand cmd = new SqlCommand(strResult, conn)) {
							cmd.CommandTimeout = 600;
							conn.Open();
							psb.AppendLine("Ln811." + DateTime.UtcNow.ToString());
							using (SqlDataReader sdr = cmd.ExecuteReader()) {
								psb.AppendLine("Ln814." + DateTime.UtcNow.ToString());
								if (sdr.Read()) {
									if (sdr.GetString(0) == "commit") {
										psb.AppendLine("Ln819." + DateTime.UtcNow.ToString());
										try {
											AsReportedTemplateHelper.SendEmail("InsertGdb Outer performance", psb.ToString());
										} catch { }
										return "true";
									}
								}
							}
						}
						try {
							AsReportedTemplateHelper.SendEmail("InsertGdb Outer performance", psb.ToString());
						} catch { }
						return "error executing sql";
					}
				}
			} catch (Exception ex) {
				string inner = "";
				if (ex.InnerException != null) {
					inner = ex.InnerException.Message;
				}
				psb.AppendLine("Ln842." + DateTime.UtcNow.ToString());
				AsReportedTemplateHelper.SendEmail("InsertGdbCommit Failure", DamDocumentID.ToString() + ex.Message + ex.StackTrace + psb.ToString());
				return "InsertGdbCommit" + ex.Message;
			}
		}
		public string InsertGdb(Guid DamDocumentID, int fileId, string successAction = "ROLLBACK TRAN;", int tries = 100, int iconum = 0) {

			StringBuilder psb = new StringBuilder();
			psb.AppendLine("Start. " + DamDocumentID.ToString() + " " + DateTime.UtcNow.ToString());
			string tintURL = @"http://auto-tablehandler-staging.factset.io/queue/document/978dfe58-c4a2-e311-9b0b-1cc1de2561d4/92";
			tintURL = @"http://chai-auto.factset.io/bank/abs?source_document_id=978dfe58-c4a2-e311-9b0b-1cc1de2561d4&source_file_id=76&iconum=24530";

			string urlPattern = @"http://auto-tablehandler-staging.factset.io/queue/document/{0}/{1}";
			urlPattern = @"http://chai-auto.factset.io/bank/abs?source_document_id={0}&source_file_id={1}&iconum={2}";
			string url = String.Format(urlPattern, DamDocumentID.ToString().ToUpper(), fileId, iconum);
			bool isTryCached = false;
			string cachedURL = "";
			try {
				cachedURL = System.Web.HttpContext.Current.Request.QueryString["result_url"];
				if (!string.IsNullOrEmpty(cachedURL)) {
					isTryCached = true;
				}
			} catch (Exception ex) {
				string debug = ex.Message;
			}
			//url = tintURL;
			TintInfo tintInfo = null;
			while (tries > 0) {
				try {
					string outputresult = "";
					if (isTryCached) {
						psb.AppendLine("Ln847." + DateTime.UtcNow.ToString());
						outputresult = GetTintFile(cachedURL);
						psb.AppendLine("Ln848." + DateTime.UtcNow.ToString());
					} else {
						psb.AppendLine("Ln849." + DateTime.UtcNow.ToString());
						outputresult = GetTintFile(url);
						psb.AppendLine("Ln851." + DateTime.UtcNow.ToString());
					}
					var settings = new JsonSerializerSettings { Error = (se, ev) => { ev.ErrorContext.Handled = true; } };
					tintInfo = Newtonsoft.Json.JsonConvert.DeserializeObject<TintInfo>(outputresult, settings);
					if (tintInfo == null) {
						throw new Exception("failed to get tint 1");
					}
					tries = -1;
				} catch (FileNotFoundException ex) {
					if (isTryCached) {
						isTryCached = false;
					} else {
						tries = 0;
					}
				} catch (Exception ex) {
					if (--tries > 0) {
						isTryCached = false;
						System.Threading.Thread.Sleep(4000);
					}

				}
			}
			psb.AppendLine("Ln869." + DateTime.UtcNow.ToString());
			if (tintInfo == null) {
				return "failed to get tint";
			}
			StringBuilder sb = new StringBuilder();
			sb.AppendLine("BEGIN TRY");
			sb.AppendLine("BEGIN TRAN");
			string s = @"
Declare @DamDocument UNIQUEIDENTIFIER = '{0}'
DECLARE @DocumentSeriesID INT
DECLARE @TableTypeID INT  
DECLARE @SfDocumentID UNIQUEIDENTIFIER 
select @SfDocumentID = ID, @DocumentSeriesID = DocumentSeriesID from Document WITH (NOLOCK) where DAMDocumentId = @DamDocument

DECLARE @GDBCodes TABLE (
    [FakeId] [bigint] IDENTITY(-1,-1) NOT NULL,
	[Description] [varchar](4096) NULL,
    [Section] [varchar](32) NULL,
    [Industry] [varchar](256) NULL
	);

DECLARE @TaggedItems TABLE (
	[DocumentId] [uniqueidentifier] NULL,
	[XBRLTag] [varchar](4096) NULL,
	[Offset] [varchar](50) NULL,
    [CellDate] datetime NULL,
	[Value] [nvarchar](500) NULL,
	[Label] [varchar](4096) NULL,
	[GDBTableId] [bigint] NULL,
	[XBRLTitle] [varchar](4096) NULL,
	[ColumnHeader] [varchar](4096) NULL
	);

DECLARE @MatchingID TABLE(
[RealId] [bigint] NOT NULL,
[FakeId] [bigint] NOT NULL
);

DECLARE @gdbID INT;
";

			string wrapup = @"
 MERGE INTO GDBCodes_1106 USING @GDBCodes AS temp ON 1 = 0
WHEN NOT MATCHED THEN
    INSERT (Description, Section, Industry)
    VALUES (temp.Description, temp.Section, temp.Industry)
    OUTPUT inserted.id, temp.FakeID
    INTO @MatchingID (RealID, FakeID);

UPDATE  ti 
SET ti.GDBTableID = m.realID
FROM @TaggedItems ti 
JOIN @MatchingID m on ti.GDBTableId = m.fakeID


INSERT GDBTaggedItems_1106 (DocumentId,XBRLTag,Offset,CellDate, Value,Label,GDBTableId,XBRLTitle,ColumnHeader)
Select DocumentId,XBRLTag,Offset,CellDate, Value,Label,GDBTableId,XBRLTitle,ColumnHeader from @TaggedItems
";

			sb.AppendLine(string.Format(s, DamDocumentID.ToString()));
			int count = 0;
			List<int> addedDts = new List<int>();
			psb.AppendLine("Ln930." + DateTime.UtcNow.ToString());
			foreach (var table in tintInfo.Tables) {
				//    if (!new string[] { "IS", "BS", "CF" }.Contains(table.Type)) continue;
				//if (count > 2) break;
				// Insert DocumentTable
				count++;

				List<int> addedRow = new List<int>();
				List<int> addedCol = new List<int>();
				int dtsCount = 0;
				foreach (var value in table.Values) {
					if (string.IsNullOrWhiteSpace(value.Offset)) continue;

					string addGDB = @"
SET @gdbID = null;
select TOP 1 @gdbID = ID FROM GDBCodes_1106  WITH (NOLOCK) WHERE Description = '{0}' and Section = '{1}' and Industry = 'Bank';
IF @gdbID is NULL
BEGIN
    select TOP 1 @gdbID = FakeId FROM @GDBCodes WHERE Description = '{0}' and Section = '{1}' and Industry = 'Bank';
END
IF @gdbID is NULL
BEGIN
    Insert into @GDBCodes
    (Description, Section, Industry) 
    values ('{0}', '{1}', 'BANK');
    select @gdbID = scope_identity();
END
";
					string addTagged = @"
IF NOT EXISTS (SELECT 1 FROM GDBTaggedItems_1106 WITH (NOLOCK) WHERE DocumentId = @DamDocument and XBRLTag ='{0}' and  Offset = '{1}' and GDBTableId = @gdbID)
BEGIN
    INSERT @TaggedItems (DocumentId,XBRLTag,Offset,CellDate,Value,Label,GDBTableId,XBRLTitle, ColumnHeader)
    VALUES (@DamDocument, '{0}', '{1}', {2}, '{3}', '{4}', @gdbID, '{5}', '{6}')


END
";
					string addSprocGdb = @"EXEC GdbUpdateCodes @DamDocument, '{0}', '{1}', '{2}', '{3}', '{4}', '{5}'";
					string label = "";
					var selectedCell = table.Cells.FirstOrDefault(u => u.offset == value.Offset);
					if (selectedCell != null) {
						var row = table.Rows.FirstOrDefault(v => v.Id == selectedCell.rowId);
						if (row != null) {
							label = row.Label;
						} else {
							label = value.XbrlTag;
						}
					} else {
						label = value.XbrlTag;
					}
					label = Truncate(label, 4000);
					string columnHeader = "";
					if (selectedCell != null) {
						var col = table.Columns.FirstOrDefault(v => v.Id == selectedCell.columnId);
						if (col != null) {
							columnHeader = col.columnHeader;
						}
					}
					columnHeader = Truncate(columnHeader, 4000);
					string xbrl = value.XbrlTag;
					if (string.IsNullOrWhiteSpace(xbrl)) {
						xbrl = "";
					}
					string xbrlTableTitle = table.XbrlTableTitle;
					if (string.IsNullOrWhiteSpace(xbrlTableTitle)) {
						xbrlTableTitle = "";
					}
					string cellDate = value.Date;
					DateTime dateTime;
					if (string.IsNullOrWhiteSpace(cellDate) && !DateTime.TryParse(cellDate, out dateTime)) {
						cellDate = "NULL";
					} else {
						cellDate = "'" + cellDate + "'";
					}
					sb.AppendLine(string.Format(addGDB, xbrl.Replace("'", "''"), table.Type.Replace("'", "''")));
					sb.AppendLine(string.Format(addTagged, xbrl.Replace("'", "''"), value.Offset, cellDate, value.OriginalValue, label.Replace("'", "''"), xbrlTableTitle.Replace("'", "''"), columnHeader.Replace(";", "''")));
				}

			}
			psb.AppendLine("Ln990." + DateTime.UtcNow.ToString());
			sb.AppendLine(wrapup);
			sb.AppendLine("select 'commit';");
			sb.AppendLine(successAction);
			sb.AppendLine("END TRY");
			sb.AppendLine("BEGIN CATCH");
			string err = @"
       SELECT  
            ERROR_NUMBER() AS ErrorNumber  
            ,ERROR_SEVERITY() AS ErrorSeverity  
            ,ERROR_STATE() AS ErrorState  
            ,ERROR_PROCEDURE() AS ErrorProcedure  
            ,ERROR_LINE() AS ErrorLine  
            ,ERROR_MESSAGE() AS ErrorMessage;  
";
			sb.AppendLine(err);
			sb.AppendLine("select 'rollback'; ROLLBACK TRAN;");
			sb.AppendLine("END CATCH");

			string retVal = sb.ToString();
			psb.AppendLine("Ln1010." + DateTime.UtcNow.ToString());
			try {
				AsReportedTemplateHelper.SendEmail("InsertGdb Inner performance", psb.ToString());
			} catch { }
			return retVal;
		}
		public static String Truncate(String input, int maxLength) {
			if (string.IsNullOrWhiteSpace(input)) {
				input = "";
			}
			if (input.Length > maxLength)
				return input.Substring(0, maxLength);
			return input;
		}

		public string InsertKpiFake(Guid DamDocumentID) {
			string tintURL = @"http://chai-auto.factset.io/queue/bank?source_document_id=978dfe58-c4a2-e311-9b0b-1cc1de2561d4&source_file_id=76&iconum=24530";

			string urlPattern = @"http://auto-tablehandler-dev.factset.io/document/{0}/0";
			string url = String.Format(urlPattern, DamDocumentID);
			url = tintURL;

			int tries = 3;
			List<Node> nodes = new List<Node>();
			TintInfo tintInfo = null;
			while (tries > 0) {
				try {
					var outputresult = GetTintFile(url);
					var settings = new JsonSerializerSettings { Error = (se, ev) => { ev.ErrorContext.Handled = true; } };
					tintInfo = Newtonsoft.Json.JsonConvert.DeserializeObject<TintInfo>(outputresult, settings);
					tries = -1;
				} catch (Exception ex) {
					if (--tries > 0) {
						System.Threading.Thread.Sleep(6000);
					}

				}
			}
			if (tintInfo == null) {
				return "failed to get tint";
			}
			StringBuilder sb = new StringBuilder();
			sb.AppendLine("SET TRANSACTION ISOLATION LEVEL SNAPSHOT;");
			sb.AppendLine("BEGIN TRY");
			sb.AppendLine("BEGIN TRAN");
			string s = @"
Declare @DamDocument UNIQUEIDENTIFIER = '978DFE58-C4A2-E311-9B0B-1CC1DE2561D4'
DECLARE @DocumentSeriesID INT = 2129
DECLARE @TableTypeID INT  
DECLARE @SfDocumentID UNIQUEIDENTIFIER 
select @SfDocumentID = ID, @DocumentSeriesID = DocumentSeriesID from Document where DAMDocumentId = @DamDocument
DECLARE @TdColId INT
DECLARE @TdRowId INT 
DECLARE @dtsId INT 

select @TableTypeID = ID from TableType where Description = 'KPI' and DocumentSeriesID = @DocumentSeriesID;
IF @TableTypeId is NULL
BEGIN
	INSERT Tabletype(Description, DocumentSeriesID) values ('KPI', @DocumentSeriesID)
	select @TableTypeID = SCOPE_IDENTITY()
END
select * from TableType where id = @TableTypeId 

DECLARE @dtID INT
INSERT DocumentTable (DocumentID,TableOrganizationID,TableTypeID,Consolidated,Unit,ScalingFactorID,TableIntID,ExceptShare)
VALUES (@SfDocumentID, 1, @TableTypeId, 1, 'A', 'A', -1, 0) 
select @dtID = SCOPE_IDENTITY()
select * from DocumentTable where ID = @dtID

DECLARE  @TableDimension TABLE(ID INT, FakeID int, DimensionTypeID int, CompanyFinancialTermID int)
DECLARE  @DocumentTimeSlice TABLE(ID INT, FakeID int)
DECLARE @tcID INT
DECLARE @cftID int

";
			sb.AppendLine(s);
			int count = 0;
			List<int> addedDts = new List<int>();

			foreach (var table in tintInfo.Tables) {
				if (new string[] { "IS", "BS", "CF" }.Contains(table.Type)) continue;
				//if (count > 3) break;
				// Insert DocumentTable
				count++;
				string reset = @"
                DELETE FROM @TableDimension;

";
				sb.AppendLine(reset);
				List<int> addedRow = new List<int>();
				List<int> addedCol = new List<int>();
				int dtsCount = 0;
				foreach (var cell in table.Cells) {
					if (string.IsNullOrWhiteSpace(cell.offset)) continue;

					string tdRow = @"
IF NOT EXISTS (SELECT 1 FROM @TableDimension WHERE FakeID = {2} and DimensionTypeID = 1)
BEGIN
    Insert into CompanyFinancialterm
    (DocumentSeriesId, TermStatusId, Description, NormalizedFlag, EncoreTermFlag) 
    values (@DocumentSeriesID, 1, '{0}', 0, 3);
    select @cftID = scope_identity();

    INSERT TableDimension (DocumentTableID,DimensionTypeID,Label,OrigLabel,Location,EndLocation,Parent,InsertedRow,AdjustedOrder)
    OUTPUT inserted.id, {2}, 1, @cftID into @TableDimension
    VALUES (@dtID, {1}, '{0}', '{0}', 1, 2, NULL, 0, {2})
END
";
					string tdCol = @"
IF NOT EXISTS (SELECT 1 FROM @TableDimension WHERE FakeID = {2} and DimensionTypeID = 2)
BEGIN
    INSERT TableDimension (DocumentTableID,DimensionTypeID,Label,OrigLabel,Location,EndLocation,Parent,InsertedRow,AdjustedOrder)
    OUTPUT inserted.id, {2}, 2, 0 into @TableDimension
    VALUES (@dtID, {1}, '{0}', '{0}', 1, 2, NULL, 0, {2})


END
";

					string dts = @"
IF NOT EXISTS (SELECT 1 FROM @DocumentTimeSlice WHERE FakeID = {0})
BEGIN
  INSERT DocumentTimeSlice (DocumentId,DocumentSeriesId,TimeSlicePeriodEndDate,ReportingPeriodEndDate,FiscalDistance,Duration
    ,PeriodType,AcquisitionFlag,AccountingStandard,ConsolidatedFlag,IsProForma,IsRecap,CompanyFiscalYear,ReportType,IsAmended,IsRestated,IsAutoCalc,ManualOrgSet,TableTypeID)
OUTPUT inserted.id, {0} into @DocumentTimeSlice
  VALUES (@SfDocumentID, @DocumentSeriesID, {1}, {2}, 0, {3}
    , '{4}', NULL, 'US', 'C', 0, 0, {5}, 'F', 0, 0, 0, 0, @TableTypeID);
 
END
";
					var row = table.Rows.FirstOrDefault(x => x.Id == cell.rowId);
					if (!addedRow.Contains(row.Id)) {
						sb.AppendLine(string.Format(tdRow, row.Label, 1, row.Id));
						addedRow.Add(row.Id);
					}
					var col = table.Columns.FirstOrDefault(x => x.Id == cell.columnId);
					if (!addedCol.Contains(col.Id)) {
						sb.AppendLine(string.Format(tdCol, col.columnHeader, 2, col.Id));
						addedCol.Add(col.Id);
					}
					TimeSlice u = null;
					foreach (var ts in tintInfo.TimeSlices) {
						if (ts.Offsets.Contains(cell.offset)) {
							if (!addedDts.Contains(ts.FakeID)) {
								ts.FakeID = ++dtsCount;
								string strTimeSlicePeriodEndDate = ts.TimeSlicePeriodEndDate == null ? @"NULL" : string.Format(@"'{0}'", ts.TimeSlicePeriodEndDate.ToString());
								string strReportingPeriodEndDate = ts.ReportingPeriodEndDate == null ? @"NULL" : string.Format(@"'{0}'", ts.ReportingPeriodEndDate.ToString());

								sb.AppendLine(string.Format(dts, dtsCount, strTimeSlicePeriodEndDate, strReportingPeriodEndDate, ts.Duration,
										ts.PeriodType, ts.CompanyFiscalYear));
								addedDts.Add(ts.FakeID);
							}
							u = ts;
							break;
						}
					}
					string tc = @"
SELECT @cftiD = CompanyFinancialTermID, @TdRowid = ID FROM @TableDimension WHERE FakeID = {7} and DimensionTypeID = 1;
SELECT @TdColid = ID FROM @TableDimension WHERE FakeID = {8} and DimensionTypeID = 2;

INSERT TableCell(Offset,CellDate,Value,CompanyFinancialTermID,ValueNumeric,NormalizedNegativeIndicator,ScalingFactorID,ScarUpdated,IsIncomePositive,XBRLTag,DocumentId,Label)
VALUES ('{0}','{1}','{2}', @cftiD, '{3}', 0, '{4}', 0, 0, '{5}',@SfDocumentID, '{6}' );
select @tcID = SCOPE_IDENTITY();

INSERT DimensionToCell(TableDimensionID, TableCellID) VALUES (@TdRowid, @tcID);
INSERT DimensionToCell(TableDimensionID, TableCellID) VALUES (@TdColid, @tcID);

select @dtsId = ID From @DocumentTimeSlice where FakeID = {9};
INSERT DocumentTimeSliceTableCell(DocumentTimeSliceId, TableCellId) values (@dtsId, @tcID);
";

					var v = table.Values.FirstOrDefault(x => x.Offset == cell.offset);
					sb.AppendLine(string.Format(tc, v.Offset, v.Date, v.OriginalValue, v.NumericValue ?? "0", v.Scaling, v.XbrlTag, row.Label, row.Id, col.Id, u.FakeID));
					// Insert Table Dimension
					// Insert Table Cell
					// Insert DimensionToCell
				}

			}
			sb.AppendLine("select 'commit'; ROLLBACK TRAN;");
			sb.AppendLine("END TRY");
			sb.AppendLine("BEGIN CATCH");
			string err = @"
       SELECT  
            ERROR_NUMBER() AS ErrorNumber  
            ,ERROR_SEVERITY() AS ErrorSeverity  
            ,ERROR_STATE() AS ErrorState  
            ,ERROR_PROCEDURE() AS ErrorProcedure  
            ,ERROR_LINE() AS ErrorLine  
            ,ERROR_MESSAGE() AS ErrorMessage;  
";
			sb.AppendLine(err);
			sb.AppendLine("select 'rollback'; ROLLBACK TRAN;");
			sb.AppendLine("END CATCH");

			string retVal = sb.ToString();
			return retVal;
		}
		public string InsertKpiFakeWrong916(Guid DamDocumentID) {
			string tintURL = @"http://chai-auto.factset.io/queue/bank?source_document_id=978dfe58-c4a2-e311-9b0b-1cc1de2561d4&source_file_id=76&iconum=24530";
			string collectedValueURL = @"http://chai-auto.factset.io/bank/collected?source_document_id=978dfe58-c4a2-e311-9b0b-1cc1de2561d4&iconum=24530";

			string urlPattern = @"http://auto-tablehandler-dev.factset.io/document/{0}/0";
			string url = String.Format(urlPattern, DamDocumentID);
			url = tintURL;

			int tries = 3;
			List<Node> nodes = new List<Node>();
			TintInfo tintInfo = null;
			while (tries > 0) {
				try {
					var outputresult = GetTintFile(url);
					tintInfo = Newtonsoft.Json.JsonConvert.DeserializeObject<TintInfo>(outputresult);
					tries = -1;
				} catch (Exception ex) {
					if (--tries > 0) {
						System.Threading.Thread.Sleep(3000);
					}

				}
			}
			if (tintInfo == null) {
				return "false";
			}
			tries = 3;
			CollectedValue[] collectedValues = null;
			while (tries > 0) {
				try {
					var outputresult = GetURL(collectedValueURL);
					collectedValues = Newtonsoft.Json.JsonConvert.DeserializeObject<CollectedValue[]>(outputresult);
					tries = -1;
				} catch (Exception ex) {
					if (--tries > 0) {
						System.Threading.Thread.Sleep(1000);
					}

				}
			}
			if (collectedValues == null) {
				return "false";
			}
			StringBuilder sb = new StringBuilder();
			StringBuilder sbDimension = new StringBuilder();
			StringBuilder sbTableCell = new StringBuilder();
			sb.AppendLine("SET TRANSACTION ISOLATION LEVEL SNAPSHOT;");
			sb.AppendLine("BEGIN TRY");
			sb.AppendLine("BEGIN TRAN");
			int count = 0;
			foreach (var table in tintInfo.Tables) {
				if (count > 0) break;
				// Insert DocumentTable
				bool addDocumentTable = true;
				foreach (var cell in table.Cells) {
					if (collectedValues.FirstOrDefault(x => x.SourceLinkID == cell.offset) != null) {
						if (addDocumentTable) {
							count = 1;
							string s = @"
DECLARE @ChangeResult TABLE (ChangeType VARCHAR(10), TableType varchar(50), Id INTEGER)
Declare @DamDocument UNIQUEIDENTIFIER = '978DFE58-C4A2-E311-9B0B-1CC1DE2561D4'
DECLARE @DocumentSeriesID INT = 2129
DECLARE @TableTypeID INT  
DECLARE @SfDocumentID UNIQUEIDENTIFIER 
select @SfDocumentID = ID, @DocumentSeriesID = DocumentSeriesID from Document where DAMDocumentId = @DamDocument
DECLARE @TdColId INT
DECLARE @TdRowId INT 

select @TableTypeID = ID from TableType where Description = 'KPI' and DocumentSeriesID = @DocumentSeriesID;
IF @TableTypeId is NULL
BEGIN
	INSERT Tabletype(Description, DocumentSeriesID) values ('KPI', @DocumentSeriesID)
	select @TableTypeID = SCOPE_IDENTITY()
END
select * from TableType where id = @TableTypeId 

DECLARE @dtID INT
INSERT DocumentTable (DocumentID,TableOrganizationID,TableTypeID,Consolidated,Unit,ScalingFactorID,TableIntID,ExceptShare)
VALUES (@SfDocumentID, 1, @TableTypeId, 1, 'A', 'A', -1, 0) 
select @dtID = SCOPE_IDENTITY()
select * from DocumentTable where ID = @dtID

DECLARE  @TableDimension TABLE(ID INT, FakeID int, DimensionTypeID int, CompanyFinancialTermID int)
DECLARE @tcID INT
DECLARE @cftID int

";
							sb.AppendLine(s);
							addDocumentTable = false;
						}
						string tdRow = @"

Insert into CompanyFinancialterm
(DocumentSeriesId, TermStatusId, Description, NormalizedFlag, EncoreTermFlag) 
values (@DocumentSeriesID, 1, '{0}', 0, 3);
select @cftID = scope_identity();

INSERT TableDimension (DocumentTableID,DimensionTypeID,Label,OrigLabel,Location,EndLocation,Parent,InsertedRow,AdjustedOrder)
OUTPUT inserted.id, {2}, 1, @cftID into @TableDimension
VALUES (@dtID, {1}, '{0}', '{0}', 1, 2, NULL, 0, {2})

";
						string tdCol = @"
INSERT TableDimension (DocumentTableID,DimensionTypeID,Label,OrigLabel,Location,EndLocation,Parent,InsertedRow,AdjustedOrder)
OUTPUT inserted.id, {2}, 2, 0 into @TableDimension
VALUES (@dtID, {1}, '{0}', '{0}', 1, 2, NULL, 0, {2})
";
						var row = table.Rows.FirstOrDefault(x => x.Id == cell.rowId);
						sb.AppendLine(string.Format(tdRow, row.Label, 1, row.Id));
						var col = table.Columns.FirstOrDefault(x => x.Id == cell.columnId);
						sb.AppendLine(string.Format(tdCol, col.columnHeader, 2, col.Id));

						string tc = @"
SELECT @cftiD = CompanyFinancialTermID, @TdRowid = ID FROM @TableDimension WHERE FakeID = {7} and DimensionTypeID = 1;
SELECT @TdColid = ID FROM @TableDimension WHERE FakeID = {8} and DimensionTypeID = 2;

INSERT TableCell(Offset,CellDate,Value,CompanyFinancialTermID,ValueNumeric,NormalizedNegativeIndicator,ScalingFactorID,ScarUpdated,IsIncomePositive,XBRLTag,DocumentId,Label)
VALUES ('{0}','{1}','{2}', @cftiD, '{3}', 0, '{4}', 0, 0, '{5}',@SfDocumentID, '{6}' );
select @tcID = SCOPE_IDENTITY();

INSERT DimensionToCell(TableDimensionID, TableCellID) VALUES (@TdRowid, @tcID);
INSERT DimensionToCell(TableDimensionID, TableCellID) VALUES (@TdColid, @tcID);

";

						var v = table.Values.FirstOrDefault(x => x.Offset == cell.offset);
						sb.AppendLine(string.Format(tc, v.Offset, v.Date, v.OriginalValue, v.NumericValue, v.Scaling, v.XbrlTag, row.Label, row.Id, col.Id));
						// Insert Table Dimension
						// Insert Table Cell
						// Insert DimensionToCell
					}
				}
			}
			sb.AppendLine("select 'commit'; ROLLBACK TRAN;");
			sb.AppendLine("END TRY");
			sb.AppendLine("BEGIN CATCH");
			string err = @"
       SELECT  
            ERROR_NUMBER() AS ErrorNumber  
            ,ERROR_SEVERITY() AS ErrorSeverity  
            ,ERROR_STATE() AS ErrorState  
            ,ERROR_PROCEDURE() AS ErrorProcedure  
            ,ERROR_LINE() AS ErrorLine  
            ,ERROR_MESSAGE() AS ErrorMessage;  
";
			sb.AppendLine(err);
			sb.AppendLine("select 'rollback'; ROLLBACK TRAN;");
			sb.AppendLine("END CATCH");

			string retVal = sb.ToString();
			return retVal;
		}

		public class SDBNode {
			[JsonProperty("id")]
			public long Id { get; set; }
			[JsonProperty("xbrlTag")]
			public string XbrlTag { get; set; }
			[JsonProperty("count")]
			public int Count { get; set; }
			[JsonProperty("totalcount")]
			public int TotalCount { get; set; }
		}
		public class SDBValueNode {
			[JsonProperty("companyName")]
			public string CompanyName { get; set; }
			[JsonProperty("iconum")]
			public string Iconum { get; set; }
			[JsonProperty("xbrlTag")]
			public string XbrlTag { get; set; }
			[JsonProperty("count")]
			public string MaxValue { get; set; }
			[JsonProperty("totalcount")]
			public string DocumentId { get; set; }
		}
		public List<SDBNode> GetGDBCode(long sdbcode) {
			string sql = @"
select h.itemcode, ti.XBRLTag, count(distinct ti.DocumentId)
from history h with (nolock)
join SDBItem sdb with (nolock) on h.itemcode = sdb.sdbcode
left join TaggedItems ti with (nolock) on ti.DocumentId = h.DamDocumentId and h.offset = ti.offset
where h.ItemCode = @sdbcode
group by h.ItemCode, ti.XBRLTag
order by count(distinct ti.DocumentId) desc
";
			List<SDBNode> nodes = new List<SDBNode>();
			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {
				conn.Open();

				using (SqlCommand cmd = new SqlCommand(sql, conn)) {
					cmd.Parameters.AddWithValue("@sdbcode", sdbcode);
					using (SqlDataReader sdr = cmd.ExecuteReader()) {
						while (sdr.Read()) {
							SDBNode node = new SDBNode();
							string id = sdr.GetStringSafe(0);
							long sdb;
							if (!long.TryParse(id, out sdb)) {
								continue;
							}
							node.Id = sdb;
							string xbrltag = sdr.GetStringSafe(1);
							if (string.IsNullOrWhiteSpace(xbrltag)) {
								xbrltag = "NULL";
							}
							node.XbrlTag = xbrltag;
							node.Count = sdr.GetInt32(2);
							nodes.Add(node);
						}
					}
				}
			}
			int totalCount = 0;
			foreach (var n in nodes) {
				totalCount += n.Count;
			}
			foreach (var n in nodes) {
				n.TotalCount = totalCount;
			}
			return nodes;
		}
		public DataTable GetGDBCodeGrid(long sdbcode) {
			string sql = @"
exec GDBGetCodes @sdbcode
";
			List<SDBNode> nodes = new List<SDBNode>();
			List<SDBValueNode> valuenodes = new List<SDBValueNode>();
			DataTable table = new DataTable();
			DataRow countRow = null;



			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {
				conn.Open();

				using (SqlCommand cmd = new SqlCommand(sql, conn)) {
					cmd.Parameters.AddWithValue("@sdbcode", sdbcode);
					using (SqlDataReader sdr = cmd.ExecuteReader()) {
						DataColumn column = new DataColumn();
						column.DataType = Type.GetType("System.String");
						column.ColumnName = "Name";
						table.Columns.Add(column);
						while (sdr.Read()) {
							SDBNode node = new SDBNode();
							long sdb = sdr.GetInt64(1);
							//long sdb;
							//if (!long.TryParse(id, out sdb))
							//{
							//    continue;
							//}
							node.Id = sdb;
							string xbrltag = sdr.GetStringSafe(2);
							if (string.IsNullOrWhiteSpace(xbrltag)) {
								xbrltag = "NULL";
							}
							node.XbrlTag = xbrltag;
							node.Count = sdr.GetInt32(3);
							nodes.Add(node);
							column = new DataColumn();
							column.DataType = System.Type.GetType("System.String");
							column.ColumnName = xbrltag;
							table.Columns.Add(column);
						}
						sdr.NextResult();
						int totalColumn = nodes.Count;

						//for (int u = 0; u < totalColumn; u++)
						//{
						//    DataColumn column;

						//    // Create new DataColumn, set DataType, ColumnName and add to DataTable.    
						//    column = new DataColumn();
						//    column.DataType = System.Type.GetType("System.String");
						//    column.ColumnName = u.ToString();
						//    table.Columns.Add(column);
						//}
						DataRow row = table.NewRow();
						DataRow row2 = table.NewRow();
						countRow = row2;
						row["Name"] = "XBRL";
						row2["Name"] = "COUNT";
						for (int u = 0; u < totalColumn; u++) {
							row[nodes[u].XbrlTag] = nodes[u].XbrlTag;
							row2[nodes[u].XbrlTag] = string.Format("{0}/{1}", nodes[u].Count, nodes[u].TotalCount);
						}
						table.Rows.Add(row);
						table.Rows.Add(row2);
						while (sdr.Read()) {
							SDBValueNode node = new SDBValueNode();
							node.CompanyName = sdr.GetStringSafe(0);
							node.Iconum = sdr.GetInt32(1).ToString();
							node.DocumentId = sdr.GetGuid(2).ToString();
							node.XbrlTag = sdr.GetStringSafe(3);
							node.MaxValue = sdr.GetStringSafe(4);
							if (valuenodes.FirstOrDefault(x => x.Iconum == node.Iconum) == null) {
								row = table.NewRow();
								row["Name"] = node.CompanyName;
								for (int u = 1; u <= totalColumn; u++) {
									row[u] = "";
								}
								table.Rows.Add(row);
							}
							valuenodes.Add(node);
							row = null;
							foreach (DataRow r in table.Rows) {
								if (r["Name"].ToString() == node.CompanyName) {
									row = r;
									break;
								}
							}
							if (row != null) {
								row[node.XbrlTag] = node.MaxValue;

							}
							//DataColumn column = table.Columns.IndexOf(node.XbrlTag)

						}

					}
				}
			}
			int totalCount = 0;
			foreach (var n in nodes) {
				totalCount += n.Count;
			}
			foreach (var n in nodes) {
				countRow[n.XbrlTag] = string.Format("{0}/{1}", n.Count, totalCount);
			}

			return table;
		}

		public DataTable GetGDBCodeGridForIconum(long sdbcode, int iconum, Guid? DocumentID = null) {
			string sql = @"
exec GDBGetCodesForIconum @sdbcode, @iconum, @documentId
";
			List<SDBNode> nodes = new List<SDBNode>();
			List<SDBValueNode> valuenodes = new List<SDBValueNode>();
			DataTable table = new DataTable();
			DataRow countRow = null;

			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {
				conn.Open();

				using (SqlCommand cmd = new SqlCommand(sql, conn)) {
					cmd.Parameters.AddWithValue("@sdbcode", sdbcode);
					cmd.Parameters.AddWithValue("@iconum", iconum);
					if (DocumentID.HasValue) {
						cmd.Parameters.AddWithValue("@documentId", DocumentID.Value);
					} else {
						cmd.Parameters.AddWithValue("@documentId", DBNull.Value);
					}
					using (SqlDataReader sdr = cmd.ExecuteReader()) {
						DataColumn column = new DataColumn();
						column.DataType = Type.GetType("System.String");
						column.ColumnName = "Name";
						table.Columns.Add(column);
						while (sdr.Read()) {
							SDBNode node = new SDBNode();
							long sdb = sdr.GetInt64(1);
							//long sdb;
							//if (!long.TryParse(id, out sdb))
							//{
							//    continue;
							//}
							node.Id = sdb;
							string xbrltag = sdr.GetStringSafe(2);
							if (string.IsNullOrWhiteSpace(xbrltag)) {
								xbrltag = "NULL";
							}
							node.XbrlTag = xbrltag;
							node.Count = sdr.GetInt32(3);
							nodes.Add(node);
							column = new DataColumn();
							column.DataType = System.Type.GetType("System.String");
							column.ColumnName = xbrltag;
							table.Columns.Add(column);
						}
						sdr.NextResult();
						int totalColumn = nodes.Count;

						//for (int u = 0; u < totalColumn; u++)
						//{
						//    DataColumn column;

						//    // Create new DataColumn, set DataType, ColumnName and add to DataTable.    
						//    column = new DataColumn();
						//    column.DataType = System.Type.GetType("System.String");
						//    column.ColumnName = u.ToString();
						//    table.Columns.Add(column);
						//}
						DataRow row = table.NewRow();
						DataRow row2 = table.NewRow();
						countRow = row2;
						row["Name"] = "XBRL";
						row2["Name"] = "COUNT";
						for (int u = 0; u < totalColumn; u++) {
							row[nodes[u].XbrlTag] = nodes[u].XbrlTag;
							row2[nodes[u].XbrlTag] = string.Format("{0}/{1}", nodes[u].Count, nodes[u].TotalCount);
						}
						table.Rows.Add(row);
						table.Rows.Add(row2);
						while (sdr.Read()) {
							SDBValueNode node = new SDBValueNode();
							node.CompanyName = sdr.GetStringSafe(0);
							node.Iconum = sdr.GetInt32(1).ToString();
							node.DocumentId = sdr.GetGuid(2).ToString();
							node.XbrlTag = sdr.GetStringSafe(3);
							node.MaxValue = sdr.GetStringSafe(4);
							if (valuenodes.FirstOrDefault(x => x.DocumentId == node.DocumentId) == null) {
								row = table.NewRow();
								row["Name"] = string.Format("{0} ({1})", node.CompanyName, node.DocumentId);
								for (int u = 1; u <= totalColumn; u++) {
									row[u] = "";
								}
								table.Rows.Add(row);
							}
							valuenodes.Add(node);
							row = null;
							foreach (DataRow r in table.Rows) {
								if (r["Name"].ToString() == string.Format("{0} ({1})", node.CompanyName, node.DocumentId)) {
									row = r;
									break;
								}
							}
							if (row != null) {
								row[node.XbrlTag] = node.MaxValue;

							}
							//DataColumn column = table.Columns.IndexOf(node.XbrlTag)

						}

					}
				}
			}
			int totalCount = 0;
			foreach (var n in nodes) {
				totalCount += n.Count;
			}
			foreach (var n in nodes) {
				countRow[n.XbrlTag] = string.Format("{0}/{1}", n.Count, totalCount);
			}

			return table;
		}

		public DataTable GetGDBCountForIconum(long sdbcode, int? iconum = null) {
			string sql = @"
exec GDBGetCountForIconum @sdbcode, @iconum
";
			List<SDBNode> nodes = new List<SDBNode>();
			List<SDBValueNode> valuenodes = new List<SDBValueNode>();
			DataTable table = new DataTable();
			DataRow countRow = null;



			using (SqlConnection conn = new SqlConnection(_sfConnectionString)) {
				conn.Open();

				using (SqlCommand cmd = new SqlCommand(sql, conn)) {
					cmd.Parameters.AddWithValue("@sdbcode", sdbcode);
					if (iconum.HasValue) {
						cmd.Parameters.AddWithValue("@iconum", iconum);
					} else {
						cmd.Parameters.AddWithValue("@iconum", DBNull.Value);
					}
					using (SqlDataReader sdr = cmd.ExecuteReader()) {
						DataColumn column = new DataColumn();
						column.DataType = Type.GetType("System.String");
						column.ColumnName = "Expression";
						table.Columns.Add(column);

						DataColumn column2 = new DataColumn();
						column2.DataType = Type.GetType("System.String");
						column2.ColumnName = "NoOfGDBs";
						table.Columns.Add(column2);

						DataColumn column3 = new DataColumn();
						column3.DataType = Type.GetType("System.String");
						column3.ColumnName = "Occurrence";
						table.Columns.Add(column3);

						while (sdr.Read()) {
							var expression = sdr.GetStringSafe(0);
							var noOfGDB = sdr.GetInt32(1);
							var occurence = sdr.GetInt32(2);
							DataRow row = table.NewRow();
							row["Expression"] = expression;
							row["NoOfGDBs"] = noOfGDB.ToString();
							row["Occurrence"] = occurence.ToString();
							table.Rows.Add(row);
						}

					}
				}
			}
			return table;
		}

		static Guid NullGuid = new Guid();
		public bool ExtendClusterByIconum(List<int> contentSetIds, int iconum) {
            this._environment = "DEV";
            List<int> iconums = new List<int>();
			iconums.Add(iconum);
			//List<int> iconums = new List<int>() { 18119 };
			_ExtendHierarchy(contentSetIds, iconums, NullGuid);
            if (this._autoclusteringfailure)
            {
                return false;
            }
            _ExtendColumns(contentSetIds, iconums, NullGuid);
            if (this._autoclusteringfailure)
            {
                return false;
            }
            return true;
		}
		public string ExtendClusterByDocument(List<int> contentSetIds, int iconum, Guid docid, int tableid = -1) {
            this._environment = "DEV";
            //List<int> iconums = new List<int>();
            //iconums.Add(iconum);
            ////List<int> iconums = new List<int>() { 18119 };
            //_ExtendHierarchy(iconums, docid, tableid);
            //         if (this._autoclusteringfailure)
            //         {
            //             return false;
            //         }
            //         _ExtendColumns(iconums, docid, tableid);
            //         if (this._autoclusteringfailure)
            //         {
            //             return false;
            //         }
            //         return true;
            return ExtendClusterByContentSetDocument(contentSetIds, iconum, docid, tableid);

        }

        public string DecideClusterByDocument(List<int> contentSetIds, int iconum, Guid docid, int tableid = -1)
        {
            this._environment = "DEV";
            string result = "OK";
            try
            {
                List<int> iconums = new List<int>();
                iconums.Add(iconum);
                result = _DecideHierarchy(contentSetIds, iconums, docid, tableid);
            }
            catch (Exception ex)
            {
                result = string.Format("FAILED|{0}", ex.Message.ToString());
            }

            return result;

        }

        public string ExtendClusterByContentSetDocument(List<int> contentSetIds, int iconum, Guid docid, int tableid = -1)
        {
            List<int> iconums = new List<int>();
            iconums.Add(iconum);
            //List<int> iconums = new List<int>() { 18119 };
            _ExtendHierarchy(contentSetIds, iconums, docid, tableid);
            if (this._autoclusteringfailure)
            {
                return string.Format("REJECTED|{0}", _failureLogger.ToString());
            }
            _ExtendColumns(contentSetIds, iconums, docid, tableid);
            if (this._autoclusteringfailure)
            {
                return string.Format("REJECTED|{0}", _failureLogger.ToString());
            }
            return "OK";
        }



        public bool ExtendClusterByIconumDev(int iconum)
        {
            this._environment = "DEV";
            List<int> iconums = new List<int>();
            iconums.Add(iconum);
            //List<int> iconums = new List<int>() { 18119 };
            //_ExtendHierarchy(1, iconums, NullGuid);
            //_ExtendColumns(1, iconums, NullGuid);
            return true;
        }
        public bool ExtendClusterByDocumentDev(int iconum, Guid docid, int tableid = -1)
        {
            this._environment = "DEV";
            List<int> iconums = new List<int>();
            iconums.Add(iconum);
            //List<int> iconums = new List<int>() { 18119 };
            //_ExtendHierarchy(1, iconums, docid, tableid);
            //_ExtendColumns(1, iconums, docid, tableid);
            return true;
        }
        private bool _ExtendColumns(List<int> contentSetIds, List<int> iconums, Guid guid, int tableid = -1)
        {
            if (guid == NullGuid)
            {
                return false; 
            }
            _levelOneLogger.AppendLineBreak("");
            _levelOneLogger.AppendLineBreak("COLUMN TYPE");
            var iconum = iconums.First();
            _levelTwoLogger.AppendLineBreak("").AppendLineBreak("iconums.First(): " + iconum);
            _levelOneLogger.AppendLineBreak(string.Format("iconum: {0}, Guid: {1}, tableid: {2}, iconumsize:{3}", iconum, guid.ToString(), tableid, iconums.Count));
            string failureEmailHeader = string.Format("iconum: {0}, Guid: {1}, tableid: {2}, iconumsize:{3}", iconum, guid.ToString(), tableid, iconums.Count);


            var tableIDs = GetTableIdsFromContentSetIds(contentSetIds);
            int successfulTableCount = 0;
            foreach (var t in tableIDs)
            {
                if (tableid > 0 && t != tableid)
                {
                    continue;
                }
                bool isCurrentTableSuccessful = false;
                _levelOneLogger.AppendLineBreak("foreach (var t in tableIDs): " + t);
                SortedDictionary<string, long> existing = null;
                SortedDictionary<string, long> existingCleanLabel = null;
                SortedDictionary<string, long> existingCleanLabelNoHierarchy = null;

                foreach (var i in iconums)
                {
                    _levelOneLogger.AppendLineBreak("iconums.First(): " + iconum);
                    Dictionary<long, long> changeList = new Dictionary<long, long>();
                    _levelOneLogger.AppendLineBreak("changeList.Count: " + changeList.Count);
                    _unslotted = new Dictionary<string, int>();
                    SortedDictionary<long, string> unmapped = new SortedDictionary<long, string>();
                    SortedDictionary<long, string> unmappedCleanLabel = new SortedDictionary<long, string>();
                    int datapointToMatch = -1;
                    if (guid != NullGuid)
                    {
                        unmapped = _GetIconumRawColumnLabels(i, guid, t); // (flat_id, raw_row_label)
                        datapointToMatch = unmapped.Count;

                        //var tableAlignmentChanges = _getChangeListByTableAlignment(i, guid, t);
                        //changeList.Eat(tableAlignmentChanges);
                        //_levelOneLogger.AppendLineBreak("changeList.Count after TableAlignment: " + changeList.Count);
                    }
                    else
                    {
                        throw new ArgumentException("guid is null");
                        unmapped = _GetIconumRawLabels(i, t);
                        datapointToMatch = unmapped.Count;
                    }
                    if (unmapped.Count == 0)
                    {
                        if (tableid > 0)
                        {
                            this._autoclusteringfailure = true;
                        }
                        isCurrentTableSuccessful = false;
                        _failureLogger.AppendLine(@"Norm Table (ID " + t + ") for RNT column label is not available.");
                    }
                    else
                    {
                        isCurrentTableSuccessful = true;
                    }
                    _levelOneLogger.AppendLineBreak("datapoints to match: " + datapointToMatch);
                    _levelOneLogger.AppendLineBreak("changeList.Count after GetIconumRawLabels: " + changeList.Count);
                    bool isDone = changeList.Count >= datapointToMatch;
                    if (!isDone) // FIRST ATTEMPT: match raw_row_label
                    {
                        if (existing == null)
                        {
                            existing = _GetExistingClusterColumnHierarchyForCompany(iconum, t); // (raw_row_label, cluster_id)
                        }
                        var temp_changeList = _getChangeListColumn(existing, unmapped); 
                        changeList.Eat(temp_changeList);
                    }                                                         //changelist[flat_id, clusterid]
                    var changeCount = changeList.Count;
                    _levelOneLogger.AppendLineBreak("changeList.Count after First attempt: " + changeList.Count);
                    isDone = changeList.Count >= datapointToMatch;
                    if (!isDone) // SECOND ATTEMPT: match cleaned_row_label
                    {
                        if (existingCleanLabel == null)
                        {
                            existingCleanLabel = _GetExistingClusterCleanColumnHierarchyForCompany(iconum, t);
                        }
                        var unmappedCleanLabel1 = _GetIconumCleanColumnLabels(i, guid, t);
                        foreach (var um in unmappedCleanLabel1)
                        {
                            if (!changeList.ContainsKey(um.Key))
                            {
                                unmappedCleanLabel[um.Key] = um.Value;
                            }
                        }

                        var temp_changeList = _getChangeListColumn(existingCleanLabel, unmappedCleanLabel); // forcing to return nothing now. Will use only table alignment.
                        changeList.Eat(temp_changeList);
                    }
                    _levelOneLogger.AppendLineBreak("changeList.Count after Second attempt: " + changeList.Count);
                    var changeCount2 = changeList.Count;
                    isDone = changeList.Count >= datapointToMatch;
                    if (!isDone)    // THIRD ATTEMPT: stripped cleaned_row_label
                    {
                        if (existingCleanLabelNoHierarchy == null)
                        {
                            existingCleanLabelNoHierarchy = new SortedDictionary<string, long>();
                            if (existingCleanLabel == null)
                            {
                                existingCleanLabel = _GetExistingClusterCleanColumnHierarchyForCompany(iconum, t);
                            }
                            foreach (var cl in existingCleanLabel)
                            {
                                var lower = fn.RemoveHierarchyNumberSpace(cl.Key);
                                if (!string.IsNullOrWhiteSpace(lower) && !existingCleanLabelNoHierarchy.ContainsKey(lower))
                                {
                                    existingCleanLabelNoHierarchy[lower] = cl.Value;
                                }
                            }
                        }
                        var unmappedCleanLabelNotMatched = new SortedDictionary<long, string>();
                        foreach (var unmap in unmappedCleanLabel)
                        {
                            if (changeList.ContainsKey(unmap.Key))
                            {
                                continue;
                            }
                            unmappedCleanLabelNotMatched[unmap.Key] = fn.RemoveHierarchyNumberSpace(unmap.Value); // (flat_id, cleaned_row_label)
                        }
                        var temp_changeList = _getChangeListColumn(existingCleanLabelNoHierarchy, unmappedCleanLabelNotMatched); // forcing to return nothing now. Will use only table alignment.
                        changeList.Eat(temp_changeList);
                    }
                    var changeCount3 = changeList.Count;
                    _levelOneLogger.AppendLineBreak("changeList.Count after 3rd Attempt: " + changeList.Count);
                    isDone = changeList.Count >= datapointToMatch;
                    existing = null;
                    existingCleanLabel = null;
                    existingCleanLabelNoHierarchy = null;
                    if (!isDone) // 4th ATTEMPT: match raw_row_label industry
                    {
                        if (existing == null)
                        {
                            existing = _GetExistingClusterColumnHierarchyForIndustry(t); // (raw_row_label, cluster_id)
                        }
                        if (existing.Count == 0)
                        {
                            if (tableid > 0)
                            {
                                this._autoclusteringfailure = true;
                            }
                            else
                            {
                                isCurrentTableSuccessful = false;
                            }
                            _failureLogger.AppendLine(@"RNT for Norm Table ID " + t + " does not exist, please run the Named Tree. There is no cluster for column type.");
                        }
                        var temp_changeList = _getChangeListColumn(existing, unmapped);
                        changeList.Eat(temp_changeList);
                    }                                                         //changelist[flat_id, clusterid]
                    changeCount = changeList.Count;
                    _levelOneLogger.AppendLineBreak("changeList.Count after 4th attempt: " + changeList.Count);
                    isDone = changeList.Count >= datapointToMatch;
                    if (!isDone) // 5th ATTEMPT: match cleaned_row_label
                    {
                        if (existingCleanLabel == null)
                        {
                            existingCleanLabel = _GetExistingClusterCleanColumnHierarchyForIndustry(t);
                        }
                        var unmappedCleanLabel1 = _GetIconumCleanColumnLabels(i, guid, t);
                        foreach (var um in unmappedCleanLabel1)
                        {
                            if (!changeList.ContainsKey(um.Key))
                            {
                                unmappedCleanLabel[um.Key] = um.Value;
                            }
                        }

                        var temp_changeList = _getChangeListColumn(existingCleanLabel, unmappedCleanLabel); // forcing to return nothing now. Will use only table alignment.
                        changeList.Eat(temp_changeList);
                    }
                    _levelOneLogger.AppendLineBreak("changeList.Count after 5th attempt: " + changeList.Count);
                    changeCount2 = changeList.Count;
                    isDone = changeList.Count >= datapointToMatch;
                    if (!isDone)    // 6th ATTEMPT: stripped cleaned_row_label
                    {
                        if (existingCleanLabelNoHierarchy == null)
                        {
                            existingCleanLabelNoHierarchy = new SortedDictionary<string, long>();
                            if (existingCleanLabel == null)
                            {
                                existingCleanLabel = _GetExistingClusterCleanColumnHierarchyForIndustry(t);
                            }
                            foreach (var cl in existingCleanLabel)
                            {
                                var lower = fn.RemoveHierarchyNumberSpace(cl.Key);
                                if (!string.IsNullOrWhiteSpace(lower) && !existingCleanLabelNoHierarchy.ContainsKey(lower))
                                {
                                    existingCleanLabelNoHierarchy[lower] = cl.Value;
                                }
                            }
                        }
                        var unmappedCleanLabelNotMatched = new SortedDictionary<long, string>();
                        foreach (var unmap in unmappedCleanLabel)
                        {
                            if (changeList.ContainsKey(unmap.Key))
                            {
                                continue;
                            }
                            unmappedCleanLabelNotMatched[unmap.Key] = fn.RemoveHierarchyNumberSpace(unmap.Value); // (flat_id, cleaned_row_label)
                        }
                        var temp_changeList = _getChangeListColumn(existingCleanLabelNoHierarchy, unmappedCleanLabelNotMatched); // forcing to return nothing now. Will use only table alignment.
                        changeList.Eat(temp_changeList);
                    }
                    changeCount3 = changeList.Count;
                    _levelOneLogger.AppendLineBreak("changeList.Count after 6th Attempt: " + changeList.Count);
                    isDone = changeList.Count >= datapointToMatch;
                    if (changeList.Count > 0)
                    {
                        _WriteChangeListToDBForHierarchy(i, changeList, _unslotted);
                    }
                    else
                    {
                        var debug = changeList.Count;
                    }
                    _levelOneLogger.AppendLineBreak("changeList.Count after WriteChangesToDB: " + changeList.Count);
                    var debugchangelist = changeList;
                    var debugunslot = _unslotted;
                    if (tableid <= 0 && isCurrentTableSuccessful)
                    {
                        successfulTableCount++;
                    }
                }
            }
            if (DebugLogLevel > 0)
            {
                string emailbody = _levelOneLogger.ToString();
                if (DebugLogLevel > 1)
                {
                    emailbody += "LEVEL 2+: " + _levelTwoLogger.ToString();
                }
                SendEmail("Visual Stitching Debug", emailbody);
            }
            if (tableid <= 0)
            {
                if (successfulTableCount > 0)
                {
                    this._autoclusteringfailure = false;
                }
                else
                {
                    this._autoclusteringfailure = true;
                }
            }
            if (this._autoclusteringfailure)
            {
                var failures = _failureLogger.ToString();
                WriteLogToDatabase(this._pgConnectionString, guid, iconum, tableid, -1, -1, failures);
                SendEmailToAnalysts("AutoClustering Failure", failureEmailHeader + failures);
            }
            else
            {
                _failureLogger = new StringBuilder();
            }
            return !this._autoclusteringfailure;
        }
        private bool _ExtendHierarchy(List<int> contentSetIds, List<int> iconums, Guid guid, int tableid = -1) {
            var iconum = iconums.First();
            _levelTwoLogger.AppendLineBreak("").AppendLineBreak("iconums.First(): " + iconum);
            _levelOneLogger.AppendLineBreak(string.Format("iconum: {0}, Guid: {1}, tableid: {2}, iconumsize:{3}", iconum, guid.ToString(), tableid, iconums.Count));
            string failureEmailHeader = string.Format("iconum: {0}, Guid: {1}, tableid: {2}, iconumsize:{3}", iconum, guid.ToString(), tableid, iconums.Count);

            var tableIDs = GetTableIdsFromContentSetIds(contentSetIds);
			if (guid == NullGuid) {
				foreach (var t in tableIDs) {
					//_CleanupHierarchy(iconum, t);
                    _levelTwoLogger.AppendLineBreak("_CleanupHierarchy(iconum, t): " + t);
                }
			}
            int successfulTableCount = 0;
			foreach (var t in tableIDs) {
                if (tableid > 0 && t != tableid)
                {
                    continue;
                }
                bool isCurrentTableSuccessful = false;
                _levelOneLogger.AppendLineBreak("foreach (var t in tableIDs): " + t);
                SortedDictionary<string, long> existing = null;
                SortedDictionary<string, long> existingCleanLabel = null;
                SortedDictionary<string, long> existingCleanLabelNoHierarchy = null;

                foreach (var i in iconums) {
                    _levelOneLogger.AppendLineBreak("iconums.First(): " + iconum);
                    Dictionary<long, long> changeList = new Dictionary<long, long>();
                    _levelOneLogger.AppendLineBreak("changeList.Count: " + changeList.Count);
                    _unslotted = new Dictionary<string, int>();
					SortedDictionary<long, string> unmapped = new SortedDictionary<long, string>();
                    SortedDictionary<long, string> unmappedCleanLabel = new SortedDictionary<long, string>();
                    int datapointToMatch = -1;
                    if (guid != NullGuid) {
						unmapped = _GetIconumRawLabels(i, guid, t); // (flat_id, raw_row_label)
                        datapointToMatch = unmapped.Count;
                        if (datapointToMatch > 0)
                        {
                            var tableAlignmentChanges = _getChangeListByTableAlignment(i, guid, t);
                            changeList.Eat(tableAlignmentChanges);
                            _levelOneLogger.AppendLineBreak("changeList.Count after TableAlignment: " + changeList.Count);
                        }
                    } else {
						unmapped = _GetIconumRawLabels(i, t);
                        datapointToMatch = unmapped.Count;
                    }
                    if (unmapped.Count == 0)
                    {
                        if (tableid > 0)
                        {
                            this._autoclusteringfailure = true;
                        }
                        isCurrentTableSuccessful = false;
                        _failureLogger.AppendLine(@"Norm Table (ID " + t + ") for RNT label is not available.");
                    }
                    else
                    {
                        isCurrentTableSuccessful = true;
                    }
                    _levelOneLogger.AppendLineBreak("datapoints to match: " + datapointToMatch);
                    _levelOneLogger.AppendLineBreak("changeList.Count after GetIconumRawLabels: " + changeList.Count);
                    bool isDone = changeList.Count >= datapointToMatch;
                    if (!isDone) // FIRST ATTEMPT: match raw_row_label
                    {
                        if (existing == null)
                        {
                            existing = _GetExistingClusterHierarchyForCompany(iconum, t); // (raw_row_label, cluster_id)
                        }
                        var temp_changeList = _getChangeList(existing, unmapped); // forcing to return nothing now. Will use only table alignment.
                        changeList.Eat(temp_changeList);
                    }                                                         //changelist[flat_id, clusterid]
                    var changeCount = changeList.Count;
                    _levelOneLogger.AppendLineBreak("changeList.Count after First attempt: " + changeList.Count);
                    isDone = changeList.Count >= datapointToMatch;
                    if (!isDone) // SECOND ATTEMPT: match cleaned_row_label
                    { 
                        if (existingCleanLabel == null)
                        {
                            existingCleanLabel = _GetExistingClusterCleanLabelForCompany(iconum, t);
                        }
                        var unmappedCleanLabel1 = _GetIconumCleanLabels(i, guid, t);
                        foreach (var um in unmappedCleanLabel1)
                        {
                            if (!changeList.ContainsKey(um.Key))
                            {
                                unmappedCleanLabel[um.Key] = um.Value;
                            }
                        }
                        
                        var temp_changeList = _getChangeList(existingCleanLabel, unmappedCleanLabel); // forcing to return nothing now. Will use only table alignment.
                        changeList.Eat(temp_changeList);
                    }
                    _levelOneLogger.AppendLineBreak("changeList.Count after Second attempt: " + changeList.Count);
                    var changeCount2 = changeList.Count;
                    isDone = changeList.Count >= datapointToMatch;
                    if (!isDone)    // THIRD ATTEMPT: stripped cleaned_row_label
                    {
                        if (existingCleanLabelNoHierarchy == null)
                        {
                            existingCleanLabelNoHierarchy = new SortedDictionary<string, long>();
                            foreach (var cl in existingCleanLabel)
                            {
                                var lower = fn.RemoveHierarchyNumberSpace(cl.Key);
                                if (!string.IsNullOrWhiteSpace(lower) && !existingCleanLabelNoHierarchy.ContainsKey(lower))
                                {
                                    existingCleanLabelNoHierarchy[lower] = cl.Value;
                                }
                            }
                        }
                        var unmappedCleanLabelNotMatched = new SortedDictionary<long, string>();
                        foreach (var unmap in unmappedCleanLabel)
                        {
                            if (changeList.ContainsKey(unmap.Key))
                            {
                                continue;
                            }
                            unmappedCleanLabelNotMatched[unmap.Key] = fn.RemoveHierarchyNumberSpace(unmap.Value); // (flat_id, cleaned_row_label)
                        }
                        var temp_changeList = _getChangeList(existingCleanLabelNoHierarchy, unmappedCleanLabelNotMatched); // forcing to return nothing now. Will use only table alignment.
                        changeList.Eat(temp_changeList);
                    }
                    var changeCount3 = changeList.Count;
                    _levelOneLogger.AppendLineBreak("changeList.Count after 3rd Attempt: " + changeList.Count);
                    isDone = changeList.Count >= datapointToMatch;
                    unmappedCleanLabel = new SortedDictionary<long, string>();
                    existing = null;
                    existingCleanLabel = null;
                    existingCleanLabelNoHierarchy = null;
                    if (!isDone) // 4th ATTEMPT: match raw_row_label for industry
                    {
                        if (existing == null)
                        {
                            existing = _GetExistingClusterHierarchyForIndustry(t); // (raw_row_label, cluster_id)
                        }
                        if (existing.Count == 0)
                        {
                            if (tableid > 0)
                            {
                                this._autoclusteringfailure = true;
                            }
                            else
                            {
                                isCurrentTableSuccessful = false;
                            }
                            _failureLogger.AppendLine(@"RNT for Norm Table ID " + t + " does not exist, please run the Named Tree. There is no cluster.");
                        }
                        else
                        {

                        }
                        var temp_changeList = _getChangeList(existing, unmapped); // forcing to return nothing now. Will use only table alignment.
                        changeList.Eat(temp_changeList);
                    }                                                         //changelist[flat_id, clusterid]
                    changeCount = changeList.Count;
                    _levelOneLogger.AppendLineBreak("changeList.Count after 4th attempt: " + changeList.Count);
                    isDone = changeList.Count >= datapointToMatch;
                    if (!isDone) // 5th ATTEMPT: match cleaned_row_label for industry
                    {
                        if (existingCleanLabel == null)
                        {
                            existingCleanLabel = _GetExistingClusterCleanLabelForIndustry(t);
                        }
                        var unmappedCleanLabel1 = _GetIconumCleanLabels(i, guid, t);
                        foreach (var um in unmappedCleanLabel1)
                        {
                            if (!changeList.ContainsKey(um.Key))
                            {
                                unmappedCleanLabel[um.Key] = um.Value;
                            }
                        }

                        var temp_changeList = _getChangeList(existingCleanLabel, unmappedCleanLabel); // forcing to return nothing now. Will use only table alignment.
                        changeList.Eat(temp_changeList);
                    }
                    _levelOneLogger.AppendLineBreak("changeList.Count after 5th attempt: " + changeList.Count);
                    changeCount2 = changeList.Count;
                    isDone = changeList.Count >= datapointToMatch;
                    if (!isDone)    // 6th ATTEMPT: stripped cleaned_row_label
                    {
                        if (existingCleanLabelNoHierarchy == null)
                        {
                            existingCleanLabelNoHierarchy = new SortedDictionary<string, long>();
                            foreach (var cl in existingCleanLabel)
                            {
                                var lower = fn.RemoveHierarchyNumberSpace(cl.Key);
                                if (!string.IsNullOrWhiteSpace(lower) && !existingCleanLabelNoHierarchy.ContainsKey(lower))
                                {
                                    existingCleanLabelNoHierarchy[lower] = cl.Value;
                                }
                            }
                        }
                        var unmappedCleanLabelNotMatched = new SortedDictionary<long, string>();
                        foreach (var unmap in unmappedCleanLabel)
                        {
                            if (changeList.ContainsKey(unmap.Key))
                            {
                                continue;
                            }
                            unmappedCleanLabelNotMatched[unmap.Key] = fn.RemoveHierarchyNumberSpace(unmap.Value); // (flat_id, cleaned_row_label)
                        }
                        var temp_changeList = _getChangeList(existingCleanLabelNoHierarchy, unmappedCleanLabelNotMatched); // forcing to return nothing now. Will use only table alignment.
                        changeList.Eat(temp_changeList);
                    }
                    changeCount3 = changeList.Count;
                    _levelOneLogger.AppendLineBreak("changeList.Count after 6th Attempt: " + changeList.Count);
                    isDone = changeList.Count >= datapointToMatch;

                    if (changeList.Count > 0)
                    {
                        _WriteChangeListToDBForHierarchy(i, changeList, _unslotted);
                    }
                    else
                    {
                        var debug = changeList.Count;
                    }
                    _levelOneLogger.AppendLineBreak("changeList.Count after WriteChangesToDB: " + changeList.Count);
                    var debugchangelist = changeList;
                    var debugunslot = _unslotted;
                    if (tableid <= 0 && isCurrentTableSuccessful)
                    {
                        successfulTableCount++;
                    }
				}
			}
            if (DebugLogLevel > 0)
            {
                string emailbody = _levelOneLogger.ToString();
                if (DebugLogLevel > 1)
                {
                    emailbody += "LEVEL 2+: " + _levelTwoLogger.ToString();
                }
                SendEmail("Visual Stitching Debug", emailbody);
            }
            if (tableid <= 0)
            {            
                if (successfulTableCount > 0)
                {
                    this._autoclusteringfailure = false;
                }
                else
                {
                    this._autoclusteringfailure = true;
                }
            }
            if (this._autoclusteringfailure)
            {
                var failures = _failureLogger.ToString();
                WriteLogToDatabase(this._pgConnectionString, guid, iconum, tableid, -1, -1, failures);
                SendEmailToAnalysts("AutoClustering Failure", failureEmailHeader + failures);
            }
            else
            {
                _failureLogger = new StringBuilder();
            }
            return !this._autoclusteringfailure;
		}

        private string _DecideHierarchy(List<int> contentSetIds, List<int> iconums, Guid guid, int tableid = -1) {
            var iconum = iconums.First();

            var tableIDs = GetTableIdsFromContentSetIds(contentSetIds);
			if (guid == NullGuid) {
                throw new Exception("Bad Document id");
			}
            int successfulTableCount = 0;
			foreach (var t in tableIDs) {
                if (tableid > 0 && t != tableid)
                {
                    continue;
                }
                bool isCurrentTableSuccessful = false;
                SortedDictionary<string, long> existing = null;
                SortedDictionary<string, long> existingCleanLabel = null;
                SortedDictionary<string, long> existingCleanLabelNoHierarchy = null;

                foreach (var i in iconums) {
                    Dictionary<long, long> changeList = new Dictionary<long, long>();
                    _unslotted = new Dictionary<string, int>();
					SortedDictionary<long, string> unmapped = new SortedDictionary<long, string>();
                    SortedDictionary<long, string> unmappedCleanLabel = new SortedDictionary<long, string>();
                    int datapointToMatch = -1;
                    if (guid != NullGuid) {
						unmapped = _GetIconumRawLabels(i, guid, t); // (flat_id, raw_row_label)
                        datapointToMatch = unmapped.Count;
                    } else {
						unmapped = _GetIconumRawLabels(i, t);
                        datapointToMatch = unmapped.Count;
                    }
                    if (unmapped.Count == 0)
                    {
                        //return "OK";
                    }
                    else
                    {
                        return "REJECTED|Not fully clustered";
                    }
				}
			}
            return "OK";
		}
//        public int IconumToContentSet(int iconum)
//        {
//            int contentSetId = 1;
//            try
//            {
//                string sqltxt = string.Format(@"
//        select distinct content_set_id from iconum_content_set_association
//where iconum = {0}
//order by content_set_id ", iconum);
//                using (var sqlConn = new NpgsqlConnection(this._pgConnectionString))
//                using (var cmd = new NpgsqlCommand(sqltxt, sqlConn))
//                {
//                    sqlConn.Open();
//                    using (var sdr = cmd.ExecuteReader())
//                    {
//                        while (sdr.Read())
//                        {
//                            contentSetId = sdr.GetInt32(0);
//                            break;  
//                        }

//                    }
//                }
//            } catch (Exception ex)
//            {
//                contentSetId = 1;
//            }
//            return contentSetId;
//        }

        public List<int> GetContentSetIdsFromIconum(int iconum)
        {
            List<int> result = new List<int>();
            try
            {
                string sqltxt = string.Format(@"
                        select c.id
                        from iconum_content_set_association i
                        join content_set c on i.content_set_id = c.id
                    where i.iconum = {0}", iconum);
                using (var sqlConn = new NpgsqlConnection(this._pgConnectionString))
                using (var cmd = new NpgsqlCommand(sqltxt, sqlConn))
                {
                    sqlConn.Open();
                    using (var sdr = cmd.ExecuteReader())
                    {
                        while (sdr.Read())
                        {
                            var contentSetId = sdr.GetInt32(0);
                            result.Add(contentSetId);
                        }

                    }
                }
            }
            catch (Exception ex)
            {
                result = new List<int>();
            }
            return result;
        }

//        private List<int> TableIDs(int contentSetId, int iconum) {
//			List<int> dataNodes = new List<int>();
//            if (contentSetId == 1)
//            {
//                dataNodes = new List<int>() { 1, 2, 4, 5 };
//                return dataNodes;
//            }
//            else
//            {
//                string sqltxt = string.Format(@"
//select distinct nt.id as norm_table_id from norm_table nt where nt.content_set_id = {0}
//union
//select distinct hti.norm_table_id from html_table_identification hti where hti.iconum = {1} and hti.norm_table_id is not null
//order by norm_table_id ", contentSetId, iconum);
//                using (var sqlConn = new NpgsqlConnection(this._pgConnectionString))
//                using (var cmd = new NpgsqlCommand(sqltxt, sqlConn))
//                {
//                    sqlConn.Open();
//                    using (var sdr = cmd.ExecuteReader())
//                    {
//                        while (sdr.Read())
//                        {
//                            int value = sdr.GetInt32(0);
//                            dataNodes.Add(value);
//                        }

//                    }
//                }
//            }
//            if (dataNodes == null || dataNodes.Count == 0)
//            {
//                dataNodes = new List<int>() { 1, 4, 5 };
//            }
//			return dataNodes;
//		}

        private List<int> GetTableIdsFromContentSetIds(List<int> contentSetIds)
        {
            if (contentSetIds == null || contentSetIds.Count <= 0)
                throw new ArgumentException("No Content Set Id provided.");

            List<int> dataNodes = new List<int>();
            string joined = String.Join(",", contentSetIds);
            string sqltxt = string.Format(@"
select distinct nt.id as norm_table_id from norm_table nt where nt.content_set_id in ({0})", joined);
            using (var sqlConn = new NpgsqlConnection(this._pgConnectionString))
            using (var cmd = new NpgsqlCommand(sqltxt, sqlConn))
            {
                sqlConn.Open();
                using (var sdr = cmd.ExecuteReader())
                {
                    while (sdr.Read())
                    {
                        int value = sdr.GetInt32(0);
                        dataNodes.Add(value);
                    }

                }
            }
            if (dataNodes.Count <= 0)
                throw new ArgumentException("Content Set IDs has no mapped norm table.");
            return dataNodes;
        }
		private bool _CleanupHierarchy(int iconum, int tableId) {
			var dict = _CleanupGetMissingHierarchy(iconum, tableId);
			_CleanUpdateClusterIdForHierarchy(iconum, tableId, dict);
			return true;
		}
		private SortedDictionary<string, long> _CleanupGetMissingHierarchy(int iconum, int tableId) {
			SortedDictionary<string, long> entries = new SortedDictionary<string, long>();

			string sqltxt = string.Format(@"
select p.item_code, max(cm.cluster_hierarchy_id)
	from prod_data p
	join norm_name_tree ntf
		on p.document_id = ntf.document_id
		and p.iconum = ntf.iconum
		and p.item_offset = ntf.item_offset
	left join cluster_mapping cm
		on cm.norm_name_tree_flat_id = ntf.id
	where ntf.iconum = {0} and ntf.norm_table_id = {1}
	group by p.item_code
	having count (distinct cm.cluster_hierarchy_id) = 1  and  count(*) > count(cm.cluster_hierarchy_id);
", iconum, tableId);
			int idx = 0;
			using (var sqlConn = new NpgsqlConnection(this._pgConnectionString))
			using (var cmd = new NpgsqlCommand(sqltxt, sqlConn)) {
				cmd.CommandTimeout = 600;
				//cmd.Parameters.AddWithValue("@iconum", iconum);
				sqlConn.Open();
				using (var sdr = cmd.ExecuteReader()) {
					while (sdr.Read()) {
						try {
							var itemCode = sdr.GetStringSafe(0);
							var id = sdr.GetInt64(1);
							entries[itemCode] = id;
						} catch {

						}
					}

				}
			}
			return entries;
		}
		private bool _CleanUpdateClusterIdForHierarchy(int iconum, int tableId, SortedDictionary<string, long> entries) {
			string sql_update_format = @"

insert into cluster_mapping (cluster_hierarchy_id, norm_name_tree_flat_id)
select {1}, ntf.id
	from prod_data p
	join norm_name_tree ntf
		on p.document_id = ntf.document_id
		and p.iconum = ntf.iconum
		and p.item_offset = ntf.item_offset
	left join cluster_mapping cm 
		on ntf.id = cm.norm_name_tree_flat_id
	where ntf.iconum = {2} and ntf.norm_table_id = {3}
		and p.item_code = '{0}'
 		and cm.norm_name_tree_flat_id is null;
";
			foreach (var e in entries) {
				// e.Value is the cluster_hieararchy_id, e.key is the itemcode. 
				string sql_update = string.Format(sql_update_format, e.Key, e.Value, iconum, tableId);
				//Console.WriteLine(sql_update);
				using (var sqlConn = new NpgsqlConnection(this._pgConnectionString)) {
					using (var cmd = new NpgsqlCommand(sql_update, sqlConn)) {
						cmd.CommandTimeout = 600;
						sqlConn.Open();
						cmd.ExecuteNonQuery();
					}
				}
			}
			return true;
		}
        public static bool WriteLogToDatabase(string connString, Guid docId, int iconum, int normTableId, int fileId, int tableId, string comments)
        {
            try
            {
                string sql_update_format = @"

insert into log_autoclustering (document_id, iconum, file_id, table_id, norm_table_id, comments)
values ('{0}', {1}, {2}, {3}, {4}, '{5}'); 
";
                string pIconum = iconum < 0 ? "NULL" : iconum.ToString();
                string pFileId = fileId < 0 ? "NULL" : fileId.ToString();
                string pTableId = tableId < 0 ? "NULL" : tableId.ToString();
                string pNormTableId = normTableId < 0 ? "NULL" : normTableId.ToString();
                string sql_update = string.Format(sql_update_format, docId, pIconum, pFileId, pTableId, pNormTableId, comments);
                using (var sqlConn = new NpgsqlConnection(connString))
                {
                    using (var cmd = new NpgsqlCommand(sql_update, sqlConn))
                    {
                        cmd.CommandTimeout = 600;
                        sqlConn.Open();
                        cmd.ExecuteNonQuery();
                    }
                }
                return true;
            }
            catch (Exception ex)
            {

            }
            return false;
        }

        public List<VisualStitching.Common.Models.ClusterError> ReadLogFromDatabase()
        {
            try
            {
                string sql = @"
select log.id, log.document_id, log.iconum, coalesce(nt.label, 'ALL'), log.creation_stamp_utc, log.comments 
from log_autoclustering log
left JOIN norm_table nt on log.norm_table_id = nt.id
 order by id desc limit 100
";
                List<VisualStitching.Common.Models.ClusterError> list = new List<VisualStitching.Common.Models.ClusterError>();
                //string sql_update = string.Format(sql, docId, pIconum, pFileId, pTableId, pNormTableId, comments);
                using (var sqlConn = new NpgsqlConnection(this._pgConnectionString))
                {
                    using (var cmd = new NpgsqlCommand(sql, sqlConn))
                    {
                        cmd.CommandTimeout = 600;
                        sqlConn.Open();
                        using (var sdr = cmd.ExecuteReader())
                        {
                            while (sdr.Read())
                            {
                                try
                                {

                                    var row = new VisualStitching.Common.Models.ClusterError();
                                    row.Id =  sdr.GetInt32(0);
                                    row.DocumentId = sdr.GetGuid(1).ToString();
                                    row.Iconum = sdr.GetNullable<int>(2);
                                    row.NormTable = sdr.GetStringSafe(3);
                                    row.CreationTimeStamp = sdr.GetDateTime(4);
                                    row.Comments = sdr.GetStringSafe(5);
                                    list.Insert(0, row);
                                }
                                catch
                                {

                                }
                            }
                        }
                    }
                }
                return list;
            }
            catch (Exception ex)
            {

            }
            return new List<VisualStitching.Common.Models.ClusterError>();
        }


        private bool _WriteChangeListToFileForHierarchy(long iconum, int tableid, Dictionary<long, long> changeList, Dictionary<string, int> unslotted) {
			string slottedpath = string.Format("{0}_table{1}_slotted_h.csv", iconum, tableid);
			string unslottedpath = string.Format("{0}_table{1}_unslotted_h.csv", iconum, tableid);
			using (System.IO.StreamWriter sw = System.IO.File.AppendText(slottedpath)) {
				foreach (var c in changeList) {
					sw.WriteLine(string.Format("{0},{1}", c.Key, c.Value));
				}
			}
			using (System.IO.StreamWriter sw = System.IO.File.AppendText(unslottedpath)) {
				foreach (var u in unslotted) {
					sw.WriteLine(string.Format("{0},{1}", u.Key, u.Value));
				}
			}

			return true;
		}
		private bool _WriteChangeListToDBForHierarchy(long iconum, Dictionary<long, long> changeList, Dictionary<string, int> unslotted) {
			foreach (var newclusterid in changeList.GroupBy(x => x.Value).Select(x => x.First().Value)) {
				var changeIds = changeList.Where(x => x.Value == newclusterid).Select(x => x.Key);
                try
                {
                    _UpdateClusterHierarchyId(changeIds, newclusterid);

                }
                catch { }
			}
			//foreach (var c in changeList)
			//{
			//    sw.WriteLine(string.Format("{0},{1}", c.Key, c.Value));
			//}

			return true;
		}

		private bool _UpdateClusterHierarchyId(IEnumerable<long> ids, long clusterId) {
			string sql_update_format = @"
            insert into cluster_mapping (cluster_hierarchy_id, norm_name_tree_flat_id)
            select {0} id, x
            FROM  	unnest(ARRAY[{1}]) x
			ON CONFLICT ON CONSTRAINT cluster_mapping_groupmapping 
			DO NOTHING;
";
			if (ids != null && ids.Count() > 0 && clusterId > 0) {
				string sql_update = string.Format(sql_update_format, clusterId, string.Join(",", ids));
				Console.WriteLine(sql_update);
				using (var sqlConn = new NpgsqlConnection(this._pgConnectionString)) {
					using (var cmd = new NpgsqlCommand(sql_update, sqlConn)) {
						cmd.CommandTimeout = 600;
						sqlConn.Open();
						int result = cmd.ExecuteNonQuery();
                        if (result > 0)
                        {
                            result++;
                        }
                    }
				}
			}

			return true;
		}

		private Dictionary<string, int> _unslotted = new Dictionary<string, int>();
        private Guid _getHistoricalDocumentFromDB(int iconum, Guid currDocId)
        {
            string sql_lastyeardocument = @"
select top 1 d.DAMDocumentId
FROM  Document d_curr
JOIN DocumentSeries ds_curr on d_curr.DocumentSeriesID = ds_curr.ID
JOIN Document d on d_curr.DocumentSeriesID = d.DocumentSeriesID and d_curr.ReportTypeID = d.ReportTypeID and d_curr.FormTypeID = d.FormTypeID
WHERE 
d_curr.DAMDocumentId = @docId
and ds_curr.CompanyID = @companyId
and d.ArdExportFlag = 1
and d.DocumentDate <= DATEADD(d, -364, d_curr.DocumentDate) 
and d.DocumentDate >= DATEADD(d, -367, d_curr.DocumentDate)
order by  d.PublicationDateTime desc

";
            Guid bestHistory = default(Guid);
            using (SqlConnection conn = new SqlConnection(_sfConnectionString))
            {
                using (SqlCommand cmd = new SqlCommand(sql_lastyeardocument, conn))
                {
                    conn.Open();
                    cmd.Parameters.AddWithValue("@docId", currDocId);
                    cmd.Parameters.AddWithValue("@companyId", iconum);
                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            bestHistory = reader.GetGuid(0);
                            break;
                        }

                    }
                }
            }
            return bestHistory;
        }

        private Guid _getNextYearDocumentFromDB(int iconum, Guid currDocId)
        {
            string sql_lastyeardocument = @"
select top 1 d.DAMDocumentId
FROM  Document d_curr
JOIN DocumentSeries ds_curr on d_curr.DocumentSeriesID = ds_curr.ID
JOIN Document d on d_curr.DocumentSeriesID = d.DocumentSeriesID and d_curr.ReportTypeID = d.ReportTypeID and d_curr.FormTypeID = d.FormTypeID
WHERE 
d_curr.DAMDocumentId = @docId
and ds_curr.CompanyID = @companyId
and d.ArdExportFlag = 1
and d.DocumentDate >= DATEADD(d, 364, d_curr.DocumentDate) 
and d.DocumentDate <= DATEADD(d, 367, d_curr.DocumentDate)
order by  d.PublicationDateTime desc

";
            Guid bestHistory = default(Guid);
            using (SqlConnection conn = new SqlConnection(_sfConnectionString))
            {
                using (SqlCommand cmd = new SqlCommand(sql_lastyeardocument, conn))
                {
                    conn.Open();
                    cmd.Parameters.AddWithValue("@docId", currDocId);
                    cmd.Parameters.AddWithValue("@companyId", iconum);
                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            bestHistory = reader.GetGuid(0);
                            break;
                        }

                    }
                }
            }
            return bestHistory;
        }
        private Guid _getHistoricalDocumentFromService(int iconum, Guid currDocId)
        {
            Guid bestHistory = default(Guid);
            string url_pattern = @"https://automate-equation.factset.io/api/Automate/BestMatchHistoricalDocument/DocumentId/{1}/Iconum/{0}/FileId/{2}/file";
            var url = string.Format(url_pattern, iconum, currDocId.ToString().ToLower(), 0);
            var outputresult = GetWebRequest(url);
            if (string.IsNullOrWhiteSpace(outputresult))
            {
                return NullGuid;
            }
            return bestHistory;
        }
        private Dictionary<long, long> FindMaxChangeList(List<NormNameTreeTable> currTables, List<NormNameTreeTable> histTables)
        {
            Dictionary<long, long> result = new Dictionary<long, long>();
            if (currTables.Count == 0 || histTables.Count == 0)
                return result;

            foreach(var c in currTables)
            {
                foreach(var h in histTables)
                {
                    var r = c.MergeWithHistoricalTable(h);
                    if (r.Count > 0)
                    {
                        var newCurrTables = currTables.Clone();
                        newCurrTables.Remove(c);
                        var newHistTables = histTables.Clone();
                        newHistTables.Remove(h);
                        var future = FindMaxChangeList(newCurrTables, newHistTables);
                        if (r.Count + future.Count > result.Count)
                        {
                            result = new Dictionary<long, long>(); // keep longest list
                            result.Eat(r);
                            result.Eat(future);
                        }
                    }
                }
            }
            return result;
        }
        private List<int> _getTableIdsForDocument(int iconum, Guid doc, int normTableId)
        {
            List<int> vs = new List<int>();
            string sqltxt = string.Format(@"

 select tf.document_id, tf.table_id
  from norm_name_tree_flat tf 
  join html_table_identification hti on hti.document_id = tf.document_id and hti.table_id = tf.table_id and hti.iconum = tf.iconum
  where tf.iconum = {0}  and hti.norm_table_id = {1} and tf.document_id ='{2}'
  group by tf.document_id, tf.table_id
", iconum, normTableId, doc);
            int idx = 0;
            using (var sqlConn = new NpgsqlConnection(this._pgConnectionString))
            using (var cmd = new NpgsqlCommand(sqltxt, sqlConn))
            {
                cmd.CommandTimeout = 600;
                //cmd.Parameters.AddWithValue("@iconum", iconum);
                sqlConn.Open();
                using (var sdr = cmd.ExecuteReader())
                {
                    while (sdr.Read())
                    {
                        try
                        {
                            var table = sdr.GetInt32(1);
                            vs.Add(table);
                        }
                        catch
                        {

                        }
                    }

                }
            }
            return vs;
        }
        private Guid _getBestMatchingDocument(int iconum, Guid currDoc, int tableId)
        {
            List<Tuple<Guid, int, int>> histDocList = new List<Tuple<Guid, int, int>>();
            string sqltxt = string.Format(@"
  select tf.document_id, tf.file_id, count(distinct tf.row_id)
  from norm_name_tree_flat tf 
  join cluster_mapping cm on tf.id = cm.norm_name_tree_flat_id
  join html_table_identification hti on hti.document_id = tf.document_id and hti.table_id = tf.table_id and hti.iconum = tf.iconum
  where tf.iconum = {0}  and hti.norm_table_id = {1}
  group by tf.document_id, tf.file_id
", iconum, tableId);
            int idx = 0;
            using (var sqlConn = new NpgsqlConnection(this._pgConnectionString))
            using (var cmd = new NpgsqlCommand(sqltxt, sqlConn))
            {
                cmd.CommandTimeout = 600;
                //cmd.Parameters.AddWithValue("@iconum", iconum);
                sqlConn.Open();
                using (var sdr = cmd.ExecuteReader())
                {
                    while (sdr.Read())
                    {
                        try
                        {
                            var guid = sdr.GetGuid(0);
                            var table = sdr.GetInt32(1);
                            var rowCount = sdr.GetInt32(2);
                            var tuple = new Tuple<Guid, int, int>(guid, table, rowCount);
                            histDocList.Add(tuple);
                        }
                        catch
                        {

                        }
                    }

                }
            }
            var next_year_guid = _getNextYearDocumentFromDB(iconum, currDoc);
            foreach (var t in histDocList)
            {
                if (t.Item1 == next_year_guid)
                {
                    return next_year_guid;
                }
            }
            var dbguid = _getHistoricalDocumentFromDB(iconum, currDoc);
            foreach (var t in histDocList)
            {
                if (t.Item1 == dbguid)
                {
                    return dbguid;
                }
            }
            var webguid = _getHistoricalDocumentFromService(iconum, currDoc);
            foreach (var t in histDocList)
            {
                if (t.Item1 == webguid)
                {
                    return webguid;
                }
            }
            return NullGuid;
            //return new Guid("5B56EC82-0731-E711-80EA-8CDCD4AF21E4");
        }

        private SortedDictionary<int, long> _getTable()
        {
            return new SortedDictionary<int, long>();
        }
        private string GetWebRequest(string url)
        {
            HttpWebRequest request = (HttpWebRequest)WebRequest.Create(url);
            request.ContentType = "application/json";
            request.Timeout = 120000;
            request.Method = "GET";
            HttpWebResponse response = null;
            try
            {
                response = (HttpWebResponse)request.GetResponse();
            }
            catch
            {
                return "";
                //throw new FileNotFoundException("call failed");
            }
            string outputresult = null;
            if (response.StatusCode == HttpStatusCode.OK)
            {
                using (var streamReader = new StreamReader(response.GetResponseStream()))
                {
                    outputresult = streamReader.ReadToEnd();
                }
            }
            else if (response.StatusCode == HttpStatusCode.Accepted || response.StatusCode == HttpStatusCode.ServiceUnavailable)
            {
                throw new Exception("call failed");
            }
            else
            {
                return "";
                //throw new FileNotFoundException("call failed");
            }
            return outputresult;

        }
        private Dictionary<long, long> _matchIdenticalTable(Guid curr_doc, int curr_raw_table_id, Guid hist_doc, int hist_raw_table_id)
        {
            SortedDictionary<int, long> curr_table = new SortedDictionary<int, long>(); // (col_id, norm_name_tree_id)
            SortedDictionary<int, long> hist_table = new SortedDictionary<int, long>(); // (col_id, norm_name_tree_id)
            Dictionary<long, long> changelist = new Dictionary<long, long>();
            // like sorting? 
            // go both way down. 
            // if there is no match, then, find the target, and swap. 
            // find number of no match? 

            // if the rows match,
            //      1. if no out ot order, and 0 mismatch.... taht's easy
            //      2. if no out of order, and 1 mismatch.... assume that 
            //      3. if no out of order, and 2 mismatch.... are they in place? yes, assume that. 
            //      3. if no out of order, and 3 mismatch ... 
            //  => if same row count, if a row is found not expected, then it's a messed up table
            //              if it's a messed up table, see if all the messed up are within themselves?
            //              no, just match by label and leave it to other algorithm. 
            //  => if same row count, no row out of order, if less than 5 mismatch line, then just match everything. 
            //  => 123388 
            // row doesn't match
            //      1. if less than 3 mismatch, match by labels in the table, then leave it to other algorithm. 
            //      2. if more than 3 mismatch, match by labels in the table, then leave it to other algorithm

            if (curr_table.Count > 0 && curr_table.Count == hist_table.Count)
            {
                for (int i = 0; i < curr_table.Count; i++)
                {
                    changelist[hist_table.ElementAt(i).Value] = curr_table.ElementAt(i).Value; // (curr_table.norm_name_tree_id, hist_table.norm_name_tree_id)
                }
            }

            return new Dictionary<long, long>();
        }
        private Dictionary<long, long> _getChangeListByTableAlignment(int iconum, Guid currDoc, int normTableId) {
            var result = new Dictionary<long, long>(); 
            var histDoc = _getBestMatchingDocument(iconum, currDoc, normTableId);
            if (histDoc == NullGuid)
                return result;
            var currTableIds = _getTableIdsForDocument(iconum, currDoc, normTableId);
            var histTableIds = _getTableIdsForDocument(iconum, histDoc, normTableId);

            List<NormNameTreeTable> currDocTables = new List<NormNameTreeTable>();
            foreach (var c in currTableIds)
            {
                NormNameTreeTable t = new NormNameTreeTable();
                t.Load(this._pgConnectionString, iconum, currDoc, c);
                currDocTables.Add(t);
            }
            List<NormNameTreeTable> histDocTables = new List<NormNameTreeTable>();
            foreach (var c in histTableIds)
            {
                NormNameTreeTable t = new NormNameTreeTable();
                t.LoadHistorical(this._pgConnectionString, iconum, histDoc, c);
                histDocTables.Add(t);
            }
            //NormNameTreeTable curr = new NormNameTreeTable();
            //curr.Load(this._pgConnectionString, iconum, currDoc, 50);
            //NormNameTreeTable hist = new NormNameTreeTable();
            //hist.Load(this._pgConnectionString, iconum, histDoc, 45);
            //result = curr.MergeWithHistoricalTable(hist);
            result = FindMaxChangeList(currDocTables, histDocTables);
            return result;
        }

        private bool _isIdenticalTable(Guid curr_doc, int curr_raw_table_id, Guid hist_doc, int hist_raw_table_id)
        {
            SortedDictionary<int, long> curr_table = new SortedDictionary<int, long>(); // (col_id, norm_name_tree_id)
            SortedDictionary<int, long> hist_table = new SortedDictionary<int, long>(); // (col_id, norm_name_tree_id)
            Dictionary<long, long> changelist = new Dictionary<long, long>();

            // if Name is identical according to HTML identfication or and number of row is same. 
            // if all row's label matches. 

            return false;
        }
        //private string RemoveHierarchyNumberSpace(string s)
        //{
        //    var nohierarchy = fn.EndLabel(s);
        //    var noNumberandSpace = fn.AlphabetOnly(nohierarchy, "");
        //    var lower = noNumberandSpace.ToLower();
        //    return lower;
        //}
        private Dictionary<long, long> _getChangeList(SortedDictionary<string, long> existing, SortedDictionary<long, string> unmapped) {
            // existing[raw label, clusterid]
            // unmapped[flat_id, rawlabel]
            // changelist[flat_id, clusterid]
            Dictionary<long, long> changelist = new Dictionary<long, long>();
			Dictionary<long, string> unslotted = new Dictionary<long, string>();
			foreach (var u in unmapped) {
				if (existing.ContainsKey(u.Value)) {
					// u.key is the itemID, existing is the cluster_id
					changelist[u.Key] = existing[u.Value]; // changelist[flat_id] = cluster_id
				} else {
					unslotted[u.Key] = u.Value;
					if (!_unslotted.ContainsKey(u.Value)) {
						_unslotted[u.Value] = 0;
					}
					_unslotted[u.Value]++;
				}
			}
			if (unslotted.Count == 0) return changelist;
			/// ------ second try
			SortedDictionary<string, string> cleanedExisting = new SortedDictionary<string, string>();
			SortedDictionary<long, string> cleanedUnmapped = new SortedDictionary<long, string>();

			foreach (var u in unslotted) {
				var sCleaned = CleanStringForStep2(u.Value);
				cleanedUnmapped[u.Key] = sCleaned;
			}
			foreach (var e in existing) {
				var sCleaned = CleanStringForStep2(e.Key);
				if (!cleanedExisting.ContainsKey(sCleaned)) {
					//cleaned version is the key
					cleanedExisting[sCleaned] = e.Key; // raw label is the value.
				}
			}
			_unslotted = new Dictionary<string, int>();
			unslotted = new Dictionary<long, string>();
			foreach (var u in cleanedUnmapped) {
				// cleanedunmap[itemid, cleaned]
				// cleanedExisting[cleaned, uncleaned]
				if (cleanedExisting.ContainsKey(u.Value)) {
					// if cleanedversion = cleanversion
					// find the 

					changelist[u.Key] = existing[cleanedExisting[u.Value]];
				} else {
					unslotted[u.Key] = u.Value;
					if (!_unslotted.ContainsKey(u.Value)) {
						_unslotted[u.Value] = 0;
					}
					_unslotted[u.Value]++;
				}
			}
			if (_unslotted.Count == 0) return changelist;
			/// ------ third try use end label only
			cleanedExisting = new SortedDictionary<string, string>();
			cleanedUnmapped = new SortedDictionary<long, string>();

			foreach (var u in unslotted) {
				var sCleaned = CleanStringForStep2(fn.EndLabel(u.Value));
				cleanedUnmapped[u.Key] = sCleaned;
			}
			foreach (var e in existing) {
				var sCleaned = CleanStringForStep2(fn.EndLabel(e.Key));
				if (!cleanedExisting.ContainsKey(sCleaned)) {
					//cleaned version is the key
					cleanedExisting[sCleaned] = e.Key; // raw label is the value.
				}
			}
			_unslotted = new Dictionary<string, int>();
			unslotted = new Dictionary<long, string>();
			foreach (var u in cleanedUnmapped) {
				// cleanedunmap[itemid, cleaned]
				// cleanedExisting[cleaned, uncleaned]
				if (cleanedExisting.ContainsKey(u.Value)) {
					// if cleanedversion = cleanversion
					// find the 

					changelist[u.Key] = existing[cleanedExisting[u.Value]];
				} else {
					unslotted[u.Key] = u.Value;
					if (!_unslotted.ContainsKey(u.Value)) {
						_unslotted[u.Value] = 0;
					}
					_unslotted[u.Value]++;
				}
			}
			if (_unslotted.Count == 0) return changelist;
			return changelist;
		}
        private Dictionary<long, long> _getChangeListColumn(SortedDictionary<string, long> existing, SortedDictionary<long, string> unmapped)
        {
            // existing[raw label, clusterid]
            // unmapped[flat_id, rawlabel]
            // changelist[flat_id, clusterid]
            Dictionary<long, long> changelist = new Dictionary<long, long>();
            Dictionary<long, string> unslotted = new Dictionary<long, string>();
            foreach (var u in unmapped)
            {
                if (existing.ContainsKey(u.Value))
                {
                    // u.key is the itemID, existing is the cluster_id
                    changelist[u.Key] = existing[u.Value]; // changelist[flat_id] = cluster_id
                }
                else
                {
                    unslotted[u.Key] = u.Value;
                    if (!_unslotted.ContainsKey(u.Value))
                    {
                        _unslotted[u.Value] = 0;
                    }
                    _unslotted[u.Value]++;
                }
            }
            if (unslotted.Count == 0) return changelist;
            /// ------ second try
            SortedDictionary<string, string> cleanedExisting = new SortedDictionary<string, string>();
            SortedDictionary<long, string> cleanedUnmapped = new SortedDictionary<long, string>();

            foreach (var u in unslotted)
            {
                var sCleaned = CleanStringForColumn(u.Value);
                cleanedUnmapped[u.Key] = sCleaned;
            }
            foreach (var e in existing)
            {
                var sCleaned = CleanStringForColumn(e.Key);
                if (!cleanedExisting.ContainsKey(sCleaned))
                {
                    //cleaned version is the key
                    cleanedExisting[sCleaned] = e.Key; // raw label is the value.
                }
            }
            _unslotted = new Dictionary<string, int>();
            unslotted = new Dictionary<long, string>();
            foreach (var u in cleanedUnmapped)
            {
                // cleanedunmap[itemid, cleaned]
                // cleanedExisting[cleaned, uncleaned]
                if (cleanedExisting.ContainsKey(u.Value))
                {
                    // if cleanedversion = cleanversion
                    // find the 

                    changelist[u.Key] = existing[cleanedExisting[u.Value]];
                }
                else
                {
                    unslotted[u.Key] = u.Value;
                    if (!_unslotted.ContainsKey(u.Value))
                    {
                        _unslotted[u.Value] = 0;
                    }
                    _unslotted[u.Value]++;
                }
            }
            if (_unslotted.Count == 0) return changelist;
            /// ------ third try use end label only
            cleanedExisting = new SortedDictionary<string, string>();
            cleanedUnmapped = new SortedDictionary<long, string>();

            foreach (var u in unslotted)
            {
                var sCleaned = CleanStringForColumn(fn.EndLabel(u.Value));
                cleanedUnmapped[u.Key] = sCleaned;
            }
            foreach (var e in existing)
            {
                var sCleaned = CleanStringForColumn(fn.EndLabel(e.Key));
                if (!cleanedExisting.ContainsKey(sCleaned))
                {
                    //cleaned version is the key
                    cleanedExisting[sCleaned] = e.Key; // raw label is the value.
                }
            }
            _unslotted = new Dictionary<string, int>();
            unslotted = new Dictionary<long, string>();
            foreach (var u in cleanedUnmapped)
            {
                // cleanedunmap[itemid, cleaned]
                // cleanedExisting[cleaned, uncleaned]
                if (cleanedExisting.ContainsKey(u.Value))
                {
                    // if cleanedversion = cleanversion
                    // find the 

                    changelist[u.Key] = existing[cleanedExisting[u.Value]];
                }
                else
                {
                    unslotted[u.Key] = u.Value;
                    if (!_unslotted.ContainsKey(u.Value))
                    {
                        _unslotted[u.Value] = 0;
                    }
                    _unslotted[u.Value]++;
                }
            }
            if (_unslotted.Count == 0) return changelist;
            return changelist;
        }
        private string CleanStringForStep2(string str) {
			//remove non alphanumeric and space
			// make singular
			// replace phrase
			// remove stem words, and replace individual words 
			var s = str;
			s = fn.AlphaNumericSpaceAndSquareBrackets(s);
			s = fn.SingularForm(s);
			s = fn.ReplacePhraseAllLevel(s);
			//s = fn.RemoveNondictionaryWordAllLevel(s);
			s = fn.NoStemWordAllLevel(s);
			s = fn.ReplacePhraseAllLevel(s);
			return s;

		}
        private string CleanStringForColumn(string str)
        {
            //remove non alphanumeric and space
            // make singular
            // replace phrase
            // remove stem words, and replace individual words 
            var s = str;
            s = fn.AlphaNumericSpaceAndSquareBrackets(s);
            s = fn.SingularForm(s);
            s = fn.ReplacePhraseAllLevel(s);
            //s = fn.RemoveNondictionaryWordAllLevel(s);
            s = fn.NoStemWordAllLevelColumn(s);
            s = fn.ReplacePhraseAllLevel(s);
            return s;

        }
        private string _sqlGetGoldCorpus(int contentSetId)
        {
            string sql = "";

            if (true || contentSetId == 1)
            {

                if (true || this._environment == "DEV")
                {
                    sql = @"	join gold_corpus_document_list gc 
                    on gc.document_id = nntf.document_id and gc.iconum = nntf.iconum ";
                }
            }
            return sql;
        }
        private string _sqlGetIsReview(int contentSetId)
        {
            string sql = "";

            if (true || contentSetId == 1)
            {
                sql = @" and ch.is_reviewed = true ";
            }
            return sql;
        }
        private SortedDictionary<string, long> _GetExistingClusterHierarchyForCompany(int iconum, int tableId) {
            int contentSetId = 1;
            SortedDictionary<string, long> entries = new SortedDictionary<string, long>();
            // TODO: think there is a bug here. not joining HTML table identification

            // TODO: need to increase timeout
			string sqltxt = string.Format(@"
select distinct ch.id, lower(nntf.raw_row_label)
	from cluster_mapping cm
	join norm_name_tree_flat nntf 
		on cm.norm_name_tree_flat_id = nntf.id " + _sqlGetGoldCorpus(contentSetId) +
    @" join cluster_hierarchy ch 
		on cm.cluster_hierarchy_id = ch.id
	join cluster_presentation_concept_type cpct 
		on ch.concept_type_id = cpct.concept_type_id
	join cluster_presentation cp 
		on cp.id = cpct.cluster_presentation_id
	join concept_type ct 
		on cpct.concept_type_id = ct.id
		and (ct.concept_association_type_id = 'R' or ct.concept_association_type_id = 'M')
	where cp.norm_table_id = {1} and nntf.iconum = {0}
		and coalesce( trim(nntf.raw_row_label),'')<>'' "
    + _sqlGetIsReview(contentSetId), iconum, tableId);
            if (this._environment == "DEV")
            {
                sqltxt = string.Format(@"
    select distinct ch.id, lower(nntf.raw_row_label)
    from cluster_hierarchy ch
    join cluster_mapping cm 
	    on cm.cluster_hierarchy_id = ch.id
    join concept_type ct
	    on ct.id = ch.concept_type_id
    join norm_name_tree_flat nntf 
	    on nntf.id = cm.norm_name_tree_flat_id
    join html_table_identification hti
	    on hti.document_id = nntf.document_id and hti.table_id =nntf.table_id and hti.file_id = nntf.file_id {0}  
	where hti.norm_table_id = {3} and hti.iconum = {2}
		and (ct.concept_association_type_id = 'R' or ct.concept_association_type_id = 'M')
		and coalesce( trim(nntf.raw_row_label),'')<>'' {1} "
     , _sqlGetGoldCorpus(contentSetId), _sqlGetIsReview(contentSetId), iconum, tableId);
            }
			int idx = 0;
			using (var sqlConn = new NpgsqlConnection(this._pgConnectionString))
			using (var cmd = new NpgsqlCommand(sqltxt, sqlConn)) {
				//cmd.Parameters.AddWithValue("@iconum", iconum);
				sqlConn.Open();
				using (var sdr = cmd.ExecuteReader()) {
					while (sdr.Read()) {
						try {
							var id = sdr.GetInt64(0);
							var rawlabel = sdr.GetStringSafe(1).ToLower();
                            if (!entries.ContainsKey(rawlabel))
                            {
                                entries[rawlabel] = id;
                            }


                        } catch {

						}
					}

				}
			}
            return entries;
		}

        private SortedDictionary<string, long> _GetExistingClusterHierarchyForIndustry(int tableId)
        {
            int contentSetId = 1;
            SortedDictionary<string, long> entries = new SortedDictionary<string, long>();
            string sqltxt2 = string.Format(@"
select distinct ch.id, lower(nntf.raw_row_label)
	from cluster_mapping cm
	join norm_name_tree_flat nntf 
		on cm.norm_name_tree_flat_id = nntf.id " + _sqlGetGoldCorpus(contentSetId) +
    @" join cluster_hierarchy ch 
		on cm.cluster_hierarchy_id = ch.id
	join cluster_presentation_concept_type cpct 
		on ch.concept_type_id = cpct.concept_type_id
	join cluster_presentation cp 
		on cp.id = cpct.cluster_presentation_id
	join concept_type ct 
		on cpct.concept_type_id = ct.id
		and (ct.concept_association_type_id = 'R' or ct.concept_association_type_id = 'M')
	where cp.norm_table_id = {1} and cp.industry_id = {0}
		and coalesce( trim(nntf.raw_row_label),'')<>'' "
    + _sqlGetIsReview(contentSetId), contentSetId, tableId);
            if (this._environment == "DEV")
            {
                sqltxt2 = string.Format(@"
    select distinct ch.id, lower(nntf.raw_row_label)
    from cluster_hierarchy ch
    join cluster_mapping cm 
	    on cm.cluster_hierarchy_id = ch.id
    join concept_type ct
	    on ct.id = ch.concept_type_id
    join norm_name_tree_flat nntf 
	    on nntf.id = cm.norm_name_tree_flat_id
    join html_table_identification hti
	    on hti.document_id = nntf.document_id and hti.table_id =nntf.table_id and hti.file_id = nntf.file_id {0}  
	where  hti.norm_table_id = {3}
		and (ct.concept_association_type_id = 'R' or ct.concept_association_type_id = 'M')
		and coalesce( trim(nntf.raw_row_label),'')<>'' {1} "
, _sqlGetGoldCorpus(contentSetId), _sqlGetIsReview(contentSetId), contentSetId, tableId);
            }
            if (entries.Count == 0)
            {
                using (var sqlConn = new NpgsqlConnection(this._pgConnectionString))
                using (var cmd = new NpgsqlCommand(sqltxt2, sqlConn))
                {
                    //cmd.Parameters.AddWithValue("@iconum", iconum);
                    sqlConn.Open();
                    using (var sdr = cmd.ExecuteReader())
                    {
                        while (sdr.Read())
                        {
                            try
                            {
                                var id = sdr.GetInt64(0);
                                var rawlabel = sdr.GetStringSafe(1).ToLower();
                                if (!entries.ContainsKey(rawlabel))
                                {
                                    entries[rawlabel] = id;
                                }


                            }
                            catch
                            {

                            }
                        }

                    }
                }
            }
            return entries;
        }
        private SortedDictionary<string, long> _GetExistingClusterColumnHierarchyForCompany(int iconum, int tableId)
        {
            int contentSetId = 1;
            SortedDictionary<string, long> entries = new SortedDictionary<string, long>();
            // TODO: think there is a bug here. not joining HTML table identification

            // TODO: need to increase timeout
            string sqltxt = string.Format(@"
select distinct ch.id, lower(nntf.raw_column_label)
	from cluster_mapping cm
	join norm_name_tree_flat nntf 
		on cm.norm_name_tree_flat_id = nntf.id " + _sqlGetGoldCorpus(contentSetId) +
    @" join cluster_hierarchy ch 
        on cm.cluster_hierarchy_id = ch.id
	join cluster_presentation_concept_type cpct 
		on ch.concept_type_id = cpct.concept_type_id
	join cluster_presentation cp 
		on cp.id = cpct.cluster_presentation_id
	join concept_type ct 
		on cpct.concept_type_id = ct.id
		and (ct.concept_association_type_id = 'C' or ct.concept_association_type_id = 'M')
	where cp.norm_table_id = {1} and nntf.iconum = {0}
		and coalesce( trim(nntf.raw_column_label),'')<>''
", iconum, tableId);
            if (this._environment == "DEV")
            {
                sqltxt = string.Format(@"
    select distinct ch.id, lower(nntf.raw_column_label)
    from cluster_hierarchy ch
    join cluster_mapping cm 
	    on cm.cluster_hierarchy_id = ch.id
    join concept_type ct
	    on ct.id = ch.concept_type_id
    join norm_name_tree_flat nntf 
	    on nntf.id = cm.norm_name_tree_flat_id
    join html_table_identification hti
	    on hti.document_id = nntf.document_id and hti.table_id =nntf.table_id and hti.file_id = nntf.file_id {0}  
	where hti.norm_table_id = {3} and hti.iconum = {2}
		and (ct.concept_association_type_id = 'C' or ct.concept_association_type_id = 'M')
		and coalesce( trim(nntf.raw_column_label),'')<>'' {1} "
, _sqlGetGoldCorpus(contentSetId), _sqlGetIsReview(contentSetId), iconum, tableId);
            }

            using (var sqlConn = new NpgsqlConnection(this._pgConnectionString))
            using (var cmd = new NpgsqlCommand(sqltxt, sqlConn))
            {
                //cmd.Parameters.AddWithValue("@iconum", iconum);
                sqlConn.Open();
                using (var sdr = cmd.ExecuteReader())
                {
                    while (sdr.Read())
                    {
                        try
                        {
                            var id = sdr.GetInt64(0);
                            var rawlabel = sdr.GetStringSafe(1).ToLower();
                            if (!entries.ContainsKey(rawlabel))
                            {
                                entries[rawlabel] = id;
                            }


                        }
                        catch
                        {

                        }
                    }

                }
            }
            return entries;
        }
        private SortedDictionary<string, long> _GetExistingClusterColumnHierarchyForIndustry(int tableId)
        {
            int contentSetId = 1;
            SortedDictionary<string, long> entries = new SortedDictionary<string, long>();
            string sqltxt2 = string.Format(@"
select distinct ch.id, lower(nntf.raw_column_label)
	from cluster_mapping cm
	join norm_name_tree_flat nntf 
		on cm.norm_name_tree_flat_id = nntf.id " + _sqlGetGoldCorpus(contentSetId) +
    @" join cluster_hierarchy ch 
		on cm.cluster_hierarchy_id = ch.id
	join cluster_presentation_concept_type cpct 
		on ch.concept_type_id = cpct.concept_type_id
	join cluster_presentation cp 
		on cp.id = cpct.cluster_presentation_id
	join concept_type ct 
		on cpct.concept_type_id = ct.id
		and (ct.concept_association_type_id = 'C' or ct.concept_association_type_id = 'M')
	where cp.norm_table_id = {1} and cp.industry_id = {0}
		and coalesce( trim(nntf.raw_column_label),'')<>''
", contentSetId, tableId);
            if (this._environment == "DEV")
            {
                sqltxt2 = string.Format(@"
select distinct ch.id, lower(nntf.raw_column_label)
	from cluster_mapping cm
	join norm_name_tree_flat nntf 
		on cm.norm_name_tree_flat_id = nntf.id " + _sqlGetGoldCorpus(contentSetId) +
    @" join html_table_identification hti 
		on hti.document_id = nntf.document_id and hti.table_id = nntf.table_id
	join cluster_hierarchy ch 
		on cm.cluster_hierarchy_id = ch.id
	join norm_table_concept_type ntct 
		on ntct.norm_table_id = hti.norm_table_id 
	join concept_type ct 
		on ch.concept_type_id = ct.id
		and (ct.concept_association_type_id = 'C' or ct.concept_association_type_id = 'M')
	where hti.norm_table_id = {1} 
		and coalesce( trim(nntf.raw_column_label),'')<>''
", contentSetId, tableId);
                sqltxt2 = string.Format(@"
    select distinct ch.id, lower(nntf.raw_column_label)
    from cluster_hierarchy ch
    join cluster_mapping cm 
	    on cm.cluster_hierarchy_id = ch.id
    join concept_type ct
	    on ct.id = ch.concept_type_id
    join norm_name_tree_flat nntf 
	    on nntf.id = cm.norm_name_tree_flat_id
    join html_table_identification hti
	    on hti.document_id = nntf.document_id and hti.table_id =nntf.table_id and hti.file_id = nntf.file_id {0}  
	where  hti.norm_table_id = {3}
		and (ct.concept_association_type_id = 'C' or ct.concept_association_type_id = 'M')
		and coalesce( trim(nntf.raw_column_label),'')<>'' {1} "
, _sqlGetGoldCorpus(contentSetId), _sqlGetIsReview(contentSetId), contentSetId, tableId);
            }
            int idx = 0;
            if (entries.Count == 0)
            {
                using (var sqlConn = new NpgsqlConnection(this._pgConnectionString))
                using (var cmd = new NpgsqlCommand(sqltxt2, sqlConn))
                {
                    //cmd.Parameters.AddWithValue("@iconum", iconum);
                    sqlConn.Open();
                    using (var sdr = cmd.ExecuteReader())
                    {
                        while (sdr.Read())
                        {
                            try
                            {
                                var id = sdr.GetInt64(0);
                                var rawlabel = sdr.GetStringSafe(1).ToLower();
                                if (!entries.ContainsKey(rawlabel))
                                {
                                    entries[rawlabel] = id;
                                }


                            }
                            catch
                            {

                            }
                        }

                    }
                }
            }
            return entries;
        }

        private SortedDictionary<string, long> _GetExistingClusterCleanColumnHierarchyForCompany(int iconum, int tableId)
        {
            int contentSetId = 1;
            SortedDictionary<string, long> entries = new SortedDictionary<string, long>();

            string sqltxt = string.Format(@"
select distinct ch.id, lower(nntf.cleaned_column_label)
	from cluster_mapping cm
	join norm_name_tree_flat nntf 
		on cm.norm_name_tree_flat_id = nntf.id " + _sqlGetGoldCorpus(contentSetId) +
    @" join cluster_hierarchy ch 
		on cm.cluster_hierarchy_id = ch.id
	join cluster_presentation_concept_type cpct 
		on ch.concept_type_id = cpct.concept_type_id
	join cluster_presentation cp 
		on cp.id = cpct.cluster_presentation_id
	join concept_type ct 
		on cpct.concept_type_id = ct.id
		and (ct.concept_association_type_id = 'C' or ct.concept_association_type_id = 'M')
	where cp.norm_table_id = {1} and nntf.iconum = {0}
		and coalesce( trim(nntf.cleaned_column_label),'')<>''
", iconum, tableId);
            if (this._environment == "DEV")
            {
                sqltxt = string.Format(@"
select distinct ch.id, lower(nntf.cleaned_column_label)
	from cluster_mapping cm
	join norm_name_tree_flat nntf 
		on cm.norm_name_tree_flat_id = nntf.id " + _sqlGetGoldCorpus(contentSetId) +
    @" 	join html_table_identification hti 
		on hti.document_id = nntf.document_id and hti.table_id = nntf.table_id
	join cluster_hierarchy ch 
		on cm.cluster_hierarchy_id = ch.id
	join norm_table_concept_type ntct 
		on ntct.norm_table_id = hti.norm_table_id 
	join concept_type ct 
		on ch.concept_type_id = ct.id
		and (ct.concept_association_type_id = 'C' or ct.concept_association_type_id = 'M')
	where hti.norm_table_id = {1} and hti.iconum = {0}
		and coalesce( trim(nntf.cleaned_column_label),'')<>''
", iconum, tableId);
                sqltxt = string.Format(@"
    select distinct ch.id, lower(nntf.cleaned_column_label)
    from cluster_hierarchy ch
    join cluster_mapping cm 
	    on cm.cluster_hierarchy_id = ch.id
    join concept_type ct
	    on ct.id = ch.concept_type_id
    join norm_name_tree_flat nntf 
	    on nntf.id = cm.norm_name_tree_flat_id
    join html_table_identification hti
	    on hti.document_id = nntf.document_id and hti.table_id =nntf.table_id and hti.file_id = nntf.file_id {0}  
	where hti.norm_table_id = {3} and hti.iconum = {2}
		and (ct.concept_association_type_id = 'C' or ct.concept_association_type_id = 'M')
		and coalesce( trim(nntf.cleaned_column_label),'')<>'' {1} "
, _sqlGetGoldCorpus(contentSetId), _sqlGetIsReview(contentSetId), iconum, tableId);
            }
            using (var sqlConn = new NpgsqlConnection(this._pgConnectionString))
            using (var cmd = new NpgsqlCommand(sqltxt, sqlConn))
            {
                //cmd.Parameters.AddWithValue("@iconum", iconum);
                sqlConn.Open();
                using (var sdr = cmd.ExecuteReader())
                {
                    while (sdr.Read())
                    {
                        try
                        {
                            var id = sdr.GetInt64(0);
                            var rawlabel = sdr.GetStringSafe(1);
                            if (!entries.ContainsKey(rawlabel))
                            {
                                entries[rawlabel] = id;
                            }


                        }
                        catch
                        {

                        }
                    }

                }
            }
            return entries;
        }
        private SortedDictionary<string, long> _GetExistingClusterCleanColumnHierarchyForIndustry(int tableId)
        {
            int contentSetId = 1;
            SortedDictionary<string, long> entries = new SortedDictionary<string, long>();
            string sqltxt2 = string.Format(@"
select distinct ch.id, lower(nntf.cleaned_column_label)
	from cluster_mapping cm
	join norm_name_tree_flat nntf 
		on cm.norm_name_tree_flat_id = nntf.id " + _sqlGetGoldCorpus(contentSetId) +
    @" join cluster_hierarchy ch 
		on cm.cluster_hierarchy_id = ch.id
	join cluster_presentation_concept_type cpct 
		on ch.concept_type_id = cpct.concept_type_id
	join cluster_presentation cp 
		on cp.id = cpct.cluster_presentation_id
	join concept_type ct 
		on cpct.concept_type_id = ct.id
		and (ct.concept_association_type_id = 'C' or ct.concept_association_type_id = 'M')
	where cp.norm_table_id = {1} and cp.industry_id = {0}
		and coalesce( trim(nntf.cleaned_column_label),'')<>''
", contentSetId, tableId);
            if (this._environment == "DEV")
            {
                sqltxt2 = string.Format(@"
select distinct ch.id, lower(nntf.cleaned_column_label)
	from cluster_mapping cm
	join norm_name_tree_flat nntf 
		on cm.norm_name_tree_flat_id = nntf.id " + _sqlGetGoldCorpus(contentSetId) +
   @" 	join html_table_identification hti 
		on hti.document_id = nntf.document_id and hti.table_id = nntf.table_id
	join cluster_hierarchy ch 
		on cm.cluster_hierarchy_id = ch.id
	join norm_table_concept_type ntct 
		on ntct.norm_table_id = hti.norm_table_id 
	join concept_type ct 
		on ch.concept_type_id = ct.id
		and (ct.concept_association_type_id = 'C' or ct.concept_association_type_id = 'M')
	where hti.norm_table_id = {1} 
		and coalesce( trim(nntf.cleaned_column_label),'')<>''
", contentSetId, tableId);
                sqltxt2 = string.Format(@"
    select distinct ch.id, lower(nntf.cleaned_column_label)
    from cluster_hierarchy ch
    join cluster_mapping cm 
	    on cm.cluster_hierarchy_id = ch.id
    join concept_type ct
	    on ct.id = ch.concept_type_id
    join norm_name_tree_flat nntf 
	    on nntf.id = cm.norm_name_tree_flat_id
    join html_table_identification hti
	    on hti.document_id = nntf.document_id and hti.table_id =nntf.table_id and hti.file_id = nntf.file_id {0}  
	where  hti.norm_table_id = {3}
		and (ct.concept_association_type_id = 'C' or ct.concept_association_type_id = 'M')
		and coalesce( trim(nntf.cleaned_column_label),'')<>'' {1} "
, _sqlGetGoldCorpus(contentSetId), _sqlGetIsReview(contentSetId), contentSetId, tableId);
            }
            if (entries.Count == 0)
            {
                using (var sqlConn = new NpgsqlConnection(this._pgConnectionString))
                using (var cmd = new NpgsqlCommand(sqltxt2, sqlConn))
                {
                    //cmd.Parameters.AddWithValue("@iconum", iconum);
                    sqlConn.Open();
                    using (var sdr = cmd.ExecuteReader())
                    {
                        while (sdr.Read())
                        {
                            try
                            {
                                var id = sdr.GetInt64(0);
                                var rawlabel = sdr.GetStringSafe(1);
                                if (!entries.ContainsKey(rawlabel))
                                {
                                    entries[rawlabel] = id;
                                }


                            }
                            catch
                            {

                            }
                        }

                    }
                }
            }
            return entries;
        }

        private SortedDictionary<string, long> _GetExistingClusterCleanLabelForCompany(int iconum, int tableId)
        {
            int contentSetId = 1;
            SortedDictionary<string, long> entries = new SortedDictionary<string, long>();

            string sqltxt = string.Format(@"
select distinct ch.id, lower(nntf.cleaned_row_label)
	from cluster_mapping cm
	join norm_name_tree_flat nntf 
		on cm.norm_name_tree_flat_id = nntf.id " + _sqlGetGoldCorpus(contentSetId) +
    @" join cluster_hierarchy ch 
		on cm.cluster_hierarchy_id = ch.id
	join cluster_presentation_concept_type cpct 
		on ch.concept_type_id = cpct.concept_type_id
	join cluster_presentation cp 
		on cp.id = cpct.cluster_presentation_id
	join concept_type ct 
		on cpct.concept_type_id = ct.id
		and (ct.concept_association_type_id = 'R' or ct.concept_association_type_id = 'M')
	where cp.norm_table_id = {1} and nntf.iconum = {0}
		and coalesce( trim(nntf.cleaned_row_label),'')<>''"
    + _sqlGetIsReview(contentSetId), iconum, tableId);
            if (this._environment == "DEV")
            {
                sqltxt = string.Format(@"
    select distinct ch.id, lower(nntf.cleaned_row_label)
    from cluster_hierarchy ch
    join cluster_mapping cm 
	    on cm.cluster_hierarchy_id = ch.id
    join concept_type ct
	    on ct.id = ch.concept_type_id
    join norm_name_tree_flat nntf 
	    on nntf.id = cm.norm_name_tree_flat_id
    join html_table_identification hti
	    on hti.document_id = nntf.document_id and hti.table_id =nntf.table_id and hti.file_id = nntf.file_id {0}  
	where hti.norm_table_id = {3} and hti.iconum = {2}
		and (ct.concept_association_type_id = 'R' or ct.concept_association_type_id = 'M')
		and coalesce( trim(nntf.cleaned_row_label),'')<>'' {1} "
, _sqlGetGoldCorpus(contentSetId), _sqlGetIsReview(contentSetId), iconum, tableId);
            }
            int idx = 0;
            using (var sqlConn = new NpgsqlConnection(this._pgConnectionString))
            using (var cmd = new NpgsqlCommand(sqltxt, sqlConn))
            {
                //cmd.Parameters.AddWithValue("@iconum", iconum);
                sqlConn.Open();
                using (var sdr = cmd.ExecuteReader())
                {
                    while (sdr.Read())
                    {
                        try
                        {
                            var id = sdr.GetInt64(0);
                            var rawlabel = sdr.GetStringSafe(1);
                            if (!entries.ContainsKey(rawlabel))
                            {
                                entries[rawlabel] = id;
                            }


                        }
                        catch
                        {

                        }
                    }

                }
            }
            return entries;
        }
        private SortedDictionary<string, long> _GetExistingClusterCleanLabelForIndustry(int tableId)
        {
            int contentSetId = 1;
            SortedDictionary<string, long> entries = new SortedDictionary<string, long>();

            string sqltxt2 = string.Format(@"
select distinct ch.id, lower(nntf.cleaned_row_label)
	from cluster_mapping cm
	join norm_name_tree_flat nntf 
		on cm.norm_name_tree_flat_id = nntf.id " + _sqlGetGoldCorpus(contentSetId) +
    @" join cluster_hierarchy ch 
		on cm.cluster_hierarchy_id = ch.id
	join cluster_presentation_concept_type cpct 
		on ch.concept_type_id = cpct.concept_type_id
	join cluster_presentation cp 
		on cp.id = cpct.cluster_presentation_id
	join concept_type ct 
		on cpct.concept_type_id = ct.id
		and (ct.concept_association_type_id = 'R' or ct.concept_association_type_id = 'M')
	where cp.norm_table_id = {1} and cp.industry_id = {0}
		and coalesce( trim(nntf.cleaned_row_label),'')<>''
" + _sqlGetIsReview(contentSetId), contentSetId, tableId);
            if (this._environment == "DEV")
            {
                sqltxt2 = string.Format(@"
    select distinct ch.id, lower(nntf.cleaned_row_label)
    from cluster_hierarchy ch
    join cluster_mapping cm 
	    on cm.cluster_hierarchy_id = ch.id
    join concept_type ct
	    on ct.id = ch.concept_type_id
    join norm_name_tree_flat nntf 
	    on nntf.id = cm.norm_name_tree_flat_id
    join html_table_identification hti
	    on hti.document_id = nntf.document_id and hti.table_id =nntf.table_id and hti.file_id = nntf.file_id {0}  
	where  hti.norm_table_id = {3}
		and (ct.concept_association_type_id = 'R' or ct.concept_association_type_id = 'M')
		and coalesce( trim(nntf.cleaned_row_label),'')<>'' {1} "
, _sqlGetGoldCorpus(contentSetId), _sqlGetIsReview(contentSetId), contentSetId, tableId);
            }
            int idx = 0;
            if (entries.Count == 0)
            {
                using (var sqlConn = new NpgsqlConnection(this._pgConnectionString))
                using (var cmd = new NpgsqlCommand(sqltxt2, sqlConn))
                {
                    //cmd.Parameters.AddWithValue("@iconum", iconum);
                    sqlConn.Open();
                    using (var sdr = cmd.ExecuteReader())
                    {
                        while (sdr.Read())
                        {
                            try
                            {
                                var id = sdr.GetInt64(0);
                                var rawlabel = sdr.GetStringSafe(1);
                                if (!entries.ContainsKey(rawlabel))
                                {
                                    entries[rawlabel] = id;
                                }


                            }
                            catch
                            {

                            }
                        }

                    }
                }
            }
            return entries;
        }
        private SortedDictionary<long, string> _GetIconumCleanLabels(int iconum, int tableId)
        {
            string sqltxt = string.Format(@"

  select distinct tf.id, tf.cleaned_row_label
	from  norm_name_tree_flat tf
  	join html_table_identification hti on hti.document_id = tf.document_id and hti.table_id = tf.table_id
where hti.norm_table_id = {0} 
	and tf.iconum = {1} 
	and (tf.cleaned_row_label = '') is not true


", tableId, iconum);
            return _GetIconumLabelsHelper(sqltxt);
        }
        private SortedDictionary<long, string> _GetIconumCleanLabels(int iconum, Guid docid, int tableId)
        {
            string sqltxt = string.Format(@"

  select distinct tf.id, tf.cleaned_row_label
	from norm_name_tree_flat tf 
  	join html_table_identification hti on hti.document_id = tf.document_id and hti.table_id = tf.table_id
where hti.norm_table_id = {0}
	and tf.iconum = {1} and tf.document_id = '{2}'
	and (tf.cleaned_row_label = '') is not true

", tableId, iconum, docid.ToString());
            return _GetIconumLabelsHelper(sqltxt);
        }

        private SortedDictionary<long, string> _GetIconumCleanColumnLabels(int iconum, Guid docid, int tableId)
        {
            string sqltxt = string.Format(@"

  select distinct tf.id, tf.cleaned_column_label
	from norm_name_tree_flat tf 
  	join html_table_identification hti on hti.document_id = tf.document_id and hti.table_id = tf.table_id
where hti.norm_table_id = {0}
	and tf.iconum = {1} and tf.document_id = '{2}'
	and (tf.cleaned_column_label = '') is not true

", tableId, iconum, docid.ToString());
            return _GetIconumLabelsHelper(sqltxt);
        }

        private SortedDictionary<long, string> _GetIconumRawLabels(int iconum, int tableId) {
			string sqltxt = string.Format(@"

  select distinct tf.id, tf.raw_row_label
	from norm_name_tree_flat tf 
  	join html_table_identification hti on hti.document_id = tf.document_id and hti.table_id = tf.table_id
where hti.norm_table_id = {0}
	and tf.iconum = {1} 
	and (tf.raw_row_label = '') is not true


", tableId, iconum);
			return _GetIconumLabelsHelper(sqltxt);
		}
		private SortedDictionary<long, string> _GetIconumRawLabels(int iconum, Guid docid, int tableId) {
			string sqltxt = string.Format(@"

  select distinct tf.id, tf.raw_row_label
	from norm_name_tree_flat tf 
  	join html_table_identification hti on hti.document_id = tf.document_id and hti.table_id = tf.table_id
where hti.norm_table_id = {0}
	and tf.iconum = {1} and tf.document_id = '{2}'
	and (tf.raw_row_label = '') is not true

", tableId, iconum, docid.ToString());
			return _GetIconumLabelsHelper(sqltxt);
		}

        private SortedDictionary<long, string> _GetIconumRawColumnLabels(int iconum, Guid docid, int tableId)
        {
            string sqltxt = string.Format(@"

  select distinct tf.id, tf.raw_column_label
	from norm_name_tree_flat tf 
  	join html_table_identification hti on hti.document_id = tf.document_id and hti.table_id = tf.table_id and hti.file_id = tf.file_id
where hti.norm_table_id = {0}
	and tf.iconum = {1} and tf.document_id = '{2}'
	and (tf.raw_column_label = '') is not true

", tableId, iconum, docid.ToString());
            return _GetIconumLabelsHelper(sqltxt);
        }


        private SortedDictionary<long, string> _GetIconumLabelsHelper(string sqltxt) {
			SortedDictionary<long, string> entries = new SortedDictionary<long, string>();


			int idx = 0;
			using (var sqlConn = new NpgsqlConnection(this._pgConnectionString))
			using (var cmd = new NpgsqlCommand(sqltxt, sqlConn)) {
				//cmd.Parameters.AddWithValue("@iconum", iconum);
				sqlConn.Open();
				using (var sdr = cmd.ExecuteReader()) {
					while (sdr.Read()) {
						try {
							var id = sdr.GetInt64(0);
							var rawlabel = sdr.GetStringSafe(1).ToLower();
							if (!string.IsNullOrWhiteSpace(rawlabel)) {
								entries[id] = rawlabel;

							}
						} catch {

						}
					}

				}
			}
			return entries;
		}

	}
    public static class Extensions
    {
        public static Dictionary<long, long> Eat(this Dictionary<long, long> survive, Dictionary<long, long> eaten)
        {
            foreach (var change in eaten)
            {
                if (!survive.ContainsKey(change.Key))
                {
                    survive[change.Key] = change.Value;
                }
            }
            return survive;
        }
        public static List<NormNameTreeTable> Clone(this List<NormNameTreeTable> listToClone)
        {
            List<NormNameTreeTable> newList = new List<NormNameTreeTable>();
            newList.AddRange(listToClone.Select(item => item));
            return newList;
        }
        public static StringBuilder AppendLineBreak(this StringBuilder sb, string line)
        {
            sb.AppendLine(line + "<BR>");
            return sb;
        }
    }
    public class NormNameTreeRow
    {
        public HashSet<long> FlatIds = new HashSet<long>();
        public string RawRowLabel;
        public string CleanedRowLabel;
        public string FinalLabel;
        public long ClusterId;
        public int DatabaseRowId;
        public int NormalizedRowId;

        public string CleanedRawRowLabel;
        public string CleanedCleanedRowLabel;
        public string CleanedFinalLabel;
    }
    public class NormNameTreeTable
    {
        public SortedList<int, NormNameTreeRow> Rows = new SortedList<int, NormNameTreeRow>();
        public NormNameTreeTable()
        {

        }
        public NormNameTreeTable Load(string connectionString, int iconum, Guid guid, int table_id)
        {
            try
            {
                return _Load(connectionString, iconum, guid, table_id);
            }
            catch
            {
                return new NormNameTreeTable();
            }
        }

        public NormNameTreeTable LoadHistorical(string connectionString, int iconum, Guid guid, int table_id)
        {
            try
            {
                return _LoadHistorical(connectionString, iconum, guid, table_id);
            }
            catch
            {
                return new NormNameTreeTable();
            }
        }

        private NormNameTreeTable _Load(string connectionString, int iconum, Guid guid, int table_id)
        {
            SortedDictionary<string, long> entries = new SortedDictionary<string, long>();

            string sqltxt = string.Format(@"
select f.id, f.raw_row_label, f.cleaned_row_label, f.final_label,
	 coalesce(cm.cluster_hierarchy_id, 0), f.row_id, f.col_id, f.file_id
from norm_name_tree_flat f 
left join cluster_mapping cm on f.id = cm.norm_name_tree_flat_id
left join cluster_hierarchy ch 
		on cm.cluster_hierarchy_id = ch.id
left join cluster_presentation_concept_type cpct 
		on ch.concept_type_id = cpct.concept_type_id
left join cluster_presentation cp 
		on cp.id = cpct.cluster_presentation_id
left join concept_type ct 
		on cpct.concept_type_id = ct.id
where f.document_id = '{1}' and f.iconum = {0}
and f.table_id = {2} and (length(f.item_offset) = 0 or f.item_offset like '%|r0')
and (ct.concept_association_type_id is null or ct.concept_association_type_id = 'R' or ct.concept_association_type_id = 'M')
order by f.file_id, f.row_id, f.col_id
", iconum, guid, table_id);
            int idx = 0;
            int curr_row_index = 0;
            var curr_row = new NormNameTreeRow();
            string last_raw_label = "";
            int last_database_row_id = -1;
            int last_file_id = -1;
            using (var sqlConn = new NpgsqlConnection(connectionString))
            using (var cmd = new NpgsqlCommand(sqltxt, sqlConn))
            {
                cmd.CommandTimeout = 600;
                //cmd.Parameters.AddWithValue("@iconum", iconum);
                sqlConn.Open();
                using (var sdr = cmd.ExecuteReader())
                {
                    while (sdr.Read())
                    {
                        try
                        {
                            var flat_id = sdr.GetInt64(0);
                            var curr_raw_label = sdr.GetStringSafe(1);
                            var cluster_id = sdr.GetInt64(4);
                            int curr_database_row_id = sdr.GetInt32(5);
                            var curr_file_id = sdr.GetInt32(7);
                            if (last_file_id >= 0)
                            {
                                if (curr_file_id != last_file_id)
                                    return this;
                            }
                            else
                            {
                                last_file_id = curr_file_id;
                            }
                            bool isNewRow = false;
                            if (last_database_row_id < 0)
                            {
                                isNewRow = true;
                            }
                            else
                            {
                                if (last_database_row_id == curr_database_row_id)
                                {
                                    if (last_raw_label != curr_raw_label)
                                    {
                                        throw new DataException("Row label doesn't match");
                                    }
                                }
                                else
                                {
                                    isNewRow = true;
                                }
                            }
                            if (isNewRow)
                            {
                                var newRow = new NormNameTreeRow();
                                curr_row = newRow;
                                curr_row.RawRowLabel = curr_raw_label;
                                curr_row.CleanedRawRowLabel = fn.RemoveHierarchyNumberSpace(curr_row.RawRowLabel);
                                curr_row.CleanedRowLabel = sdr.GetStringSafe(2);
                                curr_row.CleanedCleanedRowLabel = fn.RemoveHierarchyNumberSpace(curr_row.CleanedRowLabel);
                                curr_row.FinalLabel = sdr.GetStringSafe(3);
                                curr_row.CleanedFinalLabel = fn.RemoveHierarchyNumberSpace(curr_row.FinalLabel);
                                curr_row.ClusterId = cluster_id;
                                curr_row.DatabaseRowId = curr_database_row_id;
                                curr_row.NormalizedRowId = curr_row_index;
                                curr_row.FlatIds.Add(flat_id);
                                this.Rows.Add(curr_row_index, newRow);
                                curr_row_index++;
                                last_database_row_id = curr_database_row_id;
                                last_raw_label = curr_raw_label;
                            }
                            else
                            {
                                curr_row.FlatIds.Add(flat_id);
                            }
                        }
                        catch (Exception ex)
                        {
                            throw ex;
                        }
                    }

                }
            }
            return this;
        }

        private NormNameTreeTable _LoadHistorical(string connectionString, int iconum, Guid guid, int table_id)
        {
            SortedDictionary<string, long> entries = new SortedDictionary<string, long>();

            string sqltxt = string.Format(@"
select f.id, f.raw_row_label, f.cleaned_row_label, f.final_label,
	 coalesce(cm.cluster_hierarchy_id, 0), f.row_id, f.col_id, f.file_id
from norm_name_tree_flat f 
join gold_corpus_document_list gc 
                    on gc.document_id = f.document_id and gc.iconum = f.iconum
left join cluster_mapping cm on f.id = cm.norm_name_tree_flat_id
left join cluster_hierarchy ch 
		on cm.cluster_hierarchy_id = ch.id
left join cluster_presentation_concept_type cpct 
		on ch.concept_type_id = cpct.concept_type_id
left join cluster_presentation cp 
		on cp.id = cpct.cluster_presentation_id
left join concept_type ct 
		on cpct.concept_type_id = ct.id
where f.document_id = '{1}' and f.iconum = {0}
and f.table_id = {2} and (length(f.item_offset) = 0 or f.item_offset like '%|r0')
and (ct.concept_association_type_id is null or ct.concept_association_type_id = 'R' or ct.concept_association_type_id = 'M')
order by f.file_id, f.row_id, f.col_id
", iconum, guid, table_id);
            int idx = 0;
            int curr_row_index = 0;
            var curr_row = new NormNameTreeRow();
            string last_raw_label = "";
            int last_database_row_id = -1;
            int last_file_id = -1;
            using (var sqlConn = new NpgsqlConnection(connectionString))
            using (var cmd = new NpgsqlCommand(sqltxt, sqlConn))
            {
                cmd.CommandTimeout = 600;
                //cmd.Parameters.AddWithValue("@iconum", iconum);
                sqlConn.Open();
                using (var sdr = cmd.ExecuteReader())
                {
                    while (sdr.Read())
                    {
                        try
                        {
                            var flat_id = sdr.GetInt64(0);
                            var curr_raw_label = sdr.GetStringSafe(1);
                            var cluster_id = sdr.GetInt64(4);
                            int curr_database_row_id = sdr.GetInt32(5);
                            var curr_file_id = sdr.GetInt32(7);
                            if (last_file_id >= 0)
                            {
                                if (curr_file_id != last_file_id)
                                    return this;
                            }
                            else
                            {
                                last_file_id = curr_file_id;
                            }
                            bool isNewRow = false;
                            if (last_database_row_id < 0)
                            {
                                isNewRow = true;
                            }
                            else
                            {
                                if (last_database_row_id == curr_database_row_id)
                                {
                                    if (last_raw_label != curr_raw_label)
                                    {
                                        throw new DataException("Row label doesn't match");
                                    }
                                }
                                else
                                {
                                    isNewRow = true;
                                }
                            }
                            if (isNewRow)
                            {
                                var newRow = new NormNameTreeRow();
                                curr_row = newRow;
                                curr_row.RawRowLabel = curr_raw_label;
                                curr_row.CleanedRawRowLabel = fn.RemoveHierarchyNumberSpace(curr_row.RawRowLabel);
                                curr_row.CleanedRowLabel = sdr.GetStringSafe(2);
                                curr_row.CleanedCleanedRowLabel = fn.RemoveHierarchyNumberSpace(curr_row.CleanedRowLabel);
                                curr_row.FinalLabel = sdr.GetStringSafe(3);
                                curr_row.CleanedFinalLabel = fn.RemoveHierarchyNumberSpace(curr_row.FinalLabel);
                                curr_row.ClusterId = cluster_id;
                                curr_row.DatabaseRowId = curr_database_row_id;
                                curr_row.NormalizedRowId = curr_row_index;
                                curr_row.FlatIds.Add(flat_id);
                                this.Rows.Add(curr_row_index, newRow);
                                curr_row_index++;
                                last_database_row_id = curr_database_row_id;
                                last_raw_label = curr_raw_label;
                            }
                            else
                            {
                                curr_row.FlatIds.Add(flat_id);
                            }
                        }
                        catch (Exception ex)
                        {
                            throw ex;
                        }
                    }

                }
            }
            return this;
        }

        private bool _satisfiesWholeTableMerge(TableComparison comp)
        {
            bool result = false;
            if (comp.IsInOrder)// && comp.TotalMatchCount == comp.StraightLineMatchCount)
            {
                if (comp.FirstStraightLineMatch == 0 && comp.LastStraightLineMatch > (comp.Table1Size - 2))
                {
                    // top and bottm matches
                    if ((double)comp.StraightLineMatchCount / (double)comp.Table1Size > 0.5)
                    {
                        result = true;
                    }
                }
            }
            return result;
        }
        private bool _satisfiesPartialTableMerge(TableComparison comp)
        {
            bool result = false;
            if (comp.IsInOrder)// && comp.TotalMatchCount == comp.StraightLineMatchCount)
            {
                if (comp.FirstStraightLineMatch == 0 && comp.LastStraightLineMatch > (comp.Table1Size - 2))
                {
                    // top and bottm matches
                    if ((double)comp.StraightLineMatchCount / (double)comp.Table1Size > 0.5)
                    {
                        result = true;
                    }
                }
            }
            return result;
        }
        public Dictionary<long, long> MergeWithHistoricalTable(NormNameTreeTable hist)
        {
            Dictionary<long, long> result = new Dictionary<long, long>();
            if (this.Rows.Count == 0 || hist.Rows.Count == 0)
            {
                return result;
            }
            if (this.Rows.Count == hist.Rows.Count)
            {
                var analysisObj = _analyze(this, hist);
                if (_satisfiesWholeTableMerge(analysisObj))
                {
                    // if first line and last line matches, and match percentage is > 0.5 then straightline merge
                    // if fistline merge didn't work, and match percentage > 0.5 then merge within limited
                    result = _straightlineMergeWholeTable(this, hist);
                }
                else if (_satisfiesPartialTableMerge(analysisObj))
                {
                    result = _straightlineMergePartialTable(this, hist, analysisObj.FirstStraightLineMatch, analysisObj.LastStraightLineMatch);
                }
            }
            else if (this.Rows.Count > hist.Rows.Count)
            {
                if (this.Rows.Count - hist.Rows.Count == 1)
                {
                    // remove one row from unmatched, and treat it as same count
                }

            }
            else if (this.Rows.Count < hist.Rows.Count)
            {
                if (hist.Rows.Count - this.Rows.Count == 1)
                {
                    // remove one row from unmatched, and see treat it as same count
                }
            }


            return result;
        }

        private Dictionary<long, long> _straightlineMergeWholeTable(NormNameTreeTable curr, NormNameTreeTable hist)
        {
            Dictionary<long, long> result = new Dictionary<long, long>();
            for (int i = 0; i < curr.Rows.Count; i++)
            {
                for (int j = 0; j < hist.Rows.Count; j++)
                {
                    if (j < i) continue;
                    else if (j > i) break;

                    var hist_cluster_id = hist.Rows[j].ClusterId;
                    if (hist_cluster_id <= 0) break;
                    foreach (var flat_id in curr.Rows[i].FlatIds)
                    {
                        result.Add(flat_id, hist_cluster_id);
                    }
                }
            }
            return result;
        }

        private Dictionary<long, long> _straightlineMergePartialTable(NormNameTreeTable curr, NormNameTreeTable hist, int first, int last)
        {
            Dictionary<long, long> result = new Dictionary<long, long>();
            for (int i = first; i < last; i++)
            {
                for (int j = first; j < last; j++)
                {
                    if (j < i) continue;
                    else if (j > i) break;

                    var hist_cluster_id = hist.Rows[j].ClusterId;
                    if (hist_cluster_id <= 0) break;
                    foreach (var flat_id in curr.Rows[i].FlatIds)
                    {
                        result.Add(flat_id, hist_cluster_id);
                    }
                }
            }
            return result;
        }
        private bool _isMatch(NormNameTreeRow curr, NormNameTreeRow hist)
        {
            bool result = false;
            if ((curr.RawRowLabel == hist.RawRowLabel) ||
                (curr.CleanedRawRowLabel == hist.CleanedRawRowLabel) ||
                (curr.CleanedRowLabel == hist.CleanedRowLabel) ||
                (curr.CleanedCleanedRowLabel == hist.CleanedCleanedRowLabel) ||
                (curr.FinalLabel == hist.FinalLabel) ||
                (curr.CleanedFinalLabel == hist.CleanedFinalLabel))
            {
                result = true;
            }
            return result;
        }
        public class TableComparison
        {
            public int Table1Size = -1;
            public int Table2Size = -1;
            public bool IsInOrder = false;
            public int StraightLineMatchCount = -1;
            public int TotalMatchCount = -1;
            public int FirstStraightLineMatch = -1;
            public int LastStraightLineMatch = -1;
        }
        private TableComparison _analyze(NormNameTreeTable curr, NormNameTreeTable hist)
        {
            TableComparison result = new TableComparison();
            result.Table1Size = curr.Rows.Count;
            result.Table2Size = hist.Rows.Count;
            int lastHistoryRow = 0;
            int straightLineMatchCount = 0;
            bool isInOrder = true;
            int firstStraightLineMatch = -1;
            int lastStraightLineMatch = -1;
            HashSet<int> alreadyMatchedHistoryRow = new HashSet<int>();
            for (int i = 0; i < curr.Rows.Count; i++)
            {
                bool isMatched = false;
                int currMatched = -1;
                for (int j = 0; j < hist.Rows.Count; j++)
                {
                    if (alreadyMatchedHistoryRow.Contains(j))
                    {   // for cases with duplicated labels
                        continue;
                    }
                    if (_isMatch(curr.Rows[i], hist.Rows[j]))
                    {
                        currMatched = j;
                        alreadyMatchedHistoryRow.Add(currMatched);
                        isMatched = true;
                        break;
                    }
                }
                if (isMatched)
                {
                    if (currMatched < lastHistoryRow)
                    {
                        isInOrder = false;
                    }
                    lastHistoryRow = currMatched;
                    if (i == currMatched)
                    {
                        if (straightLineMatchCount == 0)
                        {
                            firstStraightLineMatch = currMatched;
                        }
                        straightLineMatchCount++;
                        lastStraightLineMatch = currMatched;
                    }
                }
            }
            result.StraightLineMatchCount = straightLineMatchCount;
            result.TotalMatchCount = alreadyMatchedHistoryRow.Count;
            result.IsInOrder = isInOrder;
            result.FirstStraightLineMatch = firstStraightLineMatch;
            result.LastStraightLineMatch = lastStraightLineMatch;
            return result;
        }
        private bool _isInOrderObselete(NormNameTreeTable curr, NormNameTreeTable hist)
        {
            bool result = false;
            int lastHistoryRow = 0;
            HashSet<int> alreadyMatchedHistoryRow = new HashSet<int>();
            foreach (var c in curr.Rows)
            {
                bool isMatched = false;
                int currMatched = -1;
                for (int j = 0; j < hist.Rows.Count; j++)
                {
                    if (alreadyMatchedHistoryRow.Contains(j))
                    {   // for cases with duplicated labels
                        continue;
                    }
                    if (_isMatch(c.Value, hist.Rows[j]))
                    {
                        currMatched = j;
                        alreadyMatchedHistoryRow.Add(currMatched);
                        isMatched = true;
                        break;
                    }
                }
                if (isMatched)
                {
                    if (currMatched < lastHistoryRow)
                    {
                        return false;
                    }
                    lastHistoryRow = currMatched;
                }
            }
            result = true;
            return result;
        }
        private bool _matchCount(NormNameTreeTable curr, NormNameTreeTable hist)
        {
            bool result = false;
            int lastHistoryRow = 0;
            HashSet<int> alreadyMatchedHistoryRow = new HashSet<int>();
            foreach (var c in curr.Rows)
            {
                bool isMatched = false;
                int currMatched = -1;
                for (int j = 0; j < hist.Rows.Count; j++)
                {
                    if (alreadyMatchedHistoryRow.Contains(j))
                    {   // for cases with duplicated labels
                        continue;
                    }
                    if (_isMatch(c.Value, hist.Rows[j]))
                    {
                        currMatched = j;
                        alreadyMatchedHistoryRow.Add(currMatched);
                        isMatched = true;
                        break;
                    }
                }
                if (isMatched)
                {
                    if (currMatched < lastHistoryRow)
                    {
                        return false;
                    }
                    lastHistoryRow = currMatched;
                }
            }
            result = true;
            return result;
        }


    }
}
