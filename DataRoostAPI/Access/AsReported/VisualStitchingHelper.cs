using System;
using System.IO;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using Npgsql;
using NpgsqlTypes;
using System.Net;
using Newtonsoft.Json;
using System.Linq;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using FactSet.Data.SqlClient;
using System.Text.RegularExpressions;

namespace CCS.Fundamentals.DataRoostAPI.Access.AsReported
{
    public class VisualStitchingHelper
    {

        private readonly string _sfConnectionString;

        static VisualStitchingHelper()
        {

        }

        public VisualStitchingHelper(string sfConnectionString)
        {
            this._sfConnectionString = sfConnectionString;
        }

        private string factsetIOconnString = "Host=ip-172-31-81-210.manager.factset.io;Port=32791;Username=uyQKYrcTSrnnqB;Password=NoCLf_xBeXiB0UXZjhZUNg7Zx8;Database=di8UFb70sJdA5e;sslmode=Require;Trust Server Certificate=true;";
        private string connString = "Host=ffautomation-dev-postgres.c8vzac0v5wdo.us-east-1.rds.amazonaws.com;Port=5432;Username=ffautomation_writer_user;Password=qyp0nMeA;Database=postgres;"; // sslmode=Require;Trust Server Certificate=true;

        public string GetJson(int id)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            using (
                var conn = new NpgsqlConnection(connString))
            {
                conn.Open();
                // Retrieve all rows
                using (var cmd = new NpgsqlCommand("SELECT value FROM json where id = @id LIMIT 1", conn))
                {
                    cmd.Parameters.AddWithValue("id", id);
                    using (var reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                            sb.Append(reader.GetString(0));
                    }
                }
            }
            return sb.ToString();
        }
        public string GetJsonByHash(string hashkey)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            using (
                var conn = new NpgsqlConnection(connString))
            {
                conn.Open();
                // Retrieve all rows
                using (var cmd = new NpgsqlCommand("SELECT value FROM json where hashkey = @hashkey LIMIT 1", conn))
                {
                    cmd.Parameters.AddWithValue("@hashkey", hashkey);
                    using (var reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                            sb.Append(reader.GetString(0));
                    }
                }
            }
            return sb.ToString();
        }
        public int SetJsonByHash(string hashkey, string value)
        {
            string query = @"
UPDATE json SET value=@value WHERE hashkey=@hashkey;
INSERT INTO json (value, hashkey)
       SELECT @value, @hashkey
       WHERE NOT EXISTS (SELECT 1 FROM json WHERE  hashkey=@hashkey);

SELECT coalesce(id, -1) FROM json where hashkey = @hashkey LIMIT 1;

";
            int result = 0;
            try
            {
                using (
                    var conn = new NpgsqlConnection(connString))
                {
                    conn.Open();
                    // Retrieve all rows
                    using (var cmd = new NpgsqlCommand(query, conn))
                    {
                        cmd.Parameters.AddWithValue("@hashkey", hashkey);
                        cmd.Parameters.AddWithValue("@value", value);
                        using (var reader = cmd.ExecuteReader())
                        {
                            while (reader.Read())
                                result = reader.GetInt32(0);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                result = -1;
            }
            return result;
        }
        public class TintInfo
        {

            [JsonProperty("tables")]
            public List<Table> Tables { get; set; }
            [JsonProperty("timeslices")]
            public List<TimeSlice> TimeSlices { get; set; }
        }
        public class Table
        {
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
        public class TimeSlice
        {
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
        public class Cell
        {
            [JsonProperty("rowId")]
            public int rowId { get; set; }
            [JsonProperty("columnId")]
            public int columnId { get; set; }
            [JsonProperty("offset")]
            public string offset { get; set; }

        }
        public class Row
        {
            [JsonProperty("rowId")]
            public int Id { get; set; }
            [JsonProperty("label")]
            public string Label { get; set; }
            [JsonProperty("labelHierarhcy")]
            public List<string> LabelHierarchy { get; set; }

        }
        public class Column
        {
            [JsonProperty("columnId")]
            public int Id { get; set; }
            [JsonProperty("columnHeader")]
            public string columnHeader { get; set; }

        }
        public class Value
        {
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
        public class Node
        {
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
            [JsonIgnore]
            public string Childrentitle { get; set; }

        }

        public class ReactNode {
            [JsonProperty("id")]
            public int Id { get; set; }
            [JsonProperty("title")]
            public string Title { get; set; }
            [JsonProperty("children")]
            public List<ReactNode> Nodes { get; set; }
        }


        private string GetTintFile(string url)
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
                throw new FileNotFoundException("call failed");
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

        public string GetDataTree()
        {

            int tries = 1;
            List<Node> nodes = new List<Node>();

            while (tries > 0)
            {
                try
                {
                    nodes = GetAngularTree();
                    tries = 0;
                }
                catch (Exception ex)
                {
                    if (--tries > 0)
                    {
                        System.Threading.Thread.Sleep(1000);
                    }
                    else
                    {
                        return JsonConvert.SerializeObject(new List<Node>());
                    }

                }
            }
            return JsonConvert.SerializeObject(nodes);
        }
        public string GetPostGresDataTree()
        {

            int tries = 1;
            List<Node> nodes = new List<Node>();

            while (tries > 0)
            {
                try
                {
                    nodes = GetAngularTreePostGres();
                    tries = 0;
                }
                catch (Exception ex)
                {
                    if (--tries > 0)
                    {
                        System.Threading.Thread.Sleep(1000);
                    }
                    else
                    {
                        return JsonConvert.SerializeObject(new List<Node>());
                    }

                }
            }
            return JsonConvert.SerializeObject(nodes);
        }
        public static string DeepCleanString(string str)
        {
            string result = str.ToLower();
 
            string[] remlabelWords = result.Split(' ');
            for (int i = 0; i < remlabelWords.Count(); i++)
            {
                var ii = new Regex(@"ii$");
                var ies = new Regex(@"ies$");
                var es = new Regex(@"(s|x|ch|sh)es$");
                var s = new Regex(@"([a-z]){3,}s$");
                var leases = new Regex(@"leases$");
                var less = new Regex(@"less");
                if (remlabelWords[i] == "radii")
                {
                    remlabelWords[i] = "radius";
                }
                else if (ii.IsMatch(remlabelWords[i]))
                {
                    remlabelWords[i] = Regex.Replace(remlabelWords[i], @"ii$", "us");
                }
                else if (leases.IsMatch(remlabelWords[i]))
                {
                    remlabelWords[i] = Regex.Replace(remlabelWords[i], @"leases$", "lease");
                }
                else if (less.IsMatch(remlabelWords[i]))
                {
                }
                else if (ies.IsMatch(remlabelWords[i]))
                {
                    remlabelWords[i] = Regex.Replace(remlabelWords[i], @"ies$", "y");
                }
                else if (es.IsMatch(remlabelWords[i]))
                {
                    remlabelWords[i] = Regex.Replace(remlabelWords[i], @"es$", "");
                }
                else if (s.IsMatch(remlabelWords[i]))
                {
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
        private List<Node> GetAngularTree()
        {
            const string query = @"
SELECT  [Id]
      ,[Label]
  FROM [ffdocumenthistory].[dbo].[GDBClusters_1203] order by id
			";
                List<Node> allNodes = new List<Node>();
                using (SqlConnection conn = new SqlConnection(_sfConnectionString))
                {
                    using (SqlCommand cmd = new SqlCommand(query, conn))
                    {
                        conn.Open();

                        using (SqlDataReader sdr = cmd.ExecuteReader())
                        {
                            while (sdr.Read())
                            {
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
            if (true)
            {
                Node t = new Node();
                nodes.Add(t);
                t.Id = 0;
                t.Title = "AVG-BS";
                t.Nodes = new List<Node>();
                Stack<Node> stack = new Stack<Node>();
                stack.Push(t);

                foreach (var row in allNodes)
                {
                    int i = 0;
                    var cleanedRowTitle = DeepCleanString(row.Title);
                    var labelHierarchy = cleanedRowTitle.Replace("[", "").Split(new char[] { ']' }, StringSplitOptions.RemoveEmptyEntries);
                    foreach (var labelAtlevel in labelHierarchy)
                    {
                        i++;
                        if (stack.Count <= i)
                        {
                            break;
                        }
                        if (stack.ElementAt(stack.Count - i - 1).Title != labelAtlevel)
                        {
                            while (stack.Count > i && stack.Count > 1)
                            {
                                stack.Pop();
                            }
                        }
                    }
                    var lastRoot = stack.Peek();
                    var endLabel = labelHierarchy.Last();
                    i = 0;
                    int j = 0;  
                    foreach (var labelAtlevel in labelHierarchy)
                    {
                        i++; 
                        if (stack.Count > i)
                        {// count to the last common level. 
                            continue;
                        }
                        if (stack.Peek().Title != labelAtlevel && labelAtlevel != endLabel)
                        {
                            var currentRoot = stack.Peek();
                            bool found = false;
                            foreach (var m in currentRoot.Nodes)
                            {
                                if (m.Title == labelAtlevel)
                                {
                                    lastRoot = m;
                                    stack.Push(m);
                                    found = true;
                                    break;
                                }
                            }
                            if (!found)
                            {
                                Node r = new Node();
                                r.Id = -1;
                                r.Title = labelAtlevel;
                                r.Nodes = new List<Node>();
                                lastRoot.Nodes.Add(r);
                                lastRoot = r;
                                stack.Push(r);
                            }
                        }
                        else
                        {
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
            foreach (var n in nodes)
            {
                nodeDocuments(n);
            }
            List<Node> newTree = new List<Node>();
            foreach (var n in nodes.First().Nodes)
            {
                newTree.Add(n);
            }
            return newTree;
        }


        public static string PGConnectionString()
        {
            return "Host=dsnametree.cluster-c8vzac0v5wdo.us-east-1.rds.amazonaws.com;Port=5432;Username=nametreedata_admin_user;Password=UEmtE39C;Database=nametreedata;sslmode=Require;Trust Server Certificate=true;";
        }

        private List<Node> GetAngularTreePostGres()
        {
            const string query = @"
SELECT  Id,Label, 0
  FROM cluster_name_tree  order by id
			";
            List<Node> allNodes = new List<Node>();

            using (var conn = new NpgsqlConnection(PGConnectionString()))
            {
                using (var cmd = new NpgsqlCommand(query, conn))
                {
                    conn.Open();

                    using (var sdr = cmd.ExecuteReader())
                    {
                        while (sdr.Read())
                        {
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
            if (true)
            {
                Node t = new Node();
                nodes.Add(t);
                t.Id = 0;
                t.Title = "AVG-BS";
                t.Nodes = new List<Node>();
                Stack<Node> stack = new Stack<Node>();
                stack.Push(t);

                foreach (var row in allNodes)
                {
                    int i = 0;
                    var cleanedRowTitle = DeepCleanString(row.Title);
                    var labelHierarchy = cleanedRowTitle.Replace("[", "").Split(new char[] { ']' }, StringSplitOptions.RemoveEmptyEntries);
                    if (labelHierarchy.Length == 0)
                        continue;
                    foreach (var labelAtlevel in labelHierarchy)
                    {
                        i++;
                        if (stack.Count <= i)
                        {
                            break;
                        }
                        if (stack.ElementAt(stack.Count - i - 1).Title != labelAtlevel)
                        {
                            while (stack.Count > i && stack.Count > 1)
                            {
                                stack.Pop();
                            }
                        }
                    }
                    var lastRoot = stack.Peek();
                    var endLabel = labelHierarchy.Last();
                    i = 0;
                    int j = 0;
                    foreach (var labelAtlevel in labelHierarchy)
                    {
                        i++;
                        if (stack.Count > i)
                        {// count to the last common level. 
                            continue;
                        }
                        if (stack.Peek().Title != labelAtlevel && labelAtlevel != endLabel)
                        {
                            var currentRoot = stack.Peek();
                            bool found = false;
                            foreach (var m in currentRoot.Nodes)
                            {
                                if (m.Title == labelAtlevel)
                                {
                                    lastRoot = m;
                                    stack.Push(m);
                                    found = true;
                                    break;
                                }
                            }
                            if (!found)
                            {
                                Node r = new Node();
                                r.Id = -1;
                                r.Title = labelAtlevel;
                                r.ParentId = row.ParentId;
                                r.Nodes = new List<Node>();
                                lastRoot.Nodes.Add(r);
                                lastRoot = r;
                                stack.Push(r);
                            }
                        }
                        else
                        {
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
            foreach (var n in nodes)
            {
                PGnodeDocuments2(n);
            }
            List<Node> newTree = new List<Node>();
            Node unknown = new Node();
            unknown.Id = 0;
            unknown.Title = "Unknown";
            unknown.Nodes = new List<Node>();
            foreach (var n in nodes.First().Nodes)
            {
                if (n.Title.StartsWith("total asset") || n.Title.StartsWith("total liability and shareholder equity") || n.Title.StartsWith("total asset"))
                {
                    newTree.Add(n);
                }
                else
                {
                    unknown.Nodes.Add(n);
                }
            }
            newTree.Add(unknown);
            return newTree;
        }

        private Dictionary<long, List<Tuple<Guid, string,string>>> documentCluster = new Dictionary<long, List<Tuple<Guid, string,string>>>();
        private Dictionary<long, List<Tuple<Guid, string, string, string>>> pgdocumentCluster;
        private Dictionary<long, List<Tuple<Guid, string>>> pgdocumentCluster2;

        private Dictionary<long, List<Tuple<Guid, string,string>>> initDocumentCluster()
        {
            documentCluster = new Dictionary<long, List<Tuple<Guid, string,string>>>();
            try
            {
                const string query = @"
SELECT distinct code.GDBClusterID , item.DocumentId, item.label, item.value
  FROM [ffdocumenthistory].[dbo].[GDBCodes_1203] code
  JOIN [ffdocumenthistory].[dbo].[GDBTaggedItems_1203] item on code.id = item.GDBTableId
 where code.GDBClusterID is not null
 order by GDBClusterID
			";

                using (SqlConnection conn = new SqlConnection(_sfConnectionString))
                {
                    using (SqlCommand cmd = new SqlCommand(query, conn))
                    {
                        conn.Open();
                        using (SqlDataReader sdr = cmd.ExecuteReader())
                        {
                            while (sdr.Read())
                            {
                                var id = sdr.GetInt64(0);
                                if (!documentCluster.ContainsKey(id))
                                {
                                    documentCluster.Add(id, new List<Tuple<Guid, string,string>>());
                                }
                                var g = sdr.GetGuid(1);
                                var h = sdr.GetStringSafe(2);
                                var i = sdr.GetStringSafe(3);
                                documentCluster[id].Add(new Tuple<Guid, string, string>(g, h, i));
                            }
                        }
                    }
                }
            }
            catch
            {
            }
            return documentCluster;
        }

        private Dictionary<long, List<Tuple<Guid, string, string, string>>> initPgDocumentCluster()
        {
            pgdocumentCluster = new Dictionary<long, List<Tuple<Guid, string, string, string>>>();
            try
            {
                const string query = @"
select cluster_id, document_id, raw_row_label, value, item_code from vw_clusters_documents_sdb
 order by cluster_id
			";

                using (var conn = new NpgsqlConnection(PGConnectionString()))
                {
                    using (var cmd = new NpgsqlCommand(query, conn))
                    {
                        conn.Open();
                        using (var sdr = cmd.ExecuteReader())
                        {
                            while (sdr.Read())
                            {
                                var id = sdr.GetInt64(0);
                                if (!pgdocumentCluster.ContainsKey(id))
                                {
                                    pgdocumentCluster.Add(id, new List<Tuple<Guid, string, string, string>>());
                                }
                                var g = sdr.GetGuid(1);
                                var h = sdr.GetStringSafe(2);
                                var i = sdr.GetStringSafe(3);
                                var j = sdr.GetStringSafe(4);
                                pgdocumentCluster[id].Add(new Tuple<Guid, string, string, string>(g, h, i, j));
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                var s = ex.Message;
            }
            return pgdocumentCluster;
        }
        private Dictionary<long, List<Tuple<Guid, string>>> initPgDocumentCluster2()
        {
            pgdocumentCluster2 = new Dictionary<long, List<Tuple<Guid, string>>>();
            try
            {
                const string query = @"
  SELECT DISTINCT t.cluster_id,
    t.document_id
   FROM norm_name_tree t
	 where t.cluster_id is not null
			";

                using (var conn = new NpgsqlConnection(PGConnectionString()))
                {
                    using (var cmd = new NpgsqlCommand(query, conn))
                    {
                        conn.Open();
                        using (var sdr = cmd.ExecuteReader())
                        {
                            while (sdr.Read())
                            {
                                var id = sdr.GetInt64(0);
                                if (!pgdocumentCluster2.ContainsKey(id))
                                {
                                    pgdocumentCluster2.Add(id, new List<Tuple<Guid, string>>());
                                }
                                var g = sdr.GetGuid(1);
                                var h = "";

                                pgdocumentCluster2[id].Add(new Tuple<Guid, string>(g, h));
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                var s = ex.Message;
            }
            return pgdocumentCluster2;
        }
        private Dictionary<Guid, List<string>> tickerCluster = new Dictionary<Guid, List<string>>();
        private Dictionary<Guid, List<string>> pgtickerCluster;
        private Dictionary<Guid, List<string>> initTickerCluster()
        {
            tickerCluster = new Dictionary<Guid, List<string>>();
            try
            {
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

                using (SqlConnection conn = new SqlConnection(_sfConnectionString))
                {
                    using (SqlCommand cmd = new SqlCommand(query, conn))
                    {
                        conn.Open();
                        using (SqlDataReader sdr = cmd.ExecuteReader())
                        {
                            while (sdr.Read())
                            {
                                var id = sdr.GetGuid(0);
                                if (!tickerCluster.ContainsKey(id))
                                {
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
            }
            catch
            {
            }
            return tickerCluster;
        }

        private Dictionary<Guid, List<string>> initPgTickerCluster()
        {
            pgtickerCluster = new Dictionary<Guid, List<string>>();
            try
            {
                const string query = @"
select * from vw_documents_iconums
			";

                using (var conn = new NpgsqlConnection(PGConnectionString()))
                {
                    using (var cmd = new NpgsqlCommand(query, conn))
                    {
                        conn.Open();
                        using (var sdr = cmd.ExecuteReader())
                        {
                            while (sdr.Read())
                            {
                                var id = sdr.GetGuid(0);
                                if (!tickerCluster.ContainsKey(id))
                                {
                                    pgtickerCluster.Add(id, new List<string>());
                                }
                                var g = sdr.GetInt32(1).ToString();
                                pgtickerCluster[id].Add(g);
                            }
                        }
                    }
                }
            }
            catch
            {
            }
            return pgtickerCluster;
        }
        private Node PGNodeDocument(Node n)
        {
            if (n.ParentId.HasValue)
            {
                //n.Title += string.Format(" ({0})", n.ParentId);
            }
            foreach (var child in n.Nodes)
            {
                PGNodeDocument(child);
            }
            return n;
        }
        private Node PGnodeDocuments2(Node n, string hiearchy = "")
        {
            if (n.Documents == null)
            {
                n.Documents = new List<string>();
            }
            if (n.DocumentTuples == null)
            {
                n.DocumentTuples = new List<Tuple<string, string, string, Guid, string>>();
            }
            if (pgdocumentCluster == null)
            {
                initPgDocumentCluster();
            }
            if (pgdocumentCluster2 == null)
            {
                initPgDocumentCluster2();
            }
            if (pgtickerCluster == null)
            {
                initPgTickerCluster();

            }
            var nTitle = n.Title;
            if (string.IsNullOrWhiteSpace(n.Childrentitle))
            {
                n.Childrentitle = "";
            }
            n.Childrentitle += nTitle;
            if (pgdocumentCluster.ContainsKey(n.Id))
            { // n.id is ClusterID

                foreach (var d in pgdocumentCluster[n.Id])
                {
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
                    Tuple<string, string, string, Guid, string> tuple = new Tuple<string, string, string, Guid, string>(d.Item4, d.Item2, d.Item3, d.Item1, z);
                    n.DocumentTuples.Add(tuple);

                }
                foreach (var dt in n.DocumentTuples.OrderBy(x => x.Item1))
                {
                    n.Documents.Add(dt.ToString());
                }
                n.Documents.Add(string.Format("{0}[{1}]", "", n.Title));
                var set = new HashSet<Guid>();
                foreach (var g in pgdocumentCluster[n.Id])
                {
                    set.Add(g.Item1);
                }
                n.Title += string.Format(" ({0})", set.Count);

            }
            else if (pgdocumentCluster2.ContainsKey(n.Id))
            {
                foreach (var d in pgdocumentCluster2[n.Id])
                {
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
                    Tuple<string, string, string, Guid, string> tuple = new Tuple<string, string, string, Guid, string>("", d.Item2, "", d.Item1, z);
                    n.DocumentTuples.Add(tuple);

                }
                foreach (var dt in n.DocumentTuples.OrderBy(x => x.Item1))
                {
                    n.Documents.Add(dt.ToString());
                }
                n.Documents.Add(string.Format("{0}[{1}]", "", n.Title));
                var set = new HashSet<Guid>();
                foreach (var g in pgdocumentCluster2[n.Id])
                {
                    set.Add(g.Item1);
                }
                n.Title += string.Format(" ({0})", set.Count);
            }
            else
            {
                n.Title += string.Format(" (Cid: {0})", n.Id);
            }
            foreach (var child in n.Nodes)
            {
                PGnodeDocuments2(child, string.Format("{0}[{1}]", hiearchy, nTitle));
            }
            //foreach (var child in n.Nodes)
            //{
            //    n.Childrentitle += childrenTitle(child);
            //}
            return n;
        }
        private Node nodeDocuments(Node n, string hiearchy = "")
        {
            if (n.Documents == null)
            {
                n.Documents = new List<string>();
            }
            if (n.DocumentTuples == null)
            {
                n.DocumentTuples = new List<Tuple<string, string, string, Guid, string>>();
            }
            if (documentCluster == null || documentCluster.Count < 1)
            {
                initDocumentCluster();
                initTickerCluster();
            }
            var nTitle = n.Title;
            if (string.IsNullOrWhiteSpace(n.Childrentitle))
            {
                n.Childrentitle = "";
            }
            n.Childrentitle += nTitle;
            if (documentCluster.ContainsKey(n.Id))
            {

                foreach (var d in documentCluster[n.Id])
                {
                    var doc = new List<string>();

                    string z = "";
                    string y = "";
                    //doc.Add(d.ToString());
                    if (tickerCluster.ContainsKey(d.Item1))
                    {
                        //foreach(var t in tickerCluster[d.Item1])
                        //{
                        //    doc.Add(t);

                        //}
                        z = tickerCluster[d.Item1][0];
                        y = tickerCluster[d.Item1][1];


                    }
                    //doc.Add(d.Item2);
                    //n.Documents.Add(doc);
                    Tuple<string, string, string, Guid, string> tuple = new Tuple<string, string, string, Guid, string>(y,  d.Item2, d.Item3, d.Item1, z);
                    n.DocumentTuples.Add(tuple);

                }
                foreach (var dt in n.DocumentTuples.OrderBy(x => x.Item1))
                {
                    n.Documents.Add(dt.ToString());
                }
                n.Documents.Add(string.Format("{0}[{1}]", "", n.Title));
                var set = new HashSet<Guid>();
                foreach (var g in documentCluster[n.Id])
                {
                    set.Add(g.Item1);
                }
                n.Title += string.Format(" ({0})", set.Count);

            }
            else
            {
                n.Title += string.Format(" (Cid: {0})", n.Id);
            }
            foreach (var child in n.Nodes)
            {
                nodeDocuments(child, string.Format("{0}[{1}]", hiearchy, nTitle));
            }
            //foreach (var child in n.Nodes)
            //{
            //    n.Childrentitle += childrenTitle(child);
            //}
            return n;
        }

        public string childrenTitle(Node n, string hiearchy = "")
        {
            string s = "";
            foreach (var child in n.Nodes)
            {
                //n.Childrentitle += childrenTitle(child);
                s += n.Childrentitle;
            }
            s += n.Title;
            return s;
        }

        public string GetSegmentTree(string treeName)
        {
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
            try
            {
                List<Node> allNodes = new List<Node>();
                using (SqlConnection conn = new SqlConnection(_sfConnectionString))
                {
                    using (SqlCommand cmd = new SqlCommand(query, conn))
                    {
                        conn.Open();
                        cmd.Parameters.AddWithValue("@treeName", treeName);

                        using (SqlDataReader sdr = cmd.ExecuteReader())
                        {
                            while (sdr.Read())
                            {
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
                foreach (var n in allNodes)
                {
                    if (nodes.Contains(n)) continue;
                    var parentNode = allNodes.FirstOrDefault(x => x.Id == n.ParentId.Value);
                    if (parentNode != null)
                    {
                        if (parentNode.Nodes == null) parentNode.Nodes = new List<Node>();
                        parentNode.Nodes.Add(n);
                    }
                }

                return JsonConvert.SerializeObject(nodes);

            }
            catch (Exception ex)
            {
                List<Node> errorNodes = new List<Node>();
                Node errorNode = new Node() { Id = 0, Title = "Error", Nodes = new List<Node>() };
                errorNodes.Add(errorNode);
                return JsonConvert.SerializeObject(errorNodes);
            }

        }
        public class TreeViewJSNode
        {
            [JsonProperty("id")]
            public int Id { get; set; }
            [JsonProperty("label")]
            public string Title { get; set; }
            [JsonIgnore]
            public int? ParentId { get; set; }

            [JsonProperty("children")]
            public List<TreeViewJSNode> Nodes { get; set; }
        }
        public string GetTreeViewJS(string treeName)
        {
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
            try
            {
                List<TreeViewJSNode> allNodes = new List<TreeViewJSNode>();
                using (SqlConnection conn = new SqlConnection(_sfConnectionString))
                {
                    using (SqlCommand cmd = new SqlCommand(query, conn))
                    {
                        conn.Open();
                        cmd.Parameters.AddWithValue("@treeName", treeName);

                        using (SqlDataReader sdr = cmd.ExecuteReader())
                        {
                            while (sdr.Read())
                            {
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
                foreach (var n in allNodes)
                {
                    if (nodes.Contains(n)) continue;
                    var parentNode = allNodes.FirstOrDefault(x => x.Id == n.ParentId.Value);
                    if (parentNode != null)
                    {
                        if (parentNode.Nodes == null) parentNode.Nodes = new List<TreeViewJSNode>();
                        parentNode.Nodes.Add(n);
                    }
                }

                return JsonConvert.SerializeObject(nodes);

            }
            catch (Exception ex)
            {
                List<TreeViewJSNode> errorNodes = new List<TreeViewJSNode>();
                TreeViewJSNode errorNode = new TreeViewJSNode() { Id = 0, Title = "Error", Nodes = new List<TreeViewJSNode>() };
                errorNodes.Add(errorNode);
                return JsonConvert.SerializeObject(errorNodes);
            }

        }

        public class CollectedValue
        {
            [JsonProperty("ItemCode")]
            public string ItemCode { get; set; }
            [JsonProperty("ItemName")]
            public string ItemName { get; set; }
            [JsonProperty("SourceLinkID")]
            public string SourceLinkID { get; set; }
            [JsonProperty("DataSource")]
            public string DataSource { get; set; }
        }

        private string GetURL(string url)
        {
            return GetTintFile(url);
        }

        static bool _gdbOnOff = false;
        public string GdbBackfillOff()
        {
            if (_gdbOnOff)
            {
                _gdbOnOff = !_gdbOnOff;
            }
            return _gdbOnOff.ToString();
        }
        public string GdbBackfillOn()
        {
            if (!_gdbOnOff)
            {
                _gdbOnOff = !_gdbOnOff;
                while (_gdbOnOff)
                {
                    GdbBackfill(1, false, 1800);
                }
                return "";
            }
            else
            {
                return _gdbOnOff.ToString();
            }
        }

        public string GdbBackfill(int maxThread = 10, bool retry = false, int tries = 100)
        {
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
            if (retry)
            {
                sql = sql_retry;
            }
            var threadList = new List<Task>();
            var guidList = new List<Guid>();
            List<string> messages = new List<string>();
            for (int i = 0; i < maxThread; i++)
            {
                using (SqlConnection conn = new SqlConnection(_sfConnectionString))
                {
                    conn.Open();
                    Guid docID = new Guid();

                    int fileId = 0;
                    int iconum = 0;
                    using (SqlCommand cmd = new SqlCommand(sql, conn))
                    {
                        using (SqlDataReader sdr = cmd.ExecuteReader())
                        {
                            while (sdr.Read())
                            {
                                docID = sdr.GetGuid(0);
                                fileId = sdr.GetInt32(1);
                                iconum = sdr.GetInt32(2);

                            }
                            guidList.Add(docID);
                        }
                    }
                    using (SqlCommand cmd = new SqlCommand(update_start_sql, conn))
                    {
                        cmd.Parameters.AddWithValue("@DocumentID", docID);
                        using (SqlDataReader sdr = cmd.ExecuteReader())
                        {
                            while (sdr.Read())
                            {
                            }
                        }
                    }
                    threadList.Add(Task.Run(() => InsertGdbCommitKVP(docID, fileId, tries, iconum)).ContinueWith(u => messages.Add(u.Result)));
                }
            }
            foreach (var t in threadList)
            {
                t.Wait();
            }
            using (SqlConnection conn = new SqlConnection(_sfConnectionString))
            {
                conn.Open();
                foreach (var g in guidList)
                {
                    if (!messages.Contains(g.ToString()))
                    {
                        continue;
                    }
                    using (SqlCommand cmd = new SqlCommand(update_end_sql, conn))
                    {
                        cmd.Parameters.AddWithValue("@DocumentID", g);
                        using (SqlDataReader sdr = cmd.ExecuteReader())
                        {
                            while (sdr.Read())
                            {
                            }
                        }
                    }
                    sb.Append(g.ToString() + ",");
                }
            }
            foreach (var v in messages)
            {
                sb.Append(v + "*");
            }
            return sb.ToString();
        }
        private string InsertGdbCommitKVP(Guid guid, int i, int tries = 100, int iconum = 0)
        {
            var r = InsertGdbCommit(guid, i, tries, iconum);
            if (r == "true")
            {
                return guid.ToString();
            }
            else
            {
                return r;
            }
        }
        public string InsertGdbFake(Guid DamDocumentID)
        {
            return InsertGdb(new Guid("978dfe58-c4a2-e311-9b0b-1cc1de2561d4"), 92);
        }
        public string InsertGdbCommit(Guid DamDocumentID, int fileId, int tries = 100, int iconum = 0)
        {
            StringBuilder psb = new StringBuilder();
            psb.AppendLine("StartCommit. " + DamDocumentID.ToString() + " " + DateTime.UtcNow.ToString());
            string strResult = "";

            try
            {
                psb.AppendLine("Ln794." + DateTime.UtcNow.ToString());
                strResult = InsertGdb(DamDocumentID, fileId, "COMMIT TRAN;", tries, iconum);
                psb.AppendLine("Ln796." + DateTime.UtcNow.ToString());
                if (strResult.Length < 20)
                {
                    return strResult;
                }
                else
                {
                    psb.AppendLine("Ln803." + DateTime.UtcNow.ToString());
                    using (SqlConnection conn = new SqlConnection(_sfConnectionString))
                    {

                        using (SqlCommand cmd = new SqlCommand(strResult, conn))
                        {
                            cmd.CommandTimeout = 600;
                            conn.Open();
                            psb.AppendLine("Ln811." + DateTime.UtcNow.ToString());
                            using (SqlDataReader sdr = cmd.ExecuteReader())
                            {
                                psb.AppendLine("Ln814." + DateTime.UtcNow.ToString());
                                if (sdr.Read())
                                {
                                    if (sdr.GetString(0) == "commit")
                                    {
                                        psb.AppendLine("Ln819." + DateTime.UtcNow.ToString());
                                        try
                                        {
                                            AsReportedTemplateHelper.SendEmail("InsertGdb Outer performance", psb.ToString());
                                        }
                                        catch
                                        { }
                                        return "true";
                                    }
                                }
                            }
                        }
                        try
                        {
                            AsReportedTemplateHelper.SendEmail("InsertGdb Outer performance", psb.ToString());
                        }
                        catch
                        { }
                        return "error executing sql";
                    }
                }
            } catch (Exception ex)
            {
                string inner = "";
                if (ex.InnerException != null)
                {
                    inner = ex.InnerException.Message;
                }
                psb.AppendLine("Ln842." + DateTime.UtcNow.ToString());
                AsReportedTemplateHelper.SendEmail("InsertGdbCommit Failure", DamDocumentID.ToString() + ex.Message + ex.StackTrace + psb.ToString());
                return "InsertGdbCommit" + ex.Message;
            }
        }
        public string InsertGdb(Guid DamDocumentID, int fileId, string successAction = "ROLLBACK TRAN;", int tries = 100, int iconum = 0)
        {

            StringBuilder psb = new StringBuilder();
            psb.AppendLine("Start. " + DamDocumentID.ToString() + " " + DateTime.UtcNow.ToString());
            string tintURL = @"http://auto-tablehandler-staging.factset.io/queue/document/978dfe58-c4a2-e311-9b0b-1cc1de2561d4/92";
            tintURL = @"http://chai-auto.factset.io/bank/abs?source_document_id=978dfe58-c4a2-e311-9b0b-1cc1de2561d4&source_file_id=76&iconum=24530";

            string urlPattern = @"http://auto-tablehandler-staging.factset.io/queue/document/{0}/{1}";
            urlPattern = @"http://chai-auto.factset.io/bank/abs?source_document_id={0}&source_file_id={1}&iconum={2}";
            string url = String.Format(urlPattern, DamDocumentID.ToString().ToUpper(), fileId, iconum);
            bool isTryCached = false;
            string cachedURL = "";
            try
            {
                cachedURL = System.Web.HttpContext.Current.Request.QueryString["result_url"];
                if (!string.IsNullOrEmpty(cachedURL))
                {
                    isTryCached = true;
                }
            }
            catch (Exception ex)
            {
                string debug = ex.Message;
            }
            //url = tintURL;
            TintInfo tintInfo = null;
            while (tries > 0)
            {
                try
                {
                    string outputresult = "";
                    if (isTryCached)
                    {
                        psb.AppendLine("Ln847." + DateTime.UtcNow.ToString());
                        outputresult = GetTintFile(cachedURL);
                        psb.AppendLine("Ln848." + DateTime.UtcNow.ToString());
                    }
                    else
                    {
                        psb.AppendLine("Ln849." + DateTime.UtcNow.ToString());
                        outputresult = GetTintFile(url);
                        psb.AppendLine("Ln851." + DateTime.UtcNow.ToString());
                    }
                    var settings = new JsonSerializerSettings { Error = (se, ev) => { ev.ErrorContext.Handled = true; } };
                    tintInfo = Newtonsoft.Json.JsonConvert.DeserializeObject<TintInfo>(outputresult, settings);
                    if (tintInfo == null)
                    {
                        throw new Exception("failed to get tint 1");
                    }
                    tries = -1;
                }
                catch (FileNotFoundException ex)
                {
                    if (isTryCached)
                    {
                        isTryCached = false;
                    }
                    else
                    {
                        tries = 0;
                    }
                }
                catch (Exception ex)
                {
                    if (--tries > 0)
                    {
                        isTryCached = false;
                        System.Threading.Thread.Sleep(4000);
                    }

                }
            }
            psb.AppendLine("Ln869." + DateTime.UtcNow.ToString());
            if (tintInfo == null)
            {
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
            foreach (var table in tintInfo.Tables)
            {
            //    if (!new string[] { "IS", "BS", "CF" }.Contains(table.Type)) continue;
                //if (count > 2) break;
                // Insert DocumentTable
                count++;

                List<int> addedRow = new List<int>();
                List<int> addedCol = new List<int>();
                int dtsCount = 0;
                foreach (var value in table.Values)
                {
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
                    if (selectedCell != null)
                    {
                        var row = table.Rows.FirstOrDefault(v => v.Id == selectedCell.rowId);
                        if (row != null)
                        {
                            label = row.Label;
                        }
                        else
                        {
                            label = value.XbrlTag;
                        }
                    }
                    else
                    {
                        label = value.XbrlTag;
                    }
                    label = Truncate(label, 4000);
                    string columnHeader = "";
                    if (selectedCell != null)
                    {
                        var col = table.Columns.FirstOrDefault(v => v.Id == selectedCell.columnId);
                        if (col != null)
                        {
                            columnHeader = col.columnHeader;
                        }
                    }
                    columnHeader = Truncate(columnHeader, 4000);
                    string xbrl = value.XbrlTag;
                    if (string.IsNullOrWhiteSpace(xbrl))
                    {
                        xbrl = "";
                    }
                    string xbrlTableTitle = table.XbrlTableTitle;
                    if (string.IsNullOrWhiteSpace(xbrlTableTitle))
                    {
                        xbrlTableTitle = "";
                    }
                    string cellDate = value.Date;
                    DateTime dateTime;
                    if (string.IsNullOrWhiteSpace(cellDate) && !DateTime.TryParse(cellDate, out dateTime))
                    {
                        cellDate = "NULL";
                    }
                    else
                    {
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
            try
            {
                AsReportedTemplateHelper.SendEmail("InsertGdb Inner performance", psb.ToString());
            }
            catch
            { }
            return retVal;
        }
        public static String Truncate(String input, int maxLength)
        {
            if (string.IsNullOrWhiteSpace(input))
            {
                input = "";
            }
            if (input.Length > maxLength)
                return input.Substring(0, maxLength);
            return input;
        }

        public string InsertKpiFake(Guid DamDocumentID)
        {
            string tintURL = @"http://chai-auto.factset.io/queue/bank?source_document_id=978dfe58-c4a2-e311-9b0b-1cc1de2561d4&source_file_id=76&iconum=24530";

            string urlPattern = @"http://auto-tablehandler-dev.factset.io/document/{0}/0";
            string url = String.Format(urlPattern, DamDocumentID);
            url = tintURL;

            int tries = 3;
            List<Node> nodes = new List<Node>();
            TintInfo tintInfo = null;
            while (tries > 0)
            {
                try
                {
                    var outputresult = GetTintFile(url);
                    var settings = new JsonSerializerSettings { Error = (se, ev) => { ev.ErrorContext.Handled = true; } };
                    tintInfo = Newtonsoft.Json.JsonConvert.DeserializeObject<TintInfo>(outputresult, settings);
                    tries = -1;
                }
                catch (Exception ex)
                {
                    if (--tries > 0)
                    {
                        System.Threading.Thread.Sleep(6000);
                    }

                }
            }
            if (tintInfo == null)
            {
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

            foreach (var table in tintInfo.Tables)
            {
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
                foreach (var cell in table.Cells)
                {
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
                    if (!addedRow.Contains(row.Id))
                    {
                        sb.AppendLine(string.Format(tdRow, row.Label, 1, row.Id));
                        addedRow.Add(row.Id);
                    }
                    var col = table.Columns.FirstOrDefault(x => x.Id == cell.columnId);
                    if (!addedCol.Contains(col.Id))
                    {
                        sb.AppendLine(string.Format(tdCol, col.columnHeader, 2, col.Id));
                        addedCol.Add(col.Id);
                    }
                    TimeSlice u = null;
                    foreach (var ts in tintInfo.TimeSlices)
                    {
                        if (ts.Offsets.Contains(cell.offset))
                        {
                            if (!addedDts.Contains(ts.FakeID))
                            {
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
        public string InsertKpiFakeWrong916(Guid DamDocumentID)
        {
            string tintURL = @"http://chai-auto.factset.io/queue/bank?source_document_id=978dfe58-c4a2-e311-9b0b-1cc1de2561d4&source_file_id=76&iconum=24530";
            string collectedValueURL = @"http://chai-auto.factset.io/bank/collected?source_document_id=978dfe58-c4a2-e311-9b0b-1cc1de2561d4&iconum=24530";

            string urlPattern = @"http://auto-tablehandler-dev.factset.io/document/{0}/0";
            string url = String.Format(urlPattern, DamDocumentID);
            url = tintURL;

            int tries = 3;
            List<Node> nodes = new List<Node>();
            TintInfo tintInfo = null;
            while (tries > 0)
            {
                try
                {
                    var outputresult = GetTintFile(url);
                    tintInfo = Newtonsoft.Json.JsonConvert.DeserializeObject<TintInfo>(outputresult);
                    tries = -1;
                }
                catch (Exception ex)
                {
                    if (--tries > 0)
                    {
                        System.Threading.Thread.Sleep(3000);
                    }

                }
            }
            if (tintInfo == null)
            {
                return "false";
            }
            tries = 3;
            CollectedValue[] collectedValues = null;
            while (tries > 0)
            {
                try
                {
                    var outputresult = GetURL(collectedValueURL);
                    collectedValues = Newtonsoft.Json.JsonConvert.DeserializeObject<CollectedValue[]>(outputresult);
                    tries = -1;
                }
                catch (Exception ex)
                {
                    if (--tries > 0)
                    {
                        System.Threading.Thread.Sleep(1000);
                    }

                }
            }
            if (collectedValues == null)
            {
                return "false";
            }
            StringBuilder sb = new StringBuilder();
            StringBuilder sbDimension = new StringBuilder();
            StringBuilder sbTableCell = new StringBuilder();
            sb.AppendLine("SET TRANSACTION ISOLATION LEVEL SNAPSHOT;");
            sb.AppendLine("BEGIN TRY");
            sb.AppendLine("BEGIN TRAN");
            int count = 0;
            foreach (var table in tintInfo.Tables)
            {
                if (count > 0) break;
                // Insert DocumentTable
                bool addDocumentTable = true;
                foreach(var cell in table.Cells)
                {
                    if (collectedValues.FirstOrDefault(x => x.SourceLinkID == cell.offset) != null)
                    {
                        if (addDocumentTable)
                        {
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
                        sb.AppendLine(string.Format(tc, v.Offset, v.Date, v.OriginalValue, v.NumericValue,  v.Scaling, v.XbrlTag, row.Label, row.Id, col.Id));
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

        public class SDBNode
        {
            [JsonProperty("id")]
            public long Id { get; set; }
            [JsonProperty("xbrlTag")]
            public string XbrlTag { get; set; }
            [JsonProperty("count")]
            public int Count { get; set; }
            [JsonProperty("totalcount")]
            public int TotalCount { get; set; }
        }
        public class SDBValueNode
        {
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
        public List<SDBNode> GetGDBCode(long sdbcode)
        {
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
            using (SqlConnection conn = new SqlConnection(_sfConnectionString))
            {
                conn.Open();

                using (SqlCommand cmd = new SqlCommand(sql, conn))
                {
                    cmd.Parameters.AddWithValue("@sdbcode", sdbcode);
                    using (SqlDataReader sdr = cmd.ExecuteReader())
                    {
                        while (sdr.Read())
                        {
                            SDBNode node = new SDBNode();
                            string id = sdr.GetStringSafe(0);
                            long sdb;
                            if (!long.TryParse(id, out sdb))
                            {
                                continue;
                            }
                            node.Id = sdb;
                            string xbrltag = sdr.GetStringSafe(1);
                            if (string.IsNullOrWhiteSpace(xbrltag))
                            {
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
            foreach (var n in nodes)
            {
                totalCount += n.Count;
            }
            foreach (var n in nodes)
            {
                n.TotalCount = totalCount;
            }
            return nodes;
        }
        public DataTable GetGDBCodeGrid(long sdbcode)
        {
            string sql = @"
exec GDBGetCodes @sdbcode
";
            List<SDBNode> nodes = new List<SDBNode>();
            List<SDBValueNode> valuenodes = new List<SDBValueNode>();
            DataTable table = new DataTable();
            DataRow countRow = null;



            using (SqlConnection conn = new SqlConnection(_sfConnectionString))
            {
                conn.Open();

                using (SqlCommand cmd = new SqlCommand(sql, conn))
                {
                    cmd.Parameters.AddWithValue("@sdbcode", sdbcode);
                    using (SqlDataReader sdr = cmd.ExecuteReader())
                    {
                        DataColumn column = new DataColumn();
                        column.DataType = Type.GetType("System.String");
                        column.ColumnName = "Name";
                        table.Columns.Add(column);
                        while (sdr.Read())
                        {
                            SDBNode node = new SDBNode();
                            long sdb = sdr.GetInt64(1);
                            //long sdb;
                            //if (!long.TryParse(id, out sdb))
                            //{
                            //    continue;
                            //}
                            node.Id = sdb;
                            string xbrltag = sdr.GetStringSafe(2);
                            if (string.IsNullOrWhiteSpace(xbrltag))
                            {
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
                        for (int u = 0; u < totalColumn; u++)
                        {
                            row[nodes[u].XbrlTag] = nodes[u].XbrlTag;
                            row2[nodes[u].XbrlTag] = string.Format("{0}/{1}", nodes[u].Count, nodes[u].TotalCount);
                        }
                        table.Rows.Add(row);
                        table.Rows.Add(row2);
                        while (sdr.Read())
                        {
                            SDBValueNode node = new SDBValueNode();
                            node.CompanyName = sdr.GetStringSafe(0);
                            node.Iconum = sdr.GetInt32(1).ToString();
                            node.DocumentId = sdr.GetGuid(2).ToString();
                            node.XbrlTag = sdr.GetStringSafe(3);
                            node.MaxValue = sdr.GetStringSafe(4);
                            if (valuenodes.FirstOrDefault(x => x.Iconum == node.Iconum) == null)
                            {
                                row = table.NewRow();
                                row["Name"] = node.CompanyName;
                                for (int u = 1; u <= totalColumn; u++)
                                {
                                    row[u] = "";
                                }
                                table.Rows.Add(row);
                            }
                            valuenodes.Add(node);
                            row = null;
                            foreach(DataRow r in table.Rows)
                            {
                                if (r["Name"].ToString() == node.CompanyName)
                                {
                                    row = r;
                                    break;
                                }
                            }
                            if (row != null)
                            {
                                row[node.XbrlTag] = node.MaxValue;

                            }
                            //DataColumn column = table.Columns.IndexOf(node.XbrlTag)

                        }

                    } 
                }
            }
            int totalCount = 0;
            foreach (var n in nodes)
            {
                totalCount += n.Count;
            }
            foreach (var n in nodes)
            {
                countRow[n.XbrlTag] = string.Format("{0}/{1}", n.Count, totalCount);
            }

            return table;
        }

        public DataTable GetGDBCodeGridForIconum(long sdbcode, int iconum, Guid? DocumentID = null)
        {
            string sql = @"
exec GDBGetCodesForIconum @sdbcode, @iconum, @documentId
";
            List<SDBNode> nodes = new List<SDBNode>();
            List<SDBValueNode> valuenodes = new List<SDBValueNode>();
            DataTable table = new DataTable();
            DataRow countRow = null;



            using (SqlConnection conn = new SqlConnection(_sfConnectionString))
            {
                conn.Open();

                using (SqlCommand cmd = new SqlCommand(sql, conn))
                {
                    cmd.Parameters.AddWithValue("@sdbcode", sdbcode);
                    cmd.Parameters.AddWithValue("@iconum", iconum);
                    if (DocumentID.HasValue)
                    {
                      cmd.Parameters.AddWithValue("@documentId", DocumentID.Value);
                    }
                    else
                    {
                      cmd.Parameters.AddWithValue("@documentId", DBNull.Value);
                    }
                    using (SqlDataReader sdr = cmd.ExecuteReader())
                    {
                        DataColumn column = new DataColumn();
                        column.DataType = Type.GetType("System.String");
                        column.ColumnName = "Name";
                        table.Columns.Add(column);
                        while (sdr.Read())
                        {
                            SDBNode node = new SDBNode();
                            long sdb = sdr.GetInt64(1);
                            //long sdb;
                            //if (!long.TryParse(id, out sdb))
                            //{
                            //    continue;
                            //}
                            node.Id = sdb;
                            string xbrltag = sdr.GetStringSafe(2);
                            if (string.IsNullOrWhiteSpace(xbrltag))
                            {
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
                        for (int u = 0; u < totalColumn; u++)
                        {
                            row[nodes[u].XbrlTag] = nodes[u].XbrlTag;
                            row2[nodes[u].XbrlTag] = string.Format("{0}/{1}", nodes[u].Count, nodes[u].TotalCount);
                        }
                        table.Rows.Add(row);
                        table.Rows.Add(row2);
                        while (sdr.Read())
                        {
                            SDBValueNode node = new SDBValueNode();
                            node.CompanyName = sdr.GetStringSafe(0);
                            node.Iconum = sdr.GetInt32(1).ToString();
                            node.DocumentId = sdr.GetGuid(2).ToString();
                            node.XbrlTag = sdr.GetStringSafe(3);
                            node.MaxValue = sdr.GetStringSafe(4);
                            if (valuenodes.FirstOrDefault(x => x.DocumentId == node.DocumentId) == null)
                            {
                                row = table.NewRow();
                                row["Name"] = string.Format("{0} ({1})", node.CompanyName, node.DocumentId);
                                for (int u = 1; u <= totalColumn; u++)
                                {
                                    row[u] = "";
                                }
                                table.Rows.Add(row);
                            }
                            valuenodes.Add(node);
                            row = null;
                            foreach (DataRow r in table.Rows)
                            {
                                if (r["Name"].ToString() == string.Format("{0} ({1})", node.CompanyName, node.DocumentId))
                                {
                                    row = r;
                                    break;
                                }
                            }
                            if (row != null)
                            {
                                row[node.XbrlTag] = node.MaxValue;

                            }
                            //DataColumn column = table.Columns.IndexOf(node.XbrlTag)

                        }

                    }
                }
            }
            int totalCount = 0;
            foreach (var n in nodes)
            {
                totalCount += n.Count;
            }
            foreach (var n in nodes)
            {
                countRow[n.XbrlTag] = string.Format("{0}/{1}", n.Count, totalCount);
            }

            return table;
        }

        public DataTable GetGDBCountForIconum(long sdbcode, int? iconum = null)
        {
            string sql = @"
exec GDBGetCountForIconum @sdbcode, @iconum
";
            List<SDBNode> nodes = new List<SDBNode>();
            List<SDBValueNode> valuenodes = new List<SDBValueNode>();
            DataTable table = new DataTable();
            DataRow countRow = null;



            using (SqlConnection conn = new SqlConnection(_sfConnectionString))
            {
                conn.Open();

                using (SqlCommand cmd = new SqlCommand(sql, conn))
                {
                    cmd.Parameters.AddWithValue("@sdbcode", sdbcode);
                    if (iconum.HasValue)
                    {
                        cmd.Parameters.AddWithValue("@iconum", iconum);
                    }
                    else
                    {
                        cmd.Parameters.AddWithValue("@iconum", DBNull.Value);
                    }
                    using (SqlDataReader sdr = cmd.ExecuteReader())
                    {
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

                        while (sdr.Read())
                        {
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
    }
}
