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
        }
        public class Table
        {
            [JsonProperty("id")]
            public int Id { get; set; }
            [JsonProperty("type")]
            public string Type { get; set; }
            [JsonProperty("rows")]
            public List<Row> Rows { get; set; }
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
        public class Node
        {
            [JsonProperty("id")]
            public int Id { get; set; }
            [JsonProperty("title")]
            public string Title { get; set; }
            [JsonProperty("nodes")]
            public List<Node> Nodes { get; set; }
        }
        private string GetTintFile(string url)
        {
            HttpWebRequest request = (HttpWebRequest)WebRequest.Create(url);
            request.ContentType = "application/json";
            request.Timeout = 120000;
            request.Method = "GET";
            var response = (HttpWebResponse)request.GetResponse();
            string outputresult = null;
            if (response.StatusCode == HttpStatusCode.OK)
            {
                using (var streamReader = new StreamReader(response.GetResponseStream()))
                {
                    outputresult = streamReader.ReadToEnd();
                }
            }
            else
            {
                throw new Exception("call failed");
            }
            return outputresult;

        }
        private List<Node> GetAngularTree(TintInfo result)
        {
            List<Node> nodes = new List<Node>();
            string[] big3Table = { "BS", "IS", "CF" };
            foreach (var table in result.Tables)
            {
                if (!big3Table.Contains(table.Type.ToUpper()))
                    continue;
                Node t = new Node();
                nodes.Add(t);
                t.Id = table.Id;
                t.Title = table.Type;
                t.Nodes = new List<Node>();
                Stack<Node> stack = new Stack<Node>();
                stack.Push(t);

                foreach (var row in table.Rows)
                {
                    int i = 0;
                    foreach (var labelAtlevel in row.LabelHierarchy)
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
                    var endLabel = row.LabelHierarchy.Last();
                    i = 0;
                    int j = 0; // insert
                    foreach (var labelAtlevel in row.LabelHierarchy)
                    {
                        i++;
                        if (stack.Count > i)
                        {
                            continue;
                        }
                        if (stack.Peek().Title != labelAtlevel && labelAtlevel != endLabel)
                        {
                            Node r = new Node();
                            r.Id = -1;
                            r.Title = labelAtlevel;
                            r.Nodes = new List<Node>();
                            lastRoot.Nodes.Add(r);
                            lastRoot = r;
                            stack.Push(r);
                        }
                        else
                        {
                            Node r = new Node();
                            r.Id = row.Id;
                            r.Title = endLabel;
                            r.Nodes = new List<Node>();
                            lastRoot.Nodes.Add(r);
                        }
                    }

                }
            }
            return nodes;
        }
        public string GetDataTreeFake(Guid DamDocumentID)
        {

            //string url =  @"http://auto-tablehandler-dev.factset.io/document/43c9a57f-9b11-e811-80f1-8cdcd4af21e4/38";
            string urlPattern = @"http://auto-tablehandler-dev.factset.io/document/{0}/0";
            string testURL = @"http://auto-tablehandler-dev.factset.io/queue/document/dd17a130-682b-e711-80ea-8cdcd4af21e4/31";
            string url = String.Format(urlPattern, DamDocumentID);
            url = testURL;

            int tries = 3;
            List<Node> nodes = new List<Node>();

            while (tries > 0)
            {
                try
                {
                    var outputresult = GetTintFile(url);
                    var result = Newtonsoft.Json.JsonConvert.DeserializeObject<TintInfo>(outputresult);
                    nodes = GetAngularTree(result);
                    tries = 0;
                }
                catch (Exception ex)
                {
                    if (--tries > 0)
                    {
                        System.Threading.Thread.Sleep(1000);
                    }

                }
            }
            return JsonConvert.SerializeObject(nodes);
        }
 
        public string GetDataTree(Guid DamDocumentID, int fileNo)
        {

            string urlPattern = @"http://auto-tablehandler-dev.factset.io/queue/document/{0}/{1}";
            string url = String.Format(urlPattern, DamDocumentID, fileNo);
            int tries = 3;
            List<Node> nodes = new List<Node>();

            while (tries > 0)
            {
                try
                {
                    var outputresult = GetTintFile(url);
                    var result = Newtonsoft.Json.JsonConvert.DeserializeObject<TintInfo>(outputresult);
                    nodes = GetAngularTree(result);
                    tries = 0;
                }
                catch (Exception ex)
                {
                    if (--tries > 0)
                    {
                        System.Threading.Thread.Sleep(1000);
                    }

                }
            }
            return JsonConvert.SerializeObject(nodes);
        }
    }
}
