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
                    else
                    {
                        return JsonConvert.SerializeObject(new List<Node>());
                    }

                }
            }
            return JsonConvert.SerializeObject(nodes);
        }

				public string GetDataTreeTest(Guid DamDocumentID, int fileNo) {

					string urlPattern = @"http://auto-tablehandler-dev.factset.io/queue/document/{0}/{1}";
					string url = String.Format(urlPattern, DamDocumentID, fileNo);
					int tries = 3;
					List<ReactNode> nodes = new List<ReactNode>();

					while (tries > 0) {
						try {
							var outputresult = GetTintFile(url);
							var result = Newtonsoft.Json.JsonConvert.DeserializeObject<TintInfo>(outputresult);
							nodes = GetAngularTreeTest(result);
							tries = 0;
						} catch (Exception ex) {
							if (--tries > 0) {
								System.Threading.Thread.Sleep(1000);
							} else {
								return JsonConvert.SerializeObject(new List<ReactNode>());
							}

						}
					}
					return JsonConvert.SerializeObject(nodes);
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
                List<Node>errorNodes = new List<Node>();
                Node errorNode = new Node() { Id = 0, Title = "Error", Nodes = new List<Node>()};
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
        public string InsertKpiFake(Guid DamDocumentID)
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
    }
}
