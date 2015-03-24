using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using Newtonsoft.Json;

namespace CCS.Fundamentals.DataRoostAPI.Models.AsReported {

	public class AsReportedTable {

		[JsonProperty("_id")]
		public int Id { get; set; }

		[JsonProperty("tableType")]
		public string TableType { get; set; }

		[JsonProperty("cells")]
		public List<Cell> Cells { get; set; }

		[JsonProperty("rows")]
		public List<Row> Rows { get; set; }

		[JsonProperty("columns")]
		public List<Column> Columns { get; set; }
	}
}