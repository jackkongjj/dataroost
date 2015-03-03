using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using Newtonsoft.Json;

namespace CCS.Fundamentals.DataRoostAPI.Models.AsReported {

	public class AsReportedTable {

		[JsonProperty("_id")]
		public string Id { get; set; }

		[JsonProperty("tableType")]
		public string TableType { get; set; }

		[JsonProperty("cells")]
		public Cell[] Cells { get; set; }

		[JsonProperty("rows")]
		public Row[] Rows { get; set; }

		[JsonProperty("columns")]
		public Column[] Columns { get; set; }
	}
}