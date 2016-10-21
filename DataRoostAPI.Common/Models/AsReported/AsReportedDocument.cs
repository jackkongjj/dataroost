using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using Newtonsoft.Json;

namespace DataRoostAPI.Common.Models.AsReported {

	public class AsReportedDocument : Document {

		[JsonProperty("tables")]
		public AsReportedTable[] Tables { get; set; }

		[JsonProperty("cells")]
		public Cell[] Cells { get; set; }

    }
}