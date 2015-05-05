using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using Newtonsoft.Json;

namespace DataRoostAPI.Common.Models.AsReported {

	public class AsReportedDocument {

		[JsonProperty("_id")]
		public string Id { get; set; }

		[JsonProperty("reportDate")]
		public DateTime ReportDate { get; set; }

		[JsonProperty("formType")]
		public string FormType { get; set; }

		[JsonProperty("reportType")]
		public string ReportType { get; set; }

		[JsonProperty("publicationDate")]
		public DateTime PublicationDate { get; set; }

		[JsonProperty("sfDocumentId")]
		public string SuperFastDocumentId { get; set; }

		[JsonProperty("tables")]
		public AsReportedTable[] Tables { get; set; }
	}
}