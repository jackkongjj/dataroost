using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Newtonsoft.Json;

namespace DataRoostAPI.Common.Models {
	public class Document {

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

	}
}
