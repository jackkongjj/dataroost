using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Newtonsoft.Json;

namespace DataRoostAPI.Common.Models {
	public class ExportedItem {

		[JsonProperty("iconum")]
		public string Iconum { get; set; }

		[JsonProperty("documentId")]
		public string DocumentId { get; set; }

		[JsonProperty("reportDate")]
		public DateTime ReportDate { get; set; }

		[JsonProperty("formType")]
		public string FormType { get; set; }

		[JsonProperty("publicationDate")]
		public DateTime PublicationDate { get; set; }
	}
}
