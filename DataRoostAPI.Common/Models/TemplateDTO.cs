using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using Newtonsoft.Json;

namespace DataRoostAPI.Common.Models {
	public class TemplateDTO {
		[JsonProperty("_id")]
		public string Id { get; set; }

		[JsonProperty("name")]
		public string Name { get; set; }

		[JsonProperty("updateType")]
		public string UpdateType { get; set; }

		[JsonProperty("reportType")]
		public string ReportType { get; set; }

		[JsonProperty("items")]
		public List<TemplateItemDTO> Items { get; set; }
	}
}