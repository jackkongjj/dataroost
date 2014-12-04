using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using Newtonsoft.Json;

namespace CCS.Fundamentals.DataRoostAPI.Models {
	public class TemplateDTO {
		[JsonProperty(PropertyName = "_id")]
		public string Id { get; set; }

		[JsonProperty(PropertyName = "name")]
		public string Name { get; set; }

		[JsonProperty(PropertyName = "updateType")]
		public string UpdateType { get; set; }

		[JsonProperty(PropertyName = "reportType")]
		public string ReportType { get; set; }

		[JsonProperty(PropertyName = "items")]
		public List<TemplateItemDTO> Items { get; set; }
	}
}