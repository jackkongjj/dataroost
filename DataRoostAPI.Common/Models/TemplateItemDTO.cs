using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using Newtonsoft.Json;

namespace DataRoostAPI.Common.Models {
	public class TemplateItemDTO {
		[JsonProperty("_id")]
		public string Id { get; set; }

		[JsonProperty("code")]
		public string Code { get; set; }

		[JsonProperty("description")]
		public string Description { get; set; }

		[JsonProperty("statementTypeId")]
		public string StatementTypeId { get; set; }

		[JsonProperty("usageType")]
		public string UsageType { get; set; }

		[JsonProperty("indentLevel")]
		public int IndentLevel { get; set; }

		[JsonProperty("valueType")]
		public string ValueType { get; set; }

		[JsonProperty("isSecurity")]
		public bool IsSecurity { get; set; }

		[JsonProperty("isPIT")]
		public bool IsPIT { get; set; }

		[JsonProperty("precision")]
		public int Precision { get; set; }
	}
}