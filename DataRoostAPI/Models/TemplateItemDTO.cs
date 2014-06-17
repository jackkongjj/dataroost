using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using Newtonsoft.Json;

namespace CCS.Fundamentals.DataRoostAPI.Models {
	public class TemplateItemDTO {
		[JsonProperty(PropertyName = "_id")]
		public int Id { get; set; }

		[JsonProperty(PropertyName = "code")]
		public string Code { get; set; }

		[JsonProperty(PropertyName = "description")]
		public string Description { get; set; }

		[JsonProperty(PropertyName = "statementTypeId")]
		public string StatementTypeId { get; set; }

		[JsonProperty(PropertyName = "usageType")]
		public string UsageType { get; set; }

		[JsonProperty(PropertyName = "indentLevel")]
		public int IndentLevel { get; set; }

		[JsonProperty(PropertyName = "valueType")]
		public string ValueType { get; set; }

		[JsonProperty(PropertyName = "isSecurity")]
		public bool IsSecurity { get; set; }

		[JsonProperty(PropertyName = "isPIT")]
		public bool IsPIT { get; set; }

		[JsonProperty(PropertyName = "precision")]
		public int Precision { get; set; }
	}
}