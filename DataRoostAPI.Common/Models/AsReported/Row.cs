using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using Newtonsoft.Json;

namespace DataRoostAPI.Common.Models.AsReported {

	public class Row {

		[JsonProperty("_id")]
		public int Id { get; set; }

		[JsonProperty("label")]
		public string Label { get; set; }

		[JsonProperty("order")]
		public int Order { get; set; }
	}
}