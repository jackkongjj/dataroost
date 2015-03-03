using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using Newtonsoft.Json;

namespace CCS.Fundamentals.DataRoostAPI.Models.AsReported {

	public class Column {

		[JsonProperty("_id")]
		public string Id { get; set; }

		[JsonProperty("label")]
		public string Label { get; set; }
	}
}