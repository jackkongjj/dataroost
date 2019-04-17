using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using Newtonsoft.Json;
using System.Data.Entity;

namespace DataRoostAPI.Common.Models.AsReported {

	public class RowEntity : DbContext
    {

		[JsonProperty("_id")]
		public int Id { get; set; }

		[JsonProperty("label")]
		public string Label { get; set; }

		[JsonProperty("order")]
		public int Order { get; set; }
	}
}