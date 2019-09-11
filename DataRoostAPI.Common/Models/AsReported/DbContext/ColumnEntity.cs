using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using Newtonsoft.Json;
using System.Data.Entity;

namespace DataRoostAPI.Common.Models.AsReported {

	public class ColumnEntity : DbContext
    {

		[JsonProperty("_id")]
		public int Id { get; set; }

		[JsonProperty("label")]
		public string Label { get; set; }
	}
}