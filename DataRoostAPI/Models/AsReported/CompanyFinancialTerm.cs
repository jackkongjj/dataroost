using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

using Newtonsoft.Json;

namespace CCS.Fundamentals.DataRoostAPI.Models.AsReported {
	public class CompanyFinancialTerm {

		[JsonProperty("_id")]
		public int Id { get; set; }

		[JsonProperty("description")]
		public string Description { get; set; }
	}
}