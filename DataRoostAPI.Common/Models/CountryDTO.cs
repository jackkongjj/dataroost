using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using Newtonsoft.Json;

namespace DataRoostAPI.Common.Models {
	public class CountryDTO {
		[JsonProperty("_id")]
		public string Id { get; set; }

		[JsonProperty("iso3")]
		public string Iso3 { get; set; }

		[JsonProperty("shortName")]
		public string ShortName { get; set; }

		[JsonProperty("longName")]
		public string LongName { get; set; }
	}
}