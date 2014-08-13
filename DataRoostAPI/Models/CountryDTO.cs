using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using Newtonsoft.Json;

namespace CCS.Fundamentals.DataRoostAPI.Models {
	public class CountryDTO {
		[JsonProperty(PropertyName = "_id")]
		public string Id { get; set; }

		[JsonProperty(PropertyName = "iso3")]
		public string Iso3 { get; set; }

		[JsonProperty(PropertyName = "shortName")]
		public string ShortName { get; set; }

		[JsonProperty(PropertyName = "longName")]
		public string LongName { get; set; }
	}
}