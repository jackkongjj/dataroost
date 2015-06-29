using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using Newtonsoft.Json;
using DataRoostAPI.Common.Models.Voyager;
using DataRoostAPI.Common.Models.SuperFast;

namespace DataRoostAPI.Common.Models.SfVoy {
	public class SfVoyTimeSeries {
		[JsonProperty("_id")]
		public string Id { get; set; }

		[JsonProperty("sfTimeSerie")]
		public SFTimeseriesDTO SfTimeSerie { get; set; }

		[JsonProperty("voyTimeSerie")]
		public VoyagerTimeseriesDTO VoyTimeSerie { get; set; }
	}
}
