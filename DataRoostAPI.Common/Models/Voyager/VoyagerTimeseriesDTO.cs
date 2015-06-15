using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using Newtonsoft.Json;

namespace DataRoostAPI.Common.Models.Voyager {
	public class VoyagerTimeseriesDTO : TimeseriesDTO {

		[JsonProperty("dcn")]
		public string DCN { get; set; }

	}
}