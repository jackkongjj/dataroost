using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using Newtonsoft.Json;

namespace CCS.Fundamentals.DataRoostAPI.Models.Voyager {
	public class VoyagerTimeseriesDTO : TimeseriesDTO {

		[JsonProperty(PropertyName = "dcn")]
		public Guid DCN { get; set; }

	}
}