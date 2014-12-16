using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using Newtonsoft.Json;

namespace CCS.Fundamentals.DataRoostAPI.Models {
	public class EffortDTO {

		[JsonProperty(PropertyName = "name")]
		public string Name { get; set; }
	}
}