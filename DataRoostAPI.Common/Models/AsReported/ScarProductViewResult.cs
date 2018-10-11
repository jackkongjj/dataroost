using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace DataRoostAPI.Common.Models.AsReported {
	public class ScarProductViewResult {
		public ScarProductViewResult() {
			ReturnValue = new Dictionary<string, string>();
			ReturnValue["Success"] = "F";
			ReturnValue["Message"] = "";
		}

		[JsonProperty("staticHierarchies")]
		public StaticHierarchy[] StaticHierarchies { get; set; }

		[JsonProperty("TimeSlices")]
		public TimeSlice[] TimeSlices { get; set; }

		[JsonProperty("Message")]
		public string Message { get; set; }

		[JsonProperty("returnValue")]
		public Dictionary<string, string> ReturnValue { get; set; }
	}
}
