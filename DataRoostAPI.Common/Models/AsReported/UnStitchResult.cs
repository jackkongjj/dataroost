using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace DataRoostAPI.Common.Models.AsReported {


	public class UnStitchResult {
		[JsonProperty("staticHierarchyAdjustedOrders")]
		public List<StaticHierarchyAdjustedOrder> StaticHierarchyAdjustedOrders { get; set; }

		[JsonProperty("staticHierarchies")]
		public List<StaticHierarchy> StaticHierarchies { get; set; }

		[JsonProperty("changedCells")]
		public List<SCARAPITableCell> ChangedCells { get; set; }

		[JsonProperty("returnValue")]
		public Dictionary<string, string> ReturnValue { get; set; }

	}
}
