using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace DataRoostAPI.Common.Models.AsReported {
	public class ScarResult {
		
		public ScarResult() {
			ReturnValue = new Dictionary<string, string>();
			ReturnValue["Success"] = "F";
			ReturnValue["Message"] = "";
		}

		[JsonProperty("staticHierarchyAdjustedOrders")]
		public List<StaticHierarchyAdjustedOrder> StaticHierarchyAdjustedOrders { get; set; }

		//TODO: Add Parent Cell Changes
		[JsonIgnore]
		public List<CellMTMWComponent> ParentCellChangeComponents { get; set; }

		[JsonProperty("parentMTMWChanges")]
		public Dictionary<int, Dictionary<int, bool>> ParentMTMWChanges { get; set; }

		[JsonProperty("staticHierarchies")]
		public List<StaticHierarchy> StaticHierarchies { get; set; }

		[JsonProperty("changedCells")]
		public List<SCARAPITableCell> ChangedCells { get; set; }

		[JsonProperty("TimeSlices")]
		public List<TimeSlice> TimeSlices { get; set; }

		[JsonProperty("errorMessage")]
		public string ErrorMessage { get; set; }

		[JsonIgnore]
		public Dictionary<int, List<CellMTMWComponent>> DTSToMTMWComponent { get; set; }
		[JsonIgnore]
		public Dictionary<SCARAPITableCell, int> CellToDTS { get; set; }
		[JsonProperty("returnValue")]
		public Dictionary<string, string> ReturnValue { get; set; }
	}
}
