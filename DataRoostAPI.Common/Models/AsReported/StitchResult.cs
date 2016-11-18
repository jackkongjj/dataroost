using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace DataRoostAPI.Common.Models.AsReported {


	public class StitchResult {
		[JsonProperty("staticHierarchyAdjustedOrders")]
		public List<StaticHierarchyAdjustedOrder> StaticHierarchyAdjustedOrders { get; set; }

		//TODO: Add Parent Cell Changes
		[JsonIgnore]
		public List<CellMTMWComponent> ParentCellChangeComponents { get; set; }

		[JsonProperty("staticHierarchy")]
		public StaticHierarchy StaticHierarchy { get; set; }

		[JsonIgnore]
		public Dictionary<int, List<CellMTMWComponent>> DTSToMTMWComponent { get; set; }
		[JsonIgnore]
		public Dictionary<TableCell, int> CellToDTS { get; set; }

	}

	public class CellMTMWComponent {
		public int StaticHierarchyID { get; set; }
		public int DocumentTimeSliceID { get; set; }
		public int TableCellID { get; set; }
		public decimal ValueNumeric { get; set; }
		public bool IsIncomePositive { get; set; }
		public double ScalingFactorValue { get; set; }
		public int RootStaticHierarchyID { get; set; }
		public int RootDocumentTimeSliceID { get; set; }
	}

	public class StaticHierarchyAdjustedOrder {
		[JsonProperty("staticHierarchyID")]
		public int StaticHierarchyID { get; set; }
		[JsonProperty("newAdjustedOrder")]
		public int NewAdjustedOrder { get; set; }
	}
}
