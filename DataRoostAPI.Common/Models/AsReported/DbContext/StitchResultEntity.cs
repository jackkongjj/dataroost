using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using System.Data.Entity;
namespace DataRoostAPI.Common.Models.AsReported {


	public class StitchResultEntity : DbContext {
		[JsonProperty("staticHierarchyAdjustedOrders")]
		public List<StaticHierarchyAdjustedOrder> StaticHierarchyAdjustedOrders { get; set; }

		//TODO: Add Parent Cell Changes
		[JsonIgnore]
		public List<CellMTMWComponent> ParentCellChangeComponents { get; set; }

		[JsonProperty("parentMTMWChanges")]
		public Dictionary<int, Dictionary<int, bool>> ParentMTMWChanges { get; set; }

		[JsonProperty("staticHierarchy")]
		public StaticHierarchy StaticHierarchy { get; set; }

		[JsonProperty("changedCells")]
		public List<SCARAPITableCell> ChangedCells { get; set; }

		[JsonIgnore]
		public Dictionary<int, List<CellMTMWComponent>> DTSToMTMWComponent { get; set; }
		[JsonIgnore]
    public Dictionary<SCARAPITableCell, int> CellToDTS { get; set; }

		[JsonProperty("returnValue")]
		public Dictionary<string, string> ReturnValue { get; set; }

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
