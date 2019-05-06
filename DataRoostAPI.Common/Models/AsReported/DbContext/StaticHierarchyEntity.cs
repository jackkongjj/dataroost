using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Newtonsoft.Json;
using System.Data.Entity;

namespace DataRoostAPI.Common.Models.AsReported {
	public class StaticHierarchyEntity:DbContext  {

		public static string labelRx = @"(\[[^\]]*\])*(?<desc>[^\[\]]*)";

		private Regex rx = new Regex(StaticHierarchy.labelRx);

		[JsonProperty("id")]
		public int Id { get; set; }

		[JsonProperty("companyFinancialTermId")]
		public int CompanyFinancialTermId { get; set; }

		[JsonProperty("adjustedOrder")]
		public int AdjustedOrder { get; set; }

		[JsonProperty("tableTypeId")]
		public int TableTypeId { get; set; }

		[JsonProperty("description")]
		public string Description { get; set; }

		[JsonProperty("hierarchyTypeId")]
		public char HierarchyTypeId { get; set; }

		[JsonProperty("separatorFlag")]
        public bool SeparatorFlag { get; set; }

        [JsonProperty("staticHierarchyMetaId")]
        public int StaticHierarchyMetaId { get; set; }

        [JsonProperty("staticHierarchyMetaType")]
        public string StaticHierarchyMetaType { get; set; }

		[JsonProperty("unitTypeId")]
		public int UnitTypeId { get; set; }

		[JsonProperty("isIncomePositive")]
		public bool IsIncomePositive { get; set; }

		[JsonProperty("childrenExpandDown")]
		public bool ChildrenExpandDown { get; set; }

		[JsonProperty("cells")]
        public List<SCARAPITableCell> Cells { get; set; }

		[JsonProperty("parentId")]
		public int? ParentID { get; set; }

		[JsonProperty("normalizedDescription")]
		public string NormalizedDescription {
			get {
				return rx.Match(Description).Result(@"${desc}");
			}
		}

		[JsonProperty("level")]
		public int Level { get; set; }


		public string TableTypeDescription { get; set; }
	}
}
