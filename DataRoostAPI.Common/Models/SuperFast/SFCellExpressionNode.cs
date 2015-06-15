using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

using DataRoostAPI.Common.Models.TimeseriesValues;

using Newtonsoft.Json;

namespace DataRoostAPI.Common.Models.SuperFast {
	public class SFCellExpressionNode : CellExpressionNode {
		[JsonProperty("tableCellId")]
		public int TableCellId { get; set; }

		[JsonProperty("sfDocumentId")]
		public Guid SFDocumentId { get; set; }

		[JsonProperty("companyFinancialTermId")]
		public int? CompanyFinancialTermId { get; set; }

		[JsonProperty("companyFinancialTermLabel")]
		public string CompanyFinancialTermLabel { get; set; }
	}
}