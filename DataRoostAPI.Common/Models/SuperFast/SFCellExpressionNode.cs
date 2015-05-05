using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

using DataRoostAPI.Common.Models.TimeseriesValues;

using Newtonsoft.Json;

namespace DataRoostAPI.Common.Models.SuperFast {
	public class SFCellExpressionNode : CellExpressionNode {
		[JsonProperty(PropertyName = "tableCellId")]
		public int TableCellId { get; set; }

		[JsonProperty(PropertyName = "sfDocumentId")]
		public Guid SFDocumentId { get; set; }

		[JsonProperty(PropertyName = "companyFinancialTermId")]
		public int? CompanyFinancialTermId { get; set; }

		[JsonProperty(PropertyName = "companyFinancialTermLabel")]
		public string CompanyFinancialTermLabel { get; set; }
	}
}