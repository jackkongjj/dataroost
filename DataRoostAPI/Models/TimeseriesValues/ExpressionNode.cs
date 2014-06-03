using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using Newtonsoft.Json;

namespace CCS.Fundamentals.DataRoostAPI.Models.TimeseriesValues {
	public enum ExpressionNodeType {
		Value, Cell, Subexpression
	}

	public abstract class ExpressionNode {
		[JsonProperty(PropertyName = "_type")]
		public abstract ExpressionNodeType Type { get; }
	}

	public class ValueExpressionNode : ExpressionNode {
		public override ExpressionNodeType Type { get { return ExpressionNodeType.Value; } }

		[JsonProperty(PropertyName = "value")]
		public decimal Value { get; set; }
	}

	public class CellExpressionNode : ExpressionNode {
		public override ExpressionNodeType Type { get { return ExpressionNodeType.Cell; } }

		[JsonProperty(PropertyName = "tableCellId")]
		public int TableCellId { get; set; }

		[JsonProperty(PropertyName = "sfDocumentId")]
		public Guid SFDocumentId { get; set; }

		[JsonProperty(PropertyName = "damDocumentId")]
		public Guid DAMDocumentId { get; set; }

		[JsonProperty(PropertyName = "damRootId")]
		public int DAMRootId { get; set; }

		[JsonProperty(PropertyName = "offset")]
		public FLYTOffset Offset { get; set; }


		[JsonProperty(PropertyName = "numericValue")]
		public decimal NumericValue { get; set; }

		[JsonProperty(PropertyName = "asPresentedValue")]
		public string AsPresentedValue { get; set; }

		[JsonProperty(PropertyName = "currency")]
		public string Currency { get; set; }

		[JsonProperty(PropertyName = "scalingBase10")]
		public int ScalingBase10 { get; set; }


		[JsonProperty(PropertyName = "companyFinancialTermId")]
		public int? CompanyFinancialTermId { get; set; }

		[JsonProperty(PropertyName = "companyFinancialTermLabel")]
		public string CompanyFinancialTermLabel { get; set; }
	}

	public class SubexpressionExpressionNode : ExpressionNode {
		public override ExpressionNodeType Type { get { return ExpressionNodeType.Subexpression; } }

		[JsonProperty(PropertyName = "expression")]
		public ExpressionTimeseriesValueDetailDTO Expression { get; set; }
	}
}