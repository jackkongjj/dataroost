using System;

using FFDotNetHelpers.Helpers.Serialization;

using Newtonsoft.Json;

namespace DataRoostAPI.Common.Models.TimeseriesValues {

	public enum ExpressionNodeType {

		Value,
		Cell,
		Subexpression

	}

	[JsonConverter(typeof (JsonDerivedTypeConverter))]
	public abstract class ExpressionNode {

		[JsonProperty("_type")]
		public abstract ExpressionNodeType NodeType { get; }

		[JsonProperty("_t")]
		public string Type {
			get { return GetType().ToString(); }
			set { }
		}

	}

	public class ValueExpressionNode : ExpressionNode {

		public override ExpressionNodeType NodeType {
			get { return ExpressionNodeType.Value; }
		}

		[JsonProperty("value")]
		public decimal Value { get; set; }

	}

	public class CellExpressionNode : ExpressionNode {

		public override ExpressionNodeType NodeType {
			get { return ExpressionNodeType.Cell; }
		}

		[JsonProperty("damDocumentId")]
		public Guid DAMDocumentId { get; set; }

		[JsonProperty("damRootId")]
		public int DAMRootId { get; set; }

		[JsonProperty("offset")]
		public FLYTOffset Offset { get; set; }

		[JsonProperty("numericValue")]
		public decimal NumericValue { get; set; }

		[JsonProperty("asPresentedValue")]
		public string AsPresentedValue { get; set; }

		[JsonProperty("currency")]
		public string Currency { get; set; }

		[JsonProperty("scalingBase10")]
		public int ScalingBase10 { get; set; }

	}

	public class SubexpressionExpressionNode : ExpressionNode {

		public override ExpressionNodeType NodeType {
			get { return ExpressionNodeType.Subexpression; }
		}

		[JsonProperty("expression")]
		public ExpressionTimeseriesValueDetailDTO Expression { get; set; }

	}

}
