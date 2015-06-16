using System;

using FFDotNetHelpers.Helpers.Serialization;

using Newtonsoft.Json;

namespace DataRoostAPI.Common.Models.TimeseriesValues {

	public class TimeseriesValueDTO {

		[JsonProperty("contents")]
		public string Contents { get; set; }

		[JsonProperty("valueDetails")]
		public TimeseriesValueDetailDTO ValueDetails { get; set; }

	}

	[JsonConverter(typeof (JsonDerivedTypeConverter))]
	public abstract class TimeseriesValueDetailDTO {

		[JsonProperty("_type")]
		public abstract string Type { get; }

		[JsonProperty("_t")]
		public string ClassType {
			get { return GetType().AssemblyQualifiedName; }
			set { }
		}

	}

	public class TextTimeseriesValueDetailDTO : TimeseriesValueDetailDTO {

		public override string Type {
			get { return "text"; }
		}

		[JsonProperty("text")]
		public string Text { get; set; }

	}

	public class DateTimeseriesValueDetailDTO : TimeseriesValueDetailDTO {

		public override string Type {
			get { return "date"; }
		}

		[JsonProperty("date")]
		public DateTime Date { get; set; }

	}

	public class LookupTimeseriesValueDetailDTO : TimeseriesValueDetailDTO {

		public override string Type {
			get { return "lookup"; }
		}

		[JsonProperty("lookupName")]
		public string LookupName { get; set; }

		[JsonProperty("value")]
		public string Value { get; set; }

	}

	public class ExpressionTimeseriesValueDetailDTO : TimeseriesValueDetailDTO {

		public override string Type {
			get { return "exp"; }
		}

		[JsonProperty("_id")]
		public int Id { get; set; }

		[JsonProperty("operation")]
		public string Operation { get; set; }

		[JsonProperty("leftNode")]
		public ExpressionNode LeftNode { get; set; }

		[JsonProperty("rightNode")]
		public ExpressionNode RightNode { get; set; }

	}

	public class ExpressionTimeseriesValueDetailVoySDBDTO : ExpressionTimeseriesValueDetailDTO {

		[JsonProperty("starItem")]
		public bool isStar { get; set; }

	}

}
