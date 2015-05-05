using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using Newtonsoft.Json;

namespace DataRoostAPI.Common.Models.TimeseriesValues {
	public class TimeseriesValueDTO {
		[JsonProperty(PropertyName = "contents")]
		public string Contents { get; set; }

		[JsonProperty(PropertyName = "valueDetails")]
		public TimeseriesValueDetailDTO ValueDetails { get; set; }
	}

	public abstract class TimeseriesValueDetailDTO {
		[JsonProperty(PropertyName = "_type")]
		public abstract string Type { get; }
	}

	public class TextTimeseriesValueDetailDTO : TimeseriesValueDetailDTO {
		public override string Type { get { return "text"; } }

		[JsonProperty(PropertyName = "text")]
		public string Text { get; set; }
	}

	public class DateTimeseriesValueDetailDTO : TimeseriesValueDetailDTO {
		public override string Type { get { return "date"; } }

		[JsonProperty(PropertyName = "date")]
		public DateTime Date { get; set; }
	}

	public class LookupTimeseriesValueDetailDTO : TimeseriesValueDetailDTO {
		public override string Type { get { return "lookup"; } }

		[JsonProperty(PropertyName = "lookupName")]
		public string LookupName { get; set; }

		[JsonProperty(PropertyName = "value")]
		public string Value { get; set; }
	}

	public class ExpressionTimeseriesValueDetailDTO : TimeseriesValueDetailDTO {
		public override string Type { get { return "exp"; } }

		[JsonProperty(PropertyName = "_id")]
		public int Id { get; set; }

		[JsonProperty(PropertyName = "operation")]
		public string Operation { get; set; }

		[JsonProperty(PropertyName = "leftNode")]
		public ExpressionNode LeftNode { get; set; }

		[JsonProperty(PropertyName = "rightNode")]
		public ExpressionNode RightNode { get; set; }
	}

	public class ExpressionTimeseriesValueDetailVoySDBDTO : ExpressionTimeseriesValueDetailDTO {
		[JsonProperty(PropertyName = "starItem")]
		public bool isStar { get; set; }
	}
}