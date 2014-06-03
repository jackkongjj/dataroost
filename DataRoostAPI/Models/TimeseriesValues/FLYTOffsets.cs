using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using Newtonsoft.Json;

namespace CCS.Fundamentals.DataRoostAPI.Models.TimeseriesValues {
	public abstract class FLYTOffset {
		[JsonProperty(PropertyName = "type")]
		public abstract string Type { get; }
	}

	public class HTMLOffset : FLYTOffset {
		public override string Type { get { return "H"; } }

		[JsonProperty(PropertyName = "offset")]
		public int Offset { get; set; }

		[JsonProperty(PropertyName = "length")]
		public int Length { get; set; }
	}

	public class PDFOffset : FLYTOffset {
		public override string Type { get { return "P"; } }

		[JsonProperty(PropertyName = "page")]
		public int Page { get; set; }

		[JsonProperty(PropertyName = "left")]
		public decimal Left { get; set; }

		[JsonProperty(PropertyName = "top")]
		public decimal Top { get; set; }

		[JsonProperty(PropertyName = "right")]
		public decimal Right { get; set; }

		[JsonProperty(PropertyName = "bottom")]
		public decimal Bottom { get; set; }
	}
}