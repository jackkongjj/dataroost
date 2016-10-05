using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using Newtonsoft.Json;


namespace DataRoostAPI.Common.Models.Segment {
	public class SegmentTimeSeriesDTO : TimeseriesDTO {
		[JsonProperty("versionId")]
		public string VersionId { get; set; }

		[JsonProperty("duration")]
		public int? Duration { get; set; }

		[JsonProperty("isRestated")]
		public bool? IsRestated { get; set; }

		[JsonProperty("contentSource")]
		public string ContentSource { get; set; }

		[JsonProperty("isFish")]
		public bool IsFish { get; set; }
	}
}
