using System;
using FFDotNetHelpers.Helpers.Serialization;
using Newtonsoft.Json;

namespace DataRoostAPI.Common.Models.TimeseriesValues {
	public class SegmentNode {
		[JsonProperty("segmentTitle")]
		public string SegmentTitle { get; set; }

		[JsonProperty("asReportedLabel")]
		public string AsReportedLabel { get; set; }

		[JsonProperty("asReportedSTDCode")]
		public int? AsReportedSTDCode { get; set; }

		[JsonProperty("stdCode")]
		public int? STDCode { get; set; }

		[JsonProperty("sicCode")]
		public string SICCode { get; set; }

		[JsonProperty("naicCode")]
		public string NAICCode { get; set; }

		[JsonProperty("sicStdCode")]
		public int? SICStdCode { get; set; }

		[JsonProperty("naicStdCode")]
		public int? NAICStdCode { get; set; }

		[JsonProperty("value")]
		public decimal Value { get; set; }

		[JsonProperty("mathMl")]
		public string MathMl { get; set; }

		[JsonProperty("isCorpElim")]
		public bool? IsCorpElim { get; set; }

		[JsonProperty("isExceptionalCharges")]
		public bool? IsExceptionalCharges { get; set; }

		[JsonProperty("isDiscontinued")]
		public bool? IsDiscontinued { get; set; }

		[JsonProperty("type")]
		public string Type { get; set; }

		[JsonProperty("versionId")]
		public Guid VersionId { get; set; }
	}
}
