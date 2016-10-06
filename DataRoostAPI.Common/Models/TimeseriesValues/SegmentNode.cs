using System;
using FFDotNetHelpers.Helpers.Serialization;
using Newtonsoft.Json;

namespace DataRoostAPI.Common.Models.TimeseriesValues {
	public class SegmentNode {
		[JsonIgnore]
		public string ConceptName { get; set; }
		[JsonIgnore]
		public string AccountName { get; set; }
		[JsonIgnore]
		public string SegmentTitle { get; set; }
		[JsonIgnore]
		public string AsReportedLabel { get; set; }
		public int? AsReportedSTDCode { get; set; }
		public int? STDCode { get; set; }
		public string SICCode { get; set; }
		public string NAICCode { get; set; }
		public decimal? Value { get; set; }
		public string MathMl { get; set; }
		public bool? IsCorpElim { get; set; }
		public bool? IsExceptionalCharges { get; set; }
		public bool? IsDiscontinued { get; set; }
		[JsonIgnore]
		public string Type { get; set; }
		[JsonIgnore]
		public int? SegmentId { get; set; }
	}

	public class FootNotes {
		public int SegmentId { get; set; }
		public string Area { get; set; }
		public string GeoRevType { get; set; }
	}


}
