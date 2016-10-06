using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using Newtonsoft.Json;


namespace DataRoostAPI.Common.Models.Segment {
	public class SegmentsTimeSeriesDTO {
		public string AAADisplay { get; set; }
		public int Id { get; set; }
		[JsonIgnore]
		public DateTime PeriodEndDate { get; set; }
		public int Duration { get; set; }
		public string PeriodType { get; set; }
		[JsonIgnore]
		public int CompanyFiscalYear { get; set; }
		[JsonIgnore]
		public bool IsRestated { get; set; }
		public string Currency { get; set; }
		[JsonIgnore]
		public string ContentSource { get; set; }
		public bool IsFish { get; set; }
	}
}
