using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using Newtonsoft.Json;

namespace DataRoostAPI.Common.Models.KPI {
	public class KpiTimeSeriesDTO : TimeseriesDTO {
		[JsonProperty("versionId")]
		public string VersionId { get; set; }

		[JsonProperty("duration")]
		public int? Duration { get; set; }

		[JsonProperty("acquisitionStatus")]
		public string AcquisitionStatus { get; set; }

		[JsonProperty("consolidatedType")]
		public string ConsolidatedType { get; set; }

		[JsonProperty("isProforma")]
		public bool? IsProforma { get; set; }

		[JsonProperty("isRestated")]
		public bool? IsRestated { get; set; }
	}
}
