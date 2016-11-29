using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using Newtonsoft.Json;

namespace DataRoostAPI.Common.Models.KPI {
	public class KpiTimeSeriesDTO  {
		public string AAADisplay { get; set; }
		[JsonIgnore]
		public Guid Id { get; set; }
		[JsonIgnore]
		public DateTime PeriodEndDate { get; set; }
		public int Duration { get; set; }
		[JsonIgnore]
		public string PeriodType { get; set; }
		[JsonIgnore]
		public int CompanyFiscalYear { get; set; }
		public string AcquisitionStatus { get; set; }
		public string AccountingStandard { get; set; }
		public string ConsolidatedType { get; set; }
		[JsonIgnore]
		public bool IsRecap { get; set; }
		public bool IsProforma { get; set; }
		[JsonIgnore]
		public bool IsRestated { get; set; }
	}
}
