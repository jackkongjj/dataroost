using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

using DataRoostAPI.Common.Models.TimeseriesValues;

using Newtonsoft.Json;

namespace DataRoostAPI.Common.Models {

	public class TimeseriesDTO {
		[JsonProperty("_id")]
		public string Id { get; set; }


		[JsonProperty("periodLength")]
		public int PeriodLength { get; set; }

		[JsonProperty("periodType")]
		public string PeriodType { get; set; }

		[JsonProperty("interimType")]
		public string InterimType { get; set; }

		[JsonProperty("periodEndDate")]
		public DateTime PeriodEndDate { get; set; }

		[JsonProperty("companyFiscalyear")]
		public int CompanyFiscalYear { get; set; }

		[JsonProperty("publicationDate")]
		public DateTime PublicationDate { get; set; }

		[JsonProperty("damDocumentId")]
		public Guid DamDocumentId { get; set; }

		[JsonProperty("isRecap")]
		public bool IsRecap { get; set; }

		[JsonProperty("isAutoCalc")]
		public bool IsAutoCalc { get; set; }

		[JsonProperty("voyagerFormType")]
		public string VoyagerFormType { get; set; }

		[JsonProperty("reportType")]
		public string ReportType { get; set; }

		[JsonProperty("isoCurrency")]
		public string IsoCurrency { get; set; }

		[JsonProperty("scalingFactor")]
		public string ScalingFactor { get; set; }

		[JsonProperty("values")]
		public Dictionary<string, TimeseriesValueDTO> Values { get; set; }

		[JsonProperty("perShareValues")]
		public Dictionary<int, Dictionary<string, TimeseriesValueDTO>> PerShareValues { get; set; }
	}
}