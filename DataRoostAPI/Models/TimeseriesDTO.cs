using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using CCS.Fundamentals.DataRoostAPI.Models.TimeseriesValues;
using Newtonsoft.Json;

namespace CCS.Fundamentals.DataRoostAPI.Models {

	public class TimeseriesDTO {
		[JsonProperty(PropertyName = "_id")]
		public string Id { get; set; }


		[JsonProperty(PropertyName = "periodLength")]
		public int PeriodLength { get; set; }

		[JsonProperty(PropertyName = "periodType")]
		public string PeriodType { get; set; }

		[JsonProperty(PropertyName = "interimType")]
		public string InterimType { get; set; }

		[JsonProperty(PropertyName = "periodEndDate")]
		public DateTime PeriodEndDate { get; set; }

		[JsonProperty(PropertyName = "companyFiscalyear")]
		public int CompanyFiscalYear { get; set; }

		[JsonProperty(PropertyName = "publicationDate")]
		public DateTime PublicationDate { get; set; }

		[JsonProperty(PropertyName = "damDocumentId")]
		public Guid DamDocumentId { get; set; }

		[JsonProperty(PropertyName = "isRecap")]
		public bool IsRecap { get; set; }

		[JsonProperty(PropertyName = "isAutoCalc")]
		public bool IsAutoCalc { get; set; }

		[JsonProperty(PropertyName = "voyagerFormType")]
		public string VoyagerFormType { get; set; }

		[JsonProperty(PropertyName = "reportType")]
		public string ReportType { get; set; }


		[JsonProperty(PropertyName = "values")]
		public Dictionary<int, TimeseriesValueDTO> Values { get; set; }

		[JsonProperty(PropertyName = "perShareValues")]
		public Dictionary<int, Dictionary<string, TimeseriesValueDTO>> PerShareValues { get; set; }
	}
}