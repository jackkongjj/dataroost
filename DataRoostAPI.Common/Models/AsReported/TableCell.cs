using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace DataRoostAPI.Common.Models.AsReported {
	public class TableCell {

		[JsonProperty("_id")]
		public int ID { get; set; }

		[JsonProperty("offset")]
		public string Offset { get; set; }

		[JsonProperty("cellPeriodType")]
		public string CellPeriodType { get; set; }

		[JsonProperty("periodTypeID")]
		public string PeriodTypeID { get; set; }

		[JsonProperty("cellPeriodCount")]
		public string CellPeriodCount { get; set; }

		[JsonProperty("periodLength")]
		public int? PeriodLength { get; set; }

		[JsonProperty("cellDay")]
		public string CellDay { get; set; }

		[JsonProperty("cellMonth")]
		public string CellMonth { get; set; }

		[JsonProperty("cellYear")]
		public string CellYear { get; set; }

		[JsonProperty("cellDate")]
		public DateTime? CellDate { get; set; }

		[JsonProperty("value")]
		public string Value { get; set; }

		[JsonProperty("companyFinancialTermID")]
		public int? CompanyFinancialTermID { get; set; }

		[JsonProperty("valueNumeric")]
		public decimal? ValueNumeric { get; set; }

		[JsonProperty("virtualValueNumeric")]
		public decimal? VirtualValueNumeric { get; set; }

		[JsonProperty("normalizedNegativeIndicator")]
		public bool NormalizedNegativeIndicator { get; set; }

		[JsonProperty("scalingFactorID")]
		public string ScalingFactorID { get; set; }

		[JsonProperty("asReportedScalingFactor")]
		public string AsReportedScalingFactor { get; set; }

		[JsonProperty("currency")]
		public string Currency { get; set; }

		[JsonProperty("currencyCode")]
		public string CurrencyCode { get; set; }

		[JsonProperty("cusip")]
		public string Cusip { get; set; }

		[JsonProperty("scarUpdated")]
		public bool ScarUpdated { get; set; }

		[JsonProperty("isIncomePositive")]
		public bool IsIncomePositive { get; set; }

		[JsonProperty("XBRLTag")]
		public string XBRLTag { get; set; }

		[JsonProperty("updateStampUTC")]
		public DateTime? UpdateStampUTC { get; set; }

		[JsonProperty("documentId")]
		public Guid DocumentID { get; set; }

		[JsonProperty("label")]
		public string Label { get; set; }

		//clear code for like period validation
		[JsonProperty("ARDErrorTypeId")]
		public int? ARDErrorTypeId { get; set; }

		//clear code for MTMW
		[JsonProperty("MTMWErrorTypeId")]
		public int? MTMWErrorTypeId { get; set; }

		//red background mismatch like periods
		[JsonProperty("likePeriodValidationFlag")]
		public bool LikePeriodValidationFlag { get; set; }

		//red failed MTMW validation
		[JsonProperty("MTMWValidationFlag")]
		public bool MTMWValidationFlag { get; set; }

		[JsonProperty("scalingFactorValue")]
		public double ScalingFactorValue { get; set; }
	}


}
