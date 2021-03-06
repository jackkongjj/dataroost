using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using System.Data.Entity;
namespace DataRoostAPI.Common.Models.AsReported {
	public class TimeSliceEntity : DbContext
    {
		[JsonProperty("Id")]
		public int Id { get; set; }

		[JsonProperty("DamDocumentId")]
		public Guid DamDocumentId { get; set; }

		[JsonProperty("DocumentId")]
		public Guid DocumentId { get; set; }

		[JsonProperty("DocumentSeriesId")]
		public int DocumentSeriesId { get; set; }

		[JsonProperty("TimeSlicePeriodEndDate")]
		public DateTime TimeSlicePeriodEndDate { get; set; }

		[JsonProperty("ReportingPeriodEndDate")]
		public DateTime ReportingPeriodEndDate { get; set; }

		[JsonProperty("PublicationDate")]
		public DateTime PublicationDate { get; set; }

		[JsonProperty("FiscalDistance")]
		public int FiscalDistance { get; set; }

		[JsonProperty("Duration")]
		public int Duration { get; set; }

		[JsonProperty("PeriodType")]
		public string PeriodType { get; set; }

		[JsonProperty("AcquisitionFlag")]
		public string AcquisitionFlag { get; set; }

		[JsonProperty("AccountingStandard")]
		public string AccountingStandard { get; set; }

		[JsonProperty("ConsolidatedFlag")]
		public string ConsolidatedFlag { get; set; }

		[JsonProperty("IsProForma")]
		public bool IsProForma { get; set; }

		[JsonProperty("IsRecap")]
		public bool IsRecap { get; set; }

		[JsonProperty("CompanyFiscalYear")]
		public decimal CompanyFiscalYear { get; set; }

		[JsonProperty("ReportType")]
		public string ReportType { get; set; }

		[JsonProperty("InterimType")]
		public string InterimType { get; set; }

		[JsonProperty("IsAmended")]
		public bool IsAmended { get; set; }

		[JsonProperty("IsRestated")]
		public bool IsRestated { get; set; }

		[JsonProperty("IsAutoCalc")]
		public bool IsAutoCalc { get; set; }

		[JsonProperty("ManualOrgSet")]
		public bool ManualOrgSet { get; set; }

		[JsonProperty("TableTypeID")]
		public int TableTypeID { get; set; }

		[JsonProperty("NumberOfCells")]
		public int NumberOfCells { get; set; }

		[JsonProperty("Currency")]
		public string Currency { get; set; }

		[JsonProperty("PeriodNoteID")]
		public byte? PeriodNoteID { get; set; }

		[JsonProperty("IsSummary")]
		public bool IsSummary { get; set; }

		[JsonProperty("PeriodLength")]
		public int PeriodLength { get; set; }

		[JsonIgnore]
		public List<SCARAPITableCell> Cells { get; set; }
	}

}

