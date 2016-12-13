using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataRoostAPI.Common.Models {
	public class TimeSlice {
		public Guid Id { get; set; }
		public DateTime PeriodEndDate { get; set; }
		public int PeriodLength { get; set; }
		public string PeriodType { get; set; }
		public decimal FiscalYear { get; set; }
		public string InterimType { get; set; }
		public bool IsConsolidated { get; set; }
		public bool IsProforma { get; set; }
		public string AcquisitionStatus { get; set; }
		public int FiscalDistance { get; set; }
		public DateTime ReportingPeriodEndDate { get; set; }
		public List<TimeSliceDocument> Documents { get; set; }

		public TimeSlice() {
			Documents = new List<TimeSliceDocument>();
		}
	}

	public class TimeSliceDocument {
		public string ProductTimeSliceId { get; set; }
		public Guid DocumentId { get; set; }
		public DateTime PublicationStamp { get; set; }
		public string FormType { get; set; }
		public string ReportType { get; set; }
		public bool IsAmended { get; set; }
		public bool IsRestated { get; set; }
	}
}
