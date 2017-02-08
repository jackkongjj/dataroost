using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace DataRoostAPI.Common.Models {

	public class TimeSlice  {

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
		public bool isNew { get; set; }
		public bool isEdited { get; set; }

		public TimeSlice() {
			Documents = new List<TimeSliceDocument>();
		}

		public override bool Equals(Object timeSliceObj) {
			TimeSlice obj = timeSliceObj as TimeSlice;
			if (obj == null) return false;
			return PeriodLength == obj.PeriodLength && PeriodType == obj.PeriodType && FiscalYear == obj.FiscalYear
				&& InterimType == obj.InterimType && IsConsolidated == obj.IsConsolidated && IsProforma == obj.IsProforma
				&& AcquisitionStatus == obj.AcquisitionStatus && FiscalDistance == obj.FiscalDistance && obj.ReportingPeriodEndDate == ReportingPeriodEndDate;
		}

		public override int GetHashCode() {
			return this.Id.GetHashCode();
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
		public bool isNew { get; set; }
		public bool isEdited { get; set; }
	}
}

