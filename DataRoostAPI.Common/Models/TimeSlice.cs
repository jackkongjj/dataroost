using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataRoostAPI.Common.Models {
	public class TimeSlice {
		public DateTime PeriodEndDate { get; set; }
		public int PeriodLength { get; set; }
		public string PeriodType { get; set; }
		public decimal FiscalYear { get; set; }
		public string ReportType { get; set; }
		public string InterimType { get; set; }
		public bool IsConsolidated { get; set; }
		public bool IsProforma { get; set; }
		public bool IsAmended { get; set; }
		public bool IsRestated { get; set; }
		public string AcquisionStatus { get; set; }
		public int FiscalDistance { get; set; }
		public string ProductTimeSliceId { get; set; }
		public Guid DocumentId { get; set; }
	}	
}
