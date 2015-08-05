using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace CCS.Fundamentals.DataRoostAPI.Access.SfVoy {
	public class TimeseriesIdentifier : IdentifierBase {
		//SF specific
		public Guid SFDocumentId { get; set; }
		public int CompanyFiscalYear { get; set; }
		public bool IsAutoCalc { get; set; }
		//shared
		public DateTime PeriodEndDate { get; set; }
		public string InterimType { get; set; }
		public string ReportType { get; set; }
		public string TimeSeriesCode { get; set; }
		//voy
		public string AccountType { get; set; }

		public bool HasSf { get; set; }
		public bool HasVoy { get; set; }

		public TimeseriesIdentifier(Guid sfDocumentId, int companyFiscalYear, bool isAutoCalc, DateTime periodEndDate, string interimType, string reporttype, string timeSeriesCode, string accountType, bool hasSf, bool hasVoy) {
			this.SFDocumentId = sfDocumentId;
			this.CompanyFiscalYear = companyFiscalYear;
			this.IsAutoCalc = isAutoCalc;
			this.PeriodEndDate = periodEndDate;
			this.InterimType = interimType;
			this.ReportType = reporttype;
			this.TimeSeriesCode = timeSeriesCode;
			this.AccountType = accountType;
			this.HasSf = hasSf;
			this.HasVoy = hasVoy;
		}

		public TimeseriesIdentifier(string token) {
			string[] comp = DeconstructToken(token);

			SFDocumentId = Guid.Parse(comp[0]);			
			CompanyFiscalYear = Int32.Parse(comp[1]);
			IsAutoCalc = Boolean.Parse(comp[2]);
			PeriodEndDate = DateTime.Parse(comp[3]);
			InterimType = comp[4];
			ReportType = comp[5];
			TimeSeriesCode = comp[6];
			AccountType = comp[7];
			HasSf = Boolean.Parse(comp[8]);
			HasVoy = Boolean.Parse(comp[9]);
		}

		protected override string[] getComponents() {
			return new string[] {
				SFDocumentId.ToString(),
				CompanyFiscalYear.ToString(),
				IsAutoCalc.ToString(),
				PeriodEndDate.ToString(),
				InterimType,
				ReportType,
				TimeSeriesCode,
				AccountType,
				HasSf.ToString(),
				HasVoy.ToString()
			};
		}
	}
}