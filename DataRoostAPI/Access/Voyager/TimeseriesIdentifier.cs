using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace CCS.Fundamentals.DataRoostAPI.Access.Voyager {

	public class TimeseriesIdentifier : IdentifierBase {

		public string MasterId { get; private set; }
		public int Year { get; private set; }
		public DateTime ReportDate { get; private set; }
		public string TimeSeriesCode { get; private set; }

		public TimeseriesIdentifier(string masterId, int year, DateTime reportDate, string timeSeriesCode) {
			MasterId = masterId;
			Year = year;
			ReportDate = reportDate;
			TimeSeriesCode = timeSeriesCode;
		}

		public TimeseriesIdentifier(string token) {
			string[] comp = DeconstructToken(token);

			Year = Int32.Parse(comp[0]);
			ReportDate = DateTime.Parse(comp[1]);
			TimeSeriesCode = comp[2];
			MasterId = comp[3];
		}

		protected override string[] getComponents() {
			return new string[] {
				Year.ToString(),
				ReportDate.ToString(),
				TimeSeriesCode.ToString(),
				MasterId
			};
		}
	}
}