using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace CCS.Fundamentals.DataRoostAPI.Access.Voyager {

	public class TimeseriesIdentifier : IdentifierBase {

			public int Year { get; private set; }
			public DateTime ReportDate { get; private set; }
			public string TimeSeriesCode { get; private set; }

			public TimeseriesIdentifier(int year, DateTime reportDate, string timeSeriesCode) {
				Year = year;
				ReportDate = reportDate;
				TimeSeriesCode = timeSeriesCode;
			}

			public TimeseriesIdentifier(string token) {
				string[] comp = DeconstructToken(token);

				Year = Int32.Parse(comp[0]);
				ReportDate = DateTime.Parse(comp[1]);
				TimeSeriesCode = comp[2];
			}

			protected override string[] getComponents() {
				return new string[] {
				Year.ToString(),
				ReportDate.ToString(),
				TimeSeriesCode.ToString()
			};
			}

			public override bool Equals(object obj) {
				if (base.Equals(obj)) {
					return true;
				}

				TimeseriesIdentifier idObject = obj as TimeseriesIdentifier;
				if (idObject == null) {
					return false;
				}

				if (idObject.Year == this.Year && idObject.ReportDate == this.ReportDate && idObject.TimeSeriesCode == this.TimeSeriesCode) {
					return true;
				}

				return false;
			}

			public override int GetHashCode() {
				return Year.GetHashCode() ^ ReportDate.GetHashCode() ^ TimeSeriesCode.GetHashCode();
			}

			public static bool operator ==(TimeseriesIdentifier a, TimeseriesIdentifier b) {
				if (System.Object.ReferenceEquals(a, b)) {
					return true;
				}

				if (((object)a == null) || ((object)b == null)) {
					return false;
				}

				return a.Year == b.Year && a.ReportDate == b.ReportDate && a.TimeSeriesCode == b.TimeSeriesCode;
			}

			public static bool operator !=(TimeseriesIdentifier a, TimeseriesIdentifier b) {
				return !(a == b);
			}
	}
}