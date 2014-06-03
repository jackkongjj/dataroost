using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Web;
using CCS.Fundamentals.DataRoostAPI.Models;

namespace CCS.Fundamentals.DataRoostAPI.Access.SuperFast {
	public class TimeseriesIdentifier {
		public Guid SFDocumentId { get; set; }
		public int CompanyFiscalYear { get; set; }
		public DateTime PeriodEndDate { get; set; }
		public string InterimType { get; set; }
		public bool IsAutoCalc { get; set; }

		public TimeseriesIdentifier() { }

		public TimeseriesIdentifier(TimeseriesDTO ts) {
			SFDocumentId = ts.SFDocumentId;
			CompanyFiscalYear = ts.CompanyFiscalYear;
			PeriodEndDate = ts.PeriodEndDate;
			InterimType = ts.InterimType;
			IsAutoCalc = ts.IsAutoCalc;
		}

		public TimeseriesIdentifier(string token) {
			// See comment in GetToken() below

			byte[] b = Convert.FromBase64String(token.Replace('_', '/'));
			string s = new string(Encoding.UTF8.GetChars(b));

			string[] comp = s.Split('|');
			SFDocumentId = Guid.Parse(comp[0]);
			CompanyFiscalYear = Int32.Parse(comp[1]);
			PeriodEndDate = DateTime.Parse(comp[2]);
			InterimType = comp[3];
			IsAutoCalc = Boolean.Parse(comp[4]);
		}

		public string GetToken() {
			// You should never, ever implement this conversion elsewhere, in this or any other language
			// If you find yourself looking here to do just that, you're doing it wrong and should think
			// and talk through what you're trying to do.

			string s = String.Format("{0}|{1}|{2}|{3}|{4}",
				SFDocumentId,
				CompanyFiscalYear,
				PeriodEndDate,
				InterimType,
				IsAutoCalc
			);

			byte[] b = Encoding.UTF8.GetBytes(s);
			return Convert.ToBase64String(b)
				.Replace('/', '_');
		}
	}
}