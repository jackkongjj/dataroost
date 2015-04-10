﻿using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Web;
using CCS.Fundamentals.DataRoostAPI.Models;
using CCS.Fundamentals.DataRoostAPI.Models.SuperFast;

namespace CCS.Fundamentals.DataRoostAPI.Access.SuperFast {
	public class TimeseriesIdentifier : IdentifierBase {
		public Guid SFDocumentId { get; set; }
		public int CompanyFiscalYear { get; set; }
		public DateTime PeriodEndDate { get; set; }
		public string InterimType { get; set; }
		public bool IsAutoCalc { get; set; }

		public TimeseriesIdentifier() { }

		public TimeseriesIdentifier(SFTimeseriesDTO ts) {
			SFDocumentId = ts.SFDocumentId;
			CompanyFiscalYear = ts.CompanyFiscalYear;
			PeriodEndDate = ts.PeriodEndDate;
			InterimType = ts.InterimType;
			IsAutoCalc = ts.IsAutoCalc;
		}

		public TimeseriesIdentifier(string token) {
			string[] comp = DeconstructToken(token);

			SFDocumentId = Guid.Parse(comp[0]);
			CompanyFiscalYear = Int32.Parse(comp[1]);
			PeriodEndDate = DateTime.Parse(comp[2]);
			InterimType = comp[3];
			IsAutoCalc = Boolean.Parse(comp[4]);
		}

		protected override string[] getComponents() {
			return new string[] {
				SFDocumentId.ToString(),
				CompanyFiscalYear.ToString(),
				PeriodEndDate.ToString(),
				InterimType,
				IsAutoCalc.ToString()
			};
		}
	}
}