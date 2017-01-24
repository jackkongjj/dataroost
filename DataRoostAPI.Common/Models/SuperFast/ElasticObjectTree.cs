using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace DataRoostAPI.Common.Models.SuperFast {
	public class ElasticObjectTree {
		public int? ItemDetailId;
		public int Iconum;
		public string ItemCode;
		public string ItemDescription;
		public string InterimTypeID;
		public string ReportTypeID;
		public string AccountTypeID;
		public int AutoClacFlag;
		public string FormTypeID;
		public bool EncoreFlag;
		public string IndustryDetail;
		public string IndustryGroup;
		public string IsoCountryCode;
		public string Priority;
		public Guid? DocumentId;
		public string SecurityId;
		public string Name { get; set; }
		public Guid? RowId { get; set; }

		private string _value;
		public string Value {
			get {
				return _value;
			}
			set {
				_value = value;
			}
		}
		public string ValueNumeric;
		public string Offset;
		public string OffsetLabelWithHierarchy;
		public string ExtractedLabel;
		public string XbrlTag;

		private string _offsetLabel;

		public string OffsetLabel {
			get {
				return _offsetLabel;
			}
			set {
				_offsetLabel = value;
			}
		}

		public string UIOffSetLabel {
			get {
				string Toolip = string.IsNullOrEmpty(OffsetLabel) ? OffsetLabel : string.Concat(OffsetLabel.ToCamelCase(), string.Format("{0}{1}{2}", " (", Display, ")"));
				if (!string.IsNullOrWhiteSpace(XbrlTag)) {
					Toolip += "\nXBRL: " + XbrlTag;
				}
				return Toolip;
			}
		}

		private string _CompanyFinancialTerm = string.Empty;

		public string CompanyFinancialTerm {
			get { return (string.IsNullOrEmpty(_CompanyFinancialTerm)) ? OffsetLabel : _CompanyFinancialTerm; }
			set {
				_CompanyFinancialTerm = value;
			}
		}
		public int CompanyFinancialTermId { get; set; }
		public string CurrencyCode;

		public int Key;

		private string _ScalingFactor;
		public string ScalingFactor {
			get {
				return _ScalingFactor;
			}
			set {
				_ScalingFactor = value;
			}
		}

		[JsonIgnore]
		public string CalculationDisplay {
			get {
				return (Name == "mi" || Name == "mn") ? ValueNumeric : Value;
			}
		}

		[JsonIgnore]
		public string Display {
			get {
				return (Name == "mi" || Name == "mn") ? TermReplacement() : Value;
			}
		}


		[JsonIgnore]
		public Guid TimeSeriesId;

		private string TermReplacement() {
			string p = Value;
			decimal val;
			if (decimal.TryParse(p, out val)) {
				p = (val == 0) ? "0" : string.Format("{0:###,###,###,###,###,##0.##########}", val);
			}
			return p;
		}

	}

	public static class StringConversion {
		public static string ToCamelCase(this string text) {
			return text[0].ToString().ToUpper() + text.Substring(1, text.Length - 1).ToLowerInvariant();
		}
	}

}
