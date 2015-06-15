using System;
using System.Text.RegularExpressions;

using FFDotNetHelpers.Helpers.Serialization;

using Newtonsoft.Json;

namespace DataRoostAPI.Common.Models.TimeseriesValues {

	[JsonConverter(typeof (JsonDerivedTypeConverter))]
	public abstract class FLYTOffset {

		[JsonProperty("type")]
		public abstract string OffsetType { get; }

		[JsonProperty("_t")]
		public string Type {
			get { return GetType().ToString(); }
			set { }
		}

		public static FLYTOffset Parse(string offset) {
			string firstCharacter = offset.Substring(0, 1);
			if (firstCharacter == "o") {
				return HTMLOffset.Parse(offset);
			}
			if (firstCharacter == "p") {
				return PDFOffset.Parse(offset);
			}

			throw new ArgumentException("invalid offset");
		}

	}

	public class HTMLOffset : FLYTOffset {

		public override string OffsetType {
			get { return "H"; }
		}

		[JsonProperty("offset")]
		public int Offset { get; set; }

		[JsonProperty("length")]
		public int Length { get; set; }

		[JsonProperty("spanId")]
		public string SpanId { get; set; }

		public static HTMLOffset Parse(string stringOffset) {
			string[] pieces = stringOffset.Split('|');
			int base64Offset = int.Parse(pieces[0].Remove(0, 1));
			HTMLOffset offset = new HTMLOffset();
			offset.Offset = base64Offset;
			offset.SpanId = GetSpanIdFromHtmlOffset(base64Offset);
			offset.Length = int.Parse(pieces[1].Remove(0, 1));
			return offset;
		}

		private static string GetSpanIdFromHtmlOffset(int integerOffset) {
			string base64chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnofqrstuvwxyz+/";
			string base64Offset = "";
			int currentModulo = 1;
			int divisor = 64;
			int nextStep = integerOffset;
			while (nextStep >= divisor) {
				currentModulo = nextStep%divisor;
				base64Offset = base64chars[currentModulo] + base64Offset;
				nextStep = nextStep - currentModulo;
				nextStep = nextStep/divisor;
			}
			base64Offset = base64chars[nextStep] + base64Offset;
			return string.Format("~{0}_", base64Offset);
		}

	}

	public class PDFOffset : FLYTOffset {

		public override string OffsetType {
			get { return "P"; }
		}

		[JsonProperty("page")]
		public int Page { get; set; }

		[JsonProperty("left")]
		public decimal Left { get; set; }

		[JsonProperty("top")]
		public decimal Top { get; set; }

		[JsonProperty("right")]
		public decimal Right { get; set; }

		[JsonProperty("bottom")]
		public decimal Bottom { get; set; }

		public static PDFOffset Parse(string offset) {
			PDFOffset pdfOffset = new PDFOffset();
			string onlyPosition = Regex.Replace(offset, @"p\d+", "");
			onlyPosition = Regex.Replace(onlyPosition, @"r\d+", "");
			string matchArray = Regex.Replace(onlyPosition, @"\d+/g", "");
			pdfOffset.Left = decimal.Parse(matchArray[0].ToString());
			pdfOffset.Top = decimal.Parse(matchArray[1].ToString());
			pdfOffset.Right = decimal.Parse(matchArray[2].ToString());
			pdfOffset.Bottom = decimal.Parse(matchArray[3].ToString());
			matchArray = Regex.Match(offset, @"p\d+").Value;
			pdfOffset.Page = int.Parse(Regex.Replace(matchArray[0].ToString(), "p", ""));
			return pdfOffset;
		}

	}

}
