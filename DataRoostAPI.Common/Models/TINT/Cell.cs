using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace DataRoostAPI.Common.Models.TINT {
	public class TintCell {
		public static readonly Regex rxBoundingBox = new Regex(@"^(?:(\d+);(-?\d+);(-?\d+);(-?\d+);(-?\d+))$", RegexOptions.Compiled);

		public int Id { get; private set; }
		public int Line { get; private set; }
		public string OriginalOffset { get; private set; }
		public int Column { get; private set; }
		public string ColumnType { get; private set; }
		public string Value { get; set; }
		public string OriginalValue { get; private set; }
		public bool? InsertedRow { get; private set; }
		public string ColumnYear { get; private set; }
		public string ColumnMonth { get; private set; }
		public string ColumnDay { get; private set; }
		public string PeriodType { get; private set; }
		public string PeriodLength { get; private set; }
		public string ColumnHeader { get; private set; }
		public string Currency { get; private set; }
		public string Unit { get; private set; }
		public int? OriginalLength { get; private set; }
		public string OriginalBoundingBox { get; private set; }
		public string Scaling { get; private set; }
		public string ScalingLevel { get; private set; }
		public string XbrlTag { get; private set; }
		public string OffSet { get; private set; }

		public bool HasBoundingBox { get { return OriginalOffset != null && rxBoundingBox.IsMatch(OriginalOffset); } }

		public TintCell(XElement element) {
			Id = (int)element.Attribute("id");
			Line = (int)element.Attribute("line");
			OriginalOffset = (string)element.Attribute("origOffset");
			Column = (int)element.Attribute("col");
			ColumnType = (string)element.Attribute("colType") == "Note" ? "Notes" : (string)element.Attribute("colType");
			Value = (string)element.Attribute("value") == null ? "" : (string)element.Attribute("value");
			OriginalValue = (string)element.Attribute("origValue") == null ? "" : (string)element.Attribute("origValue");
			InsertedRow = (bool?)element.Attribute("insertedRow");
			ColumnYear = (string)element.Attribute("colYear") == null ? "" : (string)element.Attribute("colYear");
			ColumnMonth = (string)element.Attribute("colMonth") == null ? "" : (string)element.Attribute("colMonth");
			ColumnDay = (string)element.Attribute("colDay") == null ? "" : (string)element.Attribute("colDay");
			PeriodType = (string)element.Attribute("colPeriodType") == null ? "" : (string)element.Attribute("colPeriodType");
			PeriodLength = (string)element.Attribute("colPeriodCount") == null ? "" : (string)element.Attribute("colPeriodCount");
			ColumnHeader = (string)element.Attribute("colHeader");
			Currency = (string)element.Attribute("currency") == null ? "" : (string)element.Attribute("currency");
			Unit = (string)element.Attribute("unit") == null ? "" : (string)element.Attribute("unit");
			OriginalLength = (int?)element.Attribute("origLength");
			Scaling = (string)element.Attribute("scaling") == null ? "" : (string)element.Attribute("scaling");
			ScalingLevel = (string)element.Attribute("scalingLevel") == null ? "" : (string)element.Attribute("scalingLevel");
			OriginalBoundingBox = (string)element.Attribute("origBBox") == null ? "" : (string)element.Attribute("origBBox");
			XbrlTag = (string)element.Attribute("xbrlTag") == null ? "" : (string)element.Attribute("xbrlTag");
			OffSet = (string)element.Attribute("offset") == null ? "" : (string)element.Attribute("offset");
		}

		public override string ToString() {
			return string.Format("[{0}], OrigOffset{{1}}, Column{{2}}, Value{{3}}", Id, OriginalOffset, Column, Value);
		}
	}

}
