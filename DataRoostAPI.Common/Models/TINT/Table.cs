using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace DataRoostAPI.Common.Models.TINT {
	public class TintTable : List<TintCell> {
		public string Type { get; private set; }
		public bool? IsConsolidated { get; private set; }
		public string Currency { get; private set; }
		public string Unit { get; private set; }
		public bool? IsExceptShares { get; private set; }
		public int Id { get; private set; }
		public string Title { get; private set; }
		public string xbrlTableTitle { get; set; }

		public TintTable(XElement element) {
			Type = (string)element.Attribute("type") == null ? "ALL" : (string)element.Attribute("type");
			IsConsolidated = true;
			Currency = (string)element.Attribute("currency") == null ? "" : (string)element.Attribute("currency");
			Unit = (string)element.Attribute("unit") == null ? "" : (string)element.Attribute("unit");
			IsExceptShares = (bool?)element.Attribute("exceptShares");
			Id = (int)element.Attribute("id");
			Title = (string)element.Attribute("title");
			xbrlTableTitle = (string)element.Attribute("xbrlTableTitle") == null ? "" : (string)element.Attribute("xbrlTableTitle");
			this.AddRange(element.Elements("cell").Select(x => new TintCell(x)).Where(c => !c.ColumnType.Equals("Notes")));
		}

		public string GetAsRawHTML() {
			StringBuilder sb = new StringBuilder();

			sb.AppendLine("<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.0 Transitional//EN\" \"");
			sb.AppendLine("http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd\">");
			sb.AppendLine("<html xmlns=\"http://www.w3.org/1999/xhtml\">");
			sb.AppendLine("<head>");
			sb.AppendLine("<meta http-equiv=\"Content-Type\" content=\"text/html; charset=iso-8859-1\" />");
			sb.AppendLine("<style>");
			sb.AppendLine("body { font: 62.5%/1.3 Verdana, Arial, Helvetica, sans-serif; }");
			sb.AppendLine("   #even { background-color: #acf;  }");
			sb.AppendLine("   #odd { backgroun-color: #def; }");
			sb.AppendLine("</style>");
			sb.AppendLine("</head>");
			sb.AppendLine("<body>");
			sb.AppendLine("   <table border=\"1\" cellpadding=\"3\" cellspacing=\"3\">");

			bool first = true;
			bool odd = true;
			foreach (TintCell c in this.OrderBy(x => x.Id)) {
				if (c.ColumnType.ToLower() == "description") {
					if (!first) {
						sb.AppendLine("</tr>");
						first = false;
					}
					if (odd) {
						sb.AppendLine("<tr id=\"odd\">");
						odd = false;
					} else {
						sb.AppendLine("<tr id=\"even\">");
						odd = true;
					}
					sb.Append("<td>");
					sb.Append(c.Value);
					sb.AppendLine("</td>");
				} else {
					sb.Append("<td>");
					sb.Append(c.Value);
					sb.AppendLine("</td>");
				}
			}
			sb.AppendLine("</table>");
			return sb.ToString();
		}
	}
}
