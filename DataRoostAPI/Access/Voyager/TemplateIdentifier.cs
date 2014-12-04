using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Web;

namespace CCS.Fundamentals.DataRoostAPI.Access.Voyager {
	public class TemplateIdentifier : IdentifierBase {
		public string UpdateType { get; set; }
		public string ReportType { get; set; }
		public string TemplateCode { get; set; }

		public static TemplateIdentifier GetTemplateIdentifier(string token) {
			byte[] b = Convert.FromBase64String(token.Replace('_', '/'));
			string s = new string(Encoding.UTF8.GetChars(b));

			string[] comp = s.Split('|');
			return new TemplateIdentifier()
			{
				UpdateType = comp[0],
				ReportType = comp[1],
				TemplateCode = comp[2],
			};
		}

		protected override string[] getComponents() {
			return new string[] {
				UpdateType,
				ReportType,
				TemplateCode,
			};
		}
	}
}