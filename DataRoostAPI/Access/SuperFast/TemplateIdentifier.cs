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
	public class TemplateIdentifier : IdentifierBase {	
		public string UpdateType { get; set; }
		public string ReportType { get; set; }
		public int TemplateType { get; set; }

		public static TemplateIdentifier GetTemplateIdentifier(string token) {
			byte[] b = Convert.FromBase64String(token.Replace('_', '/'));
			string s = new string(Encoding.UTF8.GetChars(b));

			string[] comp = s.Split('|');
			return new TemplateIdentifier()
			{
				UpdateType = comp[0],
				ReportType = comp[1],
				TemplateType = Int32.Parse(comp[2])
			};
		}

		protected override string[] getComponents() {
			return new string[] {
				UpdateType,
				ReportType,
				TemplateType.ToString()
			};
		}
	}
}