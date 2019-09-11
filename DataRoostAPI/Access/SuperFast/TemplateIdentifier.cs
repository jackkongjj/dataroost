using System;
using System.Text;

namespace CCS.Fundamentals.DataRoostAPI.Access.SuperFast {
	public class TemplateIdentifier : IdentifierBase {	
		public string UpdateType { get; set; }
		public string ReportType { get; set; }
		public int TemplateType { get; set; }
        public int TemplateId { get; set; }

        public static TemplateIdentifier GetTemplateIdentifier(string token) {
			byte[] b = Convert.FromBase64String(token.Replace('_', '/'));
			string s = new string(Encoding.UTF8.GetChars(b));

			string[] comp = s.Split('|');
			return new TemplateIdentifier()
			{
				UpdateType = comp[0],
				ReportType = comp[1],
				TemplateType = Int32.Parse(comp[2]), 
                TemplateId = Int32.Parse(comp[3])
            };
		}

		protected override string[] getComponents() {
			return new string[] {
				UpdateType,
				ReportType,
				TemplateType.ToString(), 
                TemplateId.ToString()
			};
		}
	}
}