using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace DataRoostAPI.Common.Models.TINT {
	public class Tint : List<TintTable> {
		public Tint(XElement xml) {
			foreach (var segment in xml.Elements("segment")) {
				foreach (var table in segment.Elements("table")) {
					this.Add(new TintTable(table));
				}
			}
		}
	}
}
