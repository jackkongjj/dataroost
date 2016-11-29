using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace DataRoostAPI.Common.Models.TimeseriesValues {
	public class KPINode {
		[JsonIgnore]
		public string ItemDescription { get; set; }
		public string AAAValue { get; set; }
		public string MathMl { get; set; }
		public int ItemId { get; set; }
	}
}
