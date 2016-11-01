using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataRoostAPI.Common.Models.AsReported {
	public class AsReportedTemplate {
		public List<StaticHierarchy> StaticHierarchies { get; set; }
		public List<TimeSlice> TimeSlices { get; set; }
	}
}
