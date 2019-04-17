using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.Entity;
namespace DataRoostAPI.Common.Models.AsReported {
	public class AsReportedTemplateSkeletonEntity
    {
		public List<int> StaticHierarchies { get; set; }
		public List<int> TimeSlices { get; set; }
	}
}
