using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Newtonsoft.Json;

namespace DataRoostAPI.Common.Models {
	public class CompanyPriority {

		[JsonProperty("absolutePriority")]
		public decimal? AbsolutePriority { get; set; }

		[JsonProperty("companyPriority")]
		public int? Priority { get; set; }

	}
}
