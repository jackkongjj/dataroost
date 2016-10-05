using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Newtonsoft.Json;

namespace DataRoostAPI.Common.Models {
	public class ExportedVersionInfo {
		[JsonProperty("id")]
		public string Id { get; set; }

		[JsonProperty("exportDateUtc")]
		public DateTime ExportDateUtc { get; set; }
	}
}
