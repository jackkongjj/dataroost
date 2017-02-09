using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace DataRoostAPI.Common.Models.TimeseriesValues {
	public class ARDItem {
		public int Id { get; set; }
		public string Value { get; set; }
		public double? ValueNumeric { get; set; }
		public string ScalingFactor { get; set; }
		[JsonProperty("offset")]
		public string Offset { get; set; }
		[JsonProperty("rootid")]
		public int? RootId { get; set; }
		[JsonProperty("DocumentId")]
		public Guid? DocumentID { get; set; }
		public string Label { get; set; }


	}
}
