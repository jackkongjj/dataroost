using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using Newtonsoft.Json;

namespace VisualStitching.Common.Models {

	public class ClusterError {

		[JsonProperty("id")]
		public int Id { get; set; }

		[JsonProperty("iconum")]
		public int? Iconum { get; set; }

		[JsonProperty("document_id")]
		public string DocumentId { get; set; }

		[JsonProperty("norm_table")]
		public string NormTable { get; set; }

		[JsonProperty("creation_stamp_utc")]
		public DateTime CreationTimeStamp { get; set; }

		[JsonProperty("comments")]
		public string Comments { get; set; }

	}
}