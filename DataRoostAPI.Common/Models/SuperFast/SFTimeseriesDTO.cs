using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using Newtonsoft.Json;

namespace DataRoostAPI.Common.Models.SuperFast {
	public class SFTimeseriesDTO : TimeseriesDTO {

		[JsonProperty(PropertyName = "sfDocumentId")]
		public Guid SFDocumentId { get; set; }

	}
}