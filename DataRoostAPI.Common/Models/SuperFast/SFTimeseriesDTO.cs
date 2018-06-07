using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using Newtonsoft.Json;

namespace DataRoostAPI.Common.Models.SuperFast {
	public class SFTimeseriesDTO : TimeseriesDTO {

		[JsonProperty("sfDocumentId")]
		public Guid? SFDocumentId { get; set; }
        public string CollectionTypeId { get; set; }

    }
}