using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using Newtonsoft.Json;

namespace DataRoostAPI.Common.Models.Voyager {
	public class VoyagerTemplateDTO : TemplateDTO {
		[JsonProperty("templateCode")]
		public string TemplateCode { get; set; }
	}
}