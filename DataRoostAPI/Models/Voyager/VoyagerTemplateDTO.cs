using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using Newtonsoft.Json;

namespace CCS.Fundamentals.DataRoostAPI.Models.Voyager {
	public class VoyagerTemplateDTO : TemplateDTO {
		[JsonProperty(PropertyName = "templateCode")]
		public string TemplateCode { get; set; }
	}
}