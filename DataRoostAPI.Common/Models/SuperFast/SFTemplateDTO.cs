using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using Newtonsoft.Json;

namespace DataRoostAPI.Common.Models.SuperFast {
	public class SFTemplateDTO : TemplateDTO {
		[JsonProperty("templateType")]
		public int TemplateType { get; set; }
	}
}