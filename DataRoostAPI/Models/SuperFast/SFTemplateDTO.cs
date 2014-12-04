using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using Newtonsoft.Json;

namespace CCS.Fundamentals.DataRoostAPI.Models.SuperFast {
	public class SFTemplateDTO : TemplateDTO {
		[JsonProperty(PropertyName = "templateType")]
		public int TemplateType { get; set; }
	}
}