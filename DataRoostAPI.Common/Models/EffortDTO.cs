﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using Newtonsoft.Json;

namespace DataRoostAPI.Common.Models {
	public class EffortDTO {

		[JsonProperty("name")]
		public string Name { get; set; }
	}
}