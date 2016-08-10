using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using Newtonsoft.Json;

namespace DataRoostAPI.Common.Models {
	public class EffortDTO {

        public static EffortDTO SuperCore()
        {
            return new EffortDTO() { Name = "SuperCore" };
        }

        public static EffortDTO Voyager()
        {
            return new EffortDTO() { Name = "Voyager" };
        }

		[JsonProperty("name")]
		public string Name { get; set; }
	}
}