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

				public static EffortDTO SfVoy_Join() {
						return new EffortDTO() { Name = "sfvoy_join" };
				}

				public static EffortDTO Kpi() {
					return new EffortDTO() { Name = "Kpi" };
				}

				public static EffortDTO Segments() {
					return new EffortDTO() { Name = "Segments" };
				}

		[JsonProperty("name")]
		public string Name { get; set; }
	}
}