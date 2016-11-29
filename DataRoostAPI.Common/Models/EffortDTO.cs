using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using Newtonsoft.Json;

namespace DataRoostAPI.Common.Models {
	public class EffortDTO {

        public static EffortDTO SuperCore()
        {
            return new EffortDTO() { Name = "Supercore" };
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
		  
		    public static EffortDTO AsReported(){
					return new EffortDTO() {Name = "AsReported"};
				}


		[JsonProperty("name")]
		public string Name { get; set; }
	}
}