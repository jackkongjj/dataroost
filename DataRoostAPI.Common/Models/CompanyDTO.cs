using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using Newtonsoft.Json;

namespace DataRoostAPI.Common.Models {

	public class CompanyDTO {

		[JsonProperty("_id")]
		public string Id { get; set; }

		[JsonProperty("rootPPI")]
		public string RootPPI { get; set; }

		[JsonProperty("entityPermId")]
		public string EntitiyPermId { get; set; }

		[JsonProperty("iconum")]
		public int Iconum { get; set; }

		[JsonProperty("name")]
		public string Name { get; set; }

		[JsonProperty("companyTyype")]
		public string CompanyType { get; set; }

		[JsonProperty("countryId")]
		public string CountryId { get; set; }

		[JsonProperty("country")]
		public CountryDTO Country { get; set; }

		[JsonProperty("primaryShareClassId")]
		public string PrimaryShareClassId { get; set; }

		[JsonProperty("collectionEffort")]
		public EffortDTO CollectionEffort { get; set; }

		[JsonProperty("absolutePriority")]
		public decimal? AbsolutePriority { get; set; }

		[JsonProperty("companyPriority")]
		public int? Priority { get; set; }

		[JsonProperty("shareClasses")]
		public IEnumerable<ShareClassDTO> ShareClasses { get; set; }

	}

}