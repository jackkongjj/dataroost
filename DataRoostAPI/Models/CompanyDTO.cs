using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using Newtonsoft.Json;

namespace CCS.Fundamentals.DataRoostAPI.Models {
	public class CompanyDTO {

		[JsonProperty(PropertyName = "_id")]
		public string Id { get; set; }

		[JsonProperty(PropertyName = "rootPPI")]
		public string RootPPI { get; set; }

		[JsonProperty(PropertyName = "entityPermId")]
		public string EntitiyPermId { get; set; }

		[JsonProperty(PropertyName = "iconum")]
		public int Iconum { get; set; }

		[JsonProperty(PropertyName = "name")]
		public string Name { get; set; }

		[JsonProperty(PropertyName = "companyTyype")]
		public string CompanyType { get; set; }

		[JsonProperty(PropertyName = "countryId")]
		public string CountryId { get; set; }

		[JsonProperty(PropertyName = "country")]
		public CountryDTO Country { get; set; }

		[JsonProperty(PropertyName = "primaryShareClassId")]
		public string PrimaryShareClassId { get; set; }

		[JsonProperty(PropertyName = "shareClasses")]
		public IEnumerable<ShareClassDTO> ShareClasses { get; set; }
	}
}