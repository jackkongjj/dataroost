using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using Newtonsoft.Json;

namespace DataRoostAPI.Common.Models {
	public class ShareClassDTO {
		[JsonProperty(PropertyName = "_id")]
		public string Id { get; set; }

		[JsonProperty(PropertyName = "permId")]
		public string PermId { get; set; }

		[JsonProperty(PropertyName = "ppi")]
		public string PPI { get; set; }

		[JsonProperty(PropertyName = "cusip")]
		public string Cusip { get; set; }

		[JsonProperty(PropertyName = "sedol")]
		public string Sedol { get; set; }

		[JsonProperty(PropertyName = "isin")]
		public string Isin { get; set; }

		[JsonProperty(PropertyName = "name")]
		public string Name { get; set; }

		[JsonProperty(PropertyName = "listedOn")]
		public string ListedOn { get; set; }

		[JsonProperty(PropertyName = "tickerSymbol")]
		public string TickerSymbol { get; set; }

		[JsonProperty(PropertyName = "assetClass")]
		public string AssetClass { get; set; }

		[JsonProperty(PropertyName = "inceptionDate")]
		public DateTime InceptionDate { get; set; }

		[JsonProperty(PropertyName = "termDate")]
		public DateTime? TermDate { get; set; }

		[JsonProperty(PropertyName = "issueType")]
		public string IssueType { get; set; }
	}
}