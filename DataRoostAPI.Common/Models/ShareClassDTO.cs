using System;

using FFDotNetHelpers.Helpers.Serialization;

using Newtonsoft.Json;

namespace DataRoostAPI.Common.Models {

	[JsonConverter(typeof(JsonDerivedTypeConverter))]
	public class ShareClassDTO {

		[JsonProperty("_id")]
		public string Id { get; set; }

		[JsonProperty("permId")]
		public string PermId { get; set; }

		[JsonProperty("ppi")]
		public string PPI { get; set; }

		[JsonProperty("cusip")]
		public string Cusip { get; set; }

		[JsonProperty("sedol")]
		public string Sedol { get; set; }

		[JsonProperty("isin")]
		public string Isin { get; set; }

		[JsonProperty("name")]
		public string Name { get; set; }

		[JsonProperty("listedOn")]
		public string ListedOn { get; set; }

		[JsonProperty("tickerSymbol")]
		public string TickerSymbol { get; set; }

		[JsonProperty("assetClass")]
		public string AssetClass { get; set; }

		[JsonProperty("inceptionDate")]
		public DateTime InceptionDate { get; set; }

		[JsonProperty("termDate")]
		public DateTime? TermDate { get; set; }

		[JsonProperty("issueType")]
		public string IssueType { get; set; }

		[JsonProperty("_t")]
		public string Type {
			get { return GetType().ToString(); }
			set { }
		}

	}

}
