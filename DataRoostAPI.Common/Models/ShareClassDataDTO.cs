using System;
using System.Collections.Generic;

using FFDotNetHelpers.Helpers.Serialization;

using Newtonsoft.Json;

namespace DataRoostAPI.Common.Models {

	public class ShareClassDataDTO : ShareClassDTO {

		public ShareClassDataDTO() {
		}

		public ShareClassDataDTO(ShareClassDTO shareClass, List<ShareClassDataItem> shareClassData) {
			Id = shareClass.Id;
			PermId = shareClass.PermId;
			PPI = shareClass.PPI;
			Cusip = shareClass.Cusip;
			Sedol = shareClass.Sedol;
			Isin = shareClass.Isin;
			Name = shareClass.Name;
			ListedOn = shareClass.ListedOn;
			TickerSymbol = shareClass.TickerSymbol;
			AssetClass = shareClass.AssetClass;
			InceptionDate = shareClass.InceptionDate;
			TermDate = shareClass.TermDate;
			IssueType = shareClass.IssueType;
			ShareClassData = shareClassData;
		}

		public List<ShareClassDataItem> ShareClassData { get; set; }

	}

	[JsonConverter(typeof(JsonDerivedTypeConverter))]
	public class ShareClassDataItem {

		public DateTime ReportDate { get; set; }

		public string ItemId { get; set; }

		public string Name { get; set; }

		[JsonProperty("_t")]
		public string Type {
			get { return GetType().ToString(); }
			set { }
		}

	}

	public class ShareClassDateItem : ShareClassDataItem {

		public DateTime Value { get; set; }

	}

	public class ShareClassNumericItem : ShareClassDataItem {

		public decimal Value { get; set; }

	}

}
