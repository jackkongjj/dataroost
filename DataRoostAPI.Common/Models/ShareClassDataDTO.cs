using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using Newtonsoft.Json;

namespace DataRoostAPI.Common.Models {

	public class ShareClassDataDTO : ShareClassDTO {

		public ShareClassDataDTO(ShareClassDTO shareClass, List<ShareClassDataItem> shareClassData) {
			base.Id = shareClass.Id;
			base.PermId = shareClass.PermId;
			base.PPI = shareClass.PPI;
			base.Cusip = shareClass.Cusip;
			base.Sedol = shareClass.Sedol;
			base.Isin = shareClass.Isin;
			base.Name = shareClass.Name;
			base.ListedOn = shareClass.ListedOn;
			base.TickerSymbol = shareClass.TickerSymbol;
			base.AssetClass = shareClass.AssetClass;
			base.InceptionDate = shareClass.InceptionDate;
			base.TermDate = shareClass.TermDate;
			base.IssueType = shareClass.IssueType;
			ShareClassData = shareClassData;
		}

		public List<ShareClassDataItem> ShareClassData { get; set; }
	}

	public class ShareClassDataItem {

		public DateTime ReportDate { get; set; }

		public string ItemId { get; set; }

		public string Name { get; set; }
	}

	public class ShareClassDateItem : ShareClassDataItem {

		public DateTime Value { get; set; }
	}

	public class ShareClassNumericItem : ShareClassDataItem {

		public decimal Value { get; set; }
	}
}