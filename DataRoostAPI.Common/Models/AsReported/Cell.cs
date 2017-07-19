using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using Newtonsoft.Json;

namespace DataRoostAPI.Common.Models.AsReported {

	public class Cell {

		[JsonProperty("_id")]
		public int Id { get; set; }

		[JsonProperty("cftId")]
		public int? CftId { get; set; }

		[JsonProperty("companyFinancialTermDescription")]
		public string CompanyFinancialTermDescription { get; set; }

		[JsonProperty("columnId")]
		public int ColumnId { get; set; }

		[JsonProperty("rowId")]
		public int RowId { get; set; }

		[JsonProperty("value")]
		public string Value { get; set; }

		[JsonProperty("numericValue")]
		public decimal? NumericValue { get; set; }

		[JsonProperty("periodLength")]
		public string PeriodLength { get; set; }

		[JsonProperty("periodType")]
		public string PeriodType { get; set; }

		[JsonProperty("date")]
		public DateTime? Date { get; set; }

		[JsonProperty("offset")]
		public string Offset { get; set; }

		[JsonProperty("scalingFactor")]
		public string ScalingFactor { get; set; }

		[JsonProperty("precision")]
		public string Precision { get; set; }

		[JsonProperty("currency")]
		public string Currency { get; set; }

		[JsonProperty("xbrlTag")]
		public string XbrlTag { get; set; }

		[JsonProperty("label")]
		public string Label { get; set; }

		[JsonProperty("tableName")]
		public string TableName { get; set; }

		[JsonProperty("rowOrder")]
		public int? RowOrder { get; set; }

		[JsonProperty("itemDescription")]
		public string ItemDescription { get; set; }

		[JsonProperty("scarUpdated")]
		public bool SCARUpdated { get; set; }
	}
}