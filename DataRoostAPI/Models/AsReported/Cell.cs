using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using Newtonsoft.Json;

namespace CCS.Fundamentals.DataRoostAPI.Models.AsReported {

	public class Cell {

		[JsonProperty("_id")]
		public string Id { get; set; }

		[JsonProperty("cftId")]
		public string CftId { get; set; }

		[JsonProperty("columnId")]
		public string ColumnId { get; set; }

		[JsonProperty("rowId")]
		public string RowId { get; set; }

		[JsonProperty("value")]
		public string Value { get; set; }

		[JsonProperty("numericValue")]
		public decimal NumericValue { get; set; }

		[JsonProperty("periodLength")]
		public int PeriodLength { get; set; }

		[JsonProperty("periodType")]
		public string PeriodType { get; set; }

		[JsonProperty("date")]
		public DateTime Date { get; set; }

		[JsonProperty("offset")]
		public string Offset { get; set; }

		[JsonProperty("scalingFactor")]
		public int ScalingFactor { get; set; }

		[JsonProperty("precision")]
		public int Precision { get; set; }

		[JsonProperty("currency")]
		public string Currency { get; set; }
	}
}