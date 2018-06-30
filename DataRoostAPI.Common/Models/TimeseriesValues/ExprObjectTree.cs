using Newtonsoft.Json;
using System;
using System.Web;

namespace DataRoostAPI.Common.Models.TimeseriesValues
{
    public class ExprObjectTree
    {

        [JsonProperty("expressionId")]
        public string ExpressionId { get; set; }
        [JsonProperty("key")]
        public int Key { get; set; }
        [JsonProperty("timestamp")]
        public string Timestamp { get; set; }
        [JsonProperty("itemDetailId")]
        public int? ItemDetailId { get; set; }



        #region Json Object Properties For Document

        [JsonProperty("iconum")]
        public int Iconum { get; set; }

        [JsonProperty("reportTypeID")]
        public string ReportTypeID { get; set; }

        [JsonProperty("industryDetail")]
        public string IndustryDetail { get; set; }

        [JsonProperty("industryGroup")]
        public string IndustryGroup { get; set; }

        [JsonProperty("isoCountryCode")]
        public string IsoCountryCode { get; set; }

        [JsonProperty("documentId")]
        public Guid? DocumentId { get; set; }

        #endregion

        #region Json Object Properties For Source Link

        [JsonProperty("itemCode")]
        public string ItemCode { get; set; }

        [JsonProperty("itemDescription")]
        public string ItemDescription { get; set; }

        [JsonProperty("securityId")]
        public string SecurityId { get; set; }

        [JsonProperty("name")]
        public string Name { get; set; }

        private string _value;
        [JsonProperty("value")]
        public string Value
        {
            get
            {
                return _value;
            }
            set
            {
                _value = value;
            }
        }

        [JsonProperty("valueNumeric")]
        public string ValueNumeric { get; set; }

        [JsonProperty("offset")]
        public string Offset { get; set; }

        private string _offsetLabelWithHierarchy;
        [JsonProperty("offsetLabelWithHierarchy")]
        public string OffsetLabelWithHierarchy
        {

            get { return _offsetLabelWithHierarchy; }
            set
            {
                if (!string.IsNullOrEmpty(value))
                    _offsetLabelWithHierarchy = HttpUtility.HtmlDecode(value);
                else
                    _offsetLabel = value;
            }
        }

        [JsonProperty("xbrlTag")]
        public string XbrlTag { get; set; }

        private string _offsetLabel;
        [JsonProperty("offsetLabel")]
        public string OffsetLabel
        {
            get
            {
                return _offsetLabel;
            }
            set
            {
                if (!string.IsNullOrEmpty(value))
                    _offsetLabel = HttpUtility.HtmlDecode(value);
                else
                    _offsetLabel = value;
            }
        }

        private string _CompanyFinancialTerm = string.Empty;
        [JsonProperty("companyFinancialTerm")]
        public string CompanyFinancialTerm
        {
            get { return (string.IsNullOrEmpty(_CompanyFinancialTerm)) ? OffsetLabel : _CompanyFinancialTerm; }
            set
            {
                if (!string.IsNullOrEmpty(value))
                    _CompanyFinancialTerm = HttpUtility.HtmlDecode(value);
                else
                    _CompanyFinancialTerm = value;
            }
        }

        [JsonProperty("companyFinancialTermId")]
        public int CompanyFinancialTermId { get; set; }

        [JsonProperty("currencyCode")]
        public string CurrencyCode { get; set; }

        private string _ScalingFactor;
        [JsonProperty("scalingFactor")]
        public string ScalingFactor
        {
            get
            {
                return _ScalingFactor;
            }
            set
            {
                _ScalingFactor = value;
            }
        }

        #endregion Json Object Properties


        [JsonIgnore]
        public string CalculationDisplay
        {
            get
            {
                return ((Name == "mi" || (Name == "mn" && !string.IsNullOrEmpty(ValueNumeric)))) ? ValueNumeric : Value;
            }
        }

        [JsonIgnore]
        public int TableCellId { get; set; }


        [JsonIgnore]
        public string TableCellDisplay
        {
            get
            {
                return (Name == "mi" || (Name == "mn" && !string.IsNullOrEmpty(ValueNumeric))) ? "<mi>" + TableCellId.ToString() + "</mi>" : "<mo>" + Value + "</mo>";
            }
        }

    }
}