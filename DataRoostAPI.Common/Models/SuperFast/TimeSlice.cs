using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataRoostAPI.Common.Models.SuperFast
{
    public class TimeSlice
    {
        public Guid DamDocumentId { get; set; }
        public DateTime TimeSliceDate { get; set; }
        public int PeriodLength { get; set; }
        public string PeriodTypeId { get; set; }
        public Decimal CompanyFiscalYear { get; set; }
        public string ReportTypeId { get; set; }
        public string InterimTypeId { get; set; }
        public string ConsolidatedTypeId { get; set; }
        public string CurrencyCode { get; set; }
        public string ScalingFactorId { get; set; }
        public string AccountTypeId { get; set; }
        public bool SDBValidatedFlag { get; set; }
        public bool STDValidatedFlag { get; set; }
        public string GaapTypeID { get; set; }
        public string UpdateTypeID { get; set; }
        public bool EncoreFlag { get; set; }
        public string Auto_InterimType { get; set; }
        public int AutoCalcFlag { get; set; }
        public int AuditorsOpinionID { get; set; }
        public int FormatCodeCashflowID { get; set; }
        public int LongTermInvestmentID { get; set; }
        public bool IsProspectus { get; set; }
        public bool isQX { get; set; }
        public DateTime CreationStampUtc { get; set; }
        public string CreatedByUser { get; set; }
        public DateTime LastEditedUtc { get; set; }
        public DateTime LastStdMappedUtc { get; set; }
        public string AnalystSignOffInitials { get; set; }
        public DateTime AnalystSignOffDate { get; set; }
        public string ReviewerSignOffInitials { get; set; }
        public DateTime ReviewerSignOffDate { get; set; }
        public bool IsDCV { get; set; }
        public string CollectionTypeId { get; set; }
        public int IndustryCountryAssociationID { get; set; }
        public bool IsExport { get; set; }
        public bool HIndicator { get; set; }
        public bool IsVoy { get; set; }
        public bool IsFYC { get; set; }
        public int PresentationTypeId { get; set; }
        public Guid Id { get; set; }
        public int DocSeriesId { get; set; }
    }
}
