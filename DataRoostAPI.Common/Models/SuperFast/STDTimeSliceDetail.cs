using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataRoostAPI.Common.Models.SuperFast
{
    public class STDTimeSliceDetail
    {
        public int ID { get; set; }
        public int STDItemID { get; set; }
        public string Value { get; set; }
        public string MathML { get; set; }
        public string Source { get; set; }
        public DateTime LastUpdatedDate { get; set; }
        public string LastUpdatedByUser { get; set; }
        public int StatementModelDetailId { get; set; }
        public string SecurityId { get; set; }
        public string Cusip { get; set; }
        public Guid TimeSliceId { get; set; }
        public int DocSeriesId { get; set; }
        public int ModelMasterId { get; set; }
        public int[] DataYears { get; set; }
    }
}
