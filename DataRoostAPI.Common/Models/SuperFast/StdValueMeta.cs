using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataRoostAPI.Common.Models.SuperFast
{
    public class StdValueMeta
    {
        public string itemcode { get; set; }
        public int docSeriesId { get; set; }
        public string itemdescription { get; set; }
        public bool pitflag { get; set; }
        public string Value { get; set; }
        public int CellId { get; set; }
        public string SecurityId { get; set; }
        public string NAME { get; set; }
        public string ScalingFactor { get; set; }
        public int itemsequence { get; set; }
        public string itemusagetypeid { get; set; }
        public string statementtypeid { get; set; }
        public int itemid { get; set; }
        public int Indent { get; set; }
        public int ModelMasterId { get; set; }
        public Guid damdocumentid { get; set; }
        public char itemtypeid { get; set; }
        public char viewid { get; set; }
        public Guid TimeSliceId { get; set; }
        public string Source { get; set; }
        public DateTime documentdate { get; set; }
    }
}
