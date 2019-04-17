using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DataRoostAPI.Common.Interfaces;
using Newtonsoft.Json;
using System.Data.Entity;
namespace DataRoostAPI.Common.Models.AsReported
{
    public class AsReportedTemplateEntity : DbContext
    {
        public List<StaticHierarchyEntity> StaticHierarchies { get; set; }
        public List<TimeSliceEntity> TimeSlices { get; set; }
        [JsonIgnore]
        public string Message { get; set; }
        public string GetTemplateType()
        {
            return "AsReportedTemplate";
        }
    }
}
