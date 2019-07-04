using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataRoostAPI.Common.Models.SuperFast
{
    public class ExportMaster
    {
        public IEnumerable<TimeSlice> timeSlices;
        public IEnumerable<StdValueMeta> stdValueMeta;
        public IEnumerable<STDTimeSliceDetail> stdTimeSliceDetail;
        public IEnumerable<StdItem> stdItems;
    }
}
