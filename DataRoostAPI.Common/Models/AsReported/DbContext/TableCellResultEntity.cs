using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using System.Data.Entity;
namespace DataRoostAPI.Common.Models.AsReported {
	public class TableCellResultEntity : DbContext {
        public List<SCARAPITableCell> cells { get; set; }
	}

}
