using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using Newtonsoft.Json;


namespace DataRoostAPI.Common.Models.AsReported
{

    public class AsReportedDocumentEntity : Document
    {

        [JsonProperty("tables")]
        public AsReportedTable[] Tables { get; set; }

        [JsonProperty("cells")]
        public List<Cell> Cells { get; set; }

    }

}