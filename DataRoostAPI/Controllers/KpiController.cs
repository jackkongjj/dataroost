using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web;
using System.Web.Http;
using CCS.Fundamentals.DataRoostAPI.Access;
using CCS.Fundamentals.DataRoostAPI.Access.Company;
using CCS.Fundamentals.DataRoostAPI.Access.Kpi;
using DataRoostAPI.Common.Models;
using DataRoostAPI.Common.Models.KPI;
using DataRoostAPI.Common.Models.TimeseriesValues;
using CCS.Fundamentals.DataRoostAPI.CommLogger;

namespace CCS.Fundamentals.DataRoostAPI.Controllers {
    [CommunicationLogger]
    [RoutePrefix("api/v1/companies/{CompanyId}/efforts/kpi")]
	public class KpiController : ApiController {
		[Route("datatypes/")]
		[HttpGet]
		public string[] GetDataTypes(string CompanyId) {
			return new string[] { StandardizationType.STD.ToString() };
		}

		[Route("exportedVersions/")]
		[HttpGet]
		public ExportedVersionInfo[] GetVersionInfo(string CompanyId) {
			string kpiconn = ConfigurationManager.ConnectionStrings["KPI-Diff"].ConnectionString;
			Access.Kpi.ExportVersionHelper helper = new Access.Kpi.ExportVersionHelper(kpiconn);

			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDoc-PantheonReadOnly"].ConnectionString;
			string voyConnectionString = ConfigurationManager.ConnectionStrings["Voyager"].ConnectionString;
			string lionConnectionString = ConfigurationManager.ConnectionStrings["Lion"].ConnectionString;
			string damConnectionString = ConfigurationManager.ConnectionStrings["FFDAM"].ConnectionString;

			int iconum = PermId.PermId2Iconum(CompanyId);

			CompanyHelper cc = new CompanyHelper(sfConnectionString, voyConnectionString, lionConnectionString, damConnectionString);
			return helper.GetExportedVersion(cc.GetSecPermId(iconum));
		}

		[Route("exportedVersions/{versionId}/timeseries/")]
		[HttpGet]
		public Dictionary<int, Dictionary<Guid, KpiTimeSeriesDTO>> QuerySTDTemplatesTimeseries(string CompanyId, string VersionId) {
			string kpiConnectionString = ConfigurationManager.ConnectionStrings["KPI-Diff"].ConnectionString;
			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDoc-PantheonReadOnly"].ConnectionString;
			string voyConnectionString = ConfigurationManager.ConnectionStrings["Voyager"].ConnectionString;
			string lionConnectionString = ConfigurationManager.ConnectionStrings["Lion"].ConnectionString;
			string damConnectionString = ConfigurationManager.ConnectionStrings["FFDAM"].ConnectionString;

			int iconum = PermId.PermId2Iconum(CompanyId);
			CompanyHelper cc = new CompanyHelper(sfConnectionString, voyConnectionString, lionConnectionString, damConnectionString);
			TimeseriesHelper tsh = new TimeseriesHelper(kpiConnectionString);
			return tsh.GetExportedTimeSeries(VersionId, cc.GetSecPermId(iconum));
		}

		[Route("exportedVersions/{versionId}/timeseries/{timeseriesId}")]
		[HttpGet]
		public Dictionary<string, KPINode> GetSTDTemplatesTimeseries(string CompanyId, string VersionId, string TimeseriesId) {
			string kpiConnectionString = ConfigurationManager.ConnectionStrings["KPI-Diff"].ConnectionString;
			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDoc-PantheonReadOnly"].ConnectionString;
			string voyConnectionString = ConfigurationManager.ConnectionStrings["Voyager"].ConnectionString;
			string lionConnectionString = ConfigurationManager.ConnectionStrings["Lion"].ConnectionString;
			string damConnectionString = ConfigurationManager.ConnectionStrings["FFDAM"].ConnectionString;

			int iconum = PermId.PermId2Iconum(CompanyId);
			CompanyHelper cc = new CompanyHelper(sfConnectionString, voyConnectionString, lionConnectionString, damConnectionString);
			TimeseriesHelper tsh = new TimeseriesHelper(kpiConnectionString);


			return tsh.GetTimeseriesSTDValues(TimeseriesId, VersionId);
		}

		[Route("ARDItems/{itemId}")]
		[HttpGet]
		public List<ARDItem> GetARDItems(string CompanyId, int itemId) {
			string kpiConnectionString = ConfigurationManager.ConnectionStrings["KPI-Diff"].ConnectionString;
			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDoc-PantheonReadOnly"].ConnectionString;
			string voyConnectionString = ConfigurationManager.ConnectionStrings["Voyager"].ConnectionString;
			string lionConnectionString = ConfigurationManager.ConnectionStrings["Lion"].ConnectionString;
			string damConnectionString = ConfigurationManager.ConnectionStrings["FFDAM"].ConnectionString;

			TimeseriesHelper tsh = new TimeseriesHelper(kpiConnectionString);
			return tsh.GetARDItems(itemId);
		}


		[Route("exportedVersions/{timeSeriesId}/document/")]
		[HttpGet]
		public List<Guid> GetDocumentId(string CompanyId, string timeSeriesId) {
			string kpiConnectionString = ConfigurationManager.ConnectionStrings["KPI-Diff"].ConnectionString;
			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDoc-PantheonReadOnly"].ConnectionString;
			string voyConnectionString = ConfigurationManager.ConnectionStrings["Voyager"].ConnectionString;
			string lionConnectionString = ConfigurationManager.ConnectionStrings["Lion"].ConnectionString;
			string damConnectionString = ConfigurationManager.ConnectionStrings["FFDAM"].ConnectionString;

			TimeseriesHelper tsh = new TimeseriesHelper(kpiConnectionString);

			return tsh.GetDocumentId(timeSeriesId);
		}

	}
}
