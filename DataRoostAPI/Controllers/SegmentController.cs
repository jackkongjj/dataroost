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
using CCS.Fundamentals.DataRoostAPI.Access.Segment;
using CCS.Fundamentals.DataRoostAPI.CommLogger;

using DataRoostAPI.Common.Models;
using DataRoostAPI.Common.Models.Segment;

namespace CCS.Fundamentals.DataRoostAPI.Controllers {
    [CommunicationLogger]
    [RoutePrefix("api/v1/companies/{CompanyId}/efforts/segments")]
	public class SegmentController : ApiController {
		[Route("datatypes/")]
		[HttpGet]
		public string[] GetDataTypes(string CompanyId) {
			return new string[] { StandardizationType.STD.ToString() };
		}

		[Route("exportedVersions/")]
		[HttpGet]
		public ExportedVersionInfo[] GetVersionInfo(string CompanyId) {
			string segconn = ConfigurationManager.ConnectionStrings["SFAR-Diff"].ConnectionString;
			Access.Segment.ExportVersionHelper helper = new Access.Segment.ExportVersionHelper(segconn);

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
		public Dictionary<int, Dictionary<int, SegmentsTimeSeriesDTO>> QuerySTDTemplatesTimeseries(string CompanyId, string VersionId) {
			string segConnectionString = ConfigurationManager.ConnectionStrings["SFAR-Diff"].ConnectionString;
			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDoc-PantheonReadOnly"].ConnectionString;
			string voyConnectionString = ConfigurationManager.ConnectionStrings["Voyager"].ConnectionString;
			string lionConnectionString = ConfigurationManager.ConnectionStrings["Lion"].ConnectionString;
			string damConnectionString = ConfigurationManager.ConnectionStrings["FFDAM"].ConnectionString;

			int iconum = PermId.PermId2Iconum(CompanyId);
			CompanyHelper cc = new CompanyHelper(sfConnectionString, voyConnectionString, lionConnectionString, damConnectionString);
			TimeseriesHelper tsh = new TimeseriesHelper(segConnectionString);

			return tsh.GetExportedTimeSeries(VersionId);
		}

		[Route("exportedVersions/{versionId}/timeseries/{timeseriesId}")]
		[HttpGet]
		public Dictionary<string, object> GetSTDTemplatesTimeseries(string CompanyId, string VersionId, string TimeseriesId) {
			string segConnectionString = ConfigurationManager.ConnectionStrings["SFAR-Diff"].ConnectionString;
			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDoc-PantheonReadOnly"].ConnectionString;
			string voyConnectionString = ConfigurationManager.ConnectionStrings["Voyager"].ConnectionString;
			string lionConnectionString = ConfigurationManager.ConnectionStrings["Lion"].ConnectionString;
			string damConnectionString = ConfigurationManager.ConnectionStrings["FFDAM"].ConnectionString;

			int iconum = PermId.PermId2Iconum(CompanyId);
			CompanyHelper cc = new CompanyHelper(sfConnectionString, voyConnectionString, lionConnectionString, damConnectionString);
			TimeseriesHelper tsh = new TimeseriesHelper(segConnectionString);

			return tsh.GetTimeseriesSTDValues(TimeseriesId, VersionId, cc.GetSecPermId(iconum));
		}

		[Route("exportedVersions/{timeSeriesId}/document/")]
		[HttpGet]
		public List<Guid> GetDocumentId(string CompanyId, string timeSeriesId) {
			string segConnectionString = ConfigurationManager.ConnectionStrings["SFAR-Diff"].ConnectionString;
			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDoc-PantheonReadOnly"].ConnectionString;
			string voyConnectionString = ConfigurationManager.ConnectionStrings["Voyager"].ConnectionString;
			string lionConnectionString = ConfigurationManager.ConnectionStrings["Lion"].ConnectionString;
			string damConnectionString = ConfigurationManager.ConnectionStrings["FFDAM"].ConnectionString;

			int iconum = PermId.PermId2Iconum(CompanyId);
			CompanyHelper cc = new CompanyHelper(sfConnectionString, voyConnectionString, lionConnectionString, damConnectionString);
			TimeseriesHelper tsh = new TimeseriesHelper(segConnectionString);

			return tsh.GetDocumentId(iconum, timeSeriesId);
		}


	}
}