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

using DataRoostAPI.Common.Models;

namespace CCS.Fundamentals.DataRoostAPI.Controllers {

	[RoutePrefix("api/v1/companies/{CompanyId}/efforts/segment")]
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

			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ConnectionString;
			string voyConnectionString = ConfigurationManager.ConnectionStrings["Voyager"].ConnectionString;
			string lionConnectionString = ConfigurationManager.ConnectionStrings["Lion"].ConnectionString;
			string damConnectionString = ConfigurationManager.ConnectionStrings["FFDAM"].ConnectionString;

			int iconum = PermId.PermId2Iconum(CompanyId);

			CompanyHelper cc = new CompanyHelper(sfConnectionString, voyConnectionString, lionConnectionString, damConnectionString);
			return helper.GetExportedVersion(cc.GetSecPermId(iconum));
		}

		[Route("exportedVersions/{versionId}/timeseries/")]
		[HttpGet]
		public Dictionary<int, Dictionary<int, TimeseriesDTO>> QuerySTDTemplatesTimeseries(string CompanyId, string VersionId) {
			return GetTimeSeries(CompanyId, VersionId, null, StandardizationType.STD);
		}

		[Route("exportedVersions/{versionId}/timeseries/{timeseriesId}")]
		[HttpGet]
		public Dictionary<int, Dictionary<int, TimeseriesDTO>> GetSTDTemplatesTimeseries(string CompanyId, string VersionId, string TimeseriesId) {
			return GetTimeSeries(CompanyId, VersionId, TimeseriesId, StandardizationType.STD);
		}

		private Dictionary<int, Dictionary<int, TimeseriesDTO>> GetTimeSeries(string companyId, string versionId, string timeSeriesId, StandardizationType dataType) {
			string segConnectionString = ConfigurationManager.ConnectionStrings["SFAR-Diff"].ConnectionString;
			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ConnectionString;
			string voyConnectionString = ConfigurationManager.ConnectionStrings["Voyager"].ConnectionString;
			string lionConnectionString = ConfigurationManager.ConnectionStrings["Lion"].ConnectionString;
			string damConnectionString = ConfigurationManager.ConnectionStrings["FFDAM"].ConnectionString;

			int iconum = PermId.PermId2Iconum(companyId);
			CompanyHelper cc = new CompanyHelper(sfConnectionString, voyConnectionString, lionConnectionString, damConnectionString);
			TimeseriesHelper tsh = new TimeseriesHelper(segConnectionString);

			return tsh.GetExportedTimeSeries(versionId, cc.GetSecPermId(iconum), timeSeriesId);
		}

	}
}