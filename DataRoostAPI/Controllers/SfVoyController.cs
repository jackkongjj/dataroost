using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web;
using System.Web.Http;
using CCS.Fundamentals.DataRoostAPI.Access;
using CCS.Fundamentals.DataRoostAPI.Access.SuperFast;
using sfVoy = CCS.Fundamentals.DataRoostAPI.Access.SfVoy;
using DataRoostAPI.Common.Models;
using DataRoostAPI.Common.Models.SfVoy;

namespace CCS.Fundamentals.DataRoostAPI.Controllers {
	[RoutePrefix("api/v1/companies/{CompanyId}/efforts/sfvoy_join")]
	public class SfVoyController : ApiController {
		[Route("datatypes/")]
		[HttpGet]
		public string[] GetDataTypes(string CompanyId) {
			return new string[] { StandardizationType.SDB.ToString(), StandardizationType.STD.ToString() };
		}

		[Route("datatypes/{datatype}/templates/")]
		[HttpGet]
		public TemplateDTO[] QueryTemplates(string CompanyId, StandardizationType DataType) {
			return GetTemplates(CompanyId, null, DataType);
		}

		[Route("datatypes/{datatype}/templates/{TemplateId}")]
		[HttpGet]
		public TemplateDTO[] GetTemplates(string CompanyId, StandardizationType DataType, string TemplateId) {
			return GetTemplates(CompanyId, TemplateId, DataType);
		}

		[Route("datatypes/{datatype}/templates/{TemplateId}/timeseries/")]
		[HttpGet]
		public SfVoyTimeSeries[] QueryTemplatesTimeseries(string CompanyId, StandardizationType DataType, string TemplateId) {
			return GetTimeSeries(CompanyId, TemplateId, DataType);
		}

		[Route("datatypes/{datatype}/templates/{TemplateId}/timeseries/{TimeseriesId}")]
		[HttpGet]
		public SfVoyTimeSeries[] GetSTDTemplatesTimeseries(string CompanyId, StandardizationType DataType, string TemplateId, string TimeseriesId) {
			return GetTimeSeries(CompanyId, TemplateId, DataType, timeSeriesId: TimeseriesId);
		}

		private TemplateDTO[] GetTemplates(string companyId, string templateId, StandardizationType dataTypes) {
			string connString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ConnectionString;
			int iconum = 0;
			if (!int.TryParse(companyId, out iconum))
				iconum = PermId.PermId2Iconum(companyId);

			TemplatesHelper tsh = new TemplatesHelper(connString, iconum, dataTypes);
			return tsh.GetTemplates(templateId);
		}

		private SfVoyTimeSeries[]GetTimeSeries(string companyId, string templateId, StandardizationType dataType, string timeSeriesId = null) {
			string sfConnString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ConnectionString;
			string voyConnString = ConfigurationManager.ConnectionStrings["Voyager"].ConnectionString;
			var qs = HttpUtility.ParseQueryString(HttpContext.Current.Request.QueryString.ToString());

			int iconum = 0;
			if (!int.TryParse(companyId, out iconum))
				iconum = PermId.PermId2Iconum(companyId);

			TemplateIdentifier templId = TemplateIdentifier.GetTemplateIdentifier(templateId);
			sfVoy.TimeseriesIdentifier tsId = null;
			if (timeSeriesId != null)
				tsId = new sfVoy.TimeseriesIdentifier(timeSeriesId);

			//TimeseriesHelper tsh = new TimeseriesHelper(sfConnString);
			sfVoy.TimeseriesHelper tsh = new sfVoy.TimeseriesHelper(sfConnString, voyConnString);
			SfVoyTimeSeries[] result = tsh.QueryTimeseries(iconum, templId, tsId, dataType, qs);
			return result; // tsh.QuerySDBTimeseries(iconum, templId, tsId, dataType, qs);
		}
	}
}