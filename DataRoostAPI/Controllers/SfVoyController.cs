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
using CCS.Fundamentals.DataRoostAPI.CommLogger;

namespace CCS.Fundamentals.DataRoostAPI.Controllers {
    [CommunicationLogger]
    [RoutePrefix("api/v1/companies/{CompanyId}/efforts/sfvoy_join/statementType/{statementType}")]
	public class SfVoyController : ApiController {
		[Route("datatypes/")]
		[HttpGet]
		public string[] GetDataTypes(string CompanyId) {
			return new string[] { StandardizationType.SDB.ToString(), StandardizationType.STD.ToString() };
		}

		[Route("datatypes/{datatype}/templates/")]
		[HttpGet]
		public TemplateDTO[] QueryTemplates(string CompanyId, string statementType, StandardizationType DataType) {
			return GetTemplates(CompanyId,statementType, null, DataType);
		}

		[Route("datatypes/{datatype}/templates/{TemplateId}")]
		[HttpGet]
		public TemplateDTO[] GetTemplates(string CompanyId, string statementType, StandardizationType DataType, string TemplateId) {
			return GetTemplates(CompanyId,statementType, TemplateId, DataType);
		}

		[Route("datatypes/{datatype}/templates/{TemplateId}/timeseries/")]
		[HttpGet]
		public SfVoyTimeSeries[] QueryTemplatesTimeseries(string CompanyId, string statementType, StandardizationType DataType, string TemplateId) {
			return GetTimeSeries(CompanyId,statementType, TemplateId, DataType);
		}

		[Route("datatypes/{datatype}/templates/{TemplateId}/timeseries/{TimeseriesId}")]
		[HttpGet]
		public SfVoyTimeSeries[] GetSTDTemplatesTimeseries(string CompanyId, string statementType, StandardizationType DataType, string TemplateId, string TimeseriesId) {
			return GetTimeSeries(CompanyId,statementType, TemplateId, DataType, timeSeriesId: TimeseriesId);
		}

		private TemplateDTO[] GetTemplates(string companyId, string statementType, string templateId, StandardizationType dataTypes) {
			string connString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ConnectionString;
			int iconum = 0;
			if (!int.TryParse(companyId, out iconum))
				iconum = PermId.PermId2Iconum(companyId);

			TemplatesHelper tsh = new TemplatesHelper(connString, iconum, dataTypes,statementType);
			return tsh.GetTemplates(templateId, true);
		}

		private SfVoyTimeSeries[]GetTimeSeries(string companyId, string statementType, string templateId, StandardizationType dataType, string timeSeriesId = null) {
			string sfConnString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ConnectionString;			
			var qs = HttpUtility.ParseQueryString(HttpContext.Current.Request.QueryString.ToString());

			int iconum = 0;
			if (!int.TryParse(companyId, out iconum))
				iconum = PermId.PermId2Iconum(companyId);

			TemplateIdentifier templId = TemplateIdentifier.GetTemplateIdentifier(templateId);
			sfVoy.TimeseriesIdentifier tsId = null;
			if (timeSeriesId != null)
				tsId = new sfVoy.TimeseriesIdentifier(timeSeriesId);

			//TimeseriesHelper tsh = new TimeseriesHelper(sfConnString);
			sfVoy.TimeseriesHelper tsh = new sfVoy.TimeseriesHelper(sfConnString);
			SfVoyTimeSeries[] result = tsh.QueryTimeseries(iconum, templId, tsId, dataType,statementType ,qs);
			return result; // tsh.QuerySDBTimeseries(iconum, templId, tsId, dataType, qs);
		}
	}
}