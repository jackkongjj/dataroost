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

using DataRoostAPI.Common.Models;

namespace CCS.Fundamentals.DataRoostAPI.Controllers {

	[RoutePrefix("api/v1/companies/{CompanyId}/efforts/superfast")]
	public class SuperfastController : ApiController {

		[Route("datatypes/")]
		[HttpGet]
		public string[] GetDataTypes(string CompanyId) {
			return new string[] { StandardizationType.SDB.ToString(), StandardizationType.STD.ToString() };
		}

		[Route("datatypes/sdb/templates/")]
		[HttpGet]
		public TemplateDTO[] QuerySDBTemplates(string CompanyId) {
			return GetTemplates(CompanyId, null, StandardizationType.SDB);
		}

		[Route("datatypes/sdb/templates/{TemplateId}")]
		[HttpGet]
		public TemplateDTO[] GetSDBTemplates(string CompanyId, string TemplateId) {
			return GetTemplates(CompanyId, TemplateId, StandardizationType.SDB);
		}

		[Route("datatypes/sdb/templates/{TemplateId}/timeseries/")]
		[HttpGet]
		public TimeseriesDTO[] QuerySDBTemplatesTimeseries(string CompanyId, string TemplateId) {
			return GetTimeSeries(CompanyId, TemplateId, null, StandardizationType.STD);
		}

		[Route("datatypes/sdb/templates/{TemplateId}/timeseries/{TimeseriesId}")]
		[HttpGet]
		public TimeseriesDTO[] GetSDBTemplatesTimeseries(string CompanyId, string TemplateId, string TimeseriesId) {
			return GetTimeSeries(CompanyId, TemplateId, TimeseriesId, StandardizationType.SDB);
		}

		[Route("datatypes/std/templates/")]
		[HttpGet]
		public TemplateDTO[] QuerySTDTemplates(string CompanyId) {
			return GetTemplates(CompanyId, null, StandardizationType.STD);
		}

		[Route("datatypes/std/templates/{TemplateId}")]
		[HttpGet]
		public TemplateDTO[] GetSTDTemplates(string CompanyId, string TemplateId) {
			return GetTemplates(CompanyId, TemplateId, StandardizationType.STD);
		}

		[Route("datatypes/std/templates/{TemplateId}/timeseries/")]
		[HttpGet]
		public TimeseriesDTO[] QuerySTDTemplatesTimeseries(string CompanyId, string TemplateId) {
			return GetTimeSeries(CompanyId, TemplateId, null, StandardizationType.STD);
		}

		[Route("datatypes/std/templates/{TemplateId}/timeseries/{TimeseriesId}")]
		[HttpGet]
		public TimeseriesDTO[] GetSTDTemplatesTimeseries(string CompanyId, string TemplateId, string TimeseriesId) {
			return GetTimeSeries(CompanyId, TemplateId, TimeseriesId, StandardizationType.STD);
		}

		[Route("documents/")]
		[HttpGet]
		public Document[] GetDocuments(string CompanyId, int? startYear = null, int? endYear = null, string reportType = null) {
			int iconum = PermId.PermId2Iconum(CompanyId);

			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			DocumentHelper documentHelper = new DocumentHelper(sfConnectionString);

			DateTime startDate = new DateTime(1900, 1, 1);
			if (startYear != null) {
				startDate = new DateTime((int)startYear, 1, 1);
			}
			DateTime endDate = new DateTime(2100, 12, 31);
			if (endYear != null) {
				endDate = new DateTime((int)endYear, 12, 31);
			}

			return documentHelper.GetDocuments(iconum, startDate, endDate, reportType);
		}

		private TemplateDTO[] GetTemplates(string companyId, string templateId, StandardizationType dataTypes) {
			string connString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ConnectionString;
			int iconum = 0;
			if (!int.TryParse(companyId, out iconum))
				iconum = PermId.PermId2Iconum(companyId);

			TemplatesHelper tsh = new TemplatesHelper(connString, iconum, dataTypes);
			return tsh.GetTemplates(templateId);
		}
		
		private TimeseriesDTO[] GetTimeSeries(string companyId, string templateId, string timeSeriesId, StandardizationType dataType) {
			string connString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ConnectionString;
			var qs = HttpUtility.ParseQueryString(HttpContext.Current.Request.QueryString.ToString());

			int iconum = 0;
			if (!int.TryParse(companyId, out iconum))
				iconum = PermId.PermId2Iconum(companyId);

			TemplateIdentifier templId = TemplateIdentifier.GetTemplateIdentifier(templateId);
			TimeseriesIdentifier tsId = null;
			if (timeSeriesId != null)
				tsId = new TimeseriesIdentifier(timeSeriesId);

			TimeseriesHelper tsh = new TimeseriesHelper(connString);
			return tsh.QuerySDBTimeseries(iconum, templId, tsId, dataType, qs);
		}
	}
}
