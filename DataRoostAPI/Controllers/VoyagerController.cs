using System;
using System.Collections.Generic;
using System.Linq;
using System.Configuration;
using System.Net;
using System.Net.Http;
using System.Web.Http;
using CCS.Fundamentals.DataRoostAPI.Access;
using CCS.Fundamentals.DataRoostAPI.Access.Voyager;
using CCS.Fundamentals.DataRoostAPI.Models;
using CCS.Fundamentals.DataRoostAPI.Models.Voyager;

namespace CCS.Fundamentals.DataRoostAPI.Controllers {

	[RoutePrefix("api/v1/companies/{CompanyId}/efforts/voyager")]
	public class VoyagerController : ApiController {

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
		public VoyagerTimeseriesDTO[] QuerySTDTemplatesTimeseries(string CompanyId, string TemplateId) {
			return GetTimeseries(CompanyId, TemplateId);
		}

		[Route("datatypes/std/templates/{TemplateId}/timeseries/{TimeseriesId}")]
		[HttpGet]
		public VoyagerTimeseriesDTO[] GetSTDTemplatesTimeseries(string CompanyId, string TemplateId, string TimeseriesId) {
			return GetTimeseries(CompanyId, TemplateId, TimeseriesId);
		}

		[Route("datatypes/std/templates/{TemplateId}/timeseries/")]
		[HttpGet]
		public VoyagerTimeseriesDTO[] GetSTDTemplatesTimeseries(string CompanyId, string TemplateId, int startYear, int endYear) {
			return GetTimeseries(CompanyId, TemplateId, startYear, endYear);
		}

		private TemplateDTO[] GetTemplates(string companyId, string templateId, StandardizationType dataTypes) {
			string connString = ConfigurationManager.ConnectionStrings["Voyager"].ConnectionString;
			int iconum = 0;
			if (!int.TryParse(companyId, out iconum))
				iconum = PermId.PermId2Iconum(companyId);

			TemplatesHelper tsh = new TemplatesHelper(connString, iconum, dataTypes);
			return tsh.GetTemplates(templateId);
		}

		public VoyagerTimeseriesDTO[] GetTimeseries(string companyId, string templateId, string timeseriesId) {
			string connString = ConfigurationManager.ConnectionStrings["Voyager"].ConnectionString;
			int iconum = 0;
			if (!int.TryParse(companyId, out iconum))
				iconum = PermId.PermId2Iconum(companyId);

			TemplateIdentifier templId = TemplateIdentifier.GetTemplateIdentifier(templateId);
			TimeseriesIdentifier tsId = null;
			if (timeseriesId != null)
				tsId = new TimeseriesIdentifier(timeseriesId);

			TimeseriesHelper tsh = new TimeseriesHelper(connString);
			return tsh.QuerySTDTimeseries(iconum, templId, tsId);
		}

		public VoyagerTimeseriesDTO[] GetTimeseries(string companyId, string templateId, int startYear, int endYear) {
			string connString = ConfigurationManager.ConnectionStrings["Voyager"].ConnectionString;
			int iconum = 0;
			if (!int.TryParse(companyId, out iconum))
				iconum = PermId.PermId2Iconum(companyId);

			TemplateIdentifier templId = TemplateIdentifier.GetTemplateIdentifier(templateId);
			TimeseriesHelper tsh = new TimeseriesHelper(connString);
			return tsh.QuerySTDTimeseries(iconum, templId, startYear, endYear);
		}

		public VoyagerTimeseriesDTO[] GetTimeseries(string companyId, string templateId) {
			string connString = ConfigurationManager.ConnectionStrings["Voyager"].ConnectionString;
			int iconum = 0;
			if (!int.TryParse(companyId, out iconum))
				iconum = PermId.PermId2Iconum(companyId);

			TemplateIdentifier templId = TemplateIdentifier.GetTemplateIdentifier(templateId);
			TimeseriesHelper tsh = new TimeseriesHelper(connString);
			return tsh.QuerySTDTimeseries(iconum, templId);
		}
	}
}