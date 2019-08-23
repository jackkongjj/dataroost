using System;
using System.Collections.Generic;
using System.Linq;
using System.Configuration;
using System.Net;
using System.Net.Http;
using System.Web.Http;
using CCS.Fundamentals.DataRoostAPI.Access;
using CCS.Fundamentals.DataRoostAPI.Access.Voyager;
using CCS.Fundamentals.DataRoostAPI.CommLogger;
using DataRoostAPI.Common.Models;
using DataRoostAPI.Common.Models.Voyager;

namespace CCS.Fundamentals.DataRoostAPI.Controllers {
    [CommunicationLogger]
    [RoutePrefix("api/v1/companies/{CompanyId}/efforts/voyager")]
	public class VoyagerController : ApiController {

		[Route("datatypes/")]
		[HttpGet]
		public string[] GetDataTypes(string CompanyId) {
			return new string[] { StandardizationType.STD.ToString(), StandardizationType.SDB.ToString() };
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
		public VoyagerTimeseriesDTO[] QuerySDBTemplatesTimeseries(string CompanyId, string TemplateId) {
			return GetTimeseries(CompanyId, TemplateId, StandardizationType.SDB);
		}

		[Route("datatypes/sdb/templates/{TemplateId}/timeseries/{TimeseriesId}")]
		[HttpGet]
		public VoyagerTimeseriesDTO[] GetSDBTemplatesTimeseries(string CompanyId, string TemplateId, string TimeseriesId) {
			return GetTimeseries(CompanyId, TemplateId, TimeseriesId, StandardizationType.SDB);
		}

		[Route("datatypes/sdb/templates/{TemplateId}/timeseries/")]
		[HttpGet]
		public VoyagerTimeseriesDTO[] GetSDBTemplatesTimeseries(string CompanyId, string TemplateId, int startYear, int endYear) {
			return GetTimeseries(CompanyId, TemplateId, startYear, endYear, StandardizationType.SDB);
		}

		[Route("datatypes/std/hasPension")]
		[HttpGet]
		public bool HasPensionData(string CompanyId, [FromUri]int DataYear, [FromUri]DateTime ReportDate, [FromUri]string AccountType, [FromUri]string InterimType) {
			string connString = ConfigurationManager.ConnectionStrings["Voyager"].ConnectionString;
			int iconum = 0;
			if (!int.TryParse(CompanyId, out iconum))
				iconum = PermId.PermId2Iconum(CompanyId);


			return TimeseriesHelper.HasPensionData(iconum, ReportDate.ToString("dd-MMM-yy"), DataYear, AccountType, InterimType, connString);
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
		public VoyagerTimeseriesDTO[] QuerySTDTemplatesTimeseries(string CompanyId, string TemplateId) {
			return GetTimeseries(CompanyId, TemplateId, StandardizationType.STD);
		}

		[Route("datatypes/std/templates/{TemplateId}/timeseries/{TimeseriesId}")]
		[HttpGet]
		public VoyagerTimeseriesDTO[] GetSTDTemplatesTimeseries(string CompanyId, string TemplateId, string TimeseriesId) {
			return GetTimeseries(CompanyId, TemplateId, TimeseriesId, StandardizationType.STD);
		}

		[Route("datatypes/std/templates/{TemplateId}/timeseries/")]
		[HttpGet]
		public VoyagerTimeseriesDTO[] GetSTDTemplatesTimeseries(string CompanyId, string TemplateId, int startYear, int endYear) {
			return GetTimeseries(CompanyId, TemplateId, startYear, endYear, StandardizationType.STD);
		}

		private TemplateDTO[] GetTemplates(string companyId, string templateId, StandardizationType dataTypes) {
			string connString = ConfigurationManager.ConnectionStrings["Voyager"].ConnectionString;
			int iconum = 0;
			if (!int.TryParse(companyId, out iconum))
				iconum = PermId.PermId2Iconum(companyId);

			TemplatesHelper tsh = new TemplatesHelper(connString, iconum, dataTypes);
			return tsh.GetTemplates(templateId);
		}

		private VoyagerTimeseriesDTO[] GetTimeseries(string companyId, string templateId, string timeseriesId, StandardizationType dataTypes) {
			string connString = ConfigurationManager.ConnectionStrings["Voyager"].ConnectionString;
			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDoc-PantheonReadOnly"].ConnectionString;
			int iconum = 0;
			if (!int.TryParse(companyId, out iconum))
				iconum = PermId.PermId2Iconum(companyId);

			TemplateIdentifier templId = TemplateIdentifier.GetTemplateIdentifier(templateId);
			TimeseriesIdentifier tsId = null;
			if (timeseriesId != null)
				tsId = new TimeseriesIdentifier(timeseriesId);

			TimeseriesHelper tsh = new TimeseriesHelper(connString, sfConnectionString);
			return StandardizationType.STD == dataTypes ? tsh.QuerySTDTimeseries(iconum, templId, tsId) : tsh.QuerySDBTimeseries(iconum, templId, tsId);
		}

		private VoyagerTimeseriesDTO[] GetTimeseries(string companyId, string templateId, int startYear, int endYear, StandardizationType dataTypes) {
			string connString = ConfigurationManager.ConnectionStrings["Voyager"].ConnectionString;
			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDoc-PantheonReadOnly"].ConnectionString;
			int iconum = 0;
			if (!int.TryParse(companyId, out iconum))
				iconum = PermId.PermId2Iconum(companyId);

			TemplateIdentifier templId = TemplateIdentifier.GetTemplateIdentifier(templateId);
			TimeseriesHelper tsh = new TimeseriesHelper(connString, sfConnectionString);
			return StandardizationType.STD == dataTypes ? tsh.QuerySTDTimeseries(iconum, templId, startYear, endYear) : tsh.QuerySDBTimeseries(iconum, templId, startYear, endYear);
		}

		public VoyagerTimeseriesDTO[] GetTimeseries(string companyId, string templateId, StandardizationType dataTypes) {
			string connString = ConfigurationManager.ConnectionStrings["Voyager"].ConnectionString;
			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDoc-PantheonReadOnly"].ConnectionString;
			int iconum = 0;
			if (!int.TryParse(companyId, out iconum))
				iconum = PermId.PermId2Iconum(companyId);

			TemplateIdentifier templId = TemplateIdentifier.GetTemplateIdentifier(templateId);
			TimeseriesHelper tsh = new TimeseriesHelper(connString, sfConnectionString);
			return StandardizationType.STD == dataTypes ? tsh.QuerySTDTimeseries(iconum, templId) : tsh.QuerySDBTimeseries(iconum, templId);
		}
	}
}