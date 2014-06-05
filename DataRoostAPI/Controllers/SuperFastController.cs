using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;
using CCS.Fundamentals.DataRoostAPI.Access;
using CCS.Fundamentals.DataRoostAPI.Access.SuperFast;
using CCS.Fundamentals.DataRoostAPI.Models;

namespace CCS.Fundamentals.DataRoostAPI.Controllers {

	[RoutePrefix("api/v1/companies/{CompanyId}/efforts/superfast")]
	public class SuperfastController : ApiController {

		[Route("datatypes/sdb/templates/")]
		[HttpGet]
		public TemplateDTO[] QuerySDBTemplates(string CompanyId) {
			throw new NotImplementedException();
		}

		[Route("datatypes/sdb/templates/{TemplateId}")]
		[HttpGet]
		public TemplateDTO[] GetSDBTemplates(string CompanyId, string TemplateId) {
			throw new NotImplementedException();
		}

		[Route("datatypes/sdb/templates/{TemplateId}/timeseries/")]
		[HttpGet]
		public TimeseriesDTO[] QuerySDBTemplatesTimeseries(string CompanyId, string TemplateId) {
			string connString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ConnectionString;

			int iconum = PermId.PermId2Iconum(CompanyId);

			TimeseriesHelper tsh = new TimeseriesHelper(connString);
			return tsh.QuerySDBTimeseries(iconum, TemplateId);
		}

		[Route("datatypes/sdb/templates/{TemplateId}/timeseries/{TimeseriesId}")]
		[HttpGet]
		public TimeseriesDTO[] GetSDBTemplatesTimeseries(string CompanyId, string TemplateId, string TimeseriesId) {
			string connString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ConnectionString;

			int iconum = PermId.PermId2Iconum(CompanyId);

			TimeseriesHelper tsh = new TimeseriesHelper(connString);
			return tsh.GetSDBTemplatesTimeseries(iconum, TemplateId, TimeseriesId);
		}

		[Route("datatypes/std/templates/")]
		[HttpGet]
		public TemplateDTO[] QuerySTDTemplates(string CompanyId) {
			throw new NotImplementedException();
		}

		[Route("datatypes/std/templates/{TemplateId}")]
		[HttpGet]
		public TemplateDTO[] GetSTDTemplates(string CompanyId, string TemplateId) {
			throw new NotImplementedException();
		}

		[Route("datatypes/std/templates/{TemplateId}/timeseries/")]
		[HttpGet]
		public TimeseriesDTO[] QuerySTDTemplatesTimeseries(string CompanyId, string TemplateId) {
			throw new NotImplementedException();
		}

		[Route("datatypes/std/templates/{TemplateId}/timeseries/{TimeseriesId}")]
		[HttpGet]
		public TimeseriesDTO[] GetSTDTemplatesTimeseries(string CompanyId, string TemplateId, string TimeseriesId) {
			throw new NotImplementedException();
		}

	}
}
