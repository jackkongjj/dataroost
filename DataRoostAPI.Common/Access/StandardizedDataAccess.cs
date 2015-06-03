using System;
using System.Linq;
using System.Net;
using System.Text;

using DataRoostAPI.Common.Interfaces;
using DataRoostAPI.Common.Models;

using Fundamentals.Helpers.DataAccess;

namespace DataRoostAPI.Common.Access {

	internal class StandardizedDataAccess : ApiHelper, IStandardizedDataAccess {

		private const string ROOT_URL_FORMAT = "{0}/api/v1/companies/{1}/efforts/{2}";
		private readonly string _dataRoostConnectionString;
		private readonly string _effort;

		public StandardizedDataAccess(string dataRoostConnectionString, string effort) {
			_dataRoostConnectionString = dataRoostConnectionString;
			_effort = effort;
		}

		public StandardizationType[] GetDataTypes(string companyId) {
			string requestUrl = string.Format("{0}/datatypes", GetRootUrl(companyId));
			return ExecuteGetQuery<StandardizationType[]>(requestUrl);
		}

		public TemplateDTO[] GetTemplateList(string companyId, StandardizationType standardizationType) {
			string requestUrl = string.Format("{0}/datatypes/{1}/templates", GetRootUrl(companyId), standardizationType);
			return ExecuteGetQuery<TemplateDTO[]>(requestUrl);
		}

		public TemplateDTO GetTemplate(string companyId, StandardizationType standardizationType, string templateId) {
			string requestUrl = string.Format("{0}/datatypes/{1}/templates/{2}", GetRootUrl(companyId), standardizationType, templateId);
			TemplateDTO[] templateDtos = ExecuteGetQuery<TemplateDTO[]>(requestUrl);
			return templateDtos.FirstOrDefault();
		}

		public TimeseriesDTO[] GetTimeseriesList(string companyId, StandardizationType standardizationType, string templateId) {
			string requestUrl = string.Format("{0}/datatypes/{1}/templates/{2}/timeseries", GetRootUrl(companyId), standardizationType, templateId);
			return ExecuteGetQuery<TimeseriesDTO[]>(requestUrl);
		}

		public TimeseriesDTO GetTimeseries(string companyId,
		                                   StandardizationType standardizationType,
		                                   string templateId,
		                                   string timeseriesId) {
			string requestUrl = string.Format("{0}/datatypes/{1}/templates/{2}/timeseries/{3}", GetRootUrl(companyId), standardizationType, templateId, timeseriesId);
			TimeseriesDTO[] timeseriesDtos = ExecuteGetQuery<TimeseriesDTO[]>(requestUrl);
			return timeseriesDtos.FirstOrDefault();
		}

		protected override WebClient GetDefaultWebClient() {
			WebClient defaultClient = new WebClient();

			defaultClient.Credentials = CredentialCache.DefaultNetworkCredentials;
			defaultClient.Encoding = Encoding.UTF8;

			return defaultClient;
		}

		private string GetRootUrl(string companyId) {
			return string.Format(ROOT_URL_FORMAT, _dataRoostConnectionString, companyId, _effort);
		}
	}

}
