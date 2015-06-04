using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;

using DataRoostAPI.Common.Interfaces;
using DataRoostAPI.Common.Models;

using Fundamentals.Helpers.DataAccess;

namespace DataRoostAPI.Common.Access {

	internal class CompanyDataAccess : ApiHelper, ICompanyDataAccess {

		private const string ROOT_URL_FORMAT = "{0}/api/v1/companies/{1}";
		private readonly string _dataRoostConnectionString;

		public CompanyDataAccess(string dataRoostConnectionString) {
			_dataRoostConnectionString = dataRoostConnectionString;
		}

		public CompanyDTO GetCompany(string companyId) {
			string requestUrl = GetRootUrl(companyId);
			CompanyDTO[] companyDtos = ExecuteGetQuery<CompanyDTO[]>(requestUrl);
			return companyDtos.FirstOrDefault();
		}

		public EffortDTO GetCompanyCollectionEffort(string companyId) {
			string requestUrl = string.Format("{0}/collectioneffort", GetRootUrl(companyId));
			return ExecuteGetQuery<EffortDTO>(requestUrl);
		}

		public EffortDTO[] GetEfforts(string companyId) {
			string requestUrl = string.Format("{0}/efforts", GetRootUrl(companyId));
			return ExecuteGetQuery<EffortDTO[]>(requestUrl);
		}

		public ShareClassDataDTO[] GetLatestFiscalPeriodEndSharesData(string companyId, DateTime? reportDate = null) {
			string requestUrl = string.Format("{0}/shares/latestFiscalPeriodEnd", GetRootUrl(companyId));
			return ExecuteGetQuery<ShareClassDataDTO[]>(requestUrl);
		}

		public ShareClassDataDTO[] GetCurrentShareData(string companyId) {
			string requestUrl = string.Format("{0}/shares/currentShares", GetRootUrl(companyId));
			return ExecuteGetQuery<ShareClassDataDTO[]>(requestUrl);
		}

		protected override WebClient GetDefaultWebClient() {
			WebClient defaultClient = new WebClient();

			defaultClient.Credentials = CredentialCache.DefaultNetworkCredentials;
			defaultClient.Encoding = Encoding.UTF8;

			return defaultClient;
		}

		private string GetRootUrl(string companyId) {
			return string.Format(ROOT_URL_FORMAT, _dataRoostConnectionString, companyId);
		}

	}

}
