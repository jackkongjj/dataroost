using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;

using DataRoostAPI.Common.Interfaces;
using DataRoostAPI.Common.Models;

using Fundamentals.Helpers.DataAccess;

using Newtonsoft.Json;

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

		public Dictionary<int, EffortDTO> GetCollectionEffortForCompanies(List<string> companyIds) {
			string requestUrl = string.Format("{0}/api/v1/companies/collectioneffort", _dataRoostConnectionString);
			string postParams = JsonConvert.SerializeObject(companyIds);
			using (LongRunningWebClient client = new LongRunningWebClient()) {
				client.Credentials = CredentialCache.DefaultNetworkCredentials;
				client.Encoding = Encoding.UTF8;
				client.Timeout = 1000000;
				client.Headers.Add("Content-Type", "application/json");
				string postResponse = client.UploadString(requestUrl, "POST", postParams);
				return JsonConvert.DeserializeObject<Dictionary<int, EffortDTO>>(postResponse);
			}
		}

		public EffortDTO[] GetEfforts(string companyId) {
			string requestUrl = string.Format("{0}/efforts", GetRootUrl(companyId));
			return ExecuteGetQuery<EffortDTO[]>(requestUrl);
		}

		public decimal? GetCompanyPriority(string companyId) {
			string requestUrl = string.Format("{0}/companypriority", GetRootUrl(companyId));
			return ExecuteGetQuery<decimal?>(requestUrl);
		}

		public Dictionary<int, decimal?> GetCompanyPriority(List<string> companyIds) {
			string requestUrl = string.Format("{0}/api/v1/companies/companypriority", _dataRoostConnectionString);
			string postParams = JsonConvert.SerializeObject(companyIds);
			using (LongRunningWebClient client = new LongRunningWebClient()) {
				client.Credentials = CredentialCache.DefaultNetworkCredentials;
				client.Encoding = Encoding.UTF8;
				client.Timeout = 1000000;
				client.Headers.Add("Content-Type", "application/json");
				string postResponse = client.UploadString(requestUrl, "POST", postParams);
				return JsonConvert.DeserializeObject<Dictionary<int, decimal?>>(postResponse);
			}
		}

		public ShareClassDataDTO[] GetLatestFiscalPeriodEndSharesData(string companyId, DateTime? reportDate = null, DateTime? since = null) {
			string requestUrl = string.Format("{0}/shares/latestFiscalPeriodEnd", GetRootUrl(companyId));
			if (reportDate != null || since != null) {
				requestUrl += "?";
				List<string> queryStrings = new List<string>();
				if (reportDate != null) {
					queryStrings.Add("reportDate=" + reportDate);
				}
				if (since != null) {
					queryStrings.Add("since=" + since);
				}
				requestUrl += string.Join("&", queryStrings);
			}
			return ExecuteGetQuery<ShareClassDataDTO[]>(requestUrl);
		}

		public Dictionary<int, ShareClassDataDTO[]> GetLatestFiscalPeriodEndSharesData(List<string> companyIds, DateTime? reportDate = null, DateTime? since = null) {
			string requestUrl = string.Format("{0}/api/v1/companies/shares/latestFiscalPeriodEnd", _dataRoostConnectionString);
			if (reportDate != null || since != null) {
				requestUrl += "?";
				List<string> queryStrings = new List<string>();
				if (reportDate != null) {
					queryStrings.Add("reportDate=" + reportDate);
				}
				if (since != null) {
					queryStrings.Add("since=" + since);
				}
				requestUrl += string.Join("&", queryStrings);
			}

			string postParams = JsonConvert.SerializeObject(companyIds);
			using (LongRunningWebClient client = new LongRunningWebClient()) {
				client.Credentials = CredentialCache.DefaultNetworkCredentials;
				client.Encoding = Encoding.UTF8;
				client.Timeout = 1000000;
				client.Headers.Add("Content-Type","application/json");
				string postResponse = client.UploadString(requestUrl, "POST", postParams);
				return JsonConvert.DeserializeObject<Dictionary<int, ShareClassDataDTO[]>>(postResponse);
			}
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

		private class LongRunningWebClient : WebClient {

			public int Timeout { get; set; }

			protected override WebRequest GetWebRequest(Uri uri) {
				WebRequest request = base.GetWebRequest(uri);
				request.Timeout = Timeout;
				((HttpWebRequest)request).ReadWriteTimeout = Timeout;
				return request;
			}

		}

		private string GetRootUrl(string companyId) {
			return string.Format(ROOT_URL_FORMAT, _dataRoostConnectionString, companyId);
		}

	}

}
