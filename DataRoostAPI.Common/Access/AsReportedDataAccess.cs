using System.Net;
using System.Text;

using DataRoostAPI.Common.Interfaces;
using DataRoostAPI.Common.Models.AsReported;

using Fundamentals.Helpers.DataAccess;

namespace DataRoostAPI.Common.Access {

	internal class AsReportedDataAccess : ApiHelper, IAsReportedDataAccess {

		private const string ROOT_URL_FORMAT = "{0}/api/v1/companies/{1}/efforts/asreported";
		private readonly string _dataRoostConnectionString;

		public AsReportedDataAccess(string dataRoostConnectionString) {
			_dataRoostConnectionString = dataRoostConnectionString;
		}

		public AsReportedDocument GetDocument(string companyId, string documentId) {
			string requestUrl = string.Format("{0}/documents/{1}", GetRootUrl(companyId), documentId);
			return ExecuteGetQuery<AsReportedDocument>(requestUrl);
		}

		public AsReportedDocument[] GetDocuments(string companyId,
		                                         int? startYear = null,
		                                         int? endYear = null,
		                                         string reportType = null) {
			string requestUrl = string.Format("{0}/documents?", GetRootUrl(companyId));
			if (startYear != null) {
				requestUrl += string.Format("startYear={0}&", startYear);
			}
			if (endYear != null) {
				requestUrl += string.Format("endYear={0}&", endYear);
			}
			if (reportType != null) {
				requestUrl += string.Format("reportType={0}", reportType);
			}
			return ExecuteGetQuery<AsReportedDocument[]>(requestUrl);
		}

		public CompanyFinancialTerm[] GetCompanyFinancialTerms(string companyId) {
			string requestUrl = string.Format("{0}/companyFinancialTerms", GetRootUrl(companyId));
			return ExecuteGetQuery<CompanyFinancialTerm[]>(requestUrl);
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
