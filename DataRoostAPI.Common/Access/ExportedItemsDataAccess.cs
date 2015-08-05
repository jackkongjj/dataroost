using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

using DataRoostAPI.Common.Interfaces;
using DataRoostAPI.Common.Models;

using Fundamentals.Helpers.DataAccess;

namespace DataRoostAPI.Common.Access {
	internal class ExportedItemsDataAccess : ApiHelper, IExportedItemsDataAccess {

		private readonly string _dataRoostConnectionString;

		public ExportedItemsDataAccess(string dataRoostConnectionString) {
			_dataRoostConnectionString = dataRoostConnectionString;
		}

		public ExportedItem[] GetExportedItems(StandardizationType standardizationType,
																			 DateTime startDate,
																			 DateTime? endDate = null,
																			 List<string> itemCodes = null,
																			 List<string> countries = null) {
			string requestUrl = string.Format("{0}/api/v1/exportedItems/{1}?startDate={2}", _dataRoostConnectionString, standardizationType, startDate);
			if (endDate != null) {
				requestUrl += "&endDate=" + endDate;
			}
			if (itemCodes != null) {
				requestUrl += "&itemCodes=" + string.Join(",", itemCodes);
			}
			if (countries != null) {
				requestUrl += "&countries=" + string.Join(",", countries);
			}

			return ExecuteGetQuery<ExportedItem[]>(requestUrl);
		}


		protected override WebClient GetDefaultWebClient() {
			WebClient defaultClient = new WebClient();

			defaultClient.Credentials = CredentialCache.DefaultNetworkCredentials;
			defaultClient.Encoding = Encoding.UTF8;

			return defaultClient;
		}

		public ExportedItem[] GetExportedShareItems(StandardizationType standardizationType, DateTime startDate, DateTime? endDate = null) {
			string requestUrl = string.Format("{0}/api/v1/exportedItems/{1}/shares?startDate={2}", _dataRoostConnectionString, standardizationType, startDate);
			if (endDate != null) {
				requestUrl += "&endDate=" + endDate;
			}
			return ExecuteGetQuery<ExportedItem[]>(requestUrl);
		}

	}
}
