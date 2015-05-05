using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

using Newtonsoft.Json;

namespace DataRoostAPI.Common.Access {
	internal abstract class ApiHelper {

		/// <summary>
		/// Execute GET Query and Return Deserialized Result
		/// </summary>
		/// <typeparam name="T">Type of Deserialized Result</typeparam>
		/// <param name="url">Url of Query Request</param>
		/// <returns>Deserialized result of API Query</returns>
		protected T ExecuteGetQuery<T>(string url) {
			using (WebClient client = GetDefaultWebClient()) {
				string jsonResponse = client.DownloadString(url);
				return JsonConvert.DeserializeObject<T>(jsonResponse);
			}
		}

		/// <summary>
		/// Execute POST Query and Return Deserialized Result
		/// </summary>
		/// <typeparam name="T">Type of Deserialized Result</typeparam>
		/// <param name="url">Url of Query Request</param>
		/// <param name="postParams">POST parameter string object.</param>
		/// <returns>Deserialized result of API Query</returns>
		protected T ExecutePostQuery<T>(string url, string postParams) {
			using (WebClient client = GetDefaultWebClient()) {
				string postResponse = client.UploadString(url, postParams);
				return JsonConvert.DeserializeObject<T>(postResponse);
			}
		}

		/// <summary>
		/// Get Default Web Client for API Request
		/// </summary>
		/// <returns>API Specific Initialized WebClient</returns>
		protected abstract WebClient GetDefaultWebClient();

	}
}
