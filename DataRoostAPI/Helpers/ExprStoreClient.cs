using DataRoostAPI.Common.Models.TimeseriesValues;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Web;

namespace CCS.Fundamentals.DataRoostAPI.Helpers
{
    public class ExprStoreClient
    {
        private string expressionStoreUrl;
    
        public ExprStoreClient(string expressionStoreUrl)
        {
            this.expressionStoreUrl = expressionStoreUrl;
        }
        private HttpClient httpClient = new HttpClient();

        private string PostRequest(string url, HttpContent body)
        {
            try
            {
                using (HttpResponseMessage response = httpClient.PostAsync(url, body).Result)
                using (HttpContent content = response.Content)
                {
                    return content.ReadAsStringAsync().Result;
                }
            }
            catch (Exception ex)
            {
                ex.Data.Add("FailedUrl", url);
                throw ex;
            }

        }

        public string StoreExpression(IEnumerable<ExprObjectTree> equationObj)
        {

            var jsonBody = JsonConvert.SerializeObject(equationObj);
            var body = new StringContent(jsonBody, Encoding.UTF8, "application/json");
            string url = String.Format("{0}/StoreExpression", expressionStoreUrl);
            var result = PostRequest(url, body);

            if (!string.IsNullOrEmpty(result))
            {
                var eResponse = JsonConvert.DeserializeObject<EResponse>(result);
                if (eResponse.Ok)
                    return eResponse.Response.ToString();
            }
            return null;
        }

        public bool DeleteExpressionById(IEnumerable<string> expressionIds)
        {

            var jsonBody = JsonConvert.SerializeObject(expressionIds);
            var body = new StringContent(jsonBody, Encoding.UTF8, "application/json");
            string url = String.Format("{0}/DeleteExpressionById", expressionStoreUrl);
            var result = PostRequest(url, body);

            if (!string.IsNullOrEmpty(result))
            {
                var eResponse = JsonConvert.DeserializeObject<EResponse>(result);
                return eResponse.Ok;
            }
            return false;
        }

        public List<ExprObjectTree> SearchExpressionById(IEnumerable<string> expressionIds, int? size = null)
        {

            var jsonBody = JsonConvert.SerializeObject(expressionIds);
            var body = new StringContent(jsonBody, Encoding.UTF8, "application/json");
            string url = String.Format("{0}/SearchExpressionById", expressionStoreUrl);
            if (size.HasValue)
                url += "/" + size;

            var result = PostRequest(url, body);
            if (!string.IsNullOrEmpty(result))
            {
                var eResponse = JsonConvert.DeserializeObject<ESearchResponse>(result);
                if (eResponse.Ok)
                    return eResponse.Response ?? new List<ExprObjectTree>();
            }
            return null;
        }

        public List<ExprObjectTree> SearchExpressionByQuery(Dictionary<string, object> queryParams, int? size = null)
        {

            var jsonBody = JsonConvert.SerializeObject(queryParams);
            var body = new StringContent(jsonBody, Encoding.UTF8, "application/json");
            string url = String.Format("{0}/SearchExpressionByQuery", expressionStoreUrl);
            if (size.HasValue)
                url += "/" + size;

            var result = PostRequest(url, body);
            if (!string.IsNullOrEmpty(result))
            {
                var eResponse = JsonConvert.DeserializeObject<ESearchResponse>(result);
                if (eResponse.Ok)
                    return eResponse.Response ?? new List<ExprObjectTree>();
            }
            return null;
        }

    }

    public class EResponse
    {
        public bool Ok { get; set; }
        public object Response { get; set; }
        public string Error { get; set; }
    }
    public class ESearchResponse
    {
        public bool Ok { get; set; }
        public List<ExprObjectTree> Response { get; set; }
        public string Error { get; set; }
    }
}