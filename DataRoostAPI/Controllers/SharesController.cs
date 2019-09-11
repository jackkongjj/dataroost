using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;
using System.Configuration;
using System.Web.Http.Cors;
using CCS.Fundamentals.DataRoostAPI.Access;
using CCS.Fundamentals.DataRoostAPI.Access.Company;
using CCS.Fundamentals.DataRoostAPI.CommLogger;
using DataRoostAPI.Common.Exceptions;
using DataRoostAPI.Common.Models;

namespace CCS.Fundamentals.DataRoostAPI.Controllers {
    [CommunicationLogger]
    [RoutePrefix("api/v1/companies")]
	public class SharesController : ApiController {

		[Route("{CompanyId}/shares/latestFiscalPeriodEnd/")]
		[HttpGet]
		public List<ShareClassDataDTO> GetLatestFiscalPeriodEndSharesData(string CompanyId, DateTime? reportDate = null, DateTime? since = null) {
			if (string.IsNullOrEmpty(CompanyId)) {
				return null;
			}

			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDoc-PantheonReadOnly"].ConnectionString;
			string voyConnectionString = ConfigurationManager.ConnectionStrings["Voyager"].ConnectionString;
			string lionConnectionString = ConfigurationManager.ConnectionStrings["Lion"].ConnectionString;
			string damConnectionString = ConfigurationManager.ConnectionStrings["FFDAM"].ConnectionString;

			int iconum = PermId.PermId2Iconum(CompanyId);
			List<int> iconumList = new List<int> {iconum};

			CompanyHelper helper = new CompanyHelper(sfConnectionString, voyConnectionString, lionConnectionString, damConnectionString);
			Dictionary<int, List<ShareClassDataDTO>> iconumDictionary = helper.GetCompanyShareClassData(iconumList, reportDate, since);
			if (!iconumDictionary.ContainsKey(iconum)) {
				throw new MissingIconumException(iconum);
			}

			return iconumDictionary[iconum];
		}

		[Route("shares/latestFiscalPeriodEnd/")]
		[HttpPost]
		public Dictionary<int, List<ShareClassDataDTO>> GetLatestFiscalPeriodEndSharesData(List<string> companyIds, DateTime? reportDate = null, DateTime? since = null) {
			if (companyIds == null || companyIds.Count < 1) {
				return new Dictionary<int, List<ShareClassDataDTO>>();
			}

			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDoc-PantheonReadOnly"].ConnectionString;
			string voyConnectionString = ConfigurationManager.ConnectionStrings["Voyager"].ConnectionString;
			string lionConnectionString = ConfigurationManager.ConnectionStrings["Lion"].ConnectionString;
			string damConnectionString = ConfigurationManager.ConnectionStrings["FFDAM"].ConnectionString;

			List<int> iconums = companyIds.Select(companyId => PermId.PermId2Iconum(companyId)).ToList();
			iconums = iconums.Distinct().ToList();

		    CompanyHelper helper = new CompanyHelper(sfConnectionString, voyConnectionString, lionConnectionString,
		        damConnectionString);
			return helper.GetCompanyShareClassData(iconums, reportDate, since);
		}

        [Route("{CompanyId}/shares/allFiscalPeriodEnd/")]
        [HttpGet]
        public List<ShareClassDataDTO> GetAllFiscalPeriodEndSharesData(string companyId, string stdCode, DateTime? reportDate = null, DateTime? since = null) {
            if (string.IsNullOrEmpty(companyId)) {
                return null;
            }

            string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDoc-PantheonReadOnly"].ConnectionString;
            string voyConnectionString = ConfigurationManager.ConnectionStrings["Voyager"].ConnectionString;
            string lionConnectionString = ConfigurationManager.ConnectionStrings["Lion"].ConnectionString;
            string damConnectionString = ConfigurationManager.ConnectionStrings["FFDAM"].ConnectionString;

            int iconum = PermId.PermId2Iconum(companyId);
            List<int> iconumList = new List<int> { iconum };

            CompanyHelper helper = new CompanyHelper(sfConnectionString, voyConnectionString, lionConnectionString, damConnectionString);
            Dictionary<int, List<ShareClassDataDTO>> iconumDictionary = helper.GetAllShareClassData(iconumList, stdCode, reportDate, since);
            if (!iconumDictionary.ContainsKey(iconum)) {
                throw new MissingIconumException(iconum);
            }

            return iconumDictionary[iconum];
        }

        [Route("shares/allFiscalPeriodEnd/")]
        [HttpPost]
        public Dictionary<int, List<ShareClassDataDTO>> GetAllFiscalPeriodEndSharesData(List<string> companyIds, string stdCode, DateTime? reportDate = null, DateTime? since = null) {
            if (companyIds == null || companyIds.Count < 1) {
                return new Dictionary<int, List<ShareClassDataDTO>>();
            }

            string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDoc-PantheonReadOnly"].ConnectionString;
            string voyConnectionString = ConfigurationManager.ConnectionStrings["Voyager"].ConnectionString;
            string lionConnectionString = ConfigurationManager.ConnectionStrings["Lion"].ConnectionString;
            string damConnectionString = ConfigurationManager.ConnectionStrings["FFDAM"].ConnectionString;

            List<int> iconums = companyIds.Select(companyId => PermId.PermId2Iconum(companyId)).ToList();
            iconums = iconums.Distinct().ToList();

            CompanyHelper helper = new CompanyHelper(sfConnectionString, voyConnectionString, lionConnectionString, damConnectionString);
            return helper.GetAllShareClassData(iconums, stdCode, reportDate, since);
        }

        [Route("{CompanyId}/shares/currentShares/")]
		[HttpGet]
		public ShareClassDataDTO[] GetCurrentShareData(string CompanyId) {
			if (string.IsNullOrEmpty(CompanyId)) {
				return null;
			}

			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDoc-PantheonReadOnly"].ConnectionString;
			string voyConnectionString = ConfigurationManager.ConnectionStrings["Voyager"].ConnectionString;
			string lionConnectionString = ConfigurationManager.ConnectionStrings["Lion"].ConnectionString;
			string damConnectionString = ConfigurationManager.ConnectionStrings["FFDAM"].ConnectionString;

			int iconum = PermId.PermId2Iconum(CompanyId);

			CompanyHelper helper = new CompanyHelper(sfConnectionString, voyConnectionString, lionConnectionString, damConnectionString);
			return helper.GetCurrentCompanyShareClassData(iconum).ToArray();
		}
	}
}
