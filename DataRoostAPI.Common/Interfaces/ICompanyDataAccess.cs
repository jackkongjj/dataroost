using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using DataRoostAPI.Common.Models;

namespace DataRoostAPI.Common.Interfaces {
	public interface ICompanyDataAccess {

		CompanyDTO GetCompany(string companyId);

		CompanyPriority GetCompanyPriority(string companyId);

		Dictionary<int, CompanyPriority> GetCompanyPriority(List<string> companyIds);

		EffortDTO GetCompanyCollectionEffort(string companyId);

		Dictionary<int, EffortDTO> GetCollectionEffortForCompanies(List<string> companyIds);

		EffortDTO[] GetEfforts(string companyId);

		ShareClassDataDTO[] GetLatestFiscalPeriodEndSharesData(string companyId, DateTime? reportDate = null, DateTime? since = null);

		Dictionary<int, ShareClassDataDTO[]> GetLatestFiscalPeriodEndSharesData(List<string> companyIds, DateTime? reportDate = null, DateTime? since = null);

        ShareClassDataDTO[] GetAllFiscalPeriodEndSharesData(string companyIds, string stdCode, DateTime? reportDate = null, DateTime? since = null);

        Dictionary<int, ShareClassDataDTO[]> GetAllFiscalPeriodEndSharesData(List<string> companyIds, string stdCode, DateTime? reportDate = null, DateTime? since = null);

        ShareClassDataDTO[] GetCurrentShareData(string companyId);

	}
}
