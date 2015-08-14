using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using DataRoostAPI.Common.Models;

namespace DataRoostAPI.Common.Interfaces {
	public interface ICompanyDataAccess {

		CompanyDTO GetCompany(string companyId);

		EffortDTO GetCompanyCollectionEffort(string companyId);

		Dictionary<int, EffortDTO> GetCollectionEffortForCompanies(List<string> companyIds);

		EffortDTO[] GetEfforts(string companyId);

		ShareClassDataDTO[] GetLatestFiscalPeriodEndSharesData(string companyId, DateTime? reportDate = null);

		Dictionary<int, ShareClassDataDTO[]> GetLatestFiscalPeriodEndSharesData(List<string> companyIds);

		ShareClassDataDTO[] GetCurrentShareData(string companyId);

	}
}
