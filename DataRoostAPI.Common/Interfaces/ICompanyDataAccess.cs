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

		EffortDTO[] GetEfforts(string companyId);

	}
}
