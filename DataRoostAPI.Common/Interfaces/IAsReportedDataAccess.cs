using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using DataRoostAPI.Common.Models.AsReported;

namespace DataRoostAPI.Common.Interfaces {
	public interface IAsReportedDataAccess {

		AsReportedDocument GetDocument(string companyId, string documentId);

		AsReportedDocument[] GetDocuments(string companyId,
		                                  int? startYear = null,
		                                  int? endYear = null,
		                                  string reportType = null);

		CompanyFinancialTerm[] GetCompanyFinancialTerms(string companyId);

	}
}
