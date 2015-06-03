using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using DataRoostAPI.Common.Access;
using DataRoostAPI.Common.Interfaces;
using DataRoostAPI.Common.Models;
using DataRoostAPI.Common.Models.AsReported;

namespace DataRoost.Test {
	class Program {
		static void Main(string[] args) {
			string connectionString = "http://localhost:61581";
			string iconum = "5195905";
			ICompanyDataAccess companyDataAccess = DataRoostAccessFactory.GetCompanyDataAccess(connectionString);
			CompanyDTO company = companyDataAccess.GetCompany(iconum);
			EffortDTO effort = companyDataAccess.GetCompanyCollectionEffort(iconum);
			EffortDTO[] efforts = companyDataAccess.GetEfforts(iconum);

			IAsReportedDataAccess asReportedDataAccess = DataRoostAccessFactory.GetAsReportedDataAccess(connectionString);
			AsReportedDocument[] documents = asReportedDataAccess.GetDocuments(iconum, 2013, 2014);
			AsReportedDocument document = asReportedDataAccess.GetDocument(iconum, documents.First().Id);

			IStandardizedDataAccess superFastDataAccess = DataRoostAccessFactory.GetSuperFastDataAccess(connectionString);
			StandardizationType[] dataTypes = superFastDataAccess.GetDataTypes(iconum);
			TemplateDTO[] templateDtos = superFastDataAccess.GetTemplateList(iconum, StandardizationType.SDB);
			TemplateDTO template = superFastDataAccess.GetTemplate(iconum, StandardizationType.SDB, templateDtos.First().Id);

			IStandardizedDataAccess voyagerDataAccess = DataRoostAccessFactory.GetVoyagerDataAccess(connectionString);
		}
	}
}
