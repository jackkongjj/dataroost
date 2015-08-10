using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using DataRoostAPI.Common.Access;
using DataRoostAPI.Common.Interfaces;
using DataRoostAPI.Common.Models;
using DataRoostAPI.Common.Models.AsReported;
using DataRoostAPI.Common.Models.SfVoy;

namespace DataRoost.Test {
	class Program {
		static void Main(string[] args) {
			string connectionString = "http://localhost:61581";
			string iconum = "36468";
			ICompanyDataAccess companyDataAccess = DataRoostAccessFactory.GetCompanyDataAccess(connectionString);
			CompanyDTO company = companyDataAccess.GetCompany(iconum);
			EffortDTO effort = companyDataAccess.GetCompanyCollectionEffort(iconum);
			EffortDTO[] efforts = companyDataAccess.GetEfforts(iconum);
			ShareClassDataDTO[] latestFPESharesData = companyDataAccess.GetLatestFiscalPeriodEndSharesData(iconum);
			ShareClassDataDTO[] latestFPESharesData1 = companyDataAccess.GetLatestFiscalPeriodEndSharesData("12414115");
			Dictionary<int, ShareClassDataDTO[]> companyShareData =
				companyDataAccess.GetLatestFiscalPeriodEndSharesData(new List<string> { "5195905", "5108772", iconum });
			ShareClassDataDTO[] currentSharesData = companyDataAccess.GetCurrentShareData(iconum);

			IExportedItemsDataAccess exportedItemsDataAccess = DataRoostAccessFactory.GetExportedItemsDataAccess(connectionString);
			ExportedItem[] exportedItems = exportedItemsDataAccess.GetExportedItems(StandardizationType.STD,
			                                                                        new DateTime(2015, 5, 1),
																																							null,
			                                                                        new List<string> { "05301", "05303" },
			                                                                        null);
			ExportedItem[] allShareItems = exportedItemsDataAccess.GetExportedShareItems(StandardizationType.STD, new DateTime(2015, 5, 1));

			IAsReportedDataAccess asReportedDataAccess = DataRoostAccessFactory.GetAsReportedDataAccess(connectionString);
			AsReportedDocument[] documents = asReportedDataAccess.GetDocuments(iconum, 2013, 2014);
			AsReportedDocument document = asReportedDataAccess.GetDocument(iconum, documents.First().Id);

			iconum = "216191";
			ISfVoyDataAccess sfvoyDataAccess = DataRoostAccessFactory.GetSfVoyDataAccess(connectionString);
			StandardizationType[] sfvoydataTypes = sfvoyDataAccess.GetDataTypes(iconum);
			TemplateDTO[] sfVoytemplateDtos = sfvoyDataAccess.GetTemplateList(iconum, StandardizationType.SDB);
			string sfVoyTempId = "RnxBfDE=";
			TemplateDTO sfVoytemplate = sfvoyDataAccess.GetTemplate(iconum, StandardizationType.SDB, sfVoyTempId);
			SfVoyTimeSeries[] sfVoyTSAll = sfvoyDataAccess.GetTimeseriesList(iconum, StandardizationType.SDB, sfVoytemplate.Id);
			SfVoyTimeSeries[] syVoyTSAllValue = sfvoyDataAccess.GetTimeseriesListWithValue(iconum, StandardizationType.STD, sfVoytemplate.Id, 2010);
			SfVoyTimeSeries sfVoyDetail = sfvoyDataAccess.GetTimeseries(iconum, StandardizationType.SDB, sfVoytemplate.Id, sfVoyTSAll[5].Id);
			

			IStandardizedDataAccess superFastDataAccess = DataRoostAccessFactory.GetSuperFastDataAccess(connectionString);
			StandardizationType[] dataTypes = superFastDataAccess.GetDataTypes(iconum);
			TemplateDTO[] templateDtos = superFastDataAccess.GetTemplateList(iconum, StandardizationType.SDB);
			TemplateDTO template = superFastDataAccess.GetTemplate(iconum, StandardizationType.SDB, templateDtos.First().Id);
			TimeseriesDTO[] timeseriesListAll = superFastDataAccess.GetTimeseriesList(iconum, StandardizationType.SDB, template.Id);
			TimeseriesDTO[] timeseriesListRange = superFastDataAccess.GetTimeseriesList(iconum, StandardizationType.SDB, template.Id, 2011, 2015);

			iconum = "36468";
			String templateId = "RnxBfDE=";
			string seriesId = "OWRiZTI4NzItMTA4Ny00NDJlLWFmNTAtMzgyMGRmZmUyMjY4fDIwMTV8MTEvMzAvMjAxNCAxMjowMDowMCBBTXxRMXxGYWxzZQ==";
			TimeseriesDTO detail = superFastDataAccess.GetTimeseries(iconum, StandardizationType.SDB, templateId, seriesId);

			IStandardizedDataAccess voyagerDataAccess = DataRoostAccessFactory.GetVoyagerDataAccess(connectionString);
		}
	}
}
