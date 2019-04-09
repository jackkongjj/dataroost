using System;
using System.Collections.Generic;
using System.Configuration;
using System.Web.Http;

using CCS.Fundamentals.DataRoostAPI.Access.SuperFast;
using CCS.Fundamentals.DataRoostAPI.CommLogger;

using DataRoostAPI.Common.Models;

namespace CCS.Fundamentals.DataRoostAPI.Controllers {
    [CommunicationLogger]
    [RoutePrefix("api/v1/exportedItems")]
	public class ExportedItemsController : ApiController {

		[Route("{standardizationType}")]
		[HttpGet]
		public ExportedItem[] GetExportedItems(StandardizationType standardizationType,
		                                       DateTime startDate,
		                                       DateTime? endDate = null,
																					 [FromUri] string itemCodes = null,
																					 [FromUri] string countries = null) {
			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ConnectionString;
			string damConnectionString = ConfigurationManager.ConnectionStrings["FFDAM"].ConnectionString;
			DateTime endDateTime = DateTime.UtcNow;
			if (endDate != null) {
				endDateTime = (DateTime) endDate;
			}
			List<string> itemCodeList = null;
			if (itemCodes != null) {
				itemCodeList = new List<string>(itemCodes.Split(','));
			}
			List<string> countryList = null;
			if (countries != null) {
				countryList = new List<string>(countries.Split(','));
			}

			ExportedItemsHelper superfastHelper = new ExportedItemsHelper(sfConnectionString);
			ExportedItem[] superfastExportedItems = superfastHelper.GetExportedItems(standardizationType,
			                                                                         itemCodeList,
			                                                                         startDate,
			                                                                         endDateTime,
																																							 countryList);
			Access.Voyager.ExportedItemsHelper voyagerHelper = new Access.Voyager.ExportedItemsHelper(damConnectionString);
			ExportedItem[] voyagerExportedItems = voyagerHelper.GetExportedItems(standardizationType,
			                                                                     itemCodeList,
			                                                                     startDate,
			                                                                     endDateTime,
																																					 countryList);
			List<ExportedItem> exportedItems = new List<ExportedItem>(superfastExportedItems);
			exportedItems.AddRange(voyagerExportedItems);
			return exportedItems.ToArray();
		}

		[Route("{standardizationType}/shares")]
		[HttpGet]
		public ExportedItem[] GetExportedShareItems(StandardizationType standardizationType,
		                                            DateTime startDate,
		                                            DateTime? endDate = null,
		                                            [FromUri] string countries = null) {
			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ConnectionString;
			string damConnectionString = ConfigurationManager.ConnectionStrings["FFDAM"].ConnectionString;
			DateTime endDateTime = DateTime.UtcNow;
			if (endDate != null) {
				endDateTime = (DateTime) endDate;
			}
			List<string> countryList = null;
			if (countries != null) {
				countryList = new List<string>(countries.Split(','));
			}

			ExportedItemsHelper superfastHelper = new ExportedItemsHelper(sfConnectionString);
			ExportedItem[] superfastExportedItems = superfastHelper.GetAllExportedShareItems(standardizationType,
			                                                                                 startDate,
			                                                                                 endDateTime,
																																											 countryList);
			Access.Voyager.ExportedItemsHelper voyagerHelper = new Access.Voyager.ExportedItemsHelper(damConnectionString);
			ExportedItem[] voyagerExportedItems = voyagerHelper.GetExportedItems(standardizationType,
			                                                                     null,
			                                                                     startDate,
			                                                                     endDateTime,
																																					 countryList);
			List<ExportedItem> exportedItems = new List<ExportedItem>(superfastExportedItems);
			exportedItems.AddRange(voyagerExportedItems);
			return exportedItems.ToArray();
		}

	}

}
