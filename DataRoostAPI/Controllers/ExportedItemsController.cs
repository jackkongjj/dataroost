using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;

using CCS.Fundamentals.DataRoostAPI.Access.SuperFast;

using DataRoostAPI.Common.Models;

namespace CCS.Fundamentals.DataRoostAPI.Controllers {

	[RoutePrefix("api/v1/exportedItems")]
	public class ExportedItemsController : ApiController {

		[Route("{standardizationType}")]
		[HttpGet]
		public ExportedItem[] GetExportedItems(StandardizationType standardizationType,
																					 [FromUri] string[] itemCodes,
																					 DateTime startDate,
																					 DateTime? endDate = null) {
			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ConnectionString;
			string damConnectionString = ConfigurationManager.ConnectionStrings["FFDAM"].ConnectionString;
			DateTime endDateTime = DateTime.UtcNow;
			if (endDate != null) {
				endDateTime = (DateTime)endDate;
			}
			List<string> itemCodeList = new List<string>(itemCodes);

			ExportedItemsHelper superfastHelper = new ExportedItemsHelper(sfConnectionString);
			ExportedItem[] superfastExportedItems = superfastHelper.GetExportedItems(standardizationType, itemCodeList, startDate, endDateTime);
			Access.Voyager.ExportedItemsHelper voyagerHelper = new Access.Voyager.ExportedItemsHelper(damConnectionString);
			ExportedItem[] voyagerExportedItems = voyagerHelper.GetExportedItems(standardizationType, itemCodeList, startDate, endDateTime);
			List < ExportedItem > exportedItems = new List<ExportedItem>(superfastExportedItems);
			exportedItems.AddRange(voyagerExportedItems);
			return exportedItems.ToArray();
		}

		[Route("{standardizationType}/shares")]
		[HttpGet]
		public ExportedItem[] GetExportedShareItems(StandardizationType standardizationType,
																					 DateTime startDate,
																					 DateTime? endDate = null) {
			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ConnectionString;
			string damConnectionString = ConfigurationManager.ConnectionStrings["FFDAM"].ConnectionString;
			DateTime endDateTime = DateTime.UtcNow;
			if (endDate != null) {
				endDateTime = (DateTime)endDate;
			}
			ExportedItemsHelper superfastHelper = new ExportedItemsHelper(sfConnectionString);
			ExportedItem[] superfastExportedItems = superfastHelper.GetAllExportedShareItems(standardizationType, startDate, endDateTime);
			Access.Voyager.ExportedItemsHelper voyagerHelper = new Access.Voyager.ExportedItemsHelper(damConnectionString);
			ExportedItem[] voyagerExportedItems = voyagerHelper.GetExportedItems(standardizationType, null, startDate, endDateTime);
			List<ExportedItem> exportedItems = new List<ExportedItem>(superfastExportedItems);
			exportedItems.AddRange(voyagerExportedItems);
			return exportedItems.ToArray();
		}
	}

}
