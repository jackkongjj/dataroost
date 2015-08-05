using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using DataRoostAPI.Common.Models;

namespace DataRoostAPI.Common.Interfaces {
	public interface IExportedItemsDataAccess {

		ExportedItem[] GetExportedItems(StandardizationType standardizationType,
		                                DateTime startDate,
		                                DateTime? endDate = null,
		                                List<string> itemCodes = null,
		                                List<string> countries = null);

		ExportedItem[] GetExportedShareItems(StandardizationType standardizationType,
																DateTime startDate,
																DateTime? endDate = null);

	}
}
