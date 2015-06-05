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
																		List<string> itemCodes = null,
																		DateTime? endDate = null);

		ExportedItem[] GetExportedShareItems(StandardizationType standardizationType,
																DateTime startDate,
																DateTime? endDate = null);

	}
}
