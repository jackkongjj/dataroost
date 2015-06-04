using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using DataRoostAPI.Common.Models;

namespace DataRoostAPI.Common.Interfaces {
	public interface IExportedItemsDataAccess {

		ExportedItem[] GetExportedItems(StandardizationType standardizationType,
		                                List<string> itemCodes,
		                                DateTime startDate,
		                                DateTime? endDate = null);

	}
}
