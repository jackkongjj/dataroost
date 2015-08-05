using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;

using CCS.Fundamentals.DataRoostAPI.Helpers;

using DataRoostAPI.Common.Models;

using FactSet.Data.SqlClient;

using Fundamentals.Helpers.DataAccess;

using Oracle.ManagedDataAccess.Client;

namespace CCS.Fundamentals.DataRoostAPI.Access.Voyager {

	public class ExportedItemsHelper : SqlHelper {

		private readonly string _damConnectionString;

		public ExportedItemsHelper(string damConnectionString) {
			_damConnectionString = damConnectionString;
		}

		public ExportedItem[] GetExportedItems(StandardizationType standardizationType,
		                                       List<string> itemCodes,
		                                       DateTime startDate,
		                                       DateTime endDate,
																					 List<string> countries = null) {
			const string query =
				@"select DISTINCT fds.iconum, vdcn.ReportDate as DocumentDate, dvm.Value as FormType, d.PublicationStampUtc as PublicationDate, d.id as DAmDocumentId, fds.IsoCountry
						from VoyagerUsedDCNs vdcn (nolock)
							join Documents d (nolock) on vdcn.DCN = d.DCN
							join FdsTriPpiMap fds (nolock) on fds.PPI = vdcn.PPI
									JOIN Feeds f (nolock)  
												ON d.Feedid = f.Id  
							JOIN DocumentVersionMeta dvm (nolock)  
												ON dvm.documentversionid = d.currentversion  
												AND dvm.KeyId = 1 --form type  
						where vdcn.CreateDateTime between @startDate and @endDate";

			IEnumerable<ExportedItem> items = ExecuteQuery(query,
				             new List<SqlParameter>
				             {
					             new SqlParameter("@startDate", startDate),
					             new SqlParameter("@endDate", endDate),
				             },
				             reader => {
					             return new ExportedItem
					                    {
						                    Iconum = reader.GetInt32(0).ToString(),
						                    ReportDate = reader.GetDateTime(1),
						                    FormType = reader.GetStringSafe(2),
						                    PublicationDate = reader.GetDateTime(3),
						                    DocumentId = reader.GetGuid(4).ToString(),
																Country = reader.GetStringSafe(5)
					                    };
										 });
			if (countries != null && countries.Count() > 0) {
				items = items.Where(i => i.Country.In(countries));
			}

			return items.ToArray();
		}

		protected override SqlConnection GetDatabaseConnection() {
			return new SqlConnection(_damConnectionString);
		}

	}

}