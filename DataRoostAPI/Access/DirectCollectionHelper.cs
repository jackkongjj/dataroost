using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Web;

namespace CCS.Fundamentals.DataRoostAPI.Access {
	public class DirectCollectionHelper {
		private readonly string _sfConnectionString;
		public DirectCollectionHelper(string sfConnectionString) {
			_sfConnectionString = sfConnectionString;
		}

		public bool IsIconumDC(int iconum) {
			bool result = false;
			const string query = @"if exists (SELECT * from dbo.MigrateToTimeSlice where MigrationStatusID = 1 and Iconum = @iconum)
														begin 
														 select convert(bit,1)
														end else 
														begin 
														 select convert(bit,0)
														end";
			using (SqlConnection connection = new SqlConnection(_sfConnectionString)) {
				connection.Open();
				using (var cmd = new SqlCommand(query, connection)) {
					cmd.Parameters.AddWithValue("@iconum", iconum);

					using (var reader = cmd.ExecuteReader()) {
						if (reader.Read()) {
							result = reader.GetBoolean(0);
						}
					}
				}
			}
			return result;
		}
	}
}