using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Data;
using System.Data.SqlClient;
using Oracle.ManagedDataAccess.Client;
using FactSet.Data.SqlClient;

namespace CCS.Fundamentals.DataRoostAPI.Access.Voyager {
	public class VoyagerHelper {

		private readonly string _connectionString;

		public VoyagerHelper(string connectionString) {
			_connectionString = connectionString;
		}

		public string GetPPIByIconum(int iconum) {
			string query = @"SELECT PPI_OPER FROM FDS_TRI_PPI_MAP WHERE ICO_OPER = :iconum";

			using (OracleConnection connection = new OracleConnection(_connectionString)) {
				connection.Open();
				using (OracleCommand command = new OracleCommand(query, connection)) {
					command.Parameters.Add(new OracleParameter() { OracleDbType = OracleDbType.Int32, Direction = ParameterDirection.Input, ParameterName = "iconum", Value = iconum });
					using (OracleDataReader sdr = command.ExecuteReader(CommandBehavior.SequentialAccess | CommandBehavior.SingleResult)) {
						if (sdr.Read()) {
							return sdr.GetString(0);
						}
					}
				}
				connection.Close();
			}

			return null;
		}

		public string GetPPIBase(string ppi) {
			return ppi.Substring(0, ppi.Length - 1) + "%";
		}
	}
}