using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FactSet.Data.SqlClient;

namespace DataRoostAPI.Common.Models.AsReported {
	public class TableMeta {

		public int ID { get; private set; }
		public string Name { get; private set; }
		public string ShortName { get; private set; }
		public string DisplayName { get; private set; }
		public bool PITFlag { get; private set; }
		public static List<TableMeta> WholeTableMeta = null;
		public enum TableTypes { All = 0, NonPension, Pension, Full }

		public TableMeta(int id, string name, string shortName, string displayName, bool pitFlag) {
			ID = id;
			Name = name;
			ShortName = shortName;
			DisplayName = displayName;
			PITFlag = pitFlag;
		}
		const string SQL_GET_TABLE_IN_TREE =
			@"
;WITH CteTables
AS
(
    SELECT p.ID, p.Name, p.ShortName, p.DisplayName, p.PITFlag, p.ParentID, 0 AS Level
    FROM TableMeta (nolock) AS p
    WHERE parentid is null

	UNION ALL
    
	SELECT child.ID, child.Name, child.ShortName, child.DisplayName, child.PITFlag, child.ParentID, Level + 1
    FROM TableMeta (nolock) AS child
	INNER JOIN CteTables as p
		ON child.ParentID = p.id and child.ParentID != child.ID  
)
 
SELECT ID, Name, ShortName, DisplayName, PITFlag, Level + 1 as 'Ordering'
FROM CteTables  

			";


		const string SQL_GET_PENSION_TABLE_IN_TREE =
	@"
;WITH CteTables
AS
(
    SELECT p.ID, p.Name, p.ShortName, p.DisplayName, p.PITFlag, p.ParentID, 0 AS Level
    FROM TableMeta (nolock) AS p
   WHERE name = 'Pension' and parentid is null
    
	UNION ALL
    
	SELECT child.ID, child.Name, child.ShortName, child.DisplayName, child.PITFlag, child.ParentID, Level + 1
    FROM TableMeta (nolock) AS child
	INNER JOIN CteTables as p
		ON child.ParentID = p.id and child.ParentID != child.ID  
)
 
SELECT ID, Name, ShortName, DisplayName, PITFlag, Level + 1 as 'Ordering'
FROM CteTables where ParentID is not null 


			";

		const string SQL_GET_NONPENSION_TABLE_IN_TREE =
@"
;WITH CteTables
AS
(
    SELECT p.ID, p.Name, p.ShortName, p.DisplayName, p.PITFlag, p.ParentID, 0 AS Level
    FROM TableMeta (nolock) AS p
   WHERE name <> 'Pension' and parentid is null
    
	UNION ALL
    
	SELECT child.ID, child.Name, child.ShortName, child.DisplayName, child.PITFlag, child.ParentID, Level + 1
    FROM TableMeta (nolock) AS child
	INNER JOIN CteTables as p
		ON child.ParentID = p.id and child.ParentID != child.ID  
)
 
SELECT ID, Name, ShortName, DisplayName, PITFlag, Level + 1 as 'Ordering'
FROM CteTables  

			";


		const string SQL_GET_ALL_TABLES = @"
select ID, Name, ShortName, DisplayName, PITFlag, 1 as Ordering from TableMeta (nolock) where Name in ('BS','CF','IS') 
union
select ID, Name, ShortName, DisplayName, PITFlag, 2 as Ordering from TableMeta (nolock) where Name not in ('BS','CF','IS','Pension') 
order by Ordering, DisplayName";


		public static IEnumerable<TableMeta> GetAll(string strSql, string connectionString) {

			using (SqlConnection conn = new SqlConnection(connectionString))
			using (SqlCommand cmd = new SqlCommand(strSql, conn)) {
				conn.Open();

				using (SqlDataReader sdr = cmd.ExecuteReader()) {
					return sdr.Cast<IDataRecord>().Select(r => new TableMeta(
						sdr.GetInt16(0),
						sdr.GetStringSafe(1),
						sdr.GetStringSafe(2),
						sdr.GetStringSafe(3),
						sdr.GetBoolean(4)
					)).ToArray();
				}
			}
		}

		public static IEnumerable<TableMeta> GetAll(string connectionString) {
			string strSql = SQL_GET_ALL_TABLES;
			if (WholeTableMeta == null) {
				WholeTableMeta = GetAll(strSql, connectionString).ToList();
			}
			return WholeTableMeta;
		}


		public static IEnumerable<TableMeta> GetTables(TableTypes type, string connectionString) {
			string strSql;
			switch (type) {
				case TableTypes.Pension:
					strSql = SQL_GET_PENSION_TABLE_IN_TREE; break;
				case TableTypes.NonPension:
					strSql = strSql = SQL_GET_NONPENSION_TABLE_IN_TREE; break;
				case TableTypes.Full:
					strSql = strSql = SQL_GET_TABLE_IN_TREE; break;
				default:
					strSql = SQL_GET_ALL_TABLES; break;
			}
			return GetAll(strSql, connectionString);
		}
	}
}
