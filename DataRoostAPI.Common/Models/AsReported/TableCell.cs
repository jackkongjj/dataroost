using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using FactSet.Fundamentals.Sourcelinks;
using FactSet.Parsing.Translation;

namespace DataRoostAPI.Common.Models.AsReported {
	public class TableCell {
		private readonly string _sfConnectionString;
		public TableCell(string sfConnectionString) {
			_sfConnectionString = sfConnectionString;
		}

		private readonly Regex rxHtmlBookmark = new Regex(@"o(-?\d+)\|l(\d+)", RegexOptions.Compiled);
		public int Create(string cellValue, string cellOffset, bool cellHasBoundingBox, string cellPeriodType,
												string cellPeriodLength, string cellDay, string cellMonth, string cellYear, int termId, string scaling, string tableType, byte rootId, string currency, string xbrlTag, Guid DocumentId , string Label, string tintOffSet) {
			const string sqltxt = @"
declare @tableCellId int = null

select @tableCellId= id from tablecell where documentid = @DocumentId and offset = @Offset;
if(@tableCellId is null) begin
Insert into TableCell
									(Offset, PeriodTypeID, CellPeriodType, PeriodLength, CellPeriodCount, CellDay, CellMonth, CellYear, Value, CompanyFinancialTermID, ValueNumeric, ScalingFactorID, AsReportedScalingFactor, CellDate, Currency, CurrencyCode, XBRLTag,DocumentId,Label)
								values (@Offset, @PeriodTypeID, @CellPeriodType, @PeriodLength, @CellPeriodCount, @CellDay, @CellMonth, @CellYear, @Value, @TermID, @ValueNumb, @scale, @ARscale, @cellDate, @currency, @currencyCode, @xbrlTag,@DocumentId,@Label)
set @tableCellId = scope_identity()
end
									select @tableCellId";

			using (SqlConnection sqlConn = new SqlConnection(_sfConnectionString))
			using (SqlCommand cmd = new SqlCommand(sqltxt, sqlConn)) {
				cmd.CommandTimeout = 300;
				cmd.Parameters.AddWithValue("@TermID", termId);
				cmd.Parameters.AddWithValue("@DocumentId",DocumentId);
				cmd.Parameters.AddWithValue("@Label", Label);
	
				
				cmd.Parameters.AddWithValue("@Offset", cellOffset);
				cmd.Parameters.Add(new SqlParameter("@CellDay", SqlDbType.VarChar, 6) { Value = DbParameterSafe(cellDay) });
				cmd.Parameters.Add(new SqlParameter("@Value", SqlDbType.NVarChar, 2048) { Value = cellValue == null ? "" : cellValue });
				cmd.Parameters.Add(new SqlParameter("@CellYear", SqlDbType.VarChar, 4) { Value = DbParameterSafe(cellYear) });
				cmd.Parameters.Add(new SqlParameter("@CellMonth", SqlDbType.VarChar, 12) { Value = DbParameterSafe(cellYear) });
				cmd.Parameters.Add(new SqlParameter("@CellPeriodType", SqlDbType.VarChar, 12) { Value = DbParameterSafe(cellPeriodType) });
				cmd.Parameters.Add(new SqlParameter("@CellPeriodCount", SqlDbType.VarChar, 12) { Value = DbParameterSafe(cellPeriodLength) });
				cmd.Parameters.Add(new SqlParameter("@ARscale", SqlDbType.VarChar, 64) { Value = DbParameterSafe(scaling) });
				cmd.Parameters.Add(new SqlParameter("@currency", SqlDbType.VarChar, 32) { Value = DbParameterSafe(currency) });
				cmd.Parameters.Add(new SqlParameter("@currencyCode", SqlDbType.VarChar, 32) { Value = DbParameterSafe(NormalizeCurrency(currency)) });
				cmd.Parameters.Add(new SqlParameter("@xbrlTag", SqlDbType.VarChar) { Value = DbParameterSafe(xbrlTag) });
			
				ObservableCollection<TableMeta> tm = new ObservableCollection<TableMeta>(TableMeta.GetAll(_sfConnectionString));

				cmd.Parameters.AddWithValue("@PeriodLength", cellPeriodLength);

				//Scaling
				cmd.Parameters.Add(new SqlParameter("@scale", SqlDbType.Char, 1) { Value = getNormalizedScalingFactorId(scaling) });

				//PeriodType
				if (!string.IsNullOrEmpty(tableType) && tm.Any(x => x.PITFlag && tableType.ToLower() == x.Name.ToLower()))
					cmd.Parameters.Add(new SqlParameter("@PeriodTypeID", SqlDbType.Char, 1) { Value = "P" });
				else if (cellPeriodType.ToLower().Contains("day"))
					cmd.Parameters.Add(new SqlParameter("@PeriodTypeID", SqlDbType.Char, 1) { Value = "D" });
				else if (cellPeriodType.ToLower().Contains("week"))
					cmd.Parameters.Add(new SqlParameter("@PeriodTypeID", SqlDbType.Char, 1) { Value = "W" });
				else if (cellPeriodType.ToLower().Contains("month"))
					cmd.Parameters.Add(new SqlParameter("@PeriodTypeID", SqlDbType.Char, 1) { Value = "M" });
				else if (cellPeriodType.ToLower().Contains("quarter"))
					cmd.Parameters.Add(new SqlParameter("@PeriodTypeID", SqlDbType.Char, 1) { Value = "Q" });
				else if (cellPeriodType.ToLower().Contains("year"))
					cmd.Parameters.Add(new SqlParameter("@PeriodTypeID", SqlDbType.Char, 1) { Value = "Y" });
				else if (cellPeriodType.ToLower().Contains("inception"))
					cmd.Parameters.Add(new SqlParameter("@PeriodTypeID", SqlDbType.Char, 1) { Value = "I" });
				else if (cellPeriodType.ToLower().Contains("pit"))
					cmd.Parameters.Add(new SqlParameter("@PeriodTypeID", SqlDbType.Char, 1) { Value = "P" });
				else
					cmd.Parameters.Add(new SqlParameter("@PeriodTypeID", SqlDbType.Char, 1) { Value = DBNull.Value });


				bool isNeg = false;
				double? valuenumeric = GetNormalizedValue(cellValue, out isNeg);

				if (valuenumeric.HasValue)
					cmd.Parameters.AddWithValue("@ValueNumb", valuenumeric.Value);
				else
					cmd.Parameters.AddWithValue("@ValueNumb", DBNull.Value);

				cmd.Parameters.AddWithValue("@negind", isNeg == true ? 1 : 0);



				DateTime dt = DateTime.MinValue;
				if (cellYear != "" && cellMonth != "" && cellDay != "") {
					int year = Regex.IsMatch(cellYear, @"^\d+$") ? int.Parse(cellYear) : -1;

					int month;
					if (Regex.IsMatch(cellMonth, @"^\d+$")) {
						month = int.Parse(cellMonth);
					} else {
						try {
							month = Convert.ToDateTime(cellMonth + " 01, 1900").Month;
						} catch (Exception) { month = -1; }
					}

					int day = Regex.IsMatch(cellDay, @"^\d+$") ? int.Parse(cellDay) : -1;


					if (month != -1 && year != -1 && day != -1 && year > 1753 && year < 9999) {
						try {
							dt = new DateTime(year, month, day);
						} catch { /*Do nothing, bad date format*/}
					}
				}

				if (dt != DateTime.MinValue && dt <= DateTime.Now) {
					cmd.Parameters.AddWithValue("@cellDate", dt);
				} else {
					cmd.Parameters.AddWithValue("@cellDate", DBNull.Value);
				}

				sqlConn.Open();
				return Convert.ToInt32(cmd.ExecuteScalar());
			}
		}

		public string getNormalizedScalingFactorId(string scaling) {
			if (scaling.ToLower().Contains("trillion"))
				return "R";
			else if (scaling.ToLower().Contains("billion"))
				return "B";
			else if (scaling.ToLower().Contains("million"))
				return "M";
			else if (scaling.ToLower().Contains("thousand"))
				return "T";
			else if (scaling.ToLower().Contains("hundred"))
				return "H";
			else if (scaling.ToLower().Contains("unit"))
				return "U";
			else if (scaling.ToLower().Contains("cent"))
				return "C";
			else
				return "A";
		}

		public object DbParameterSafe(object val) {
			if (val == null) return DBNull.Value;
			else if (string.IsNullOrEmpty(val.ToString())) return DBNull.Value;
			return val;
		}

		public int CreateNewTerm(int series, string description) {
			Regex rxLabel = new Regex(@"(\[[^\]]*\])*(?<desc>[^\[\]]*)", RegexOptions.Compiled);
			const string sqltxt = @"Insert into CompanyFinancialTerm
															(DocumentSeriesID, TermStatusID, Description, EncoreTermFlag) values (@dsid, 0, @Desc, -1)
															select cast(scope_identity() as int)";

			using (SqlConnection sqlConn = new SqlConnection(_sfConnectionString))
			using (SqlCommand cmd = new SqlCommand(sqltxt, sqlConn)) {
				cmd.Parameters.AddWithValue("@dsid", series);
				cmd.Parameters.Add(new SqlParameter("@Desc", SqlDbType.VarChar, 1024) { Value = rxLabel.Match(description).Result(@"${desc}") });
				sqlConn.Open();
				return Convert.ToInt32(cmd.ExecuteScalar());
			}
		}

		private string NormalizeCurrency(string currency) {
			if (currency.ToLower().Trim() == "$" ||
		((currency.ToLower().Contains("us") || currency.ToLower().Contains("u.s")) &&
		currency.ToLower().Contains("dollar")))
				return "USD";
			else if (currency.ToLower().Contains("cdn$") ||
					(currency.ToLower().Contains("canadian") && currency.ToLower().Contains("dollar")))
				return "CAD";
			else if (currency.ToLower().Contains("jpy") ||
					currency.ToLower().Contains("yen"))
				return "JPY";
			else if (currency.ToLower().Contains("nzd"))
				return "NZD";
			else
				return null;
		}

		public double? GetNormalizedValue(string cellValue, out bool isNeg) {
			isNeg = false;

			cellValue = Regex.Replace(cellValue, @"\$", "");
			cellValue = Regex.Replace(cellValue, ",", "");

			if (Regex.IsMatch(cellValue, @"^\s*-+\s*$")) {
				isNeg = false;
				cellValue = "0";
			} else if (Regex.Match(cellValue, @"-[\s\d\.]*").Success) { //Logic for - ## sign
				isNeg = true;
				cellValue = Regex.Replace(cellValue, @"-", "");
			} else if (Regex.Match(cellValue, @"\([\s\d\.]+\)").Success) { //Logic for (##)
				isNeg = true;
				cellValue = Regex.Replace(cellValue, @"\(", "");
				cellValue = Regex.Replace(cellValue, @"\)", "");
			} else if (Regex.Match(cellValue, @"△").Success) { //Logic for "△ ##" Japanese negative
				isNeg = true;
				cellValue = Regex.Replace(cellValue, @"△", "");
			}

			double valNumb;

			if (double.TryParse(cellValue, out valNumb)) {
				if (isNeg)
					return valNumb * -1;
				else
					return valNumb;
			} else {
				return 0;
			}

		}

	}
}
