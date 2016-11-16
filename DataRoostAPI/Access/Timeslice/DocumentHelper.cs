using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Web;
using CCS.Fundamentals.DataRoostAPI.Access;
using DataRoostAPI.Common.Models;
using FactSet.Data.SqlClient;

namespace CCS.Fundamentals.DataRoostAPI.Access.Timeslice {
	public class DocumentHelper {
		
		private readonly string _sfConnectionString;
		private readonly string _kpiConnectionString;
		private readonly string _segmentsConnectionString;
		private readonly string _sfarConnectionString;
		enum Months : int { Jan = 1, Feb = 2, Mar = 3, Apr = 4, May = 5, Jun = 6, Jul = 7, Aug = 8, Sep = 9, Oct = 10, Nov = 11, Dec = 12 };
		public DocumentHelper(string sfConnectionString, string kpiConnectionString, string segmentsConnectionString, string sfarConnectionString) {
			_sfConnectionString = sfConnectionString;
			_kpiConnectionString = kpiConnectionString;
			_segmentsConnectionString = segmentsConnectionString;
			_sfarConnectionString = sfarConnectionString;

		}

		public bool MigrateIconumTimeSlices(int Iconum) {
			try {
				int? EntityId = InsertEntity(Iconum);
				foreach (var timeSlice in GetSfTimeSeries(Iconum)) {
					InsertTimeSlice(timeSlice, EntityId.Value, true, "Supercore");
				}
				foreach (var timeSlice in GetKPITimeSeries(Iconum)) {
					InsertTimeSlice(timeSlice, EntityId.Value, true, "KPI");
				}
				foreach (var timeSlice in GetSegmentsTimeSeries(Iconum)) {
					InsertTimeSlice(timeSlice, EntityId.Value, true, "Segments");
				}
			} catch {
				return false;
			}
			return true;
		}

		public bool CreateTimeSlice(int Iconum, TimeSlice TimeSlice) {
			try {
				int? EntityId = InsertEntity(Iconum);
				InsertTimeSlice(TimeSlice, EntityId.Value);
			} catch {
				return false;
			}
			return true;
		}

		private void InsertTimeSlice(TimeSlice timeSlice, int EntityId, bool IsMigration = false,  string Product = "") {
			string query = @"
											declare @TimeSliceId uniqueidentifier = null

declare @duration int 
select @duration = DurationInDays * @PeriodLength  from TimeSlice.PeriodType WHERE id = @PeriodTypeID



select @TimeSliceId = ts.ID from TimeSlice.TimeSlice ts
join TimeSlice.PeriodType pt on pt.ID = ts.PeriodTypeID
where EntityId = @EntityId
and   PeriodLength * pt.DurationInDays between @duration - 30 and @duration+30
and FiscalYear = @FiscalYear and ReportTypeID = @ReportTypeID and InterimTypeID = @InterimTypeID and IsConsolidated = @IsConsolidated
and IsProforma = @IsProforma and IsAmended = @IsAmended and IsRestated = @IsRestated and AcquisionStatusID = @AcquisionStatusID and FiscalDistance = @FiscalDistance



declare @Ids table(Id uniqueidentifier)

if(@TimeSliceId is null) begin
insert into TimeSlice.TimeSlice (EntityId,TimeSlicePeriodEndDate,PeriodLength,PeriodTypeID,FiscalYear,
ReportTypeID,InterimTypeID,IsConsolidated,IsProforma,IsAmended,IsRestated,AcquisionStatusID,FiscalDistance)
output inserted.id into @Ids
values (@EntityId,@TimeSlicePeriodEndDate,@PeriodLength,@PeriodTypeID,@FiscalYear,
@ReportTypeID,@InterimTypeID,@IsConsolidated,@IsProforma,@IsAmended,@IsRestated,@AcquisionStatusID,@FiscalDistance)
select @TimeSliceId = Id from @Ids



end 
if not exists(select * from TimeSlice.TimeSliceDocumentMap where TimeSliceId = @TimeSliceId and DocumentId = @DocumentId) begin
 INSERT into TimeSlice.TimeSliceDocumentMap (TimeSliceId , DocumentId) values (@TimeSliceId,@DocumentId)
end
";
			if (IsMigration) {
				query += "insert into TimeSlice.BackFillLog (Product,ProductTimeSliceId,CommonTimeSliceId,EntityId) values (@Product,@ProductTimeSliceId,@TimeSliceId,@EntityId)";
			}
			using (SqlConnection sqlConn = new SqlConnection(_sfarConnectionString)) {
				sqlConn.Open();
					using (SqlCommand cmd = new SqlCommand(query, sqlConn)) {
						cmd.CommandType = CommandType.Text;
						cmd.Parameters.AddWithValue("@EntityId", EntityId);
						cmd.Parameters.AddWithValue("@TimeSlicePeriodEndDate", timeSlice.PeriodEndDate);
						cmd.Parameters.AddWithValue("@PeriodLength", timeSlice.PeriodLength);
						cmd.Parameters.AddWithValue("@PeriodTypeID", timeSlice.PeriodType);
						cmd.Parameters.AddWithValue("@FiscalYear", timeSlice.FiscalYear);
						cmd.Parameters.AddWithValue("@ReportTypeID", timeSlice.ReportType);
						cmd.Parameters.AddWithValue("@InterimTypeID", timeSlice.InterimType);
						cmd.Parameters.AddWithValue("@IsConsolidated", timeSlice.IsConsolidated);
						cmd.Parameters.AddWithValue("@IsProforma", timeSlice.IsProforma);
						cmd.Parameters.AddWithValue("@IsAmended", timeSlice.IsAmended);
						cmd.Parameters.AddWithValue("@IsRestated", timeSlice.IsRestated);
						cmd.Parameters.AddWithValue("@AcquisionStatusID", timeSlice.AcquisionStatus);
						cmd.Parameters.AddWithValue("@FiscalDistance", timeSlice.FiscalDistance);
						cmd.Parameters.AddWithValue("@DocumentId", timeSlice.DocumentId);
						if (IsMigration) {
							cmd.Parameters.AddWithValue("@Product", Product);
							cmd.Parameters.AddWithValue("@ProductTimeSliceId", timeSlice.ProductTimeSliceId);
						}
						cmd.ExecuteNonQuery();
					}
			}

		}

		private List<TimeSlice> GetSfTimeSeries(int Iconum) {
			List<TimeSlice> ts = new List<TimeSlice>();
			string query = @"select ts.TimeSliceDate , ts.PeriodLength, ts.PeriodTypeID, ts.CompanyFiscalYear, case ts.ReportTypeID when 'P' then 'P' ELSE 'F' end as ReportTypeId,
case ts.InterimTypeID when 'XX' then 'AR' else ts.InterimTypeID end, case ts.ConsolidatedTypeID when 'C' then convert(bit,1) else convert(bit,0) END as IsConsolidated, case ts.AccountTypeID when 'P' then convert(bit,1) else convert(bit,0) end as IsProforma, convert(bit,0) as IsAmended, 
case ts.AccountTypeID when 'R' then convert(bit,1) else convert(bit,0) end as IsRestated  ,
 case ts.AccountTypeID when 'E' then 'P' when 'U' then 'U' ELSE 'S' end as AcquisionStatusID ,dbo.fnc_ComputeFiscalDistance(ts.ID,d.DocumentDate) as FiscalDistance,
d.damdocumentid , ts.ID
from dbo.TimeSlice ts 
join document d on d.id = ts.DocumentID
join dbo.DocumentSeries ds on ds.id = d.documentseriesid
where ds.CompanyID = @Iconum";
			using (SqlConnection sqlConn = new SqlConnection(_sfConnectionString))
			using (SqlCommand cmd = new SqlCommand(query, sqlConn)) {
				cmd.CommandType = CommandType.Text;
				cmd.Parameters.AddWithValue("@Iconum", Iconum);
				sqlConn.Open();
				using (SqlDataReader sdr = cmd.ExecuteReader()) {
					while (sdr.Read()) {
						ts.Add(new TimeSlice
						{
							PeriodEndDate = sdr.GetDateTime(0),
							PeriodLength = sdr.GetInt32(1),
							PeriodType = sdr.GetStringSafe(2),
							FiscalYear = sdr.GetDecimal(3),
							ReportType = sdr.GetStringSafe(4),
							InterimType = sdr.GetStringSafe(5),
							IsConsolidated = sdr.GetBoolean(6),
							IsProforma = sdr.GetBoolean(7),
							IsAmended = sdr.GetBoolean(8),
							IsRestated = sdr.GetBoolean(9),
							AcquisionStatus = sdr.GetStringSafe(10),
							FiscalDistance = sdr.GetInt32(11),
							DocumentId = sdr.GetGuid(12),
							ProductTimeSliceId = sdr.GetGuid(13).ToString()
						});
					}
				}
			}
			return ts;
		}

		private List<TimeSlice> GetKPITimeSeries(int Iconum) {
			List<TimeSlice> ts = new List<TimeSlice>();
			string query = @"DECLARE @IsoCountry varchar(10) = null

select @IsoCountry = IsoCountry from FdsTriPpiMap where iconum = @Iconum

if(@IsoCountry is null)begin

select @IsoCountry = ISO_Country from FilerMst where iconum = @Iconum

end

SELECT  ts.TimeSlicePeriodEndDate, case ts.Duration when 90 then 3 when 120 then 4 when 180 then 6 when 270 then 9 when 360 then 1 end ,
case ts.Duration when 360 then 'Y' else 'M' end, d.ReportTypeID as ReportType , 
case PeriodTypeID when 'A' then 'AR' when 'S1' then 'I1' when 'S2' then 'I2' when 'S12' then 'IF' when 'T8' then 'Q8' when 'Q12' then 'QX'
else PeriodTypeID end, case ts.ConsolidatedTypeID when 'C' then convert(bit,1) else convert(bit,0) end, IsProforma ,d.IsAmended as IsAmended, IsRestated, 
case AcquisitionStatusID when 'P' then 'P' when 'S' then 'U' else 'S' end, FiscalDistance,
dv.DocumentID as DocumentId , ts.ID, @IsoCountry
from TimeSlice ts
join DocumentView dv on ts.ID = dv.TimeSliceID
join Document d on d.ID = dv.DocumentID
where Iconum = @Iconum";
			string IsoCountry = string.Empty;
			using (SqlConnection sqlConn = new SqlConnection(_kpiConnectionString))
			using (SqlCommand cmd = new SqlCommand(query, sqlConn)) {
				cmd.CommandType = CommandType.Text;
				cmd.Parameters.AddWithValue("@Iconum", Iconum);
				sqlConn.Open();
				using (SqlDataReader sdr = cmd.ExecuteReader()) {
					while (sdr.Read()) {
						IsoCountry = sdr.GetStringSafe(13);
						ts.Add(new TimeSlice
						{
							PeriodEndDate = sdr.GetDateTime(0),
							PeriodLength = sdr.GetInt32(1),
							PeriodType = sdr.GetStringSafe(2),
							FiscalYear = CalcDataYear(sdr.GetStringSafe(2), sdr.GetDateTime(0), IsoCountry == "US"),
							ReportType = sdr.GetStringSafe(3),
							InterimType = sdr.GetStringSafe(4),
							IsConsolidated = sdr.GetBoolean(5),
							IsProforma = sdr.GetBoolean(6),
							IsAmended = sdr.GetBoolean(7),
							IsRestated = sdr.GetBoolean(8),
							AcquisionStatus = sdr.GetStringSafe(9),
							FiscalDistance = sdr.GetByte(10),
							DocumentId = sdr.GetGuid(11),
							ProductTimeSliceId = sdr.GetGuid(12).ToString(),

						});

					}
				}
			}
			return ts;
		}
		
		private List<TimeSlice> GetSegmentsTimeSeries(int Iconum) {
			List<TimeSlice> ts = new List<TimeSlice>();
			string query = @"SELECT ts.PeriodEndDate,1 ,'Y',ts.FiscalYear,ts.ReportType,'AR',case ts.Consolidated when 'C' then convert(bit,1) else convert(bit,0) end,ts.IsProForma,
ts.IsAmended,ts.IsRestated,case ts.Predecessor when 'P' then 'P' when 'S' then 'U' when 'N' then 'S' end ,ts.FiscalDistance,tsd.DocumentID,ts.ID
 from Seg.TimeSeries ts
join Seg.TimeSeriesDocument tsd on tsd.TimeSeriesID = ts.ID and tsd.VersionID = ts.VersionID
join Seg.Version v on v.ID = tsd.VersionID
where v.Iconum = @Iconum
";
			using (SqlConnection sqlConn = new SqlConnection(_segmentsConnectionString))
			using (SqlCommand cmd = new SqlCommand(query, sqlConn)) {
				cmd.CommandType = CommandType.Text;
				cmd.Parameters.AddWithValue("@Iconum", Iconum);
				sqlConn.Open();
				using (SqlDataReader sdr = cmd.ExecuteReader()) {
					while (sdr.Read()) {
						ts.Add(new TimeSlice
						{
							PeriodEndDate = sdr.GetDateTime(0),
							PeriodLength = sdr.GetInt32(1),
							PeriodType = sdr.GetStringSafe(2),
							FiscalYear = sdr.GetInt32(3),
							ReportType = sdr.GetStringSafe(4),
							InterimType = sdr.GetStringSafe(5),
							IsConsolidated = sdr.GetBoolean(6),
							IsProforma = sdr.GetBoolean(7),
							IsAmended = sdr.GetBoolean(8),
							IsRestated = sdr.GetBoolean(9),
							AcquisionStatus = sdr.GetStringSafe(10),
							FiscalDistance = sdr.GetInt32(11),
							DocumentId = sdr.GetGuid(12),
							ProductTimeSliceId = sdr.GetInt32(13).ToString()
						});
					}
				}
			}
			return ts;
		}

		private int? InsertEntity(int Iconum) {
			int? EntityId = null;
			string permId = PermId.Iconum2PermId(Iconum);
			if (string.IsNullOrEmpty(permId)) {
				return EntityId;
			}
			string query = @"
											declare @EntityId int = null
											select @EntityId = Id from TimeSlice.Entity where Iconum = @Iconum and PermId = @PermId
											if(@EntityId is null)begin 
												insert into TimeSlice.Entity(Iconum,PermId) values (@Iconum,@PermId)
												set @EntityId = SCOPE_IDENTITY();
											end
											select @EntityId";
			using (SqlConnection sqlConn = new SqlConnection(_sfarConnectionString))
			using (SqlCommand cmd = new SqlCommand(query, sqlConn)) {
				cmd.CommandType = CommandType.Text;
				cmd.Parameters.AddWithValue("@Iconum", Iconum);
				cmd.Parameters.AddWithValue("@PermId", permId);
				sqlConn.Open();
				using (SqlDataReader sdr = cmd.ExecuteReader()) {
					if (sdr.Read()) {
						EntityId = sdr.GetNullable<int>(0);
					}
				}
			}
			return EntityId;

		}

		private int CalcDataYear(string InterimType, DateTime TimeSlicePeriodEndDate, bool US) {
			Months endMonth = US ? Months.Feb : Months.Jan;
			int endDay = US ? 10 : 15;

			//Determine US Cutoff
			switch (InterimType.Trim()) {
				case "T1":
				case "T1_YTD":
				case "T2":
					endMonth = US ? Months.Feb : Months.Jan;
					endDay = US ? 11 : 16;
					break;
				case "T3":
					var year = TimeSlicePeriodEndDate.Year;
					DateTime startDate = new DateTime(year, 1, 16);
					if (US) {
						startDate = new DateTime(year - 1, 11, 16);
					}
					if (TimeSlicePeriodEndDate >= startDate && TimeSlicePeriodEndDate <= startDate.AddMonths(4))
						return startDate.Year;
					else
						return TimeSlicePeriodEndDate.Year;
				case "Q1":
				case "Q1_YTD":
					endMonth = US ? Months.May : Months.Apr;
					endDay = US ? 11 : 16;
					break;
				case "Q2":
				case "Q6":
				case "S1":
				case "S1_YTD":
					endMonth = US ? Months.Aug : Months.Jul;
					endDay = US ? 11 : 16;
					break;
				case "Q3":
				case "Q9":
					endMonth = US ? Months.Nov : Months.Oct;
					endDay = US ? 11 : 16;
					break;
				default: //Q4, IF, XX
					endMonth = US ? Months.Feb : Months.Jan;
					endDay = US ? 10 : 15;
					break;
			}

			switch (InterimType.Trim()) {
				case "Q4":
				case "A":
				case "S2":
				case "S12":
				case "Q12":
				case "T12":
					if ((int)endMonth < TimeSlicePeriodEndDate.Month) {
						return TimeSlicePeriodEndDate.Year;
					} else if ((int)endMonth > TimeSlicePeriodEndDate.Month) {
						return TimeSlicePeriodEndDate.Year - 1;
					} else {
						if (endDay < TimeSlicePeriodEndDate.Day) {
							return TimeSlicePeriodEndDate.Year;
						} else { // if (Day >= TimeSeriesDate.Day) {
							return TimeSlicePeriodEndDate.Year - 1;
						}
					}
				case "S1":
				case "Q1":
				case "S1_YTD":
				case "Q1_YTD":
				case "Q2":
				case "Q3":
				case "Q6":
				case "Q9":
					if ((int)endMonth < TimeSlicePeriodEndDate.Month) {
						return TimeSlicePeriodEndDate.Year + 1;
					} else if ((int)endMonth > TimeSlicePeriodEndDate.Month) {
						return TimeSlicePeriodEndDate.Year;
					} else {
						if (endDay <= TimeSlicePeriodEndDate.Day) {
							return TimeSlicePeriodEndDate.Year + 1;
						} else { // if (Day > TimeSeriesDate.Day) {
							return TimeSlicePeriodEndDate.Year;
						}
					}
				case "T1":
				case "T1_YTD":
					var t3Date = TimeSlicePeriodEndDate.AddMonths(8);
					if (t3Date.Year > TimeSlicePeriodEndDate.Year) {
						if (t3Date.Month <= (int)endMonth) {
							if (t3Date.Day < endDay) return t3Date.Year - 1;
							return t3Date.Year;
						}
					}
					return t3Date.Year;
				case "T2":
					var t3 = TimeSlicePeriodEndDate.AddMonths(4);
					if (t3.Year > TimeSlicePeriodEndDate.Year) {
						if (t3.Month <= (int)endMonth) {
							if (t3.Day < endDay) return t3.Year - 1;
							return t3.Year;
						}
					}
					return t3.Year;
				default:
					return TimeSlicePeriodEndDate.Year;
			}
		}

	}
}