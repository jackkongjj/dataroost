using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Dynamic;
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
		private readonly string _damConnectionString;
		enum Months : int { Jan = 1, Feb = 2, Mar = 3, Apr = 4, May = 5, Jun = 6, Jul = 7, Aug = 8, Sep = 9, Oct = 10, Nov = 11, Dec = 12 };
		public DocumentHelper(string sfConnectionString, string kpiConnectionString, string segmentsConnectionString, string sfarConnectionString , string damConnectionString) {
			_sfConnectionString = sfConnectionString;
			_kpiConnectionString = kpiConnectionString;
			_segmentsConnectionString = segmentsConnectionString;
			_sfarConnectionString = sfarConnectionString;
			_damConnectionString = damConnectionString;
		}

		public bool MigrateIconumTimeSlices(int Iconum) {

			if (EntityExists(Iconum)) {
				return true;
			}
				int? EntityId = InsertEntity(Iconum);
				foreach (var timeSlice in GetSfTimeSeries(Iconum)) {
					InsertTimeSlice(timeSlice, EntityId.Value, "Supercore");
				}
				foreach (var timeSlice in GetKPITimeSeries(Iconum)) {
					InsertTimeSlice(timeSlice, EntityId.Value, "KPI");
				}
				foreach (var timeSlice in GetSegmentsTimeSeries(Iconum)) {
					InsertTimeSlice(timeSlice, EntityId.Value, "Segments");
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

		private void InsertTimeSlice(TimeSlice timeSlice, int EntityId,  string Product = "") {
			string query = @"
											declare @TimeSliceId uniqueidentifier = null

declare @duration int 
select @duration = DurationInDays * @PeriodLength  from TimeSlice.PeriodType WHERE id = @PeriodTypeID



select @TimeSliceId = ts.ID from TimeSlice.TimeSlice ts
join TimeSlice.PeriodType pt on pt.ID = ts.PeriodTypeID
where EntityId = @EntityId
and   PeriodLength * pt.DurationInDays between @duration - 30 and @duration+30
and FiscalYear = @FiscalYear and InterimTypeID = @InterimTypeID and IsConsolidated = @IsConsolidated
and IsProforma = @IsProforma  and AcquisitionStatusID = @AcquisitionStatusID and FiscalDistance = @FiscalDistance
and ReportingPeriodEndDate = @ReportingPeriodEndDate


declare @Ids table(Id uniqueidentifier)

if(@TimeSliceId is null) begin
insert into TimeSlice.TimeSlice (EntityId,TimeSlicePeriodEndDate,PeriodLength,PeriodTypeID,FiscalYear,
InterimTypeID,IsConsolidated,IsProforma,AcquisitionStatusID,FiscalDistance,ReportingPeriodEndDate)
output inserted.id into @Ids
values (@EntityId,@TimeSlicePeriodEndDate,@PeriodLength,@PeriodTypeID,@FiscalYear,
@InterimTypeID,@IsConsolidated,@IsProforma,@AcquisitionStatusID,@FiscalDistance,@ReportingPeriodEndDate)
select @TimeSliceId = Id from @Ids



end 

select @TimeSliceId

";

			string docSql = @"
declare @TimeSliceDocumentMap int = null
select @TimeSliceDocumentMap = Id from TimeSlice.TimeSliceDocumentMap where TimeSliceId = @TimeSliceId and DocumentId = @DocumentId
if (@TimeSliceDocumentMap is null) begin
 INSERT into TimeSlice.TimeSliceDocumentMap (TimeSliceId , DocumentId,ReportTypeID,IsAmended,IsRestated,PublicationStampUtc,FormType) 
 values (@TimeSliceId , @DocumentId,@ReportTypeID,@IsAmended,@IsRestated,@PublicationStampUtc,@FormType) 
 set @TimeSliceDocumentMap = SCOPE_IDENTITY();
end

insert into TimeSlice.BackFillLog (Product,ProductTimeSliceId,TimeSliceDocumentMapId)
values (@Product,@ProductTimeSliceId,@TimeSliceDocumentMap)


";
			Guid TimeSliceId = Guid.Empty;
			using (SqlConnection sqlConn = new SqlConnection(_sfarConnectionString)) {
				sqlConn.Open();
					using (SqlCommand cmd = new SqlCommand(query, sqlConn)) {
						cmd.CommandType = CommandType.Text;
						cmd.Parameters.AddWithValue("@EntityId", EntityId);
						cmd.Parameters.AddWithValue("@TimeSlicePeriodEndDate", timeSlice.PeriodEndDate);
						cmd.Parameters.AddWithValue("@PeriodLength", timeSlice.PeriodLength);
						cmd.Parameters.AddWithValue("@PeriodTypeID", timeSlice.PeriodType);
						cmd.Parameters.AddWithValue("@FiscalYear", timeSlice.FiscalYear);
						cmd.Parameters.AddWithValue("@InterimTypeID", timeSlice.InterimType);
						cmd.Parameters.AddWithValue("@IsConsolidated", timeSlice.IsConsolidated);
						cmd.Parameters.AddWithValue("@IsProforma", timeSlice.IsProforma);
						cmd.Parameters.AddWithValue("@AcquisitionStatusID", timeSlice.AcquisitionStatus);
						cmd.Parameters.AddWithValue("@FiscalDistance", timeSlice.FiscalDistance);
						cmd.Parameters.AddWithValue("@ReportingPeriodEndDate", timeSlice.ReportingPeriodEndDate);
					
						using(SqlDataReader sdr = cmd.ExecuteReader()){
							if(sdr.Read())
							TimeSliceId = sdr.GetGuid(0);
						}
					}
						foreach(var doc in timeSlice.Documents){
							using (SqlCommand cmd = new SqlCommand(docSql, sqlConn)) {
								cmd.CommandType = CommandType.Text;
								cmd.Parameters.AddWithValue("@ReportTypeID", doc.ReportType);
								cmd.Parameters.AddWithValue("@IsAmended", doc.IsAmended);
						    cmd.Parameters.AddWithValue("@IsRestated", doc.IsRestated);
								cmd.Parameters.AddWithValue("@DocumentId", doc.DocumentId);
								cmd.Parameters.AddWithValue("@PublicationStampUtc", doc.PublicationStamp);
								cmd.Parameters.AddWithValue("@FormType", doc.FormType);
								cmd.Parameters.AddWithValue("@TimeSliceId", TimeSliceId);
								cmd.Parameters.AddWithValue("@Product", Product);
							  cmd.Parameters.AddWithValue("@ProductTimeSliceId", doc.ProductTimeSliceId);
								cmd.ExecuteNonQuery();
							}
						}
					
			}

		}

		private List<TimeSlice> GetSfTimeSeries(int Iconum) {
			List<TimeSlice> ts = new List<TimeSlice>();
			string query = @"select ts.TimeSliceDate , ts.PeriodLength, ts.PeriodTypeID, ts.CompanyFiscalYear, case ts.ReportTypeID when 'P' then 'P' ELSE 'F' end as ReportTypeId,
case ts.InterimTypeID when 'XX' then 'AR' else ts.InterimTypeID end, case ts.ConsolidatedTypeID when 'C' then convert(bit,1) else convert(bit,0) END as IsConsolidated, case ts.AccountTypeID when 'P' then convert(bit,1) else convert(bit,0) end as IsProforma, convert(bit,0) as IsAmended, 
case ts.AccountTypeID when 'R' then convert(bit,1) else convert(bit,0) end as IsRestated  ,
 case ts.AccountTypeID when 'E' then 'P' when 'U' then 'U' ELSE 'S' end as AcquisitionStatusID ,dbo.fnc_ComputeFiscalDistance(ts.TimeSliceDate,ts.PeriodTypeID,d.DocumentDate) as FiscalDistance,
d.damdocumentid , ts.ID , d.PublicationDateTime,d.FormTypeID,d.DocumentDate
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
						TimeSlice t = new TimeSlice
						{
							PeriodEndDate = sdr.GetDateTime(0),
							PeriodLength = sdr.GetInt32(1),
							PeriodType = sdr.GetStringSafe(2),
							FiscalYear = sdr.GetDecimal(3),
							InterimType = sdr.GetStringSafe(5),
							IsConsolidated = sdr.GetBoolean(6),
							IsProforma = sdr.GetBoolean(7),
							AcquisitionStatus = sdr.GetStringSafe(10),
							FiscalDistance = sdr.GetInt32(11),
							ReportingPeriodEndDate = sdr.GetDateTime(16)
							
						};
						t.Documents.Add(new TimeSliceDocument
						{
							ReportType = sdr.GetStringSafe(4),
							IsAmended = sdr.GetBoolean(8),
							IsRestated = sdr.GetBoolean(9),
							DocumentId = sdr.GetGuid(12),
							ProductTimeSliceId = sdr.GetGuid(13).ToString(),
							PublicationStamp = sdr.GetDateTime(14),
							FormType = sdr.GetStringSafe(15)
						});
						ts.Add(t);
						
					}
				}
			}
			return ts;
		}

		private List<TimeSlice> GetKPITimeSeries(int Iconum) {
			List<TimeSlice> ts = new List<TimeSlice>();
			string query = @"use ffkpi 
DECLARE @IsoCountry varchar(10) = null

select @IsoCountry = IsoCountry from FdsTriPpiMap where iconum = @Iconum

if(@IsoCountry is null)begin

select @IsoCountry = ISO_Country from FilerMst where iconum = @Iconum

end

SELECT  ts.TimeSlicePeriodEndDate, case ts.Duration when 90 then 3 when 120 then 4 when 180 then 6 when 270 then 9 when 360 then 1 end ,
case ts.Duration when 360 then 'Y' else 'M' end, d.ReportTypeID as ReportType , 
case PeriodTypeID when 'A' then 'AR' when 'S1' then 'I1' when 'S2' then 'I2' when 'S12' then 'IF' when 'T8' then 'Q8' when 'Q12' then 'QX'
else PeriodTypeID end, case ts.ConsolidatedTypeID when 'C' then convert(bit,1) else convert(bit,0) end, IsProforma ,d.IsAmended as IsAmended, IsRestated, 
case AcquisitionStatusID when 'P' then 'P' when 'S' then 'U' else 'S' end, isnull(u.fiscaldistance,0) ,
dv.DocumentID as DocumentId , ts.ID, @IsoCountry,d.PublicationDateTime,d.FormType,d.ReportingPeriodEndDate
from TimeSlice ts
join DocumentView dv on ts.ID = dv.TimeSliceID
join Document d on d.ID = dv.DocumentID
outer apply  [dbo].[fnc_ComputeFiscalDistance_NEW] (ts.id, d.ReportingPeriodEndDate) u
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
						TimeSlice t = new TimeSlice
						{
							PeriodEndDate = sdr.GetDateTime(0),
							PeriodLength = sdr.GetInt32(1),
							PeriodType = sdr.GetStringSafe(2),
							FiscalYear = CalcDataYear(sdr.GetStringSafe(2), sdr.GetDateTime(0), IsoCountry == "US"),
							InterimType = sdr.GetStringSafe(4),
							IsConsolidated = sdr.GetBoolean(5),
							IsProforma = sdr.GetBoolean(6),
							AcquisitionStatus = sdr.GetStringSafe(9),
							FiscalDistance = sdr.GetByte(10),
							ReportingPeriodEndDate = sdr.GetDateTime(16)

						};
						t.Documents.Add(new TimeSliceDocument
						{
							ReportType = sdr.GetStringSafe(3),
							IsAmended = sdr.GetBoolean(7),
							IsRestated = sdr.GetBoolean(8),
							DocumentId = sdr.GetGuid(11),
							ProductTimeSliceId = sdr.GetGuid(12).ToString(),
							PublicationStamp = sdr.GetDateTime(14),
							FormType = sdr.GetStringSafe(15)
						});
						ts.Add(t);

					}
				}
			}
			return ts;
		}
		
		private List<TimeSlice> GetSegmentsTimeSeries(int Iconum) {
			List<TimeSlice> ts = new List<TimeSlice>();
			string query = @"SELECT ts.PeriodEndDate,1 ,'Y',ts.FiscalYear,ts.ReportType,'AR',case ts.Consolidated when 'C' then convert(bit,1) else convert(bit,0) end,ts.IsProForma,
ts.IsAmended,ts.IsRestated,case ts.Predecessor when 'P' then 'P' when 'S' then 'U' when 'N' then 'S' end ,dbo.fnc_ComputeFiscalDistance(ts.PeriodEndDate,ts.PeriodType,ts.ReportingPeriodEndDate) as FiscalDistance,tsd.DocumentID,ts.ID,ts.ReportingPeriodEndDate
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
						TimeSlice t = new TimeSlice
						{
							PeriodEndDate = sdr.GetDateTime(0),
							PeriodLength = sdr.GetInt32(1),
							PeriodType = sdr.GetStringSafe(2),
							FiscalYear = sdr.GetInt32(3),
							
							InterimType = sdr.GetStringSafe(5),
							IsConsolidated = sdr.GetBoolean(6),
							IsProforma = sdr.GetBoolean(7),
							AcquisitionStatus = sdr.GetStringSafe(10),
							FiscalDistance = sdr.GetInt32(11),
							ReportingPeriodEndDate = sdr.GetDateTime(14)
						};
						TimeSliceDocument d = new TimeSliceDocument
						{
							ReportType = sdr.GetStringSafe(4),
							IsAmended = sdr.GetBoolean(8),
							IsRestated = sdr.GetBoolean(9),
							DocumentId = sdr.GetGuid(12),
							ProductTimeSliceId = sdr.GetInt32(13).ToString()
						};
						GetDocumentMeta(d);
						t.Documents.Add(d);
						ts.Add(t);
					}
				}
			}
			return ts;
		}


		private List<object> GetMappedTimeSlices(int Iconum, string Product, string Year) {
			List<object> timeSlices = new List<object>();
			string query = @"select bf.ProductTimeSliceId from TimeSlice.TimeSlice ts 
join TimeSlice.TimeSliceDocumentMap tds on tds.TimeSliceId = ts.ID
join TimeSlice.BackFillLog bf on bf.TimeSliceDocumentMapId = tds.ID
join TimeSlice.Entity te on te.ID = ts.EntityId
where te.Iconum = @Iconum and ts.FiscalYear = @FiscalYear and bf.Product = @Product";
			using (SqlConnection sqlConn = new SqlConnection(_sfarConnectionString))
			using (SqlCommand cmd = new SqlCommand(query, sqlConn)) {
				cmd.CommandType = CommandType.Text;
				cmd.Parameters.AddWithValue("@Iconum", Iconum);
				cmd.Parameters.AddWithValue("@FiscalYear", Year);
				cmd.Parameters.AddWithValue("@Product", Product);
				sqlConn.Open();
				using (SqlDataReader rdr = cmd.ExecuteReader()) {
					while (rdr.Read()) {

						timeSlices.Add(rdr.GetValue(0));
					}
				}

			}
			return timeSlices;
		}

		public object GetProductTimeSlices(int Iconum, string Product,string Year) {
			var details = new List<object>();
			string query = "";
			string connectionString = "";
			DataTable dt = new DataTable();
			if (Product == "KPI") {
				query = @"SELECT ts.TimeSlicePeriodEndDate, ts.Duration, ts.PeriodTypeID, ts.AcquisitionStatusID, ts.IsProforma, ts.FiscalDistance, ts.IsRestated  from TimeSlice ts 
join @mappedTimeSlice mts on mts.id = ts.ID";
				 connectionString = _kpiConnectionString;
				 dt.Columns.Add("ID", typeof(Guid));
			} else if (Product == "Segments") {
				query = @"SELECT ts.PeriodEndDate, ts.FiscalDistance, ts.Duration, ts.PeriodType , ts.Predecessor, ts.Consolidated, ts.IsProForma, ts.ReportType, ts.IsAmended, ts.IsRestated, ts.FiscalYear
 from seg.TimeSeries ts
 join @mappedTimeSlice mts on mts.id = ts.ID 
where ts.Iconum = @Iconum";
				connectionString = _segmentsConnectionString;
				dt.Columns.Add("ID", typeof(int));
			} else if (Product == "Supercore") {
				query = @"SELECT top 10 ts.TimeSliceDate, ts.PeriodLength, ts.PeriodTypeID, ts.CompanyFiscalYear, ts.ReportTypeID, ts.InterimTypeID, ts.ConsolidatedTypeID, ts.AccountTypeID, ts.EncoreFlag 
from dbo.TimeSlice ts 
join @mappedTimeSlice mts on mts.id = ts.ID";
				connectionString = _sfConnectionString;
				dt.Columns.Add("ID", typeof(Guid));
			}
			List<object> timeSlices = GetMappedTimeSlices(Iconum,Product,Year);

			timeSlices.ForEach(o => dt.Rows.Add(o));
		
			using (SqlConnection sqlConn = new SqlConnection(connectionString))
			using (SqlCommand cmd = new SqlCommand(query, sqlConn)) {
				cmd.CommandType = CommandType.Text;
				cmd.Parameters.AddWithValue("@Iconum", Iconum);
				SqlParameter p = new SqlParameter("@mappedTimeSlice", SqlDbType.Structured);
				p.Value = dt;
				p.TypeName = Product == "Segments" ? "[dbo].[tblType_IntList]" : "[dbo].[tblType_GuidList]";
				cmd.Parameters.Add(p);
				sqlConn.Open();
				using (SqlDataReader rdr = cmd.ExecuteReader()) {
					while (rdr.Read()) {
						Dictionary<string, object> ts = new Dictionary<string,object>();
						
						for (int i = 0; i < rdr.FieldCount; i++) {
							ts.Add(rdr.GetName(i), rdr.IsDBNull(i) ? null : rdr.GetValue(i));
							
						}
						details.Add(ts);
					}
				}
				return details;
			}
		}


		private bool EntityExists(int Iconum) {
			bool EntityExists = false;
			string permId = PermId.Iconum2PermId(Iconum);
			if (string.IsNullOrEmpty(permId)) {
				return EntityExists;
			}
			string query = @"
											declare @EntityExists bit = null
											select @EntityExists = Id from TimeSlice.Entity where Iconum = @Iconum and PermId = @PermId
											if(@EntityExists is not null)begin 
												set @EntityExists = 1;
											end
											select isnull(@EntityExists,0)";
			using (SqlConnection sqlConn = new SqlConnection(_sfarConnectionString))
			using (SqlCommand cmd = new SqlCommand(query, sqlConn)) {
				cmd.CommandType = CommandType.Text;
				cmd.Parameters.AddWithValue("@Iconum", Iconum);
				cmd.Parameters.AddWithValue("@PermId", permId);
				sqlConn.Open();
				using (SqlDataReader sdr = cmd.ExecuteReader()) {
					if (sdr.Read()) {
						EntityExists = sdr.GetBoolean(0);
					}
				}
			}
			return EntityExists;
		}

		private int? InsertEntity(int Iconum) {
			int? EntityId = null;
			string permId = PermId.Iconum2PermId(Iconum);
			if (string.IsNullOrEmpty(permId)) {
				return EntityId;
			}
			string query = @"
											declare @EntityId int = null
											insert into TimeSlice.Entity(Iconum,PermId) values (@Iconum,@PermId)
											set @EntityId = SCOPE_IDENTITY();											
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

		public List<TimeSlice> GetTimeSlices(int Iconum, string FiscalYear = "", Guid? DocumentId = null) {
			List<TimeSlice> ts = new List<TimeSlice>();
			string query = @"select ts.ID,TimeSlicePeriodEndDate,PeriodLength,PeriodTypeID,FiscalYear,ReportTypeID,
InterimTypeID,IsConsolidated,IsProforma,IsAmended,IsRestated,AcquisitionStatusID,
FiscalDistance,tsd.DocumentId,tsd.PublicationStampUtc,tsd.FormType,ts.ReportingPeriodEndDate from TimeSlice.TimeSlice ts 
join TimeSlice.Entity e on e.ID = ts.EntityId
join TimeSlice.TimeSliceDocumentMap tsd on tsd.TimeSliceId = ts.ID";
			if (DocumentId == null) {
				query += @" where e.Iconum = @Iconum and FiscalYear = isnull(@FiscalYear,FiscalYear)";
			} else {
				query += @" where ts.ID in (
select tdm.TimeSliceId from TimeSlice.TimeSliceDocumentMap tdm where tdm.DocumentId = @DocumentId
)";
			}	

			using (SqlConnection sqlConn = new SqlConnection(_sfarConnectionString))
			using (SqlCommand cmd = new SqlCommand(query, sqlConn)) {
				cmd.CommandType = CommandType.Text;

				if (DocumentId == null) {
					cmd.Parameters.AddWithValue("@Iconum", Iconum);
					if (string.IsNullOrEmpty(FiscalYear)) {
						cmd.Parameters.AddWithValue("@FiscalYear", DBNull.Value);
					} else {
						cmd.Parameters.AddWithValue("@FiscalYear", FiscalYear);
					}
				} else {
					cmd.Parameters.AddWithValue("@DocumentId", DocumentId.Value);
				}
				sqlConn.Open();
				using (SqlDataReader sdr = cmd.ExecuteReader()) {
					while (sdr.Read()) {
						TimeSlice t = ts.FirstOrDefault(o => o.Id == sdr.GetGuid(0));
						
						if (t == null) {
							t = new TimeSlice
							{
								Id = sdr.GetGuid(0),
								PeriodEndDate = sdr.GetDateTime(1),
								PeriodLength = sdr.GetInt32(2),
								PeriodType = sdr.GetStringSafe(3),
								FiscalYear = sdr.GetDecimal(4),
								InterimType = sdr.GetStringSafe(6),
								IsConsolidated = sdr.GetBoolean(7),
								IsProforma = sdr.GetBoolean(8),
								AcquisitionStatus = sdr.GetStringSafe(11),
								FiscalDistance = sdr.GetInt32(12),
						    ReportingPeriodEndDate = sdr.GetDateTime(16)
							};
							ts.Add(t);
						} 
					
						t.Documents.Add(new TimeSliceDocument
						{

							ReportType = sdr.GetStringSafe(5),
							IsAmended = sdr.GetBoolean(9),
							IsRestated = sdr.GetBoolean(10),
							DocumentId = sdr.GetGuid(13),
							PublicationStamp = sdr.GetDateTime(14),
							FormType = sdr.GetStringSafe(15)
						});

						
					}
				}
			}
			return ts;
		}

		public void GetDocumentMeta(TimeSliceDocument Document) {
			string query = @"
										select  d.PublicationStampUtc,dvm.FormType from Documents d
join vw_DocumentMetaPivot dvm on dvm.DocumentId = d.id
where d.id   = @DocumentId";
			using (SqlConnection sqlConn = new SqlConnection(_damConnectionString))
			using (SqlCommand cmd = new SqlCommand(query, sqlConn)) {
				cmd.CommandType = CommandType.Text;
				cmd.Parameters.AddWithValue("@DocumentId", Document.DocumentId);

				sqlConn.Open();
				using (SqlDataReader sdr = cmd.ExecuteReader()) {
					if (sdr.Read()) {
						Document.PublicationStamp = sdr.GetDateTime(0);
						Document.FormType = sdr.GetStringSafe(1);
					}
				}
			}
		}

		public object GetDocumentMeta(string DocumentId) {
			
			string query = @"
										select dvc.iconum,year(d.PublicationStampUtc), d.PublicationStampUtc,dvm.FormType,d.id,dvm.Period  from Documents d 
join DocumentVersionCompanies dvc on dvc.DocumentVersionId = d.CurrentVersion
join vw_DocumentMetaPivot dvm on dvm.DocumentId = d.id
where dvc.IsPrimary = 1 and dvc.Confidence = 1 and d.id = @DocumentId";
			using (SqlConnection sqlConn = new SqlConnection(_damConnectionString))
			using (SqlCommand cmd = new SqlCommand(query, sqlConn)) {
				cmd.CommandType = CommandType.Text;
				cmd.Parameters.AddWithValue("@DocumentId", DocumentId);
				
				sqlConn.Open();
				using (SqlDataReader sdr = cmd.ExecuteReader()) {
					if (sdr.Read()) {
						return new { Iconum = sdr.GetInt32(0), Year = sdr.GetInt32(1), PublicationStampUtc = sdr.GetDateTime(2) , FormType = sdr.GetStringSafe(3),
												 DocumentId = sdr.GetGuid(4),
												 ReportPED = sdr.GetDateTime(5),
												 ReportType = GetReporttype(sdr.GetGuid(4))
						};
					}
				}
			}
			return null;
		}

		public List<object> GetRPEDDocumentsForIconum(int Iconum, DateTime reportPeriodEndDate) {
			List<object> documents = new List<object>();
			string query = @"
									select  d.PublicationStampUtc,dvm.FormType,d.id,dvm.Period from Documents d
join vw_DocumentMetaPivot dvm on dvm.DocumentId = d.id
join DocumentVersionCompanies dvc on dvc.DocumentVersionId = d.CurrentVersion
where dvc.IsPrimary = 1 and dvc.Confidence = 1 and dvc.iconum = @Iconum and dvm.period between @reportPeriodEndDate - 30 and @reportPeriodEndDate + 30";
			using (SqlConnection sqlConn = new SqlConnection(_damConnectionString))
			using (SqlCommand cmd = new SqlCommand(query, sqlConn)) {
				cmd.CommandType = CommandType.Text;
				cmd.Parameters.AddWithValue("@Iconum", Iconum);
				cmd.Parameters.AddWithValue("@reportPeriodEndDate", reportPeriodEndDate);
				sqlConn.Open();
				using (SqlDataReader sdr = cmd.ExecuteReader()) {
					while (sdr.Read()) {
						documents.Add(new { PublicationStampUtc = sdr.GetDateTime(0) , DocumentId = sdr.GetGuid(2), 
							FormType = sdr.GetStringSafe(1) , ReportingPeriodEndDate = sdr.GetDateTimeSafe(3) ,
							ReportType = GetReporttype(sdr.GetGuid(2))
						});
					}
				}
			}
			return documents;
		}

		private string GetReporttype(Guid DocumentId){
			string ReportType = "";
			string query = @"
									SELECT ReportTypeID from TimeSlice.TimeSliceDocumentMap where DocumentId = @DocumentId";
			using (SqlConnection sqlConn = new SqlConnection(_sfarConnectionString))
			using (SqlCommand cmd = new SqlCommand(query, sqlConn)) {
				cmd.CommandType = CommandType.Text;
				cmd.Parameters.AddWithValue("@DocumentId", DocumentId);
				sqlConn.Open();
				using (SqlDataReader sdr = cmd.ExecuteReader()) {
					if (sdr.Read()) {
						ReportType = sdr.GetStringSafe(0);
					}
				}
			}
			return ReportType;
		}

		public bool RemoveDocumentLink(Guid DocumentId , Guid TimeSliceId) {
	
			string query = @"
										delete from  [TimeSlice].[TimeSliceDocumentMap] where TimeSliceId = @TimeSliceId and DocumentId = @DocumentId";
			try {
				using (SqlConnection sqlConn = new SqlConnection(_sfarConnectionString))
				using (SqlCommand cmd = new SqlCommand(query, sqlConn)) {
					cmd.CommandType = CommandType.Text;
					cmd.Parameters.AddWithValue("@DocumentId", DocumentId);
					cmd.Parameters.AddWithValue("@TimeSliceId", TimeSliceId);
					sqlConn.Open();
					cmd.ExecuteNonQuery();
				}
			} catch {
				return false;
			}

			return true;
		}
	}
}