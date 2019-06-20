using DataRoostAPI.Common.Models.SuperFast;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.IO.Compression;

namespace CCS.Fundamentals.DataRoostAPI.Helpers
{
    public static class PantheonHelper
    {
        private static string getDocumentSeriesID = @"Select ID From DocumentSeries With (NoLock) Where CompanyID = @iconum";
        private static string getSTDExpressionsForTimeSlice = @"[Supercore].[GetStdTimeSliceDetailForTimeSlice]";

        public static int GetDocSeriesId(int Iconum)
        {
            int docSeriesId = 0;
            try
            {
                using (SqlConnection ffdoc = new SqlConnection(ConfigurationManager.ConnectionStrings["FFDocumentHistoryReadOnly"].ToString()))
                using (SqlCommand sql = new SqlCommand(getDocumentSeriesID, ffdoc))
                {
                    sql.Parameters.AddWithValue("@Iconum", Iconum);
                    sql.CommandType = CommandType.Text;
                    ffdoc.Open();
                    using (SqlDataReader sdr = sql.ExecuteReader())
                    {
                        if (sdr.Read())
                        {
                            docSeriesId = sdr.GetInt32(0);
                        }
                    }
                }
            }
            catch(Exception ex)
            {
                throw ex;
            }
            return docSeriesId;
        }

        public static ExportMaster GetStdTimeSliceDetailForTimeSlice(int iconum, Guid damDocumentId = default(Guid))
        {
            int docSeriesId = GetDocSeriesId(iconum);
            ExportMaster exportDocumentMeta = new ExportMaster();
            List<TimeSlice> timeSlices = new List<TimeSlice>();
            List<STDTimeSliceDetail> stdTimeSliceDetails = new List<STDTimeSliceDetail>();
            try
            {
                using (SqlConnection ffdoc = new SqlConnection(ConfigurationManager.ConnectionStrings["FFDocumentHistoryReadOnly"].ToString()))
                {
                    ffdoc.Open();
                    using (SqlCommand sql = new SqlCommand(getSTDExpressionsForTimeSlice, ffdoc))
                    {
                        sql.Parameters.AddWithValue("@docSeriesId", docSeriesId);
                        sql.Parameters.AddWithValue("@damDocumentId", damDocumentId);
                        sql.CommandType = CommandType.StoredProcedure;
                        using (SqlDataReader sdr = sql.ExecuteReader())
                        {
                            while (sdr.Read())
                            {
                                TimeSlice ts = new TimeSlice();
                                ts.DamDocumentId = damDocumentId;
                                ts.DocSeriesId = docSeriesId;
                                ts.Id = sdr.GetGuid(0);
                                ts.TimeSliceDate = sdr.GetDateTime(1);
                                ts.PeriodLength = sdr.GetInt32(2);
                                ts.PeriodTypeId = sdr.GetString(3);
                                ts.CompanyFiscalYear = sdr.GetDecimal(4);
                                ts.ReportTypeId = sdr.GetString(5);
                                ts.InterimTypeId = !sdr.IsDBNull(6) ? sdr.GetString(6) : "";
                                ts.ConsolidatedTypeId = sdr.GetString(7);
                                ts.CurrencyCode = sdr.GetString(8);
                                ts.ScalingFactorId = sdr.GetString(9);
                                ts.AccountTypeId = sdr.GetString(10);
                                ts.SDBValidatedFlag = sdr.GetBoolean(11);
                                ts.STDValidatedFlag = sdr.GetBoolean(12);
                                ts.GaapTypeID = !sdr.IsDBNull(13) ? sdr.GetString(13) : "";
                                ts.UpdateTypeID = sdr.GetString(14);
                                ts.EncoreFlag = sdr.GetBoolean(15);
                                ts.Auto_InterimType = !sdr.IsDBNull(16) ? sdr.GetString(16) : "";
                                ts.AutoCalcFlag = sdr.GetInt32(17);
                                ts.AuditorsOpinionID = !sdr.IsDBNull(18) ? sdr.GetInt32(18) : 0;
                                ts.FormatCodeCashflowID = !sdr.IsDBNull(19) ? sdr.GetInt32(19) : 0;
                                ts.LongTermInvestmentID = !sdr.IsDBNull(20) ? sdr.GetInt32(20) : 0;
                                ts.IsProspectus = sdr.GetBoolean(21);
                                ts.isQX = sdr.GetBoolean(22);
                                ts.IsDCV = sdr.GetBoolean(31);
                                ts.CollectionTypeId = !sdr.IsDBNull(32) ? sdr.GetString(32) : "";
                                ts.IndustryCountryAssociationID = sdr.GetInt32(33);
                                ts.DocSeriesId = sdr.GetInt32(34);
                                ts.IsExport = sdr.GetBoolean(35);
                                ts.HIndicator = !sdr.IsDBNull(36) ? sdr.GetBoolean(36) : false;
                                ts.IsVoy = !sdr.IsDBNull(37) ? sdr.GetBoolean(37) : false;
                                ts.IsFYC = sdr.GetBoolean(38);
                                ts.PresentationTypeId = sdr.GetInt32(39);
                                timeSlices.Add(ts);
                            }

                            sdr.NextResult();

                            while (sdr.Read())
                            {
                                STDTimeSliceDetail std = new STDTimeSliceDetail();
                                std.DocSeriesId = docSeriesId;
                                std.ID = sdr.GetInt32(0);
                                std.TimeSliceId = sdr.GetGuid(1);
                                std.STDItemID = sdr.GetInt32(2);
                                std.Value = sdr.GetString(3);
                                std.StatementModelDetailId = sdr.GetInt32(7);
                                std.SecurityId = !sdr.IsDBNull(8) ? sdr.GetString(8) : "";
                                std.Cusip = !sdr.IsDBNull(9) ? sdr.GetString(9) : "";
                                std.ModelMasterId = sdr.GetInt32(10);
                                stdTimeSliceDetails.Add(std);
                            }
                        }
                    }
                }
                exportDocumentMeta.timeSlices = timeSlices;
                exportDocumentMeta.stdTimeSliceDetail = stdTimeSliceDetails;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return exportDocumentMeta;
        }

        public static List<StdItem> GetAllStdItems()
        {
            List<StdItem> stdItems = new List<StdItem>();
            string getStdItems = "Select * From dbo.STDItem With(NoLock) Order By Id";
            try
            {
                using (SqlConnection ffdoc = new SqlConnection("Application Name=InteractiveMultiAgent;Data Source=tcp:FFDochistsql-reporting.prod.factset.com;Initial Catalog=FFDocumentHistory;User=svc_ff_StandardizedCol;Password=Cn008hV8rI;Connect Timeout=180;MultipleActiveResultSets=True;"))
                {
                    ffdoc.Open();
                    using (SqlCommand sql = new SqlCommand(getStdItems, ffdoc))
                    {
                        sql.CommandType = CommandType.Text;
                        using (SqlDataReader sdr = sql.ExecuteReader())
                        {
                            while (sdr.Read())
                            {
                                StdItem stdItem = new StdItem();
                                stdItem.Description = sdr.GetString(0);
                                stdItem.id = sdr.GetInt32(15);
                                stdItem.StdCode = sdr.GetString(14);
                                stdItems.Add(stdItem);
                            }
                        }
                    }
                }
            }
            catch(Exception ex)
            {
                throw ex;
            }
            return stdItems;
        }

        public static ExportMaster GetDataFromDFS(Guid DamDocumentId, int iconum)
        {
            string dfsPath = ConfigurationManager.AppSettings["PantheonBackupDFS"];
            ExportMaster dictionary = UnZIPFromDFS(dfsPath, DamDocumentId.ToString() + '_' + iconum.ToString());
            return dictionary;
        }

        public static ExportMaster UnZIPFromDFS(string dfsPath, string FileName)
        {
            ExportMaster outputExport = new ExportMaster();
            try
            {
                using (FileStream fs = new FileStream(Path.Combine(dfsPath, FileName), FileMode.Open))
                {
                    using (GZipStream compressor = new GZipStream(fs, CompressionMode.Decompress))
                    {
                        using (var streamWriter = new StreamReader(compressor))
                        {
                            using (var jsonWriter = new JsonTextReader(streamWriter))
                            {
                                var jsonSerializer = new JsonSerializer();
                                outputExport = jsonSerializer.Deserialize<ExportMaster>(jsonWriter);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return outputExport;
        }
    }
}