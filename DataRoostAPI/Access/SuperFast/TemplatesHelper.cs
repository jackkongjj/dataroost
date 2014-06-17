using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Web;
using CCS.Fundamentals.DataRoostAPI.Models;
using FactSet.Data.SqlClient;

namespace CCS.Fundamentals.DataRoostAPI.Access.SuperFast {
	public enum StandardizationType {
		SDB,
		STD
	}

	public class TemplatesHelper {
		private readonly string connectionString;
		private int iconum;
		private StandardizationType dataType;

		public TemplatesHelper(string connectionString, int iconum, StandardizationType dataType) {
			this.connectionString = connectionString;
			this.iconum = iconum;
			this.dataType = dataType;
		}

		public TemplateDTO[] GetTemplates(string templateId) {
			const string SQL_SDB_Templates = @"select distinct sdm.TemplateName, std.ReportTypeID, std.UpdateTypeID, std.TemplateTypeId
    from CompanyIndustry ci 
    Join SDBtemplateDetail std 
        on ci.IndustryDetailID = std.IndustryDetailId
    join DocumentSeries ds 
        on ci.Iconum = ds.CompanyId
    join Document d 
        on ds.Id = d.DocumentSeriesId
    join FDSTriPPIMap f 
        on d.PPI = f.PPI
    join SDBCountryGroupCountries sc 
        on sc.CountriesIsoCountry = f.IsoCountry
    join SDBCountryGroup sg 
        on sc.SDBCountryGroupID = sg.Id
        and std.SDBCountryGroupID = sg.ID
    join SDBTemplateMaster sdm 
        on std.SDBTemplateMasterId = sdm.Id
where ci.Iconum = @iconum and 
(std.ReportTypeID = isnull(@reportTypeId, std.ReportTypeID) 
and std.UpdateTypeID = isnull(@updateTypeId, std.UpdateTypeID) and std.TemplateTypeId = isnull(@templateTypeId, std.TemplateTypeId))";

			const string SQL_STD_Templates = @"select distinct sdm.TemplateName, std.ReportTypeID, std.UpdateTypeID, std.TemplateTypeId
    from CompanyIndustry ci 
    join STDTemplateDetail std 
        on ci.IndustryDetailID = std.IndustryDetailId
    join DocumentSeries ds 
        on ci.Iconum = ds.CompanyId
    join Document d 
        on ds.Id = d.DocumentSeriesId
    join FDSTriPPIMap f 
        on d.PPI = f.PPI
    join STDCountryGroupCountries sc 
        on sc.CountriesIsoCountry = f.IsoCountry
    join STDCountryGroup sg 
        on sc.STDCountryGroupID = sg.Id
        and std.STDCountryGroupID = sg.ID
    join STDTemplateMaster sdm 
        on std.STDTemplateMasterCode = sdm.Code
where ci.Iconum = @iconum and 
(std.ReportTypeID = isnull(@reportTypeId, std.ReportTypeID) 
and std.UpdateTypeID = isnull(@updateTypeId, std.UpdateTypeID) and std.TemplateTypeId = isnull(@templateTypeId, std.TemplateTypeId))";

			List<TemplateDTO> templates = new List<TemplateDTO>();
			bool requestedSpecificTemplate = (templateId != null);

			using (SqlConnection conn = new SqlConnection(connectionString)) {
				conn.Open();
				using (SqlCommand cmd = new SqlCommand(this.dataType == StandardizationType.SDB ? SQL_SDB_Templates : SQL_STD_Templates, conn)) {
					cmd.Parameters.Add(new SqlParameter("@iconum", SqlDbType.Int) { Value = this.iconum });
					if (!requestedSpecificTemplate) {
						cmd.Parameters.Add(new SqlParameter("@reportTypeId", SqlDbType.NVarChar, 64) { Value = DBNull.Value });
						cmd.Parameters.Add(new SqlParameter("@updateTypeId", SqlDbType.NVarChar, 64) { Value = DBNull.Value });
						cmd.Parameters.Add(new SqlParameter("@templateTypeId", SqlDbType.NVarChar, 64) { Value = DBNull.Value });
					} else {
						var templateIdToken = TemplateIdentifier.GetTemplateIdentifier(templateId);
						cmd.Parameters.Add(new SqlParameter("@reportTypeId", SqlDbType.NVarChar, 64) { Value = templateIdToken.ReportType });
						cmd.Parameters.Add(new SqlParameter("@updateTypeId", SqlDbType.NVarChar, 64) { Value = templateIdToken.UpdateType });
						cmd.Parameters.Add(new SqlParameter("@templateTypeId", SqlDbType.Int) { Value = templateIdToken.TemplateType });
					}

					using (SqlDataReader reader = cmd.ExecuteReader()) {
						templates.AddRange(
										reader.Cast<IDataRecord>().Select(r => new TemplateDTO()
										{
											Id = new TemplateIdentifier() {
												UpdateType = reader.GetString(2),
												ReportType = reader.GetString(1),
												TemplateType = reader.GetInt32(3)
											}.GetToken(),
											Name = reader.GetString(0),
											ReportType = reader.GetString(1),
											UpdateType = reader.GetString(2),
											TemplateType = reader.GetInt32(3)
										}));
					}
				}
			}

			if (requestedSpecificTemplate) {
				foreach (var template in templates) {
					template.Items = PopulateTemplateItem(TemplateIdentifier.GetTemplateIdentifier(template.Id));
				}
			}
			return templates.ToArray();
		}

		public int GetTemplateMasterId(TemplateIdentifier templateId) {
			const string SQL_SDB_Master = @"
select distinct sdm.Id
from CompanyIndustry ci 
join SDBtemplateDetail std on ci.IndustryDetailID = std.IndustryDetailId
join DocumentSeries ds on ci.Iconum = ds.CompanyId
join Document d on ds.Id = d.DocumentSeriesId
join FDSTriPPIMap f on d.PPI = f.PPI
join SDBCountryGroupCountries sc on sc.CountriesIsoCountry = f.IsoCountry
join SDBCountryGroup sg on sc.SDBCountryGroupID = sg.Id
    and std.SDBCountryGroupID = sg.ID
join SDBTemplateMaster sdm on std.SDBTemplateMasterId = sdm.Id
where ci.Iconum = @iconum
	and std.ReportTypeID = @reportTypeId
	and std.UpdateTypeID = @updateTypeId
	and std.TemplateTypeId = @templateTypeId
";

			using (SqlConnection conn = new SqlConnection(connectionString)) {
				conn.Open();

				if (dataType == StandardizationType.STD)
					throw new InvalidOperationException("Unable to get Master for STD Template");

				using (SqlCommand cmd = new SqlCommand(this.dataType == StandardizationType.SDB ? SQL_SDB_Master : null, conn)) {
					cmd.Parameters.Add(new SqlParameter("@iconum", SqlDbType.Int) { Value = this.iconum });
					cmd.Parameters.Add(new SqlParameter("@reportTypeId", SqlDbType.NVarChar, 64) { Value = templateId.ReportType });
					cmd.Parameters.Add(new SqlParameter("@updateTypeId", SqlDbType.NVarChar, 64) { Value = templateId.UpdateType });
					cmd.Parameters.Add(new SqlParameter("@templateTypeId", SqlDbType.Int) { Value = templateId.TemplateType });

					using (SqlDataReader reader = cmd.ExecuteReader(CommandBehavior.SingleResult | CommandBehavior.SingleRow)) {
						reader.Read();
						return reader.GetInt32(0);
					}
				}
			}
		}

		private List<TemplateItemDTO> PopulateTemplateItem(TemplateIdentifier templateId) {
			const string SQL_SDB_Items = @"WITH TemplateMasterID AS
(
	select distinct sdm.id 
    from CompanyIndustry ci 
    Join SDBtemplateDetail std 
        on ci.IndustryDetailID = std.IndustryDetailId
    join DocumentSeries ds 
        on ci.Iconum = ds.CompanyId
    join Document d 
        on ds.Id = d.DocumentSeriesId
    join FDSTriPPIMap f 
        on d.PPI = f.PPI
    join SDBCountryGroupCountries sc 
        on sc.CountriesIsoCountry = f.IsoCountry
    join SDBCountryGroup sg 
        on sc.SDBCountryGroupID = sg.Id
        and std.SDBCountryGroupID = sg.ID
    join SDBTemplateMaster sdm 
        on std.SDBTemplateMasterId = sdm.Id
	where ci.Iconum = @iconum
	AND  std.ReportTypeID = @reportTypeId
	AND std.UpdateTypeID = @updateTypeId
	AND std.TemplateTypeId = @templateTypeId
)
select s.Id, [code] = s.SDBCode, [sdbDescription] = s.Description, [statementTypeId] = st.ID, [usageType] = iut.ID, 
	[indentLevel] = sti.SDBItemLevel, [valueType] = sit.Id, s.SecurityFlag, s.PITFlag, [precision] = s.NoDecimals
from SDBTemplateItem sti (nolock)
join SDBitem s (Nolock)
	on sti.SDBItemID = s.ID
join StatementType st (nolock)
	on s.StatementTypeId = st.Id
join SDBItemTypes sit (nolock)
	on s.SDBitemTypeId = sit.Id
join ItemUsageType iut (nolock)
	on s.ItemUsageTypeId = iut.Id
join TemplateMasterID tmi on tmi.id = sti.SDBTemplateMasterId
order by sti.SDBItemSequence asc";

			const string SQL_STD_Items = @"WITH TemplateMasterID AS
(
	select distinct sdm.code
		from CompanyIndustry ci 
		join STDTemplateDetail std 
			on ci.IndustryDetailID = std.IndustryDetailId
		join DocumentSeries ds 
			on ci.Iconum = ds.CompanyId
		join Document d 
			on ds.Id = d.DocumentSeriesId
		join FDSTriPPIMap f 
			on d.PPI = f.PPI
		join STDCountryGroupCountries sc 
			on sc.CountriesIsoCountry = f.IsoCountry
		join STDCountryGroup sg 
			on sc.STDCountryGroupID = sg.Id
			and std.STDCountryGroupID = sg.ID
		join STDTemplateMaster sdm 
			on std.STDTemplateMasterCode = sdm.Code
	where ci.Iconum = @iconum
	AND  std.ReportTypeID = @reportTypeId
	AND std.UpdateTypeID = @updateTypeId
	AND std.TemplateTypeId = @templateTypeId
)
select s.Id, [code] = s.STDCode, [sdbDescription] = s.ItemShortName, [statementTypeId] = st.ID, [usageType] = iut.ID, 
	[indentLevel] = 0, [valueType] = sit.Id, s.SecurityFlag, s.PITFlag, [precision] = s.NoDecimals
from STDTemplateItem sti (nolock)
join STDitem s (Nolock) on sti.STDItemID = s.ID
join StatementType st (nolock) on s.StatementTypeId = st.Id
join STDItemTypes sit (nolock) on s.STDitemTypeId = sit.Id
join ItemUsageType iut (nolock) on s.ItemUsageTypeId = iut.Id 
join TemplateMasterID tmi on tmi.code = sti.STDTemplateMasterCode
order by sti.STDItemSequence asc";

			using (SqlConnection conn = new SqlConnection(connectionString)) {
				conn.Open();
				using (SqlCommand cmd = new SqlCommand(this.dataType == StandardizationType.SDB ? SQL_SDB_Items : SQL_STD_Items, conn)) {
					cmd.Parameters.Add(new SqlParameter("@iconum", SqlDbType.Int) { Value = this.iconum });
					cmd.Parameters.Add(new SqlParameter("@reportTypeId", SqlDbType.NVarChar, 64) { Value = templateId.ReportType });
					cmd.Parameters.Add(new SqlParameter("@updateTypeId", SqlDbType.NVarChar, 64) { Value = templateId.UpdateType });
					cmd.Parameters.Add(new SqlParameter("@templateTypeId", SqlDbType.Int) { Value = templateId.TemplateType });
					using (SqlDataReader reader = cmd.ExecuteReader()) {
						return reader.Cast<IDataRecord>().Select(r => new TemplateItemDTO()
						{
							Id = reader.GetInt32(0),
							Code = reader.GetString(1),
							Description = reader.GetString(2),
							StatementTypeId = reader.GetString(3),
							UsageType = reader.GetString(4),
							IndentLevel = reader.GetInt32(5),
							ValueType = reader.GetString(6),
							IsSecurity = reader.GetBoolean(7),
							IsPIT = reader.GetBoolean(8),
							Precision = reader.GetByte(9)
						}).ToList<TemplateItemDTO>();
					}
				}
			}
		}
	}
}