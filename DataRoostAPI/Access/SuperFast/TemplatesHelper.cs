using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Web;

using DataRoostAPI.Common.Models;
using DataRoostAPI.Common.Models.SuperFast;

using FactSet.Data.SqlClient;

namespace CCS.Fundamentals.DataRoostAPI.Access.SuperFast {

	public class TemplatesHelper {
		private readonly string connectionString;
		private int iconum;
		private StandardizationType dataType;

		public TemplatesHelper(string connectionString, int iconum, StandardizationType dataType) {
			this.connectionString = connectionString;
			this.iconum = iconum;
			this.dataType = dataType;
		}

		public TemplateDTO[] GetTemplates(string templateId, bool showMemoField = false) {
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
and std.UpdateTypeID = isnull(@updateTypeId, std.UpdateTypeID) and std.TemplateTypeId = isnull(@templateTypeId, std.TemplateTypeId))
union  --temporary always show pension template
select sdm.TemplateName, 'A', ut.id, 1 from sdbtemplatemaster sdm, updatetype ut where sdm.templatename = 'SF Full - Pension' and ut.[description] = 'Pension Update'";

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
and std.UpdateTypeID = isnull(@updateTypeId, std.UpdateTypeID) and std.TemplateTypeId = isnull(@templateTypeId, std.TemplateTypeId))
union  --temporary always show pension template
select sdm.TemplateName, 'A', ut.id, 1 from STDTemplateMaster sdm, updatetype ut where sdm.templatename = 'SF Full - Pension' and ut.[description] = 'Pension Update'";

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
										reader.Cast<IDataRecord>().Select(r => new SFTemplateDTO()
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
					template.Items = PopulateTemplateItem(TemplateIdentifier.GetTemplateIdentifier(template.Id), showMemoField);
				}
			}
			return templates.ToArray();
		}

		private List<TemplateItemDTO> PopulateTemplateItem(TemplateIdentifier templateId, bool showMemoField) {
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
	UNION
	select id from SDBTemplateMaster where TemplateName = 'SF Full - Pension' AND  'A' = @reportTypeId
	AND 'N' = @updateTypeId
	AND 1 = @templateTypeId
)
select Id, code, sdbDescription, statementTypeId, usageType, indentLevel, valueType, SecurityFlag, PITFlag, [precision]
from (
	select s.Id, [code] = s.SDBCode, [sdbDescription] = s.Description, [statementTypeId] = st.ID, [usageType] = iut.ID, 
		[indentLevel] = sti.SDBItemLevel, [valueType] = sit.Id, s.SecurityFlag, s.PITFlag, [precision] = s.NoDecimals, sti.SDBItemSequence
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
	union
	select s.Id, [code] = s.SDBCode, [sdbDescription] = s.Description, [statementTypeId] = st.ID, [usageType] = iut.ID, 
		[indentLevel] = 0, [valueType] = sit.Id, s.SecurityFlag, s.PITFlag, [precision] = s.NoDecimals, sti.SDBItemSequence
	from SDBTemplateItemMemo sti (nolock)
	join SDBitem s (Nolock)
		on sti.SDBItemID = s.ID
	join StatementType st (nolock)
		on s.StatementTypeId = st.Id
	join SDBItemTypes sit (nolock)
		on s.SDBitemTypeId = sit.Id
	join ItemUsageType iut (nolock)
		on s.ItemUsageTypeId = iut.Id
	where sti.isVisible = @showMemoField
) as A
order by A.SDBItemSequence asc";

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
	UNION
	select code from STDTemplateMaster where TemplateName = 'SF Full - Pension' AND  'A' = @reportTypeId
	AND 'N' = @updateTypeId
	AND 1 = @templateTypeId
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
					cmd.Parameters.Add(new SqlParameter("@showMemoField", SqlDbType.Bit) { Value = showMemoField });
					using (SqlDataReader reader = cmd.ExecuteReader()) {
						return reader.Cast<IDataRecord>().Select(r => new TemplateItemDTO()
						{
							Id = reader.GetInt32(0).ToString(),
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