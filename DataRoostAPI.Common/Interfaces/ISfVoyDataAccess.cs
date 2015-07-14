using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using DataRoostAPI.Common.Models;
using DataRoostAPI.Common.Models.SfVoy;

namespace DataRoostAPI.Common.Interfaces {
	public interface ISfVoyDataAccess {
		StandardizationType[] GetDataTypes(string companyId);

		TemplateDTO[] GetTemplateList(string companyId, StandardizationType standardizationType);

		TemplateDTO GetTemplate(string companyId, StandardizationType standardizationType, string templateId);

		SfVoyTimeSeries[] GetTimeseriesList(string companyId, StandardizationType standardizationType, string templateId);

		SfVoyTimeSeries[] GetTimeseriesList(string companyId, StandardizationType standardizationType, string templateId, int startYear, int endYear);

		SfVoyTimeSeries[] GetTimeseriesListWithValue(string companyId, StandardizationType standardizationType, string templateId, int year);

		SfVoyTimeSeries GetTimeseries(string companyId, StandardizationType standardizationType, string templateId, string timeseriesId);

	}
}
