using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using DataRoostAPI.Common.Models;

namespace DataRoostAPI.Common.Interfaces {
	public interface IStandardizedDataAccess {

		StandardizationType[] GetDataTypes(string companyId);

		TemplateDTO[] GetTemplateList(string companyId, StandardizationType standardizationType);

		TemplateDTO GetTemplate(string companyId, StandardizationType standardizationType, string templateId);

		TimeseriesDTO[] GetTimeseriesList(string companyId, StandardizationType standardizationType, string templateId);

		TimeseriesDTO GetTimeseries(string companyId, StandardizationType standardizationType, string templateId, string timeseriesId);

	}
}
