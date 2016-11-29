using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;
using CCS.Fundamentals.DataRoostAPI.Access;
using CCS.Fundamentals.DataRoostAPI.Access.Timeslice;
using DataRoostAPI.Common.Models;

namespace CCS.Fundamentals.DataRoostAPI.Controllers {
	[RoutePrefix("api/v1/companies/{CompanyId}/TimeSlice")]
	public class TimeSliceController : ApiController {


		[Route("migrate")]
		[HttpGet]
		public bool MigrateTimeSlice(string CompanyId) {
			int iconum = PermId.PermId2Iconum(CompanyId);

			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			string kpiConnectionString = ConfigurationManager.ConnectionStrings["FFKPI"].ToString();
			string sfarConnectionString = ConfigurationManager.ConnectionStrings["SFAR-Diff"].ToString();
			string segmentsConnectionString = ConfigurationManager.ConnectionStrings["FFSegments"].ToString();
			DocumentHelper documentHelper = new DocumentHelper(sfConnectionString, kpiConnectionString,segmentsConnectionString,sfarConnectionString);
		  return documentHelper.MigrateIconumTimeSlices(iconum);
		}


		[Route("create")]
		[HttpPost]
		public bool CreateTimeSlice(string CompanyId , [FromBody] TimeSlice TimeSlice) {
			int iconum = PermId.PermId2Iconum(CompanyId);

			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			string kpiConnectionString = ConfigurationManager.ConnectionStrings["FFKPI"].ToString();
			string sfarConnectionString = ConfigurationManager.ConnectionStrings["SFAR-Diff"].ToString();
			string segmentsConnectionString = ConfigurationManager.ConnectionStrings["FFSegments"].ToString();
			DocumentHelper documentHelper = new DocumentHelper(sfConnectionString, kpiConnectionString, segmentsConnectionString, sfarConnectionString);
			return documentHelper.CreateTimeSlice(iconum,TimeSlice);
		}


	}
}
