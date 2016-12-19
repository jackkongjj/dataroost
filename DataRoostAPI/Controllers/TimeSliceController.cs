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
		public bool MigrateIconumTimeSlices(string CompanyId) {
			int iconum = PermId.PermId2Iconum(CompanyId);

			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			string kpiConnectionString = ConfigurationManager.ConnectionStrings["FFKPI"].ToString();
			string sfarConnectionString = ConfigurationManager.ConnectionStrings["SFAR-Diff"].ToString();
			string segmentsConnectionString = ConfigurationManager.ConnectionStrings["FFSegments"].ToString();
			string damConnectionString = ConfigurationManager.ConnectionStrings["FFDAM"].ConnectionString;
			DocumentHelper documentHelper = new DocumentHelper(sfConnectionString, kpiConnectionString,segmentsConnectionString,sfarConnectionString,damConnectionString);
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
			string damConnectionString = ConfigurationManager.ConnectionStrings["FFDAM"].ConnectionString;
			DocumentHelper documentHelper = new DocumentHelper(sfConnectionString, kpiConnectionString, segmentsConnectionString, sfarConnectionString,damConnectionString);
			return documentHelper.CreateTimeSlice(iconum,TimeSlice);
		}

		[Route("Iconum")]
		[HttpGet]
		public List<TimeSlice> GetTimeSlices(string CompanyId) {
			int iconum = PermId.PermId2Iconum(CompanyId);

			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			string kpiConnectionString = ConfigurationManager.ConnectionStrings["FFKPI"].ToString();
			string sfarConnectionString = ConfigurationManager.ConnectionStrings["SFAR-Diff"].ToString();
			string segmentsConnectionString = ConfigurationManager.ConnectionStrings["FFSegments"].ToString();
			string damConnectionString = ConfigurationManager.ConnectionStrings["FFDAM"].ConnectionString;
			DocumentHelper documentHelper = new DocumentHelper(sfConnectionString, kpiConnectionString, segmentsConnectionString, sfarConnectionString,damConnectionString);
			return documentHelper.GetTimeSlices(iconum);
		}


		[Route("DocumentMeta/{DocumentId}")]
		[HttpGet]
		public object GetDocumentMeta(string DocumentId) {
		
			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			string kpiConnectionString = ConfigurationManager.ConnectionStrings["FFKPI"].ToString();
			string sfarConnectionString = ConfigurationManager.ConnectionStrings["SFAR-Diff"].ToString();
			string segmentsConnectionString = ConfigurationManager.ConnectionStrings["FFSegments"].ToString();
			string damConnectionString = ConfigurationManager.ConnectionStrings["FFDAM"].ConnectionString;
			DocumentHelper documentHelper = new DocumentHelper(sfConnectionString, kpiConnectionString, segmentsConnectionString, sfarConnectionString,damConnectionString);
			return documentHelper.GetDocumentMeta(DocumentId);
		}

		[Route("Year/{Year}")]
		[HttpGet]
		public List<TimeSlice> GetTimeSlices(string CompanyId, string Year) {
			int iconum = PermId.PermId2Iconum(CompanyId);

			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			string kpiConnectionString = ConfigurationManager.ConnectionStrings["FFKPI"].ToString();
			string sfarConnectionString = ConfigurationManager.ConnectionStrings["SFAR-Diff"].ToString();
			string segmentsConnectionString = ConfigurationManager.ConnectionStrings["FFSegments"].ToString();
			string damConnectionString = ConfigurationManager.ConnectionStrings["FFDAM"].ConnectionString;
			DocumentHelper documentHelper = new DocumentHelper(sfConnectionString, kpiConnectionString, segmentsConnectionString, sfarConnectionString, damConnectionString);
			return documentHelper.GetTimeSlices(iconum, Year);
		}



		[Route("Document/{DocumentId}")]
		[HttpGet]
		public List<TimeSlice> GetTimeSlices(string CompanyId, Guid DocumentId) {
			int iconum = PermId.PermId2Iconum(CompanyId);

			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			string kpiConnectionString = ConfigurationManager.ConnectionStrings["FFKPI"].ToString();
			string sfarConnectionString = ConfigurationManager.ConnectionStrings["SFAR-Diff"].ToString();
			string segmentsConnectionString = ConfigurationManager.ConnectionStrings["FFSegments"].ToString();
			string damConnectionString = ConfigurationManager.ConnectionStrings["FFDAM"].ConnectionString;
			DocumentHelper documentHelper = new DocumentHelper(sfConnectionString, kpiConnectionString, segmentsConnectionString, sfarConnectionString, damConnectionString);
			return documentHelper.GetTimeSlices(iconum, "", DocumentId);
		}


		[Route("Product/{ProductId}/Year/{Year}/Diff/{IsDiff}")]
		[HttpGet]
		public object GetProductTimeSlices(string CompanyId, string ProductId, string Year, bool IsDiff) {
			int iconum = PermId.PermId2Iconum(CompanyId);

			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			string kpiConnectionString = ConfigurationManager.ConnectionStrings["FFKPI"].ToString();
			string sfarConnectionString = ConfigurationManager.ConnectionStrings["SFAR-Diff"].ToString();
			string segmentsConnectionString = ConfigurationManager.ConnectionStrings["FFSegments"].ToString();
			string damConnectionString = ConfigurationManager.ConnectionStrings["FFDAM"].ConnectionString;
			DocumentHelper documentHelper = new DocumentHelper(sfConnectionString, kpiConnectionString, segmentsConnectionString, sfarConnectionString, damConnectionString);
			return IsDiff ? documentHelper.DiffTimeSlices(iconum,ProductId,Year,null) : documentHelper.GetProductTimeSlices(iconum, ProductId,Year);
		}

		[Route("Product/{ProductId}/Document/{DocumentId}/Diff/{IsDiff}")]
		[HttpGet]
		public object GetProductTimeSlices(string CompanyId, string ProductId, Guid DocumentId, bool IsDiff) {
			int iconum = PermId.PermId2Iconum(CompanyId);

			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			string kpiConnectionString = ConfigurationManager.ConnectionStrings["FFKPI"].ToString();
			string sfarConnectionString = ConfigurationManager.ConnectionStrings["SFAR-Diff"].ToString();
			string segmentsConnectionString = ConfigurationManager.ConnectionStrings["FFSegments"].ToString();
			string damConnectionString = ConfigurationManager.ConnectionStrings["FFDAM"].ConnectionString;
			DocumentHelper documentHelper = new DocumentHelper(sfConnectionString, kpiConnectionString, segmentsConnectionString, sfarConnectionString, damConnectionString);
			return IsDiff ? documentHelper.DiffTimeSlices(iconum, ProductId, "", DocumentId) :  documentHelper.GetProductTimeSlices(iconum, ProductId, "", DocumentId);
		}


		[Route("RPEDDocuments")]
		[HttpGet]
		public List<object> GetRPEDDocumentsForIconum(string CompanyId, [FromUri]DateTime reportPeriodEndDate) {
			int iconum = PermId.PermId2Iconum(CompanyId);

			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			string kpiConnectionString = ConfigurationManager.ConnectionStrings["FFKPI"].ToString();
			string sfarConnectionString = ConfigurationManager.ConnectionStrings["SFAR-Diff"].ToString();
			string segmentsConnectionString = ConfigurationManager.ConnectionStrings["FFSegments"].ToString();
			string damConnectionString = ConfigurationManager.ConnectionStrings["FFDAM"].ConnectionString;
			DocumentHelper documentHelper = new DocumentHelper(sfConnectionString, kpiConnectionString, segmentsConnectionString, sfarConnectionString, damConnectionString);
			return documentHelper.GetRPEDDocumentsForIconum(iconum, reportPeriodEndDate);
		}


		[Route("{TimeSliceId}/Document/{DocumentId}")]
		[HttpGet]
		public bool RemoveDocumentLink(string CompanyId, Guid DocumentId, Guid TimeSliceId) {
			int iconum = PermId.PermId2Iconum(CompanyId);

			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			string kpiConnectionString = ConfigurationManager.ConnectionStrings["FFKPI"].ToString();
			string sfarConnectionString = ConfigurationManager.ConnectionStrings["SFAR-Diff"].ToString();
			string segmentsConnectionString = ConfigurationManager.ConnectionStrings["FFSegments"].ToString();
			string damConnectionString = ConfigurationManager.ConnectionStrings["FFDAM"].ConnectionString;
			DocumentHelper documentHelper = new DocumentHelper(sfConnectionString, kpiConnectionString, segmentsConnectionString, sfarConnectionString, damConnectionString);
			return documentHelper.RemoveDocumentLink(DocumentId,TimeSliceId);
		}

		[Route("{TimeSliceId}")]
		[HttpPost]
		public bool UpsertDocumentLink(Guid TimeSliceId,[FromBody]TimeSliceDocument Document) {
	
			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			string kpiConnectionString = ConfigurationManager.ConnectionStrings["FFKPI"].ToString();
			string sfarConnectionString = ConfigurationManager.ConnectionStrings["SFAR-Diff"].ToString();
			string segmentsConnectionString = ConfigurationManager.ConnectionStrings["FFSegments"].ToString();
			string damConnectionString = ConfigurationManager.ConnectionStrings["FFDAM"].ConnectionString;
			DocumentHelper documentHelper = new DocumentHelper(sfConnectionString, kpiConnectionString, segmentsConnectionString, sfarConnectionString, damConnectionString);
			return documentHelper.UpsertDocumentLink(Document, TimeSliceId);
		}




	}
}
