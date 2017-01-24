﻿using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;
using CCS.Fundamentals.DataRoostAPI.Access.TINT;
using DataRoostAPI.Common.Models.TINT;

namespace CCS.Fundamentals.DataRoostAPI.Controllers {
	[RoutePrefix("api/v1/companies/{CompanyId}/efforts/tint")]
	public class TintController : ApiController {

		[Route("documents/{documentId}")]
		[HttpGet]
		public Dictionary<byte, Tint> GetTINT(string documentId) {

			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			string damConnectionString = ConfigurationManager.ConnectionStrings["FFDAM"].ToString();
			DocumentHelper documentHelper = new DocumentHelper(sfConnectionString, damConnectionString);
			return documentHelper.GetTintFiles(documentId);
		}


		[Route("insert/{documentId}")]
		[HttpGet]
		public void GetTINT(string documentId, string CompanyId) {

			string sfConnectionString = ConfigurationManager.ConnectionStrings["FFDocumentHistory"].ToString();
			string damConnectionString = ConfigurationManager.ConnectionStrings["FFDAM"].ToString();
			CCS.Fundamentals.DataRoostAPI.Access.AsReported.DocumentHelper documentHelper = new CCS.Fundamentals.DataRoostAPI.Access.AsReported.DocumentHelper(sfConnectionString, damConnectionString);
			documentHelper.InsertTINTOffsets(documentId, int.Parse(CompanyId));
		}

	}

}
