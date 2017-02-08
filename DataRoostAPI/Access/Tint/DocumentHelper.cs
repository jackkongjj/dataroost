using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Web;
using System.Xml.Linq;
using FactSet.DocumentAcquisition.BatchProcessing.Net;
using DataRoostAPI.Common.Models.TINT;
using FactSet.Util;

namespace CCS.Fundamentals.DataRoostAPI.Access.TINT {
	public class DocumentHelper {

		private readonly string _sfConnectionString;
		private readonly string _damConnectionString;

		public DocumentHelper(string sfConnectionString, string damConnectionString) {
			_sfConnectionString = sfConnectionString;
			_damConnectionString = damConnectionString;
		}

		public Dictionary<byte, Tint> GetTintFiles(string documentId) {

			Dictionary<byte, Tint> tintFiles = new Dictionary<byte, Tint>();
			foreach (var documentFiles in GetFceDocumentFiles(new Guid(documentId), "Superfast").Where(d => d.FileType == FactSet.DocumentAcquisition.Data.FileType.GetValue("TINT")).OrderBy(o => o.FileId).GroupBy(o => o.RootId)) {
				DocumentFile tintFile = documentFiles.Last();
				XElement TintElement = null;
				((Action)delegate { TintElement = UnZip(tintFile.Fetch(true)); }).TryTimes(3);
				if (TintElement != null) {
					tintFiles.Add(documentFiles.Key, new Tint(TintElement));
				}
			}
			return tintFiles;
		}

		private XElement UnZip(Stream stream) {
			using (GZipStream gzip = new GZipStream(stream, CompressionMode.Decompress))
			using (StreamReader sr = new StreamReader(gzip, Encoding.UTF8)) {
				return XElement.Load(sr);
			}
		}

		private List<DocumentFile> GetFceDocumentFiles(Guid damDocumentId, string productname) {

			List<byte> documentRoots = new List<byte>();
			const string SQL_FETCH =
@"select drp.RootId
	from vw_DocumentRootProductRelevancy drp 
where drp.DocumentId = @DocumentId
	and drp.ProductDisplayName = @ProdName
	and drp.IsRelevant = 1";

			using (SqlConnection conn = new SqlConnection(_damConnectionString))
			using (SqlCommand cmd = new SqlCommand(SQL_FETCH, conn)) {
				cmd.Parameters.AddWithValue("@DocumentId", damDocumentId);
				cmd.Parameters.AddWithValue("@ProdName", productname);
				conn.Open();
				using (SqlDataReader dr = cmd.ExecuteReader(CommandBehavior.CloseConnection | CommandBehavior.SequentialAccess | CommandBehavior.SingleResult)) {
					documentRoots = dr.Cast<IDataRecord>().Select(r => r.GetByte(0)).ToList();
				}
			}

			List<DocumentFile> files = DocProxyHelper.GetDocumentFiles(damDocumentId).ToList();

			var result = files.Where(f => documentRoots.Contains(f.RootId)).ToList();

			return result;

		}

	}
}