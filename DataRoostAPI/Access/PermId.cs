using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace CCS.Fundamentals.DataRoostAPI.Access {
	public class PermId {

		private static string _IconumLookup = "0123456789BCDFGHJKLMNPQRSTVWXYZ";
		public static int PermId2Iconum(string permId) {
			if (!permId.EndsWith("-E"))
				throw new InvalidOperationException("Unable to convert PermId to Iconum");

			int ico = 0;
			for (int i = 0; i < 6; i++) {
				int comp = _IconumLookup.IndexOf(permId[i]);
				if (comp < 0)
					throw new InvalidOperationException();

				ico += (int)Math.Pow(31, 5 - i) * comp;
			}

			return ico;
		}

		public static string Iconum2PermId(int iconum) {
			if (iconum < 0 || iconum > 887503680)
				throw new ArgumentException("Invalid Iconum");

			string permId = "";

			int iter = iconum;
			for (int i = 5; i >= 0; i--) {
				int p = (int)Math.Pow(31, i);
				int div = iter / p;

				permId += _IconumLookup[div];
				iter = iter % p;
			}

			if (permId.Length != 6)
				throw new InvalidOperationException("Invalid Length Resulted");

			return permId + "-E";
		}
	}
}