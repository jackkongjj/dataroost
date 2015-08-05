using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace CCS.Fundamentals.DataRoostAPI.Helpers {
	public static class StringExtensions {

		public static bool In(this string thisString, IEnumerable<string> stringList) {
			foreach (string comparator in stringList) {
				if (thisString == comparator) {
					return true;
				}
			}

			return false;
		}
	}
}