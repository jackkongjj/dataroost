using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataRoostAPI.Common.Exceptions {
	public class MissingIconumException : ApplicationException {

		public int Iconum { get; private set; }

		public MissingIconumException(int iconum)
			: base(string.Format("Iconum {0} not found", iconum)) {
			Iconum = iconum;
		}
	}
}
