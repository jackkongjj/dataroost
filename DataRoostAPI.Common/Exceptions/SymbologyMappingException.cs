using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataRoostAPI.Common.Exceptions {
	public class SymbologyMappingException : ApplicationException {

		public SymbologyMappingException(string message) : base(message) {
		}
	}
}
