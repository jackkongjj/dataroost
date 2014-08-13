using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Web;

namespace CCS.Fundamentals.DataRoostAPI.Access.SuperFast {
	public abstract class IdentifierBase {

		public string GetToken() {
			// See comment in DeconstructToken()
			string s = string.Join("|", getComponents());

			byte[] b = Encoding.UTF8.GetBytes(s);
			return Convert.ToBase64String(b)
				.Replace('/', '_');
		}

		protected static string[] DeconstructToken(string token) {
			// You should never, _ever_ implement this conversion elsewhere, in this or any other language
			// If you find yourself looking here to do just that, you're doing it wrong and should think
			// and talk through what you're trying to do.

			string b64 = token.Replace('_', '/');
			byte[] b = Convert.FromBase64String(b64);
			string s = new string(Encoding.UTF8.GetChars(b));

			return s.Split('|');
		}

		protected abstract string[] getComponents();
	}
}