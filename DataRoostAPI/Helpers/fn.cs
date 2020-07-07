using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;
using System.Text.RegularExpressions;
using System.IO;

namespace CCS.Fundamentals.DataRoostAPI.Helpers {
    public class fn
    {
        static Dictionary<string, string> StemDictionary = new Dictionary<string, string>()
        {
            {"afs", "" },
            //{"and", "" },
            {"the", "" },
            {"by", "" },
            {"of", "" },
            {"to", "" },
            {"from", "" },
            {"for", "" },
            {"under", "" },
            {"all", "" },
            {"in", "" },
            {"with", "" },
            {"at", "" },
            {"on", "" },
            {"or", "" },
            {"over", "" },
            {"stackholder", "shareholder" },
            {"stockholder", "shareholder" },
            {"net", "" },
            {"guaranteed", "" },
            {"office", "" },
            {"total", "" }
        };

        static Dictionary<string, string> ReplacePhraseDict = new Dictionary<string, string>()
        {
            {"afs", "" }, //available for sale
            {"available for sale", "" },
            {"fte", "" }, //full time equivalent
            {"full time equivalent", "" },
            {"provided by", "" },
            {"cash equivalent", "cash" },
            {"cash due bank", "cash" },
            {"increase", "change" },
            {"decrease", "change" },
            {"frb", "federal reserve" },
            {"fhlb", "federal home loan bank" },
            {"lhfs", "loan held for sale" },
            {"lhfi", "loan held for investment" },
            {"pci", "purchased credit impaired" },
            {"cd", "certificate deposit" },
            {"cds", "certificate deposit" },
            {"cre", "commercial real estate" },
            {"domestic", "us" },
            {"international", "non us" },
            {"foreign", "non us" },
            {"stockholder", "shareholder" }
        };

        public static string Car(string s)
        {
            if (string.IsNullOrEmpty(s))
            {
                return "";
            }
            string car = "";

            var i = s.IndexOf('[');
            var j = s.IndexOf(']');
            if (i < 0 && j < 0)
            {
                return s;
            }
            else if (i < 0 || j < 0)
            {
                return car;
            }
            else if (i >= j)
            {
                return car;
            }
            else
            {
                car = SubstringIx(s, i, j);
            }
            return car;
        }
        public static string Cdr(string s)
        {
            if (string.IsNullOrEmpty(s))
            {
                return "";
            }
            string cdr = "";

            var i = s.IndexOf('[');
            var j = s.IndexOf(']');
            if (i < 0 && j < 0)
            {
                return "";
            }
            else if (i < 0 || j < 0)
            {
                return s;
            }
            else if (i >= j)
            {
                return s;
            }
            else
            {
                cdr = SubstringIx(s, j + 1, s.Length - 1);
            }
            return cdr;
        }
        public static string RevCar(string s)
        {
            if (string.IsNullOrEmpty(s))
            {
                return "";
            }
            string cdr = "";

            var i = s.LastIndexOf('[');
            var j = s.LastIndexOf(']');
            if (i < 0 && j < 0)
            {
                return "";
            }
            else if (i < 0 || j < 0)
            {
                return s;
            }
            else if (i >= j)
            {
                return s;
            }
            else
            {
                cdr = SubstringIx(s, i, j);
            }
            return cdr;
        }
        public static string SubstringIx(string value, int startIndex, int endIndex)
        {
            try
            {
                if (value == null) throw new ArgumentNullException();
                if (endIndex > value.Length) throw new IndexOutOfRangeException("End index must be less than or equal to the length of the string.");
                if (startIndex < 0 || startIndex > value.Length + 1) throw new IndexOutOfRangeException("Start index must be between zero and the length of the string minus one");
                if (startIndex >= endIndex) return "";
            }
            catch
            {
                return value;
            }
            var length = endIndex - startIndex;
            return value.Substring(startIndex, length + 1);
        }
        public static string RevCdr(string s)
        {
            if (string.IsNullOrEmpty(s))
            {
                return "";
            }
            string cdr = "";

            var i = s.LastIndexOf('[');
            var j = s.LastIndexOf(']');
            if (i < 0 && j < 0)
            {
                return "";
            }
            else if (i < 0 || j < 0)
            {
                return s;
            }
            else if (i >= j)
            {
                return s;
            }
            else
            {
                cdr = SubstringIx(s, 0, i - 1);
            }
            return cdr;
        }
        public static string Prefix(string s, string pattern)
        {
            if (string.IsNullOrEmpty(s))
            {
                return "";
            }
            if (string.IsNullOrEmpty(pattern))
            {
                return s;
            }
            string cdr = "";

            var i = s.IndexOf(pattern);
            if (i < 0)
            {
                return s;
            }
            else
            {
                cdr = SubstringIx(s, 0, i - 1);
            }
            return cdr;
        }
        public static string Unbox(string s)
        {
            if (string.IsNullOrWhiteSpace(s))
            {
                return "";
            }
            return s.Replace("[", "").Replace("]", "");
        }
        public static string Box(string s)
        {
            if (string.IsNullOrWhiteSpace(s))
            {
                return "";
            }
            return string.Format("[{0}]", s);
        }
        public static int Count(string str)
        {
            if (string.IsNullOrWhiteSpace(str))
            {
                return 0;
            }
            //var i = s.Count(x => x == ']');
            //var j = s.Count(x => x == '[');
            //return Math.Min(i, j);
            //var result = str.Replace("[", "");
            //string[] remlabelWords = result.Split(']', StringSplitOptions.RemoveEmptyEntries);
            //return remlabelWords.Length; 
            string pattern = @"\[[a-zA-Z0-9\s]+\]";

            MatchCollection matches = Regex.Matches(str, pattern);
            return matches.Count;

        }
        public static string NoRepeat(string s)
        {

            if (Count(s) > 4)
            {
                var car = "";
                var cdr = s;
                bool working = true;
                string unique = "";
                while (working)
                {
                    car = Car(cdr);
                    unique += car;
                    if (string.IsNullOrWhiteSpace(car))
                    {
                        working = false;
                    }
                    else
                    {
                        cdr = cdr.Replace(car, "");
                    }
                }
                unique += cdr;
                return unique;
            }
            else
            {
                return s;
            }
        }
        public static string EndLabel(string s)
        {
            if (string.IsNullOrEmpty(s))
            {
                return "";
            }
            var i = s.LastIndexOf(']');
            if (i > 0 && i < s.Length)
            {
                s = s.Substring(i + 1);
                s = s.TrimStart().TrimEnd();
            }
            return s;
        }
        public static string Hierarchy(string s)
        {
            if (string.IsNullOrEmpty(s))
            {
                return "";
            }
            var i = s.LastIndexOf(']');
            if (i > 0 && i < s.Length)
            {
                return s.Substring(0, i + 1);
            }
            return "";
        }

        public static string AlphaNumericOnly(string input, string newStr = " ")
        {
            input = input.Replace("[", " ").Replace("]", " ").Replace("-", " ");
            var rgx = new System.Text.RegularExpressions.Regex("[^a-zA-Z0-9\\s]");
            return rgx.Replace(input, newStr);
        }
        public static string AlphaNumericSpaceAndSquareBrackets(string input, string newStr = " ")
        {
            //input = input.Replace("-", " ");
            var rgx = new System.Text.RegularExpressions.Regex("[^a-zA-Z0-9\\s\\[\\]]");
            return rgx.Replace(input, newStr);
        }

        public static string SingularForm(string str)
        {
            if (str.Contains("]"))
            {
                var result = str.Replace("[", "");
                string[] remlabelWords = result.Split(new char[] { ']' }, StringSplitOptions.RemoveEmptyEntries);
                result = "";
                foreach (var r in remlabelWords)
                {
                    if (string.IsNullOrWhiteSpace(r))
                    {
                        continue;
                    }
                    var s = SingularFormSingleLevel(r.TrimStart().TrimEnd()).TrimStart().TrimEnd();
                    if (!string.IsNullOrWhiteSpace(s))
                    {
                        result += string.Format("[{0}]", s);
                    }

                }
                return result;
            }
            else
            {
                return SingularFormSingleLevel(str);
            }

        }
        public static string SingularFormSingleLevel(string str)
        {
            var result = str;
            string[] remlabelWords = result.Split(new char[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);
            for (int i = 0; i < remlabelWords.Count(); i++)
            {
                var r = new Regex(@"[A-Za-z]\d+$");
                if (r.IsMatch(remlabelWords[i]))
                {
                    remlabelWords[i] = Regex.Replace(remlabelWords[i], @"\d+$", "");
                }
                //if (!EnglishDict.Contains(remlabelWords[i]))
                //{
                //    remlabelWords[i] = "";
                //}
                var ii = new Regex(@"ii$");
                var ies = new Regex(@"ies$");
                var excess = new Regex(@"excess$");
                var ses = new Regex(@"(a|e|i|o|u)ses$");
                var es = new Regex(@"(s|x|ch|sh)es$");
                var s = new Regex(@"([a-z]){3,}s$");
                var leases = new Regex(@"(lease|expense)s$");
                var less = new Regex(@"(l|n)(e|o)ss$");
                var sis = new Regex(@"sis$");
                var unitedstates = new Regex(@"(non|\b)us$");
                if (remlabelWords[i] == "radii")
                {
                    remlabelWords[i] = "radius";
                }
                else if (ii.IsMatch(remlabelWords[i]))
                {
                    remlabelWords[i] = Regex.Replace(remlabelWords[i], @"ii$", "us");
                }
                else if (leases.IsMatch(remlabelWords[i]))
                {
                    remlabelWords[i] = Regex.Replace(remlabelWords[i], @"es$", "e");
                }
                else if (ses.IsMatch(remlabelWords[i]))
                {
                    remlabelWords[i] = Regex.Replace(remlabelWords[i], @"es$", "e");
                }
                else if (less.IsMatch(remlabelWords[i]) || unitedstates.IsMatch(remlabelWords[i]))
                {
                }
                else if (sis.IsMatch(remlabelWords[i]) || excess.IsMatch(remlabelWords[i]))
                {
                }
                else if (ies.IsMatch(remlabelWords[i]))
                {
                    remlabelWords[i] = Regex.Replace(remlabelWords[i], @"ies$", "y");
                }
                else if (es.IsMatch(remlabelWords[i]))
                {
                    remlabelWords[i] = Regex.Replace(remlabelWords[i], @"es$", "");
                }
                else if (s.IsMatch(remlabelWords[i]))
                {
                    remlabelWords[i] = Regex.Replace(remlabelWords[i], @"s$", "");
                }
                //spelling.Text = remlabelWords[i];
                //spelling.SpellCheck();
                //if (!oSpell.TestWord(remlabelWords[i]))
                //{
                //    remlabelWords[i] = "";
                //}
            }
            result = string.Join<string>(" ", remlabelWords.Where(x => x.Length > 1 && !string.IsNullOrWhiteSpace(x)));// && !_stops.ContainsKey(c.ToLower())));
            //result = NoStemWordSingleLevel(result);
            if (string.IsNullOrWhiteSpace(result))
            {
                return str;
            }
            return result;
        }
        public static string ReplacePhraseAllLevel(string str)
        {
            if (str.Contains("]"))
            {
                var result = str.Replace("[", "");
                string[] remlabelWords = result.Split(new char[] { ']' }, StringSplitOptions.RemoveEmptyEntries);
                result = "";
                foreach (var r in remlabelWords)
                {
                    if (string.IsNullOrWhiteSpace(r))
                    {
                        continue;
                    }
                    var s = ReplacePhraseSingleLevel(r.TrimStart().TrimEnd()).TrimStart().TrimEnd();
                    if (!string.IsNullOrWhiteSpace(s))
                    {
                        result += string.Format("[{0}]", s);
                    }

                }
                return result;
            }
            else
            {
                return ReplacePhraseSingleLevel(str);
            }

        }
        public static string ReplacePhraseSingleLevel(string str)
        {
            var replaced = str;
            foreach (var d in ReplacePhraseDict)
            {
                string pattern = @"\b" + d.Key + @"\b";
                var phrase = new Regex(pattern);
                if (phrase.IsMatch(replaced))
                {
                    replaced = Regex.Replace(replaced, pattern, d.Value, RegexOptions.IgnoreCase);
                }
            }
            return replaced;
            //var result = str;
            //string[] remlabelWords = result.Split(' ', StringSplitOptions.RemoveEmptyEntries);
            //for (int i = 0; i < remlabelWords.Count(); i++)
            //{
            //    if (!EnglishDict.Contains(remlabelWords[i]))
            //    {
            //        Console.WriteLine("non english: " + remlabelWords[i]);
            //        remlabelWords[i] = "";
            //    }
            //}
            //result = string.Join(' ', remlabelWords.Where(x => x.Length > 1 && !string.IsNullOrWhiteSpace(x)));// && !_stops.ContainsKey(c.ToLower())));
            //if (string.IsNullOrWhiteSpace(result))
            //{
            //    return str;
            //}
            //return result;
        }
        public static string NoStemWordAllLevel(string str)
        {
            if (string.IsNullOrWhiteSpace(str))
            {
                return str;
            }
            if (str.Contains("]"))
            {
                var result = str.Replace("[", "");
                string[] remlabelWords = result.Split(new char[] { ']' }, StringSplitOptions.RemoveEmptyEntries);
                result = "";
                foreach (var r in remlabelWords)
                {
                    if (string.IsNullOrWhiteSpace(r))
                    {
                        continue;
                    }
                    var s = NoStemWordSingleLevel(r.TrimStart().TrimEnd()).TrimStart().TrimEnd();
                    if (!string.IsNullOrWhiteSpace(s))
                    {
                        result += string.Format("[{0}]", s);
                    }

                }
                return result;
            }
            else
            {
                return NoStemWordSingleLevel(str);
            }
        }
        public static string NoStemWordSingleLevel(string str, string wordToRemove = "")
        {
            var result = str;
            if (string.IsNullOrWhiteSpace(result))
            {
                return result;
            }
            if (string.IsNullOrWhiteSpace(wordToRemove))
            {
                string[] remlabelWords = result.Split(new char[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);
                for (int i = 0; i < remlabelWords.Count(); i++)
                {
                    if (StemDictionary.ContainsKey(remlabelWords[i]))
                    {
                        remlabelWords[i] = StemDictionary[remlabelWords[i]];
                    }
                }

                result = string.Join<string>(" ", remlabelWords.Where(x => x.Length > 1 && !string.IsNullOrWhiteSpace(x)).Distinct());// && !_stops.ContainsKey(c.ToLower())));
                if (!result.Contains("liability and "))
                {
                    result = NoStemWordSingleLevel(result, "and");
                }
                if (string.IsNullOrWhiteSpace(result))
                {
                    return str;
                }
            }
            else
            {
                string[] remlabelWords = result.Split(new char[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);
                for (int i = 0; i < remlabelWords.Count(); i++)
                {
                    if (string.IsNullOrWhiteSpace(remlabelWords[i]))
                    {
                        continue;
                    }
                    if (remlabelWords[i] == wordToRemove)
                    {
                        remlabelWords[i] = "";
                    }
                }

                result = string.Join<string>(" ", remlabelWords.Where(x => x.Length > 1 && !string.IsNullOrWhiteSpace(x)).Distinct());// && !_stops.ContainsKey(c.ToLower())));
                if (string.IsNullOrWhiteSpace(result))
                {
                    return str;
                }
            }

            return result;
        }

        public static string ReplaceFirst(string text, string search, string replace)
        {
            int pos = text.IndexOf(search);
            if (pos < 0)
            {
                return text;
            }
            return text.Substring(0, pos) + replace + text.Substring(pos + search.Length);
        }

 
 
        public static string LongestSubstring(String X, String Y)
        {
            var m = X.Length;
            var n = Y.Length;
            // Create a table to store lengths of longest common 
            // suffixes of substrings. Note that LCSuff[i][j] 
            // contains length of longest common suffix of X[0..i-1] 
            // and Y[0..j-1]. The first row and first column entries 
            // have no logical meaning, they are used only for 
            // simplicity of program 
            int[,] LCSuff = new int[m + 1, n + 1];

            // To store length of the longest common substring 
            int len = 0;

            // To store the index of the cell which contains the 
            // maximum value. This cell's index helps in building 
            // up the longest common substring from right to left. 
            int row = 0, col = 0;

            /* Following steps build LCSuff[m+1][n+1] in bottom 
            up fashion. */
            for (int i = 0; i <= m; i++)
            {
                for (int j = 0; j <= n; j++)
                {
                    if (i == 0 || j == 0)
                        LCSuff[i, j] = 0;

                    else if (X[i - 1] == Y[j - 1])
                    {
                        LCSuff[i, j] = LCSuff[i - 1, j - 1] + 1;
                        if (len < LCSuff[i, j])
                        {
                            len = LCSuff[i, j];
                            row = i;
                            col = j;
                        }
                    }
                    else
                        LCSuff[i, j] = 0;
                }
            }

            // if true, then no common substring exists 
            if (len == 0)
            {
                //Console.Write("No Common Substring");
                return "";
            }

            // allocate space for the longest common substring 
            String resultStr = "";

            // traverse up diagonally form the (row, col) cell 
            // until LCSuff[row][col] != 0 
            while (LCSuff[row, col] != 0)
            {
                resultStr = X[row - 1] + resultStr; // or Y[col-1] 
                --len;

                // move diagonally up to previous cell 
                row--;
                col--;
            }
            return resultStr;
        }
 
    }
}