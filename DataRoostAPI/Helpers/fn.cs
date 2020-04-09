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

namespace CCS.Fundamentals.DataRoostAPI.Helpers
{
    public class fn
    {

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