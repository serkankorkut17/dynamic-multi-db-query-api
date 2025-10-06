using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace DynamicDbQueryApi.Helpers
{
    public class StringHelpers
    {
        // String timestamp veya date formatında olduğunu kontrol etme
        public static bool IsDateOrTimestamp(string s)
        {
            if (string.IsNullOrWhiteSpace(s)) return false;
            s = s.Trim();

            // 'YYYY-MM-DD' ya da YYYY-MM-DD
            var dateOnly = new Regex(@"^'?(\d{4}-\d{2}-\d{2})'?$", RegexOptions.Compiled);

            // 'YYYY-MM-DD HH:MM:SS' ya da 'YYYY-MM-DDTHH:MM:SS' ya da YYYY-MM-DD HH:MM:SS ya da YYYY-MM-DDTHH:MM:SS
            var tsSpace = new Regex(@"^'?(\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2})(\.\d{1,3})?'?$", RegexOptions.Compiled);

            // 'YYYY-MM-DDTHH:MM:SSZ' ya da 'YYYY-MM-DDTHH:MM:SS+03:00' ya da YYYY-MM-DDTHH:MM:SSZ ya da YYYY-MM-DDTHH:MM:SS+03:00
            var tsIso = new Regex(@"^'?(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(\.\d{1,3})?(Z|[+\-]\d{2}:\d{2})?'?$", RegexOptions.Compiled);

            return dateOnly.IsMatch(s) || tsSpace.IsMatch(s) || tsIso.IsMatch(s);
        }

        // En dıştaki parantezleri kaldırma
        public static string StripOuterParens(string s)
        {
            if (string.IsNullOrWhiteSpace(s)) return s;
            s = s.Trim();

            while (s.StartsWith('(') && s.EndsWith(')'))
            {
                int closeIdx = FindClosingParenthesis(s, 0);
                if (closeIdx == s.Length - 1)
                {
                    // En dıştaki parantezler dengeli ve kapatılıyor, kaldır
                    s = s.Substring(1, s.Length - 2).Trim();
                }
                else
                {
                    break; // En dıştaki parantezler dengeli değil
                }
            }

            return s;
        }

        // En dıştaki parantezin kapanışını bulma
        public static int FindClosingParenthesis(string str, int openIdx)
        {
            int depth = 0;
            for (int i = openIdx; i < str.Length; i++)
            {
                if (str[i] == '(')
                {
                    depth++;
                }
                else if (str[i] == ')')
                {
                    depth--;
                    if (depth == 0) return i;
                }
            }
            return -1; // Not found
        }

        // Tek tırnaklı string sonunu bulma
        public static int FindClosingQuote(string str, int startIdx)
        {
            for (int i = startIdx + 1; i < str.Length; i++)
            {
                if (str[i] == '\'')
                {
                    // Eğer öncesinde \ yoksa kapatma tırnağıdır
                    if (i == 0 || str[i - 1] != '\\')
                    {
                        return i;
                    }
                }
            }
            return -1; // Not found
        }

        // En dıştaki süslü parantezin kapanışını bulma
        public static int FindClosingBracket(string str, int startIdx)
        {
            for (int i = startIdx + 1; i < str.Length; i++)
            {
                if (str[i] == '}')
                {
                    return i;
                }
            }
            return -1; // Not found
        }

        // , lerden bölme (parantez içi ve string içi değilse)
        public static List<string> SplitByCommas(string input)
        {
            List<string> result = new List<string>();
            for (int i = 0; i < input.Length; i++)
            {
                if (input[i] == '(')
                {
                    // Parantez içindeki virgülleri yok say
                    int closeIdx = FindClosingParenthesis(input, i);
                    if (closeIdx == -1)
                    {
                        throw new Exception("Could not find closing parenthesis in FETCH columns.");
                    }
                    i = closeIdx;
                }

                else if (input[i] == '\'')
                {
                    // String içindeki virgülleri yok say
                    int closeIdx = FindClosingQuote(input, i);
                    if (closeIdx == -1)
                    {
                        throw new Exception("Could not find closing quote in FETCH columns.");
                    }
                    i = closeIdx;
                }

                else if (input[i] == '{')
                {
                    // Süslü parantez içindeki virgülleri yok say
                    int closeIdx = FindClosingBracket(input, i);
                    if (closeIdx == -1)
                    {
                        throw new Exception("Could not find closing bracket in FETCH columns.");
                    }
                    i = closeIdx;
                }

                else if (input[i] == ',')
                {
                    // Virgül bulundu, böl
                    result.Add(input.Substring(0, i).Trim());
                    input = input.Substring(i + 1).Trim();
                    i = -1;
                }
            }
            // Son parçayı ekle
            if (!string.IsNullOrWhiteSpace(input))
            {
                result.Add(input.Trim());
            }

            return result;
        }

        public static List<string> SplitByWhitespaces(string input)
        {
            List<string> result = new List<string>();
            for (int i = 0; i < input.Length; i++)
            {
                if (input[i] == '(')
                {
                    // Parantez içindeki virgülleri yok say
                    int closeIdx = FindClosingParenthesis(input, i);
                    if (closeIdx == -1)
                    {
                        throw new Exception("Could not find closing parenthesis in FETCH columns.");
                    }
                    i = closeIdx;
                }

                else if (input[i] == '\'')
                {
                    // String içindeki virgülleri yok say
                    int closeIdx = FindClosingQuote(input, i);
                    if (closeIdx == -1)
                    {
                        throw new Exception("Could not find closing quote in FETCH columns.");
                    }
                    i = closeIdx;
                }

                else if (input[i] == '{')
                {
                    // Süslü parantez içindeki virgülleri yok say
                    int closeIdx = FindClosingBracket(input, i);
                    if (closeIdx == -1)
                    {
                        throw new Exception("Could not find closing bracket in FETCH columns.");
                    }
                    i = closeIdx;
                }

                else if (char.IsWhiteSpace(input[i]))
                {
                    // Boşluk bulundu, böl
                    result.Add(input.Substring(0, i).Trim());
                    input = input.Substring(i + 1).Trim();
                    i = -1;
                }
            }
            // Son parçayı ekle
            if (!string.IsNullOrWhiteSpace(input))
            {
                result.Add(input.Trim());
            }

            return result;
        }

        // Parantezlerin dengeli olup olmadığını kontrol etme
        public static bool CheckIfParanthesesBalanced(string input)
        {
            int balance = 0;
            foreach (char c in input)
            {
                if (c == '(') balance++;
                else if (c == ')') balance--;

                // Eğer kapanış parantezi açılış parantezinden önce gelirse
                if (balance < 0) return false;
            }
            return balance == 0;
        }

        public static string RemoveExtraSpaces(string s)
        {
            if (string.IsNullOrWhiteSpace(s)) return s;
            var parts = s.Split(new char[] { ' ', '\t', '\n', '\r' }, StringSplitOptions.RemoveEmptyEntries);
            return string.Join(' ', parts);
        }

        public static List<string> ExtractTopLevelOperands(ref string body, out string replacedBody)
        {
            var operands = new List<string>();
            var operators = new List<string>();

            if (string.IsNullOrWhiteSpace(body))
            {
                replacedBody = "";
                return operands;
            }

            int i = 0;
            var sb = new StringBuilder();
            string s = body;

            while (i < s.Length)
            {
                char c = s[i];

                // String içindeki alıntıları atla
                if (c == '\'')
                {
                    int endQuote = FindClosingQuote(s, i);
                    if (endQuote == -1) throw new Exception("Unmatched quote.");
                    sb.Append(s.Substring(i, endQuote - i + 1));
                    i = endQuote + 1;
                    continue;
                }

                // Parantez içeriğini atla
                if (c == '(')
                {
                    int endParen = FindClosingParenthesis(s, i);
                    if (endParen == -1) throw new Exception("Unmatched parenthesis.");
                    sb.Append(s.Substring(i, endParen - i + 1));
                    i = endParen + 1;
                    continue;
                }

                // Top-level operator kontrolü
                if (MatchWordAt(s, i, "AND") && IsWordBoundary(s, i, 3))
                {
                    AddOperand();
                    operators.Add("AND");
                    i += 3;
                    continue;
                }

                if (MatchWordAt(s, i, "OR") && IsWordBoundary(s, i, 2))
                {
                    AddOperand();
                    operators.Add("OR");
                    i += 2;
                    continue;
                }

                sb.Append(c);
                i++;
            }

            AddOperand();

            replacedBody = BuildReplacedBody(operands, operators);
            return operands;

            // --- Local helpers ---
            void AddOperand()
            {
                var piece = sb.ToString().Trim();
                sb.Clear();
                if (!string.IsNullOrEmpty(piece))
                    operands.Add(StripOuterParens(piece));
            }

            static bool MatchWordAt(string str, int pos, string word)
            {
                return pos + word.Length <= str.Length &&
                       string.Compare(str, pos, word, 0, word.Length, StringComparison.OrdinalIgnoreCase) == 0;
            }

            static bool IsWordBoundary(string str, int pos, int len)
            {
                bool prevOk = pos - 1 < 0 || char.IsWhiteSpace(str[pos - 1]) || str[pos - 1] is '(' or ')';
                bool nextOk = pos + len >= str.Length || char.IsWhiteSpace(str[pos + len]) || str[pos + len] is '(' or ')';
                return prevOk && nextOk;
            }

            static string BuildReplacedBody(List<string> operands, List<string> operators)
            {
                var sb = new StringBuilder();
                for (int k = 0; k < operands.Count; k++)
                {
                    if (k > 0)
                    {
                        sb.Append(' ');
                        sb.Append(operators[k - 1]);
                        sb.Append(' ');
                    }
                    sb.Append($"${k}");
                }
                return sb.ToString();
            }
        }

        public static string NormalizeString(string expr)
        {
            if (string.IsNullOrWhiteSpace(expr)) return expr;
            // Geçersiz karakterleri alt çizgi ile değiştir
            var cleaned = Regex.Replace(expr, @"[^\w]", "_");
            return cleaned.Trim('_');
        }
    }
}