using System;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;

public static class ExpressionSplitter
{
    // Public entry:
    // body - giriş ifade (ör. "(a OR b) AND (c OR d) AND e")
    // returns list of operand strings (inner paren resolved)
    // out replacedBody -> "$0 AND $1 AND $2" (operatörleri korur)
    public static List<string> ExtractTopLevelOperands(ref string body, out string replacedBody)
    {
        if (body == null) throw new ArgumentNullException(nameof(body));

        // 1) önce iç parantezleri içten dışa çıkarıp token'larla değiştir
        var nested = ExtractNestedExpressions(ref body);

        // body artık parantez içermez; içinde $n token'ları olabilir
        // Örnek: "$0 AND $1 AND surname = 'korkut'"

        // 2) şimdi top-level AND/OR pozisyonlarına göre parçala
        var operands = SplitByTopLevelLogicalOperators(body, out var seps);

        // 3) her operand'ı "resolve" et: eğer $n ise nested[n], değilse olduğu gibi al
        var result = new List<string>();
        foreach (var op in operands)
        {
            var t = op.Trim();
            var m = Regex.Match(t, @"^\$(\d+)$");
            if (m.Success)
            {
                int idx = int.Parse(m.Groups[1].Value);
                if (idx < 0 || idx >= nested.Count) throw new Exception("Invalid token index");
                // nested element may still contain tokens (if deeper nested), but ExtractNestedExpressions processed inner ones
                result.Add(nested[idx].Trim());
            }
            else
            {
                result.Add(t);
            }
        }

        // 4) yeni replacedBody üret: $0 OP $1 OP $2 ... (OP from seps)
        var sb = new StringBuilder();
        for (int i = 0; i < result.Count; i++)
        {
            if (i > 0)
            {
                sb.Append(' ');
                sb.Append(seps[i - 1]); // separator between operands
                sb.Append(' ');
            }
            sb.Append($"${i}");
        }

        replacedBody = sb.ToString();
        return result;
    }

    // ---------- helper: extract innermost parentheses and replace with $n tokens ----------
    // returns list of inner expressions in the order they were added (inner-first)
    static List<string> ExtractNestedExpressions(ref string body)
    {
        var list = new List<string>();
        while (true)
        {
            // find innermost '('
            int open = LastIndexOfUnquoted(body, '(');
            if (open == -1) break;

            int close = FindMatchingClose(body, open);
            if (close == -1) throw new Exception("Mismatched parentheses");

            // extract content between open and close
            var inner = body.Substring(open + 1, close - open - 1);

            // before adding, process inner recursively: it might still contain parens (we use recursive loop)
            // (this while loop already finds innermost, so inner should not contain unprocessed '(')
            // add inner trimmed
            list.Add(inner.Trim());
            int tokenIndex = list.Count - 1;

            // replace "(...)" with token $index
            body = body.Substring(0, open) + $"${tokenIndex}" + body.Substring(close + 1);
        }

        return list;
    }

    // Find the matching close paren for an opening paren at openIndex.
    // Ignores parens inside quoted strings.
    static int FindMatchingClose(string s, int openIndex)
    {
        if (openIndex < 0 || openIndex >= s.Length || s[openIndex] != '(') return -1;
        int depth = 1;
        bool inQuote = false;
        for (int i = openIndex + 1; i < s.Length; i++)
        {
            var c = s[i];

            if (c == '\'')
            {
                // toggle quote, but respect escaped single quote ''
                if (inQuote)
                {
                    if (i + 1 < s.Length && s[i + 1] == '\'')
                    {
                        i++; // skip escaped quote
                        continue;
                    }
                    inQuote = false;
                }
                else
                {
                    inQuote = true;
                }
                continue;
            }

            if (inQuote) continue;

            if (c == '(') depth++;
            else if (c == ')')
            {
                depth--;
                if (depth == 0) return i;
            }
        }
        return -1;
    }

    // LastIndexOf but skipping characters inside quoted strings
    static int LastIndexOfUnquoted(string s, char target)
    {
        bool inQuote = false;
        for (int i = s.Length - 1; i >= 0; i--)
        {
            var c = s[i];
            if (c == '\'')
            {
                // handle escaped ''
                if (inQuote)
                {
                    if (i - 1 >= 0 && s[i - 1] == '\'') { i--; continue; }
                    inQuote = false;
                }
                else
                {
                    // entering quote scanning backward: find previous unmatched quote
                    inQuote = true;
                }
                continue;
            }
            if (inQuote) continue;
            if (c == target) return i;
        }
        return -1;
    }

    // ---------- split by top-level AND/OR (skips quoted strings and $n tokens) ----------
    // returns operands list and out list of separators (like "AND", "OR") between them
    static List<string> SplitByTopLevelLogicalOperators(string s, out List<string> separators)
    {
        var operands = new List<string>();
        separators = new List<string>();

        int i = 0;
        int n = s.Length;
        int lastPos = 0;

        while (i < n)
        {
            char c = s[i];

            // skip whitespace
            if (char.IsWhiteSpace(c)) { i++; continue; }

            // skip token $<digits>
            if (c == '$')
            {
                i++;
                while (i < n && char.IsDigit(s[i])) i++;
                continue;
            }

            // skip quoted string
            if (c == '\'')
            {
                i++;
                while (i < n)
                {
                    if (s[i] == '\'')
                    {
                        if (i + 1 < n && s[i + 1] == '\'') { i += 2; continue; } // escaped ''
                        i++; break;
                    }
                    i++;
                }
                continue;
            }

            // Try match AND / OR at this top-level position
            if (MatchWordAt(s, i, "AND") || MatchWordAt(s, i, "OR"))
            {
                // ensure word boundaries
                int opLen = MatchWordAt(s, i, "AND") ? 3 : 2;
                bool prevOk = (i == 0) || char.IsWhiteSpace(s[i - 1]) || s[i - 1] == ')';
                int opEnd = i + opLen;
                bool nextOk = (opEnd >= n) || char.IsWhiteSpace(s[opEnd]) || s[opEnd] == '(';

                if (prevOk && nextOk)
                {
                    // operand is substring from lastPos..i
                    var operand = s.Substring(lastPos, i - lastPos).Trim();
                    operands.Add(operand);

                    var op = s.Substring(i, opLen).ToUpperInvariant();
                    separators.Add(op);

                    // advance i and set lastPos after operator
                    i = opEnd;
                    lastPos = i;
                    continue;
                }
            }

            i++;
        }

        // last operand
        var last = s.Substring(lastPos).Trim();
        if (!string.IsNullOrEmpty(last) || operands.Count == 0) // if nothing found, still add whole string
            operands.Add(last);

        // trim operands
        for (int k = 0; k < operands.Count; k++) operands[k] = operands[k].Trim();

        return operands;
    }

    static bool MatchWordAt(string s, int pos, string word)
    {
        if (pos + word.Length > s.Length) return false;
        return s.Substring(pos, word.Length).Equals(word, StringComparison.OrdinalIgnoreCase);
    }
}
