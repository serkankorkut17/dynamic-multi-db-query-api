using System;
using System.Collections.Generic;
using System.Text;

public static class ExpressionSplitter2
{
    // En dıştaki parantez içi operandları çıkarma ve yerlerine $0, $1, ... koyma (bozuk versiyon)
    public static List<string> ExtractTopLevelOperands(ref string body, out string replacedBody)
    {
        var expressions = new List<string>();
        var sb = new StringBuilder();
        int depth = 0;
        int start = -1;
        int counter = 0;

        for (int i = 0; i < body.Length; i++)
        {
            char c = body[i];

            if (c == '(')
            {
                if (depth == 0)
                {
                    // Parantez başlangıcı
                    start = i + 1;
                }
                depth++;
            }
            else if (c == ')')
            {
                depth--;

                if (depth == 0 && start >= 0)
                {
                    string inner = body.Substring(start, i - start);
                    string placeholder = $"${counter++}";
                    expressions.Add(inner.Trim());
                    sb.Append(placeholder);
                    start = -1;
                }
            }
            else
            {
                if (depth == 0)
                {
                    sb.Append(c);
                }
            }
        }
        replacedBody = sb.ToString().Trim();
        return expressions;
    }
    // Basit versiyon: üst-seviye operandları alır, nested içeriğe girmez.
    public static List<string> SimplerExtractTopLevelOperands(ref string body, out string replacedBody)
    {
        var operands = new List<string>();
        var ops = new List<string>(); // AND / OR between operands

        if (string.IsNullOrWhiteSpace(body))
        {
            replacedBody = "";
            return operands;
        }

        var sb = new StringBuilder();
        int depth = 0;
        bool inQuote = false;
        int i = 0;
        string s = body;

        while (i < s.Length)
        {
            char c = s[i];

            // toggle quote, handle escaped ''
            if (c == '\'')
            {
                sb.Append(c);
                i++;
                while (i < s.Length)
                {
                    sb.Append(s[i]);
                    if (s[i] == '\'')
                    {
                        if (i + 1 < s.Length && s[i + 1] == '\'')
                        {
                            // escaped quote, include and skip next
                            sb.Append('\'');
                            i += 2;
                            continue;
                        }
                        else
                        {
                            i++;
                            break;
                        }
                    }
                    i++;
                }
                continue;
            }

            if (c == '(')
            {
                depth++;
                sb.Append(c);
                i++;
                continue;
            }

            if (c == ')')
            {
                depth = Math.Max(0, depth - 1);
                sb.Append(c);
                i++;
                continue;
            }

            // only detect top-level operators when depth==0
            if (depth == 0)
            {
                // try AND
                if (MatchWordAt(s, i, "AND") && IsWordBoundary(s, i, 3))
                {
                    AddOperandFromSb();
                    ops.Add("AND");
                    i += 3;
                    continue;
                }

                // try OR
                if (MatchWordAt(s, i, "OR") && IsWordBoundary(s, i, 2))
                {
                    AddOperandFromSb();
                    ops.Add("OR");
                    i += 2;
                    continue;
                }
            }

            // default: copy char
            sb.Append(c);
            i++;
        }

        // last operand
        AddOperandFromSb();

        // build replacedBody: $0 <op0> $1 <op1> $2 ...
        var outSb = new StringBuilder();
        for (int k = 0; k < operands.Count; k++)
        {
            if (k > 0)
            {
                outSb.Append(' ');
                outSb.Append(ops[k - 1]);
                outSb.Append(' ');
            }
            outSb.Append($"${k}");
        }

        replacedBody = outSb.ToString();
        return operands;

        // local helpers
        void AddOperandFromSb()
        {
            var piece = sb.ToString().Trim();
            sb.Clear();
            if (string.IsNullOrEmpty(piece)) return;
            piece = StripOuterParens(piece);
            operands.Add(piece);
        }
    }

    static bool MatchWordAt(string s, int pos, string word)
    {
        if (pos + word.Length > s.Length) return false;
        return string.Compare(s, pos, word, 0, word.Length, StringComparison.OrdinalIgnoreCase) == 0;
    }

    static bool IsWordBoundary(string s, int pos, int len)
    {
        int before = pos - 1;
        int after = pos + len;
        bool prevOk = before < 0 || char.IsWhiteSpace(s[before]) || s[before] == '(' || s[before] == ')';
        bool nextOk = after >= s.Length || char.IsWhiteSpace(s[after]) || s[after] == '(' || s[after] == ')';
        return prevOk && nextOk;
    }

    static string StripOuterParens(string input)
    {
        if (string.IsNullOrWhiteSpace(input)) return input;
        var s = input.Trim();
        if (s.Length >= 2 && s[0] == '(' && s[^1] == ')')
        {
            // Check that outer parens are balanced and span the whole string
            int depth = 0;
            bool inQuote = false;
            for (int i = 0; i < s.Length; i++)
            {
                char c = s[i];
                if (c == '\'')
                {
                    // skip over quoted content (handle '' escape)
                    i++;
                    while (i < s.Length)
                    {
                        if (s[i] == '\'')
                        {
                            if (i + 1 < s.Length && s[i + 1] == '\'') { i += 2; continue; }
                            break;
                        }
                        i++;
                    }
                    continue;
                }
                if (c == '(') depth++;
                else if (c == ')') depth--;
                // if depth becomes 0 before last char, outer parens don't enclose whole string
                if (depth == 0 && i < s.Length - 1) return s;
            }

            if (depth == 0)
            {
                // remove one outer layer
                return s.Substring(1, s.Length - 2).Trim();
            }
        }
        return s;
    }
}
