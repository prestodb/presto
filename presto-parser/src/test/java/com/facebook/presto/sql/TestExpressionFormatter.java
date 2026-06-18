/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.sql;

import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.StringLiteral;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.sql.ExpressionFormatter.formatStringLiteral;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestExpressionFormatter
{
    private static final SqlParser SQL_PARSER = new SqlParser();

    @Test
    public void testFormatStringLiteralPreservesNewlines()
    {
        String input = "line1\nline2\nline3";
        String formatted = formatStringLiteral(input);

        assertTrue(formatted.contains("\n"), "Newlines should be preserved in output");
        assertEquals(formatted, "'line1\nline2\nline3'");
    }

    @Test
    public void testFormatStringLiteralPreservesTabs()
    {
        String input = "col1\tcol2\tcol3";
        String formatted = formatStringLiteral(input);

        assertTrue(formatted.contains("\t"), "Tabs should be preserved in output");
        assertEquals(formatted, "'col1\tcol2\tcol3'");
    }

    @Test
    public void testFormatStringLiteralPreservesCarriageReturns()
    {
        String input = "line1\rline2";
        String formatted = formatStringLiteral(input);

        assertTrue(formatted.contains("\r"), "Carriage returns should be preserved in output");
        assertEquals(formatted, "'line1\rline2'");
    }

    @Test
    public void testFormatStringLiteralWithMixedWhitespace()
    {
        String input = "def foo():\n\treturn 'bar'\r\n";
        String formatted = formatStringLiteral(input);

        assertTrue(formatted.contains("\n"), "Newlines should be preserved");
        assertTrue(formatted.contains("\t"), "Tabs should be preserved");
        assertTrue(formatted.contains("\r"), "Carriage returns should be preserved");
        assertEquals(formatted, "'def foo():\n\treturn ''bar''\r\n'");
    }

    @Test
    public void testFormatStringLiteralWithPythonCode()
    {
        String pythonCode = "def process(x):\n" +
                "\tif x > 0:\n" +
                "\t\treturn x * 2\n" +
                "\treturn 0";
        String formatted = formatStringLiteral(pythonCode);

        assertTrue(formatted.contains("\n"), "Newlines in Python code should be preserved");
        assertTrue(formatted.contains("\t"), "Tabs in Python code should be preserved");
        assertEquals(formatted, "'" + pythonCode + "'");
    }

    @Test
    public void testFormatStringLiteralPreservesRegularStrings()
    {
        String input = "hello world";
        String formatted = formatStringLiteral(input);
        assertEquals(formatted, "'hello world'");
    }

    @Test
    public void testFormatStringLiteralEscapesSingleQuotes()
    {
        String input = "it's a test";
        String formatted = formatStringLiteral(input);
        assertEquals(formatted, "'it''s a test'");
    }

    @Test
    public void testFormatStringLiteralWithUnicodeCharacters()
    {
        String input = "hello \u0001 world";
        String formatted = formatStringLiteral(input);

        assertTrue(formatted.startsWith("U&'"), "Non-printable characters should use Unicode format");
        assertTrue(formatted.contains("0001"), "Unicode escape should be present");
    }

    @Test
    public void testFormatStringLiteralPreservesWhitespaceInUnicodeMode()
    {
        String input = "test\n\u0001\ttab";
        String formatted = formatStringLiteral(input);

        assertTrue(formatted.startsWith("U&'"), "Should use Unicode format for non-printable chars");
        assertTrue(formatted.contains("\n"), "Newlines should be preserved even in Unicode mode");
        assertTrue(formatted.contains("\t"), "Tabs should be preserved even in Unicode mode");
    }

    @Test
    public void testStringLiteralRoundTrip()
    {
        String pythonCode = "def add(a, b):\n\treturn a + b";

        Expression parsed = SQL_PARSER.createExpression("'" + pythonCode + "'", new ParsingOptions());
        String formattedExpression = ExpressionFormatter.formatExpression(parsed, Optional.empty());

        Expression reparsed = SQL_PARSER.createExpression(formattedExpression, new ParsingOptions());

        assertTrue(parsed instanceof StringLiteral);
        assertTrue(reparsed instanceof StringLiteral);
        assertEquals(((StringLiteral) reparsed).getValue(), pythonCode);
    }

    @Test
    public void testMultiLineStringWithIndentation()
    {
        String pythonUdf = "def my_udf(x):\n\tif x > 10:\n\t\treturn x * 2\n\telse:\n\t\treturn x + 5";

        Expression parsed = SQL_PARSER.createExpression("'" + pythonUdf + "'", new ParsingOptions());
        String formatted = ExpressionFormatter.formatExpression(parsed, Optional.empty());

        assertTrue(formatted.contains("\n"), "Multi-line content should preserve newlines");
        assertTrue(formatted.contains("\t"), "Indentation should be preserved");
    }

    @Test
    public void testEmptyString()
    {
        String input = "";
        String formatted = formatStringLiteral(input);
        assertEquals(formatted, "''");
    }

    @Test
    public void testStringWithOnlyWhitespace()
    {
        String input = "\n\t\r";
        String formatted = formatStringLiteral(input);
        assertEquals(formatted, "'\n\t\r'");
    }

    @Test
    public void testStringWithBackslash()
    {
        String input = "path\\to\\file";
        String formatted = formatStringLiteral(input);
        assertEquals(formatted, "'path\\to\\file'");
    }

    @Test
    public void testStringWithBackslashAndNewline()
    {
        String input = "line1\\\nline2";
        String formatted = formatStringLiteral(input);
        assertEquals(formatted, "'line1\\\nline2'");
    }

    @Test
    public void testStringComparisonSemanticEquivalence()
    {
        // Verify that literal whitespace and Unicode escapes parse to identical values
        String valueWithNewline = "text\nmore\ttext";

        Expression literalForm = SQL_PARSER.createExpression("'" + valueWithNewline + "'", new ParsingOptions());
        Expression unicodeForm = SQL_PARSER.createExpression("U&'text\\000Amore\\0009text'", new ParsingOptions());

        assertTrue(literalForm instanceof StringLiteral);
        assertTrue(unicodeForm instanceof StringLiteral);
        assertEquals(((StringLiteral) literalForm).getValue(), valueWithNewline);
        assertEquals(((StringLiteral) unicodeForm).getValue(), valueWithNewline);
        assertEquals(((StringLiteral) literalForm).getValue(), ((StringLiteral) unicodeForm).getValue());
    }

    @Test
    public void testQueryWithNewlineComparisonRoundTrip()
    {
        // Validate that comparisons maintain correctness through format/parse cycles
        String originalValue = "line1\nline2";

        Expression parsed = SQL_PARSER.createExpression("column = '" + originalValue + "'", new ParsingOptions());
        String formatted = ExpressionFormatter.formatExpression(parsed, Optional.empty());
        Expression reparsed = SQL_PARSER.createExpression(formatted, new ParsingOptions());

        assertEquals(ExpressionFormatter.formatExpression(parsed, Optional.empty()),
                ExpressionFormatter.formatExpression(reparsed, Optional.empty()));
    }

    @Test
    public void testRegexPatternWithNewlinesPreserved()
    {
        // Verify regex patterns with newlines are preserved through format/parse cycle
        String regexPattern = "line1\nline2.*\nline3";

        Expression parsed = SQL_PARSER.createExpression("'" + regexPattern + "'", new ParsingOptions());
        String formatted = ExpressionFormatter.formatExpression(parsed, Optional.empty());

        assertTrue(formatted.contains("\n"), "Regex pattern should preserve literal newlines");

        Expression reparsed = SQL_PARSER.createExpression(formatted, new ParsingOptions());
        assertEquals(((StringLiteral) reparsed).getValue(), regexPattern,
                "Regex pattern value should be preserved through format/parse cycle");
    }

    @Test
    public void testWhitespaceInComparisonPreservesSemantics()
    {
        // Validate that comparisons against whitespace preserve value semantics
        String textWithWhitespace = "before\nmiddle\tafter";

        Expression original = SQL_PARSER.createExpression("column = '" + textWithWhitespace + "'", new ParsingOptions());
        String reformatted = ExpressionFormatter.formatExpression(original, Optional.empty());
        Expression reparsed = SQL_PARSER.createExpression(reformatted, new ParsingOptions());

        assertTrue(reformatted.contains("\n") && reformatted.contains("\t"),
                "Reformatted version should contain literal whitespace");

        assertEquals(ExpressionFormatter.formatExpression(original, Optional.empty()),
                ExpressionFormatter.formatExpression(reparsed, Optional.empty()),
                "Comparison expression must be semantically preserved through format/parse cycle");
    }
}
