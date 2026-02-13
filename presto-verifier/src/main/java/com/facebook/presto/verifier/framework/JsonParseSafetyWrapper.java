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
package com.facebook.presto.verifier.framework;

import com.facebook.airlift.log.Logger;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Post-processor to fix unsafe json_parse() calls in rewritten queries.
 * <p>
 * Problem: Some query rewrites (e.g., typeof() compatibility rewrites) generate
 * json_parse() calls without TRY() wrappers, causing failures on malformed JSON.
 * <p>
 * This utility wraps json_parse() calls with TRY() to handle malformed JSON gracefully.
 */
public final class JsonParseSafetyWrapper
{
    private JsonParseSafetyWrapper()
    {
    }

    private static final Logger log = Logger.get(JsonParseSafetyWrapper.class);

    // Pattern to match json_parse( that is NOT already wrapped in TRY(
    // Uses negative lookbehind to avoid double-wrapping.
    // Handles case-insensitive TRY with optional whitespace between TRY and (.
    private static final Pattern JSON_PARSE_PATTERN = Pattern.compile(
            "(?<![Tt][Rr][Yy]\\s{0,10}\\(\\s{0,10})\\b(json_parse)\\s*\\(",
            Pattern.CASE_INSENSITIVE);

    /**
     * Wraps unsafe json_parse() calls with TRY() to handle malformed JSON.
     * <p>
     * Transforms:
     *   json_extract(json_parse(field), path)
     * Into:
     *   json_extract(TRY(json_parse(field)), path)
     *
     * @param sql SQL query string that may contain unsafe json_parse() calls
     * @return Fixed SQL with TRY() wrappers around json_parse()
     */
    public static String wrapUnsafeJsonParse(String sql)
    {
        if (sql == null || sql.isEmpty()) {
            return sql;
        }

        Matcher matcher = JSON_PARSE_PATTERN.matcher(sql);
        if (!matcher.find()) {
            return sql;
        }

        matcher.reset();

        StringBuilder result = new StringBuilder();
        int lastEnd = 0;

        while (matcher.find()) {
            // Skip if match is inside a string literal
            if (isInsideStringLiteral(sql, matcher.start())) {
                result.append(sql, lastEnd, matcher.end());
                lastEnd = matcher.end();
                continue;
            }

            result.append(sql, lastEnd, matcher.start());

            int parenStart = matcher.end();
            int closingParen = findMatchingParen(sql, parenStart - 1);

            if (closingParen == -1) {
                log.warn("Could not find matching parenthesis for json_parse at position %d", matcher.start());
                result.append(matcher.group());
                lastEnd = matcher.end();
                continue;
            }

            String jsonParseCall = sql.substring(matcher.start(), closingParen + 1);
            result.append("TRY(").append(jsonParseCall).append(")");

            lastEnd = closingParen + 1;
        }

        result.append(sql.substring(lastEnd));

        String fixedSql = result.toString();

        if (!fixedSql.equals(sql)) {
            log.debug("Wrapped json_parse() calls with TRY()");
        }

        return fixedSql;
    }

    /**
     * Finds the matching closing parenthesis for an opening parenthesis.
     *
     * @param sql SQL string
     * @param openParenPos Position of the opening parenthesis
     * @return Position of matching closing parenthesis, or -1 if not found
     */
    private static int findMatchingParen(String sql, int openParenPos)
    {
        if (openParenPos < 0 || openParenPos >= sql.length() || sql.charAt(openParenPos) != '(') {
            return -1;
        }

        int depth = 1;
        int pos = openParenPos + 1;
        boolean inSingleQuote = false;
        boolean inDoubleQuote = false;

        while (pos < sql.length() && depth > 0) {
            char c = sql.charAt(pos);

            // Handle escaped characters by counting consecutive backslashes
            if (c == '\'' && !inDoubleQuote) {
                if (!isEscaped(sql, pos)) {
                    inSingleQuote = !inSingleQuote;
                }
            }
            else if (c == '"' && !inSingleQuote) {
                if (!isEscaped(sql, pos)) {
                    inDoubleQuote = !inDoubleQuote;
                }
            }
            else if (!inSingleQuote && !inDoubleQuote) {
                if (c == '(') {
                    depth++;
                }
                else if (c == ')') {
                    depth--;
                }
            }

            if (depth == 0) {
                return pos;
            }

            pos++;
        }

        return -1;
    }

    /**
     * Checks if the character at the given position is escaped by counting
     * consecutive backslashes before it. An odd number of backslashes means escaped.
     */
    private static boolean isEscaped(String sql, int pos)
    {
        int backslashCount = 0;
        int checkPos = pos - 1;
        while (checkPos >= 0 && sql.charAt(checkPos) == '\\') {
            backslashCount++;
            checkPos--;
        }
        return backslashCount % 2 == 1;
    }

    /**
     * Checks if the given position is inside a string literal (single or double quoted).
     */
    private static boolean isInsideStringLiteral(String sql, int pos)
    {
        boolean inSingleQuote = false;
        boolean inDoubleQuote = false;

        for (int i = 0; i < pos && i < sql.length(); i++) {
            char c = sql.charAt(i);
            if (c == '\'' && !inDoubleQuote && !isEscaped(sql, i)) {
                inSingleQuote = !inSingleQuote;
            }
            else if (c == '"' && !inSingleQuote && !isEscaped(sql, i)) {
                inDoubleQuote = !inDoubleQuote;
            }
        }

        return inSingleQuote || inDoubleQuote;
    }
}
