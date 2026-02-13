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

import org.testng.annotations.Test;

import static com.facebook.presto.verifier.framework.JsonParseSafetyWrapper.wrapUnsafeJsonParse;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class TestJsonParseSafetyWrapper
{
    @Test
    public void testWrapUnsafeJsonParse()
    {
        // Null and empty input
        assertNull(wrapUnsafeJsonParse(null));
        assertEquals(wrapUnsafeJsonParse(""), "");

        // No json_parse present - should return unchanged
        assertEquals(
                wrapUnsafeJsonParse("SELECT * FROM table1 WHERE id = 1"),
                "SELECT * FROM table1 WHERE id = 1");

        // Simple json_parse wrapping
        assertEquals(
                wrapUnsafeJsonParse("SELECT json_parse(column1) FROM table1"),
                "SELECT TRY(json_parse(column1)) FROM table1");

        // Nested inside another function
        assertEquals(
                wrapUnsafeJsonParse("SELECT json_extract(json_parse(data), '$.key') FROM table1"),
                "SELECT json_extract(TRY(json_parse(data)), '$.key') FROM table1");

        // Multiple json_parse calls in same query
        assertEquals(
                wrapUnsafeJsonParse("SELECT json_parse(a), json_parse(b) FROM table1"),
                "SELECT TRY(json_parse(a)), TRY(json_parse(b)) FROM table1");

        // Already wrapped in TRY - should NOT double-wrap
        assertEquals(
                wrapUnsafeJsonParse("SELECT TRY(json_parse(column1)) FROM table1"),
                "SELECT TRY(json_parse(column1)) FROM table1");

        // Mixed: some wrapped, some not
        assertEquals(
                wrapUnsafeJsonParse("SELECT TRY(json_parse(a)), json_parse(b) FROM table1"),
                "SELECT TRY(json_parse(a)), TRY(json_parse(b)) FROM table1");

        // json_parse with CAST
        assertEquals(
                wrapUnsafeJsonParse("SELECT CAST(json_parse(data) AS MAP(VARCHAR, VARCHAR)) FROM table1"),
                "SELECT CAST(TRY(json_parse(data)) AS MAP(VARCHAR, VARCHAR)) FROM table1");

        // Nested parentheses inside json_parse argument
        assertEquals(
                wrapUnsafeJsonParse("SELECT json_parse(concat(a, b, (SELECT c FROM t))) FROM table1"),
                "SELECT TRY(json_parse(concat(a, b, (SELECT c FROM t)))) FROM table1");

        // Case-insensitive json_parse (JSON_PARSE, Json_Parse)
        assertEquals(
                wrapUnsafeJsonParse("SELECT JSON_PARSE(data), Json_Parse(data2) FROM table1"),
                "SELECT TRY(JSON_PARSE(data)), TRY(Json_Parse(data2)) FROM table1");

        // String literal containing parentheses
        assertEquals(
                wrapUnsafeJsonParse("SELECT json_parse('test(value)') FROM table1"),
                "SELECT TRY(json_parse('test(value)')) FROM table1");

        // Complex query with multiple json_parse in different clauses
        assertEquals(
                wrapUnsafeJsonParse("SELECT a, json_extract(json_parse(data), '$.field'), b FROM table1 WHERE json_parse(filter) IS NOT NULL"),
                "SELECT a, json_extract(TRY(json_parse(data)), '$.field'), b FROM table1 WHERE TRY(json_parse(filter)) IS NOT NULL");

        // Double-quoted identifier
        assertEquals(
                wrapUnsafeJsonParse("SELECT json_parse(\"column\") FROM table1"),
                "SELECT TRY(json_parse(\"column\")) FROM table1");

        // json_parse text inside string literal should NOT be wrapped (only the real function call)
        assertEquals(
                wrapUnsafeJsonParse("SELECT json_parse(data), 'json_parse(not_a_function)' FROM table1"),
                "SELECT TRY(json_parse(data)), 'json_parse(not_a_function)' FROM table1");

        // Whitespace between json_parse and opening paren
        assertEquals(
                wrapUnsafeJsonParse("SELECT json_parse  (  data  ) FROM table1"),
                "SELECT TRY(json_parse  (  data  )) FROM table1");

        // json_parse in subquery
        assertEquals(
                wrapUnsafeJsonParse("SELECT * FROM (SELECT json_parse(data) AS parsed FROM table1) t"),
                "SELECT * FROM (SELECT TRY(json_parse(data)) AS parsed FROM table1) t");

        // Case-insensitive TRY (lowercase try, mixed case Try) - should NOT double-wrap
        assertEquals(
                wrapUnsafeJsonParse("SELECT try(json_parse(column1)) FROM table1"),
                "SELECT try(json_parse(column1)) FROM table1");
        assertEquals(
                wrapUnsafeJsonParse("SELECT Try(json_parse(column1)), TRY(json_parse(column2)) FROM table1"),
                "SELECT Try(json_parse(column1)), TRY(json_parse(column2)) FROM table1");

        // Whitespace AFTER TRY( before json_parse - should NOT double-wrap
        assertEquals(
                wrapUnsafeJsonParse("SELECT TRY( json_parse(column1)) FROM table1"),
                "SELECT TRY( json_parse(column1)) FROM table1");
        assertEquals(
                wrapUnsafeJsonParse("SELECT TRY(   json_parse(column1)) FROM table1"),
                "SELECT TRY(   json_parse(column1)) FROM table1");

        // Whitespace BETWEEN TRY and ( - should NOT double-wrap
        assertEquals(
                wrapUnsafeJsonParse("SELECT TRY (json_parse(column1)) FROM table1"),
                "SELECT TRY (json_parse(column1)) FROM table1");
        assertEquals(
                wrapUnsafeJsonParse("SELECT TRY   (  json_parse(column1)) FROM table1"),
                "SELECT TRY   (  json_parse(column1)) FROM table1");

        // Escaped backslash in string - should handle correctly
        assertEquals(
                wrapUnsafeJsonParse("SELECT json_parse('{\"path\": \"c:\\\\\"}') FROM table1"),
                "SELECT TRY(json_parse('{\"path\": \"c:\\\\\"}')) FROM table1");

        // Escaped quote in string
        assertEquals(
                wrapUnsafeJsonParse("SELECT json_parse('{\"name\": \"test\\'s value\"}') FROM table1"),
                "SELECT TRY(json_parse('{\"name\": \"test\\'s value\"}')) FROM table1");

        // Mixed: various TRY variants with whitespace, some wrapped some not
        assertEquals(
                wrapUnsafeJsonParse("SELECT TRY( json_parse(a)), json_parse(b), try(json_parse(c)) FROM table1"),
                "SELECT TRY( json_parse(a)), TRY(json_parse(b)), try(json_parse(c)) FROM table1");

        // Malformed SQL - missing closing paren (graceful degradation, return unchanged)
        assertEquals(
                wrapUnsafeJsonParse("SELECT json_parse(data"),
                "SELECT json_parse(data");

        // Malformed SQL - unbalanced parens
        assertEquals(
                wrapUnsafeJsonParse("SELECT json_parse((data"),
                "SELECT json_parse((data");
    }
}
