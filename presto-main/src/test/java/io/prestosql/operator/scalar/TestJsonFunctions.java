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
package io.prestosql.operator.scalar;

import org.testng.annotations.Test;

import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.type.JsonType.JSON;
import static java.lang.String.format;

public class TestJsonFunctions
        extends AbstractTestFunctions
{
    @Test
    public void testIsJsonScalar()
    {
        assertFunction("IS_JSON_SCALAR(null)", BOOLEAN, null);

        assertFunction("IS_JSON_SCALAR(JSON 'null')", BOOLEAN, true);
        assertFunction("IS_JSON_SCALAR(JSON 'true')", BOOLEAN, true);
        assertFunction("IS_JSON_SCALAR(JSON '1')", BOOLEAN, true);
        assertFunction("IS_JSON_SCALAR(JSON '\"str\"')", BOOLEAN, true);
        assertFunction("IS_JSON_SCALAR('null')", BOOLEAN, true);
        assertFunction("IS_JSON_SCALAR('true')", BOOLEAN, true);
        assertFunction("IS_JSON_SCALAR('1')", BOOLEAN, true);
        assertFunction("IS_JSON_SCALAR('\"str\"')", BOOLEAN, true);

        assertFunction("IS_JSON_SCALAR(JSON '[1, 2, 3]')", BOOLEAN, false);
        assertFunction("IS_JSON_SCALAR(JSON '{\"a\": 1, \"b\": 2}')", BOOLEAN, false);
        assertFunction("IS_JSON_SCALAR('[1, 2, 3]')", BOOLEAN, false);
        assertFunction("IS_JSON_SCALAR('{\"a\": 1, \"b\": 2}')", BOOLEAN, false);

        assertInvalidFunction("IS_JSON_SCALAR('')", INVALID_FUNCTION_ARGUMENT, "Invalid JSON value: ");
        assertInvalidFunction("IS_JSON_SCALAR('[1')", INVALID_FUNCTION_ARGUMENT, "Invalid JSON value: [1");
        assertInvalidFunction("IS_JSON_SCALAR('1 trailing')", INVALID_FUNCTION_ARGUMENT, "Invalid JSON value: 1 trailing");
        assertInvalidFunction("IS_JSON_SCALAR('[1, 2] trailing')", INVALID_FUNCTION_ARGUMENT, "Invalid JSON value: [1, 2] trailing");
    }

    @Test
    public void testJsonArrayLength()
    {
        assertFunction("JSON_ARRAY_LENGTH('[]')", BIGINT, 0L);
        assertFunction("JSON_ARRAY_LENGTH('[1]')", BIGINT, 1L);
        assertFunction("JSON_ARRAY_LENGTH('[1, \"foo\", null]')", BIGINT, 3L);
        assertFunction("JSON_ARRAY_LENGTH('[2, 4, {\"a\": [8, 9]}, [], [5], 4]')", BIGINT, 6L);
        assertFunction("JSON_ARRAY_LENGTH(JSON '[]')", BIGINT, 0L);
        assertFunction("JSON_ARRAY_LENGTH(JSON '[1]')", BIGINT, 1L);
        assertFunction("JSON_ARRAY_LENGTH(JSON '[1, \"foo\", null]')", BIGINT, 3L);
        assertFunction("JSON_ARRAY_LENGTH(JSON '[2, 4, {\"a\": [8, 9]}, [], [5], 4]')", BIGINT, 6L);
        assertFunction("JSON_ARRAY_LENGTH(null)", BIGINT, null);
    }

    @Test
    public void testJsonArrayContainsBoolean()
    {
        assertFunction("JSON_ARRAY_CONTAINS('[]', true)", BOOLEAN, false);
        assertFunction("JSON_ARRAY_CONTAINS('[true]', true)", BOOLEAN, true);
        assertFunction("JSON_ARRAY_CONTAINS('[false]', false)", BOOLEAN, true);
        assertFunction("JSON_ARRAY_CONTAINS('[true, false]', false)", BOOLEAN, true);
        assertFunction("JSON_ARRAY_CONTAINS('[false, true]', true)", BOOLEAN, true);
        assertFunction("JSON_ARRAY_CONTAINS('[1]', true)", BOOLEAN, false);
        assertFunction("JSON_ARRAY_CONTAINS('[[true]]', true)", BOOLEAN, false);
        assertFunction("JSON_ARRAY_CONTAINS('[1, \"foo\", null, \"true\"]', true)", BOOLEAN, false);
        assertFunction("JSON_ARRAY_CONTAINS('[2, 4, {\"a\": [8, 9]}, [], [5], false]', false)", BOOLEAN, true);
        assertFunction("JSON_ARRAY_CONTAINS(JSON '[]', true)", BOOLEAN, false);
        assertFunction("JSON_ARRAY_CONTAINS(JSON '[true]', true)", BOOLEAN, true);
        assertFunction("JSON_ARRAY_CONTAINS(JSON '[false]', false)", BOOLEAN, true);
        assertFunction("JSON_ARRAY_CONTAINS(JSON '[true, false]', false)", BOOLEAN, true);
        assertFunction("JSON_ARRAY_CONTAINS(JSON '[false, true]', true)", BOOLEAN, true);
        assertFunction("JSON_ARRAY_CONTAINS(JSON '[1]', true)", BOOLEAN, false);
        assertFunction("JSON_ARRAY_CONTAINS(JSON '[[true]]', true)", BOOLEAN, false);
        assertFunction("JSON_ARRAY_CONTAINS(JSON '[1, \"foo\", null, \"true\"]', true)", BOOLEAN, false);
        assertFunction("JSON_ARRAY_CONTAINS(JSON '[2, 4, {\"a\": [8, 9]}, [], [5], false]', false)", BOOLEAN, true);
        assertFunction("JSON_ARRAY_CONTAINS(null, true)", BOOLEAN, null);
        assertFunction("JSON_ARRAY_CONTAINS(null, null)", BOOLEAN, null);
        assertFunction("JSON_ARRAY_CONTAINS('[]', null)", BOOLEAN, null);
    }

    @Test
    public void testJsonArrayContainsLong()
    {
        assertFunction("JSON_ARRAY_CONTAINS('[]', 1)", BOOLEAN, false);
        assertFunction("JSON_ARRAY_CONTAINS('[3]', 3)", BOOLEAN, true);
        assertFunction("JSON_ARRAY_CONTAINS('[-4]', -4)", BOOLEAN, true);
        assertFunction("JSON_ARRAY_CONTAINS('[1.0]', 1)", BOOLEAN, false);
        assertFunction("JSON_ARRAY_CONTAINS('[[2]]', 2)", BOOLEAN, false);
        assertFunction("JSON_ARRAY_CONTAINS('[1, \"foo\", null, \"8\"]', 8)", BOOLEAN, false);
        assertFunction("JSON_ARRAY_CONTAINS('[2, 4, {\"a\": [8, 9]}, [], [5], 6]', 6)", BOOLEAN, true);
        assertFunction("JSON_ARRAY_CONTAINS('[92233720368547758071]', -9)", BOOLEAN, false);
        assertFunction("JSON_ARRAY_CONTAINS(JSON '[]', 1)", BOOLEAN, false);
        assertFunction("JSON_ARRAY_CONTAINS(JSON '[3]', 3)", BOOLEAN, true);
        assertFunction("JSON_ARRAY_CONTAINS(JSON '[-4]', -4)", BOOLEAN, true);
        assertFunction("JSON_ARRAY_CONTAINS(JSON '[1.0]', 1)", BOOLEAN, false);
        assertFunction("JSON_ARRAY_CONTAINS(JSON '[[2]]', 2)", BOOLEAN, false);
        assertFunction("JSON_ARRAY_CONTAINS(JSON '[1, \"foo\", null, \"8\"]', 8)", BOOLEAN, false);
        assertFunction("JSON_ARRAY_CONTAINS(JSON '[2, 4, {\"a\": [8, 9]}, [], [5], 6]', 6)", BOOLEAN, true);
        assertFunction("JSON_ARRAY_CONTAINS(JSON '[92233720368547758071]', -9)", BOOLEAN, false);
        assertFunction("JSON_ARRAY_CONTAINS(null, 1)", BOOLEAN, null);
        assertFunction("JSON_ARRAY_CONTAINS(null, null)", BOOLEAN, null);
        assertFunction("JSON_ARRAY_CONTAINS('[3]', null)", BOOLEAN, null);
    }

    @Test
    public void testJsonArrayContainsDouble()
    {
        assertFunction("JSON_ARRAY_CONTAINS('[]', 1)", BOOLEAN, false);
        assertFunction("JSON_ARRAY_CONTAINS('[1.5]', 1.5)", BOOLEAN, true);
        assertFunction("JSON_ARRAY_CONTAINS('[-9.5]', -9.5)", BOOLEAN, true);
        assertFunction("JSON_ARRAY_CONTAINS('[1]', 1.0)", BOOLEAN, false);
        assertFunction("JSON_ARRAY_CONTAINS('[[2.5]]', 2.5)", BOOLEAN, false);
        assertFunction("JSON_ARRAY_CONTAINS('[1, \"foo\", null, \"8.2\"]', 8.2)", BOOLEAN, false);
        assertFunction("JSON_ARRAY_CONTAINS('[2, 4, {\"a\": [8, 9]}, [], [5], 6.1]', 6.1)", BOOLEAN, true);
        assertFunction("JSON_ARRAY_CONTAINS('[9.6E400]', 4.2)", BOOLEAN, false);
        assertFunction("JSON_ARRAY_CONTAINS(JSON '[]', 1)", BOOLEAN, false);
        assertFunction("JSON_ARRAY_CONTAINS(JSON '[1.5]', 1.5)", BOOLEAN, true);
        assertFunction("JSON_ARRAY_CONTAINS(JSON '[-9.5]', -9.5)", BOOLEAN, true);
        assertFunction("JSON_ARRAY_CONTAINS(JSON '[1]', 1.0)", BOOLEAN, false);
        assertFunction("JSON_ARRAY_CONTAINS(JSON '[[2.5]]', 2.5)", BOOLEAN, false);
        assertFunction("JSON_ARRAY_CONTAINS(JSON '[1, \"foo\", null, \"8.2\"]', 8.2)", BOOLEAN, false);
        assertFunction("JSON_ARRAY_CONTAINS(JSON '[2, 4, {\"a\": [8, 9]}, [], [5], 6.1]', 6.1)", BOOLEAN, true);
        assertFunction("JSON_ARRAY_CONTAINS(JSON '[9.6E400]', 4.2)", BOOLEAN, false);
        assertFunction("JSON_ARRAY_CONTAINS(null, 1.5)", BOOLEAN, null);
        assertFunction("JSON_ARRAY_CONTAINS(null, null)", BOOLEAN, null);
        assertFunction("JSON_ARRAY_CONTAINS('[3.5]', null)", BOOLEAN, null);
    }

    @Test
    public void testJsonArrayContainsString()
    {
        assertFunction("JSON_ARRAY_CONTAINS('[]', 'x')", BOOLEAN, false);
        assertFunction("JSON_ARRAY_CONTAINS('[\"foo\"]', 'foo')", BOOLEAN, true);
        assertFunction("JSON_ARRAY_CONTAINS('[\"foo\", null]', cast(null as varchar))", BOOLEAN, null);
        assertFunction("JSON_ARRAY_CONTAINS('[\"8\"]', '8')", BOOLEAN, true);
        assertFunction("JSON_ARRAY_CONTAINS('[1, \"foo\", null]', 'foo')", BOOLEAN, true);
        assertFunction("JSON_ARRAY_CONTAINS('[1, 5]', '5')", BOOLEAN, false);
        assertFunction("JSON_ARRAY_CONTAINS('[2, 4, {\"a\": [8, 9]}, [], [5], \"6\"]', '6')", BOOLEAN, true);
        assertFunction("JSON_ARRAY_CONTAINS(JSON '[]', 'x')", BOOLEAN, false);
        assertFunction("JSON_ARRAY_CONTAINS(JSON '[\"foo\"]', 'foo')", BOOLEAN, true);
        assertFunction("JSON_ARRAY_CONTAINS(JSON '[\"foo\", null]', cast(null as varchar))", BOOLEAN, null);
        assertFunction("JSON_ARRAY_CONTAINS(JSON '[\"8\"]', '8')", BOOLEAN, true);
        assertFunction("JSON_ARRAY_CONTAINS(JSON '[1, \"foo\", null]', 'foo')", BOOLEAN, true);
        assertFunction("JSON_ARRAY_CONTAINS(JSON '[1, 5]', '5')", BOOLEAN, false);
        assertFunction("JSON_ARRAY_CONTAINS(JSON '[2, 4, {\"a\": [8, 9]}, [], [5], \"6\"]', '6')", BOOLEAN, true);
        assertFunction("JSON_ARRAY_CONTAINS(null, 'x')", BOOLEAN, null);
        assertFunction("JSON_ARRAY_CONTAINS(null, '')", BOOLEAN, null);
        assertFunction("JSON_ARRAY_CONTAINS(null, null)", BOOLEAN, null);
        assertFunction("JSON_ARRAY_CONTAINS('[\"\"]', null)", BOOLEAN, null);
        assertFunction("JSON_ARRAY_CONTAINS('[\"\"]', '')", BOOLEAN, true);
        assertFunction("JSON_ARRAY_CONTAINS('[\"\"]', 'x')", BOOLEAN, false);
    }

    @Test
    public void testJsonArrayGetLong()
    {
        assertFunction("JSON_ARRAY_GET('[1]', 0)", JSON, utf8Slice(String.valueOf(1)));
        assertFunction("JSON_ARRAY_GET('[2, 7, 4]', 1)", JSON, utf8Slice(String.valueOf(7)));
        assertFunction("JSON_ARRAY_GET('[2, 7, 4, 6, 8, 1, 0]', 6)", JSON, utf8Slice(String.valueOf(0)));
        assertFunction("JSON_ARRAY_GET('[]', 0)", JSON, null);
        assertFunction("JSON_ARRAY_GET('[1, 3, 2]', 3)", JSON, null);
        assertFunction("JSON_ARRAY_GET('[2, 7, 4, 6, 8, 1, 0]', -1)", JSON, utf8Slice(String.valueOf(0)));
        assertFunction("JSON_ARRAY_GET('[2, 7, 4, 6, 8, 1, 0]', -2)", JSON, utf8Slice(String.valueOf(1)));
        assertFunction("JSON_ARRAY_GET('[2, 7, 4, 6, 8, 1, 0]', -7)", JSON, utf8Slice(String.valueOf(2)));
        assertFunction("JSON_ARRAY_GET('[2, 7, 4, 6, 8, 1, 0]', -8)", JSON, null);
        assertFunction("JSON_ARRAY_GET(JSON '[1]', 0)", JSON, utf8Slice(String.valueOf(1)));
        assertFunction("JSON_ARRAY_GET(JSON '[2, 7, 4]', 1)", JSON, utf8Slice(String.valueOf(7)));
        assertFunction("JSON_ARRAY_GET(JSON '[2, 7, 4, 6, 8, 1, 0]', 6)", JSON, utf8Slice(String.valueOf(0)));
        assertFunction("JSON_ARRAY_GET(JSON '[]', 0)", JSON, null);
        assertFunction("JSON_ARRAY_GET(JSON '[1, 3, 2]', 3)", JSON, null);
        assertFunction("JSON_ARRAY_GET(JSON '[2, 7, 4, 6, 8, 1, 0]', -1)", JSON, utf8Slice(String.valueOf(0)));
        assertFunction("JSON_ARRAY_GET(JSON '[2, 7, 4, 6, 8, 1, 0]', -2)", JSON, utf8Slice(String.valueOf(1)));
        assertFunction("JSON_ARRAY_GET(JSON '[2, 7, 4, 6, 8, 1, 0]', -7)", JSON, utf8Slice(String.valueOf(2)));
        assertFunction("JSON_ARRAY_GET(JSON '[2, 7, 4, 6, 8, 1, 0]', -8)", JSON, null);
        assertFunction("JSON_ARRAY_GET('[]', null)", JSON, null);
        assertFunction("JSON_ARRAY_GET('[1]', null)", JSON, null);
        assertFunction("JSON_ARRAY_GET('', null)", JSON, null);
        assertFunction("JSON_ARRAY_GET('', 1)", JSON, null);
        assertFunction("JSON_ARRAY_GET('', -1)", JSON, null);
        assertFunction("JSON_ARRAY_GET('[1]', -9223372036854775807 - 1)", JSON, null);
    }

    @Test
    public void testJsonArrayGetString()
    {
        assertFunction("JSON_ARRAY_GET('[\"jhfa\"]', 0)", JSON, "jhfa");
        assertFunction("JSON_ARRAY_GET('[\"jhfa\", null]', 1)", JSON, null);
        assertFunction("JSON_ARRAY_GET('[\"as\", \"fgs\", \"tehgf\"]', 1)", JSON, "fgs");
        assertFunction("JSON_ARRAY_GET('[\"as\", \"fgs\", \"tehgf\", \"gjyj\", \"jut\"]', 4)", JSON, "jut");
        assertFunction("JSON_ARRAY_GET(JSON '[\"jhfa\"]', 0)", JSON, "jhfa");
        assertFunction("JSON_ARRAY_GET(JSON '[\"jhfa\", null]', 1)", JSON, null);
        assertFunction("JSON_ARRAY_GET(JSON '[\"as\", \"fgs\", \"tehgf\"]', 1)", JSON, "fgs");
        assertFunction("JSON_ARRAY_GET(JSON '[\"as\", \"fgs\", \"tehgf\", \"gjyj\", \"jut\"]', 4)", JSON, "jut");
        assertFunction("JSON_ARRAY_GET('[\"\"]', 0)", JSON, "");
        assertFunction("JSON_ARRAY_GET('[]', 0)", JSON, null);
        assertFunction("JSON_ARRAY_GET('[null]', 0)", JSON, null);
        assertFunction("JSON_ARRAY_GET('[]', null)", JSON, null);
        assertFunction("JSON_ARRAY_GET('[1]', -9223372036854775807 - 1)", JSON, null);
    }

    @Test
    public void testJsonArrayGetDouble()
    {
        assertFunction("JSON_ARRAY_GET('[3.14]', 0)", JSON, utf8Slice(String.valueOf(3.14)));
        assertFunction("JSON_ARRAY_GET('[3.14, null]', 1)", JSON, null);
        assertFunction("JSON_ARRAY_GET('[1.12, 3.54, 2.89]', 1)", JSON, utf8Slice(String.valueOf(3.54)));
        assertFunction("JSON_ARRAY_GET('[0.58, 9.7, 7.6, 11.2, 5.02]', 4)", JSON, utf8Slice(String.valueOf(5.02)));
        assertFunction("JSON_ARRAY_GET(JSON '[3.14]', 0)", JSON, utf8Slice(String.valueOf(3.14)));
        assertFunction("JSON_ARRAY_GET(JSON '[3.14, null]', 1)", JSON, null);
        assertFunction("JSON_ARRAY_GET(JSON '[1.12, 3.54, 2.89]', 1)", JSON, utf8Slice(String.valueOf(3.54)));
        assertFunction("JSON_ARRAY_GET(JSON '[0.58, 9.7, 7.6, 11.2, 5.02]', 4)", JSON, utf8Slice(String.valueOf(5.02)));
        assertFunction("JSON_ARRAY_GET('[1.0]', -1)", JSON, utf8Slice(String.valueOf(1.0)));
        assertFunction("JSON_ARRAY_GET('[1.0]', null)", JSON, null);
    }

    @Test
    public void testJsonArrayGetBoolean()
    {
        assertFunction("JSON_ARRAY_GET('[true]', 0)", JSON, utf8Slice(String.valueOf(true)));
        assertFunction("JSON_ARRAY_GET('[true, null]', 1)", JSON, null);
        assertFunction("JSON_ARRAY_GET('[false, false, true]', 1)", JSON, utf8Slice(String.valueOf(false)));
        assertFunction(
                "JSON_ARRAY_GET('[true, false, false, true, true, false]', 5)",
                JSON,
                utf8Slice(String.valueOf(false)));
        assertFunction("JSON_ARRAY_GET(JSON '[true]', 0)", JSON, utf8Slice(String.valueOf(true)));
        assertFunction("JSON_ARRAY_GET(JSON '[true, null]', 1)", JSON, null);
        assertFunction("JSON_ARRAY_GET(JSON '[false, false, true]', 1)", JSON, utf8Slice(String.valueOf(false)));
        assertFunction(
                "JSON_ARRAY_GET(JSON '[true, false, false, true, true, false]', 5)",
                JSON,
                utf8Slice(String.valueOf(false)));
        assertFunction("JSON_ARRAY_GET('[true]', -1)", JSON, utf8Slice(String.valueOf(true)));
    }

    @Test
    public void testJsonArrayGetNonScalar()
    {
        assertFunction("JSON_ARRAY_GET('[{\"hello\":\"world\"}]', 0)", JSON, utf8Slice(String.valueOf("{\"hello\":\"world\"}")));
        assertFunction("JSON_ARRAY_GET('[{\"hello\":\"world\"}, [1,2,3]]', 1)", JSON, utf8Slice(String.valueOf("[1,2,3]")));
        assertFunction("JSON_ARRAY_GET('[{\"hello\":\"world\"}, [1,2, {\"x\" : 2} ]]', 1)", JSON, utf8Slice(String.valueOf("[1,2,{\"x\":2}]")));
        assertFunction("JSON_ARRAY_GET('[{\"hello\":\"world\"}, {\"a\":[{\"x\":99}]}]', 1)", JSON, utf8Slice(String.valueOf("{\"a\":[{\"x\":99}]}")));
        assertFunction("JSON_ARRAY_GET('[{\"hello\":\"world\"}, {\"a\":[{\"x\":99}]}]', -1)", JSON, utf8Slice(String.valueOf("{\"a\":[{\"x\":99}]}")));
        assertFunction("JSON_ARRAY_GET('[{\"hello\": null}]', 0)", JSON, utf8Slice(String.valueOf("{\"hello\":null}")));
        assertFunction("JSON_ARRAY_GET('[{\"\":\"\"}]', 0)", JSON, utf8Slice(String.valueOf("{\"\":\"\"}")));
        assertFunction("JSON_ARRAY_GET('[{null:null}]', 0)", JSON, null);
        assertFunction("JSON_ARRAY_GET('[{null:\"\"}]', 0)", JSON, null);
        assertFunction("JSON_ARRAY_GET('[{\"\":null}]', 0)", JSON, utf8Slice(String.valueOf("{\"\":null}")));
        assertFunction("JSON_ARRAY_GET('[{\"\":null}]', -1)", JSON, utf8Slice(String.valueOf("{\"\":null}")));
    }

    @Test
    public void testJsonArrayContainsInvalid()
    {
        for (String value : new String[] {"'x'", "2.5", "8", "true", "cast(null as varchar)"}) {
            for (String array : new String[] {"", "123", "[", "[1,0,]", "[1,,0]"}) {
                assertFunction(format("JSON_ARRAY_CONTAINS('%s', %s)", array, value), BOOLEAN, null);
            }
        }
    }

    @Test
    public void testInvalidJsonParse()
    {
        assertInvalidFunction("JSON 'INVALID'", INVALID_FUNCTION_ARGUMENT);
        assertInvalidFunction("JSON_PARSE('INVALID')", INVALID_FUNCTION_ARGUMENT);
        assertInvalidFunction("JSON_PARSE('\"x\": 1')", INVALID_FUNCTION_ARGUMENT);
    }

    @Test
    public void testJsonFormat()
    {
        assertFunction("JSON_FORMAT(JSON '[\"a\", \"b\"]')", VARCHAR, "[\"a\",\"b\"]");
    }

    @Test
    public void testJsonSize()
    {
        assertFunction(format("JSON_SIZE('%s', '%s')", "{\"x\": {\"a\" : 1, \"b\" : 2} }", "$"), BIGINT, 1L);
        assertFunction(format("JSON_SIZE('%s', '%s')", "{\"x\": {\"a\" : 1, \"b\" : 2} }", "$.x"), BIGINT, 2L);
        assertFunction(format("JSON_SIZE('%s', '%s')", "{\"x\": {\"a\" : 1, \"b\" : [1,2,3], \"c\" : {\"w\":9}} }", "$.x"), BIGINT, 3L);
        assertFunction(format("JSON_SIZE('%s', '%s')", "{\"x\": {\"a\" : 1, \"b\" : 2} }", "$.x.a"), BIGINT, 0L);
        assertFunction(format("JSON_SIZE('%s', '%s')", "[1,2,3]", "$"), BIGINT, 3L);
        assertFunction(format("JSON_SIZE('%s', CHAR '%s')", "[1,2,3]", "$"), BIGINT, 3L);
        assertFunction(format("JSON_SIZE(null, '%s')", "$"), BIGINT, null);
        assertFunction(format("JSON_SIZE('%s', '%s')", "INVALID_JSON", "$"), BIGINT, null);
        assertFunction(format("JSON_SIZE('%s', null)", "[1,2,3]"), BIGINT, null);
        assertFunction(format("JSON_SIZE(JSON '%s', '%s')", "{\"x\": {\"a\" : 1, \"b\" : 2} }", "$"), BIGINT, 1L);
        assertFunction(format("JSON_SIZE(JSON '%s', '%s')", "{\"x\": {\"a\" : 1, \"b\" : 2} }", "$.x"), BIGINT, 2L);
        assertFunction(format("JSON_SIZE(JSON '%s', '%s')", "{\"x\": {\"a\" : 1, \"b\" : [1,2,3], \"c\" : {\"w\":9}} }", "$.x"), BIGINT, 3L);
        assertFunction(format("JSON_SIZE(JSON '%s', '%s')", "{\"x\": {\"a\" : 1, \"b\" : 2} }", "$.x.a"), BIGINT, 0L);
        assertFunction(format("JSON_SIZE(JSON '%s', '%s')", "[1,2,3]", "$"), BIGINT, 3L);
        assertFunction(format("JSON_SIZE(null, '%s')", "$"), BIGINT, null);
        assertFunction(format("JSON_SIZE(JSON '%s', null)", "[1,2,3]"), BIGINT, null);
        assertInvalidFunction(format("JSON_SIZE('%s', '%s')", "{\"\":\"\"}", ""), "Invalid JSON path: ''");
        assertInvalidFunction(format("JSON_SIZE('%s', CHAR '%s')", "{\"\":\"\"}", " "), "Invalid JSON path: ' '");
        assertInvalidFunction(format("JSON_SIZE('%s', '%s')", "{\"\":\"\"}", "."), "Invalid JSON path: '.'");
        assertInvalidFunction(format("JSON_SIZE('%s', '%s')", "{\"\":\"\"}", "null"), "Invalid JSON path: 'null'");
        assertInvalidFunction(format("JSON_SIZE('%s', '%s')", "{\"\":\"\"}", null), "Invalid JSON path: 'null'");
    }
}
