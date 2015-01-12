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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.spi.PrestoException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestJsonFunctions
{
    private FunctionAssertions functionAssertions;

    @BeforeClass
    public void setUp()
    {
        functionAssertions = new FunctionAssertions();
    }

    @Test
    public void testJsonArrayLength()
    {
        assertFunction("JSON_ARRAY_LENGTH('[]')", 0);
        assertFunction("JSON_ARRAY_LENGTH('[1]')", 1);
        assertFunction("JSON_ARRAY_LENGTH('[1, \"foo\", null]')", 3);
        assertFunction("JSON_ARRAY_LENGTH('[2, 4, {\"a\": [8, 9]}, [], [5], 4]')", 6);
        assertFunction("JSON_ARRAY_LENGTH(CAST('[]' AS JSON))", 0);
        assertFunction("JSON_ARRAY_LENGTH(CAST('[1]' AS JSON))", 1);
        assertFunction("JSON_ARRAY_LENGTH(CAST('[1, \"foo\", null]' AS JSON))", 3);
        assertFunction("JSON_ARRAY_LENGTH(CAST('[2, 4, {\"a\": [8, 9]}, [], [5], 4]' AS JSON))", 6);
        assertFunction("JSON_ARRAY_LENGTH(null)", null);
    }

    @Test
    public void testJsonArrayContainsBoolean()
    {
        assertFunction("JSON_ARRAY_CONTAINS('[]', true)", false);
        assertFunction("JSON_ARRAY_CONTAINS('[true]', true)", true);
        assertFunction("JSON_ARRAY_CONTAINS('[false]', false)", true);
        assertFunction("JSON_ARRAY_CONTAINS('[true, false]', false)", true);
        assertFunction("JSON_ARRAY_CONTAINS('[false, true]', true)", true);
        assertFunction("JSON_ARRAY_CONTAINS('[1]', true)", false);
        assertFunction("JSON_ARRAY_CONTAINS('[[true]]', true)", false);
        assertFunction("JSON_ARRAY_CONTAINS('[1, \"foo\", null, \"true\"]', true)", false);
        assertFunction("JSON_ARRAY_CONTAINS('[2, 4, {\"a\": [8, 9]}, [], [5], false]', false)", true);
        assertFunction("JSON_ARRAY_CONTAINS(CAST('[]' AS JSON), true)", false);
        assertFunction("JSON_ARRAY_CONTAINS(CAST('[true]' AS JSON), true)", true);
        assertFunction("JSON_ARRAY_CONTAINS(CAST('[false]' AS JSON), false)", true);
        assertFunction("JSON_ARRAY_CONTAINS(CAST('[true, false]' AS JSON), false)", true);
        assertFunction("JSON_ARRAY_CONTAINS(CAST('[false, true]' AS JSON), true)", true);
        assertFunction("JSON_ARRAY_CONTAINS(CAST('[1]' AS JSON), true)", false);
        assertFunction("JSON_ARRAY_CONTAINS(CAST('[[true]]' AS JSON), true)", false);
        assertFunction("JSON_ARRAY_CONTAINS(CAST('[1, \"foo\", null, \"true\"]' AS JSON), true)", false);
        assertFunction("JSON_ARRAY_CONTAINS(CAST('[2, 4, {\"a\": [8, 9]}, [], [5], false]' AS JSON), false)", true);
        assertFunction("JSON_ARRAY_CONTAINS(null, true)", null);
        assertFunction("JSON_ARRAY_CONTAINS(null, null)", null);
        assertFunction("JSON_ARRAY_CONTAINS('[]', null)", null);
    }

    @Test
    public void testJsonArrayContainsLong()
    {
        assertFunction("JSON_ARRAY_CONTAINS('[]', 1)", false);
        assertFunction("JSON_ARRAY_CONTAINS('[3]', 3)", true);
        assertFunction("JSON_ARRAY_CONTAINS('[-4]', -4)", true);
        assertFunction("JSON_ARRAY_CONTAINS('[1.0]', 1)", false);
        assertFunction("JSON_ARRAY_CONTAINS('[[2]]', 2)", false);
        assertFunction("JSON_ARRAY_CONTAINS('[1, \"foo\", null, \"8\"]', 8)", false);
        assertFunction("JSON_ARRAY_CONTAINS('[2, 4, {\"a\": [8, 9]}, [], [5], 6]', 6)", true);
        assertFunction("JSON_ARRAY_CONTAINS('[92233720368547758071]', -9)", false);
        assertFunction("JSON_ARRAY_CONTAINS(CAST('[]' AS JSON), 1)", false);
        assertFunction("JSON_ARRAY_CONTAINS(CAST('[3]' AS JSON), 3)", true);
        assertFunction("JSON_ARRAY_CONTAINS(CAST('[-4]' AS JSON), -4)", true);
        assertFunction("JSON_ARRAY_CONTAINS(CAST('[1.0]' AS JSON), 1)", false);
        assertFunction("JSON_ARRAY_CONTAINS(CAST('[[2]]' AS JSON), 2)", false);
        assertFunction("JSON_ARRAY_CONTAINS(CAST('[1, \"foo\", null, \"8\"]' AS JSON), 8)", false);
        assertFunction("JSON_ARRAY_CONTAINS(CAST('[2, 4, {\"a\": [8, 9]}, [], [5], 6]' AS JSON), 6)", true);
        assertFunction("JSON_ARRAY_CONTAINS(CAST('[92233720368547758071]' AS JSON), -9)", false);
        assertFunction("JSON_ARRAY_CONTAINS(null, 1)", null);
        assertFunction("JSON_ARRAY_CONTAINS(null, null)", null);
        assertFunction("JSON_ARRAY_CONTAINS('[3]', null)", null);
    }

    @Test
    public void testJsonArrayContainsDouble()
    {
        assertFunction("JSON_ARRAY_CONTAINS('[]', 1)", false);
        assertFunction("JSON_ARRAY_CONTAINS('[1.5]', 1.5)", true);
        assertFunction("JSON_ARRAY_CONTAINS('[-9.5]', -9.5)", true);
        assertFunction("JSON_ARRAY_CONTAINS('[1]', 1.0)", false);
        assertFunction("JSON_ARRAY_CONTAINS('[[2.5]]', 2.5)", false);
        assertFunction("JSON_ARRAY_CONTAINS('[1, \"foo\", null, \"8.2\"]', 8.2)", false);
        assertFunction("JSON_ARRAY_CONTAINS('[2, 4, {\"a\": [8, 9]}, [], [5], 6.1]', 6.1)", true);
        assertFunction("JSON_ARRAY_CONTAINS('[9.6E400]', 4.2)", false);
        assertFunction("JSON_ARRAY_CONTAINS(CAST('[]' AS JSON), 1)", false);
        assertFunction("JSON_ARRAY_CONTAINS(CAST('[1.5]' AS JSON), 1.5)", true);
        assertFunction("JSON_ARRAY_CONTAINS(CAST('[-9.5]' AS JSON), -9.5)", true);
        assertFunction("JSON_ARRAY_CONTAINS(CAST('[1]' AS JSON), 1.0)", false);
        assertFunction("JSON_ARRAY_CONTAINS(CAST('[[2.5]]' AS JSON), 2.5)", false);
        assertFunction("JSON_ARRAY_CONTAINS(CAST('[1, \"foo\", null, \"8.2\"]' AS JSON), 8.2)", false);
        assertFunction("JSON_ARRAY_CONTAINS(CAST('[2, 4, {\"a\": [8, 9]}, [], [5], 6.1]' AS JSON), 6.1)", true);
        assertFunction("JSON_ARRAY_CONTAINS(CAST('[9.6E400]' AS JSON), 4.2)", false);
        assertFunction("JSON_ARRAY_CONTAINS(null, 1.5)", null);
        assertFunction("JSON_ARRAY_CONTAINS(null, null)", null);
        assertFunction("JSON_ARRAY_CONTAINS('[3.5]', null)", null);
    }

    @Test
    public void testJsonArrayContainsString()
    {
        assertFunction("JSON_ARRAY_CONTAINS('[]', 'x')", false);
        assertFunction("JSON_ARRAY_CONTAINS('[\"foo\"]', 'foo')", true);
        assertFunction("JSON_ARRAY_CONTAINS('[\"foo\", null]', cast(null as varchar))", null);
        assertFunction("JSON_ARRAY_CONTAINS('[\"8\"]', '8')", true);
        assertFunction("JSON_ARRAY_CONTAINS('[1, \"foo\", null]', 'foo')", true);
        assertFunction("JSON_ARRAY_CONTAINS('[1, 5]', '5')", false);
        assertFunction("JSON_ARRAY_CONTAINS('[2, 4, {\"a\": [8, 9]}, [], [5], \"6\"]', '6')", true);
        assertFunction("JSON_ARRAY_CONTAINS(CAST('[]' AS JSON), 'x')", false);
        assertFunction("JSON_ARRAY_CONTAINS(CAST('[\"foo\"]' AS JSON), 'foo')", true);
        assertFunction("JSON_ARRAY_CONTAINS(CAST('[\"foo\", null]' AS JSON), cast(null as varchar))", null);
        assertFunction("JSON_ARRAY_CONTAINS(CAST('[\"8\"]' AS JSON), '8')", true);
        assertFunction("JSON_ARRAY_CONTAINS(CAST('[1, \"foo\", null]' AS JSON), 'foo')", true);
        assertFunction("JSON_ARRAY_CONTAINS(CAST('[1, 5]' AS JSON), '5')", false);
        assertFunction("JSON_ARRAY_CONTAINS(CAST('[2, 4, {\"a\": [8, 9]}, [], [5], \"6\"]' AS JSON), '6')", true);
        assertFunction("JSON_ARRAY_CONTAINS(null, 'x')", null);
        assertFunction("JSON_ARRAY_CONTAINS(null, '')", null);
        assertFunction("JSON_ARRAY_CONTAINS(null, null)", null);
        assertFunction("JSON_ARRAY_CONTAINS('[\"\"]', null)", null);
        assertFunction("JSON_ARRAY_CONTAINS('[\"\"]', '')", true);
        assertFunction("JSON_ARRAY_CONTAINS('[\"\"]', 'x')", false);
    }

    @Test
    public void testJsonArrayGetLong()
    {
        assertFunction("JSON_ARRAY_GET('[1]', 0)", utf8Slice(String.valueOf(1)));
        assertFunction("JSON_ARRAY_GET('[2, 7, 4]', 1)", utf8Slice(String.valueOf(7)));
        assertFunction("JSON_ARRAY_GET('[2, 7, 4, 6, 8, 1, 0]', 6)", utf8Slice(String.valueOf(0)));
        assertFunction("JSON_ARRAY_GET('[]', 0)", null);
        assertFunction("JSON_ARRAY_GET('[1, 3, 2]', 3)", null);
        assertFunction("JSON_ARRAY_GET('[2, 7, 4, 6, 8, 1, 0]', -1)", utf8Slice(String.valueOf(0)));
        assertFunction("JSON_ARRAY_GET('[2, 7, 4, 6, 8, 1, 0]', -2)", utf8Slice(String.valueOf(1)));
        assertFunction("JSON_ARRAY_GET('[2, 7, 4, 6, 8, 1, 0]', -7)", utf8Slice(String.valueOf(2)));
        assertFunction("JSON_ARRAY_GET('[2, 7, 4, 6, 8, 1, 0]', -8)", null);
        assertFunction("JSON_ARRAY_GET(CAST('[1]' AS JSON), 0)", utf8Slice(String.valueOf(1)));
        assertFunction("JSON_ARRAY_GET(CAST('[2, 7, 4]' AS JSON), 1)", utf8Slice(String.valueOf(7)));
        assertFunction("JSON_ARRAY_GET(CAST('[2, 7, 4, 6, 8, 1, 0]' AS JSON), 6)", utf8Slice(String.valueOf(0)));
        assertFunction("JSON_ARRAY_GET(CAST('[]' AS JSON), 0)", null);
        assertFunction("JSON_ARRAY_GET(CAST('[1, 3, 2]' AS JSON), 3)", null);
        assertFunction("JSON_ARRAY_GET(CAST('[2, 7, 4, 6, 8, 1, 0]' AS JSON), -1)", utf8Slice(String.valueOf(0)));
        assertFunction("JSON_ARRAY_GET(CAST('[2, 7, 4, 6, 8, 1, 0]' AS JSON), -2)", utf8Slice(String.valueOf(1)));
        assertFunction("JSON_ARRAY_GET(CAST('[2, 7, 4, 6, 8, 1, 0]' AS JSON), -7)", utf8Slice(String.valueOf(2)));
        assertFunction("JSON_ARRAY_GET(CAST('[2, 7, 4, 6, 8, 1, 0]' AS JSON), -8)", null);
        assertFunction("JSON_ARRAY_GET('[]', null)", null);
        assertFunction("JSON_ARRAY_GET('[1]', null)", null);
        assertFunction("JSON_ARRAY_GET('', null)", null);
        assertFunction("JSON_ARRAY_GET('', 1)", null);
        assertFunction("JSON_ARRAY_GET('', -1)", null);
    }

    @Test
    public void testJsonArrayGetString()
    {
        assertFunction("JSON_ARRAY_GET('[\"jhfa\"]', 0)", "jhfa");
        assertFunction("JSON_ARRAY_GET('[\"jhfa\", null]', 1)", null);
        assertFunction("JSON_ARRAY_GET('[\"as\", \"fgs\", \"tehgf\"]', 1)", "fgs");
        assertFunction("JSON_ARRAY_GET('[\"as\", \"fgs\", \"tehgf\", \"gjyj\", \"jut\"]', 4)", "jut");
        assertFunction("JSON_ARRAY_GET(CAST('[\"jhfa\"]' AS JSON), 0)", "jhfa");
        assertFunction("JSON_ARRAY_GET(CAST('[\"jhfa\", null]' AS JSON), 1)", null);
        assertFunction("JSON_ARRAY_GET(CAST('[\"as\", \"fgs\", \"tehgf\"]' AS JSON), 1)", "fgs");
        assertFunction("JSON_ARRAY_GET(CAST('[\"as\", \"fgs\", \"tehgf\", \"gjyj\", \"jut\"]' AS JSON), 4)", "jut");
        assertFunction("JSON_ARRAY_GET('[\"\"]', 0)", "");
        assertFunction("JSON_ARRAY_GET('[]', 0)", null);
        assertFunction("JSON_ARRAY_GET('[null]', 0)", null);
        assertFunction("JSON_ARRAY_GET('[]', null)", null);
    }

    @Test
    public void testJsonArrayGetDouble()
    {
        assertFunction("JSON_ARRAY_GET('[3.14]', 0)", utf8Slice(String.valueOf(3.14)));
        assertFunction("JSON_ARRAY_GET('[3.14, null]', 1)", null);
        assertFunction("JSON_ARRAY_GET('[1.12, 3.54, 2.89]', 1)", utf8Slice(String.valueOf(3.54)));
        assertFunction("JSON_ARRAY_GET('[0.58, 9.7, 7.6, 11.2, 5.02]', 4)", utf8Slice(String.valueOf(5.02)));
        assertFunction("JSON_ARRAY_GET(CAST('[3.14]' AS JSON), 0)", utf8Slice(String.valueOf(3.14)));
        assertFunction("JSON_ARRAY_GET(CAST('[3.14, null]' AS JSON), 1)", null);
        assertFunction("JSON_ARRAY_GET(CAST('[1.12, 3.54, 2.89]' AS JSON), 1)", utf8Slice(String.valueOf(3.54)));
        assertFunction("JSON_ARRAY_GET(CAST('[0.58, 9.7, 7.6, 11.2, 5.02]' AS JSON), 4)", utf8Slice(String.valueOf(5.02)));
        assertFunction("JSON_ARRAY_GET('[1.0]', -1)", utf8Slice(String.valueOf(1.0)));
        assertFunction("JSON_ARRAY_GET('[1.0]', null)", null);
    }

    @Test
    public void testJsonArrayGetBoolean()
    {
        assertFunction("JSON_ARRAY_GET('[true]', 0)", utf8Slice(String.valueOf(true)));
        assertFunction("JSON_ARRAY_GET('[true, null]', 1)", null);
        assertFunction("JSON_ARRAY_GET('[false, false, true]', 1)", utf8Slice(String.valueOf(false)));
        assertFunction(
                "JSON_ARRAY_GET('[true, false, false, true, true, false]', 5)",
                utf8Slice(String.valueOf(false))
        );
        assertFunction("JSON_ARRAY_GET(CAST('[true]' AS JSON), 0)", utf8Slice(String.valueOf(true)));
        assertFunction("JSON_ARRAY_GET(CAST('[true, null]' AS JSON), 1)", null);
        assertFunction("JSON_ARRAY_GET(CAST('[false, false, true]' AS JSON), 1)", utf8Slice(String.valueOf(false)));
        assertFunction(
                "JSON_ARRAY_GET(CAST('[true, false, false, true, true, false]' AS JSON), 5)",
                utf8Slice(String.valueOf(false))
        );
        assertFunction("JSON_ARRAY_GET('[true]', -1)", utf8Slice(String.valueOf(true)));
    }

    @Test
    public void testJsonArrayGetNonScalar()
    {
        assertFunction("JSON_ARRAY_GET('[{\"hello\":\"world\"}]', 0)", utf8Slice(String.valueOf("{\"hello\":\"world\"}")));
        assertFunction("JSON_ARRAY_GET('[{\"hello\":\"world\"}, [1,2,3]]', 1)", utf8Slice(String.valueOf("[1,2,3]")));
        assertFunction("JSON_ARRAY_GET('[{\"hello\":\"world\"}, [1,2, {\"x\" : 2} ]]', 1)", utf8Slice(String.valueOf("[1,2,{\"x\":2}]")));
        assertFunction("JSON_ARRAY_GET('[{\"hello\":\"world\"}, {\"a\":[{\"x\":99}]}]', 1)", utf8Slice(String.valueOf("{\"a\":[{\"x\":99}]}")));
        assertFunction("JSON_ARRAY_GET('[{\"hello\":\"world\"}, {\"a\":[{\"x\":99}]}]', -1)", utf8Slice(String.valueOf("{\"a\":[{\"x\":99}]}")));
        assertFunction("JSON_ARRAY_GET('[{\"hello\": null}]', 0)", utf8Slice(String.valueOf("{\"hello\":null}")));
        assertFunction("JSON_ARRAY_GET('[{\"\":\"\"}]', 0)", utf8Slice(String.valueOf("{\"\":\"\"}")));
        assertFunction("JSON_ARRAY_GET('[{null:null}]', 0)", null);
        assertFunction("JSON_ARRAY_GET('[{null:\"\"}]', 0)", null);
        assertFunction("JSON_ARRAY_GET('[{\"\":null}]', 0)", utf8Slice(String.valueOf("{\"\":null}")));
        assertFunction("JSON_ARRAY_GET('[{\"\":null}]', -1)", utf8Slice(String.valueOf("{\"\":null}")));
    }

    @Test
    public void testJsonArrayContainsInvalid()
    {
        for (String value : new String[] {"'x'", "2.5", "8", "true", "cast(null as varchar)"}) {
            for (String array : new String[] {"", "123", "[", "[1,0,]", "[1,,0]"}) {
                assertFunction(format("JSON_ARRAY_CONTAINS('%s', %s)", array, value), null);
            }
        }
    }

    @Test
    public void testInvalidJsonCast()
    {
        try {
            assertFunction("CAST('INVALID' AS JSON)", null);
            fail();
        }
        catch (PrestoException e) {
            // Expected
        }
    }

    @Test
    public void testJsonSize()
    {
        assertFunction(format("JSON_SIZE('%s', '%s')", "{\"x\": {\"a\" : 1, \"b\" : 2} }", "$"), 1);
        assertFunction(format("JSON_SIZE('%s', '%s')", "{\"x\": {\"a\" : 1, \"b\" : 2} }", "$.x"), 2);
        assertFunction(format("JSON_SIZE('%s', '%s')", "{\"x\": {\"a\" : 1, \"b\" : [1,2,3], \"c\" : {\"w\":9}} }", "$.x"), 3);
        assertFunction(format("JSON_SIZE('%s', '%s')", "{\"x\": {\"a\" : 1, \"b\" : 2} }", "$.x.a"), 0);
        assertFunction(format("JSON_SIZE('%s', '%s')", "[1,2,3]", "$"), 3);
        assertFunction(format("JSON_SIZE(null, '%s')", "$"), null);
        assertFunction(format("JSON_SIZE('%s', '%s')", "INVALID_JSON", "$"), null);
        assertFunction(format("JSON_SIZE('%s', null)", "[1,2,3]"), null);
        assertFunction(format("JSON_SIZE(CAST('%s' AS JSON), '%s')", "{\"x\": {\"a\" : 1, \"b\" : 2} }", "$"), 1);
        assertFunction(format("JSON_SIZE(CAST('%s' AS JSON), '%s')", "{\"x\": {\"a\" : 1, \"b\" : 2} }", "$.x"), 2);
        assertFunction(format("JSON_SIZE(CAST('%s' AS JSON), '%s')", "{\"x\": {\"a\" : 1, \"b\" : [1,2,3], \"c\" : {\"w\":9}} }", "$.x"), 3);
        assertFunction(format("JSON_SIZE(CAST('%s' AS JSON), '%s')", "{\"x\": {\"a\" : 1, \"b\" : 2} }", "$.x.a"), 0);
        assertFunction(format("JSON_SIZE(CAST('%s' AS JSON), '%s')", "[1,2,3]", "$"), 3);
        assertFunction(format("JSON_SIZE(null, '%s')", "$"), null);
        assertFunction(format("JSON_SIZE(CAST('%s' AS JSON), null)", "[1,2,3]"), null);
        assertInvalidFunction(format("JSON_SIZE('%s', '%s')", "{\"\":\"\"}", ""), "Invalid JSON path: ''");
        assertInvalidFunction(format("JSON_SIZE('%s', '%s')", "{\"\":\"\"}", "."), "Invalid JSON path: '.'");
        assertInvalidFunction(format("JSON_SIZE('%s', '%s')", "{\"\":\"\"}", "null"), "Invalid JSON path: 'null'");
        assertInvalidFunction(format("JSON_SIZE('%s', '%s')", "{\"\":\"\"}", null), "Invalid JSON path: 'null'");
    }

    @Test
    public void testJsonEquality()
    {
        assertFunction("CAST('[1,2,3]' AS JSON) = CAST('[1,2,3]' AS JSON)", true);
        assertFunction("CAST('{\"a\":1, \"b\":2}' AS JSON) = CAST('{\"b\":2, \"a\":1}' AS JSON)", true);
        assertFunction("CAST('{\"a\":1, \"b\":2}' AS JSON) = CAST(MAP(ARRAY['b','a'], ARRAY[2,1]) AS JSON)", true);
        assertFunction("CAST('null' AS JSON) = CAST('null' AS JSON)", true);
        assertFunction("CAST('true' AS JSON) = CAST('true' AS JSON)", true);
        assertFunction("CAST('{\"x\":\"y\"}' AS JSON) = CAST('{\"x\":\"y\"}' AS JSON)", true);
        assertFunction("CAST('[1,2,3]' AS JSON) = CAST('[2,3,1]' AS JSON)", false);
        assertFunction("CAST('{\"p_1\": 1, \"p_2\":\"v_2\", \"p_3\":null, \"p_4\":true, \"p_5\": {\"p_1\":1}}' AS JSON) = " +
                "CAST('{\"p_2\":\"v_2\", \"p_4\":true, \"p_1\": 1, \"p_3\":null, \"p_5\": {\"p_1\":1}}' AS JSON)", true);

        assertFunction("CAST('[1,2,3]' AS JSON) != CAST('[1,2,3]' AS JSON)", false);
        assertFunction("CAST('{\"a\":1, \"b\":2}' AS JSON) != CAST('{\"b\":2, \"a\":1}' AS JSON)", false);
        assertFunction("CAST('null' AS JSON) != CAST('null' AS JSON)", false);
        assertFunction("CAST('true' AS JSON) != CAST('true' AS JSON)", false);
        assertFunction("CAST('{\"x\":\"y\"}' AS JSON) != CAST('{\"x\":\"y\"}' AS JSON)", false);
        assertFunction("CAST('[1,2,3]' AS JSON) != CAST('[2,3,1]' AS JSON)", true);
        assertFunction("CAST('{\"p_1\": 1, \"p_2\":\"v_2\", \"p_3\":null, \"p_4\":true, \"p_5\": {\"p_1\":1}}' AS JSON) != " +
                "CAST('{\"p_2\":\"v_2\", \"p_4\":true, \"p_1\": 1, \"p_3\":null, \"p_5\": {\"p_1\":1}}' AS JSON)", false);
    }

    private void assertFunction(String projection, Object expected)
    {
        functionAssertions.assertFunction(projection, expected);
    }

    private void assertInvalidFunction(String projection, String message)
    {
        try {
            assertFunction(projection, null);
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), INVALID_FUNCTION_ARGUMENT.toErrorCode());
            assertEquals(e.getMessage(), message);
        }
    }
}
