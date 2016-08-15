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
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;

import static com.facebook.presto.operator.scalar.JsonExtract.JsonExtractor;
import static com.facebook.presto.operator.scalar.JsonExtract.JsonValueJsonExtractor;
import static com.facebook.presto.operator.scalar.JsonExtract.ObjectFieldJsonExtractor;
import static com.facebook.presto.operator.scalar.JsonExtract.ScalarValueJsonExtractor;
import static com.facebook.presto.operator.scalar.JsonExtract.generateExtractor;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.airlift.slice.Slices.utf8Slice;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestJsonExtract
{
    @Test
    public void testJsonTokenizer()
    {
        assertEquals(tokenizePath("$"), ImmutableList.of());
        assertEquals(tokenizePath("$"), ImmutableList.of());
        assertEquals(tokenizePath("$.foo"), ImmutableList.of("foo"));
        assertEquals(tokenizePath("$[\"foo\"]"), ImmutableList.of("foo"));
        assertEquals(tokenizePath("$[\"foo.bar\"]"), ImmutableList.of("foo.bar"));
        assertEquals(tokenizePath("$[42]"), ImmutableList.of("42"));
        assertEquals(tokenizePath("$.42"), ImmutableList.of("42"));
        assertEquals(tokenizePath("$.42.63"), ImmutableList.of("42", "63"));
        assertEquals(tokenizePath("$.foo.42.bar.63"), ImmutableList.of("foo", "42", "bar", "63"));
        assertEquals(tokenizePath("$.x.foo"), ImmutableList.of("x", "foo"));
        assertEquals(tokenizePath("$.x[\"foo\"]"), ImmutableList.of("x", "foo"));
        assertEquals(tokenizePath("$.x[42]"), ImmutableList.of("x", "42"));
        assertEquals(tokenizePath("$.foo_42._bar63"), ImmutableList.of("foo_42", "_bar63"));
        assertEquals(tokenizePath("$[foo_42][_bar63]"), ImmutableList.of("foo_42", "_bar63"));
        assertEquals(tokenizePath("$.foo:42.:bar63"), ImmutableList.of("foo:42", ":bar63"));
        assertEquals(tokenizePath("$[\"foo:42\"][\":bar63\"]"), ImmutableList.of("foo:42", ":bar63"));

        assertPathToken("foo");

        assertQuotedPathToken("-1.1");
        assertQuotedPathToken("!@#$%^&*()[]{}/?'");
        assertQuotedPathToken("ab\\u0001c");
        assertQuotedPathToken("ab\0c");
        assertQuotedPathToken("ab\t\n\rc");
        assertQuotedPathToken(".");
        assertQuotedPathToken("$");
        assertQuotedPathToken("]");
        assertQuotedPathToken("[");
        assertQuotedPathToken("'");
        assertQuotedPathToken("!@#$%^&*(){}[]<>?/\\|.,`~\r\n\t \0");

        // colon in subscript must be quoted
        assertInvalidPath("$[foo:bar]");

        // whitespace is not allowed
        assertInvalidPath(" $.x");
        assertInvalidPath(" $.x ");
        assertInvalidPath("$. x");
        assertInvalidPath("$ .x");
        assertInvalidPath("$\n.x");
        assertInvalidPath("$.x [42]");
        assertInvalidPath("$.x[ 42]");
        assertInvalidPath("$.x[42 ]");
        assertInvalidPath("$.x[ \"foo\"]");
        assertInvalidPath("$.x[\"foo\" ]");
    }

    private static void assertPathToken(String fieldName)
    {
        assertTrue(fieldName.indexOf('"') < 0);
        assertEquals(tokenizePath("$." + fieldName), ImmutableList.of(fieldName));
        assertEquals(tokenizePath("$.foo." + fieldName + ".bar"), ImmutableList.of("foo", fieldName, "bar"));
        assertPathTokenQuoting(fieldName);
    }

    private static void assertQuotedPathToken(String fieldName)
    {
        assertPathTokenQuoting(fieldName);
        // without quoting we should get an error
        assertInvalidPath("$." + fieldName);
    }

    private static void assertPathTokenQuoting(String fieldName)
    {
        assertTrue(fieldName.indexOf('"') < 0);
        assertEquals(tokenizePath("$[\"" + fieldName + "\"]"), ImmutableList.of(fieldName));
        assertEquals(tokenizePath("$.foo[\"" + fieldName + "\"].bar"), ImmutableList.of("foo", fieldName, "bar"));
    }

    public static void assertInvalidPath(String path)
    {
        try {
            tokenizePath(path);
            fail("Expected PrestoException");
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), INVALID_FUNCTION_ARGUMENT.toErrorCode());
        }
    }

    @Test
    public void testScalarValueJsonExtractor()
            throws Exception
    {
        ScalarValueJsonExtractor extractor = new ScalarValueJsonExtractor();

        // Check scalar values
        assertEquals(doExtract(extractor, "123"), "123");
        assertEquals(doExtract(extractor, "-1"), "-1");
        assertEquals(doExtract(extractor, "0.01"), "0.01");
        assertEquals(doExtract(extractor, "\"abc\""), "abc");
        assertEquals(doExtract(extractor, "\"\""), "");
        assertEquals(doExtract(extractor, "null"), null);

        // Test character escaped values
        assertEquals(doExtract(extractor, "\"ab\\u0001c\""), "ab\001c");
        assertEquals(doExtract(extractor, "\"ab\\u0002c\""), "ab\002c");

        // Complex types should return null
        assertEquals(doExtract(extractor, "[1, 2, 3]"), null);
        assertEquals(doExtract(extractor, "{\"a\": 1}"), null);
    }

    @Test
    public void testJsonValueJsonExtractor()
            throws Exception
    {
        JsonValueJsonExtractor extractor = new JsonValueJsonExtractor();

        // Check scalar values
        assertEquals(doExtract(extractor, "123"), "123");
        assertEquals(doExtract(extractor, "-1"), "-1");
        assertEquals(doExtract(extractor, "0.01"), "0.01");
        assertEquals(doExtract(extractor, "\"abc\""), "\"abc\"");
        assertEquals(doExtract(extractor, "\"\""), "\"\"");
        assertEquals(doExtract(extractor, "null"), "null");

        // Test character escaped values
        assertEquals(doExtract(extractor, "\"ab\\u0001c\""), "\"ab\\u0001c\"");
        assertEquals(doExtract(extractor, "\"ab\\u0002c\""), "\"ab\\u0002c\"");

        // Complex types should return json values
        assertEquals(doExtract(extractor, "[1, 2, 3]"), "[1,2,3]");
        assertEquals(doExtract(extractor, "{\"a\": 1}"), "{\"a\":1}");
    }

    @Test
    public void testArrayElementJsonExtractor()
            throws Exception
    {
        ObjectFieldJsonExtractor<Slice> firstExtractor = new ObjectFieldJsonExtractor<>("0", new ScalarValueJsonExtractor());
        ObjectFieldJsonExtractor<Slice> secondExtractor = new ObjectFieldJsonExtractor<>("1", new ScalarValueJsonExtractor());

        assertEquals(doExtract(firstExtractor, "[]"), null);
        assertEquals(doExtract(firstExtractor, "[1, 2, 3]"), "1");
        assertEquals(doExtract(secondExtractor, "[1, 2]"), "2");
        assertEquals(doExtract(secondExtractor, "[1, null]"), null);
        // Out of bounds
        assertEquals(doExtract(secondExtractor, "[1]"), null);
        // Check skipping complex structures
        assertEquals(doExtract(secondExtractor, "[{\"a\": 1}, 2, 3]"), "2");
    }

    @Test
    public void testObjectFieldJsonExtractor()
            throws Exception
    {
        ObjectFieldJsonExtractor<Slice> extractor = new ObjectFieldJsonExtractor<>("fuu", new ScalarValueJsonExtractor());

        assertEquals(doExtract(extractor, "{}"), null);
        assertEquals(doExtract(extractor, "{\"a\": 1}"), null);
        assertEquals(doExtract(extractor, "{\"fuu\": 1}"), "1");
        assertEquals(doExtract(extractor, "{\"a\": 0, \"fuu\": 1}"), "1");
        // Check skipping complex structures
        assertEquals(doExtract(extractor, "{\"a\": [1, 2, 3], \"fuu\": 1}"), "1");
    }

    @Test
    public void testFullScalarExtract()
            throws Exception
    {
        assertEquals(doScalarExtract("{}", "$"), null);
        assertEquals(doScalarExtract("{\"fuu\": {\"bar\": 1}}", "$.fuu"), null); // Null b/c value is complex type
        assertEquals(doScalarExtract("{\"fuu\": 1}", "$.fuu"), "1");
        assertEquals(doScalarExtract("{\"fuu\": 1}", "$[fuu]"), "1");
        assertEquals(doScalarExtract("{\"fuu\": 1}", "$[\"fuu\"]"), "1");
        assertEquals(doScalarExtract("{\"fuu\": null}", "$.fuu"), null);
        assertEquals(doScalarExtract("{\"fuu\": 1}", "$.bar"), null);
        assertEquals(doScalarExtract("{\"fuu\": [\"\\u0001\"]}", "$.fuu[0]"), "\001"); // Test escaped characters
        assertEquals(doScalarExtract("{\"fuu\": 1, \"bar\": \"abc\"}", "$.bar"), "abc");
        assertEquals(doScalarExtract("{\"fuu\": [0.1, 1, 2]}", "$.fuu[0]"), "0.1");
        assertEquals(doScalarExtract("{\"fuu\": [0, [100, 101], 2]}", "$.fuu[1]"), null); // Null b/c value is complex type
        assertEquals(doScalarExtract("{\"fuu\": [0, [100, 101], 2]}", "$.fuu[1][1]"), "101");
        assertEquals(doScalarExtract("{\"fuu\": [0, {\"bar\": {\"key\" : [\"value\"]}}, 2]}", "$.fuu[1].bar.key[0]"), "value");

        // Test non-object extraction
        assertEquals(doScalarExtract("[0, 1, 2]", "$[0]"), "0");
        assertEquals(doScalarExtract("\"abc\"", "$"), "abc");
        assertEquals(doScalarExtract("123", "$"), "123");
        assertEquals(doScalarExtract("null", "$"), null);

        // Test numeric path expression matches arrays and objects
        assertEquals(doScalarExtract("[0, 1, 2]", "$.1"), "1");
        assertEquals(doScalarExtract("[0, 1, 2]", "$[1]"), "1");
        assertEquals(doScalarExtract("[0, 1, 2]", "$[\"1\"]"), "1");
        assertEquals(doScalarExtract("{\"0\" : 0, \"1\" : 1, \"2\" : 2, }", "$.1"), "1");
        assertEquals(doScalarExtract("{\"0\" : 0, \"1\" : 1, \"2\" : 2, }", "$[1]"), "1");
        assertEquals(doScalarExtract("{\"0\" : 0, \"1\" : 1, \"2\" : 2, }", "$[\"1\"]"), "1");

        // Test fields starting with a digit
        assertEquals(doScalarExtract("{\"15day\" : 0, \"30day\" : 1, \"90day\" : 2, }", "$.30day"), "1");
        assertEquals(doScalarExtract("{\"15day\" : 0, \"30day\" : 1, \"90day\" : 2, }", "$[30day]"), "1");
        assertEquals(doScalarExtract("{\"15day\" : 0, \"30day\" : 1, \"90day\" : 2, }", "$[\"30day\"]"), "1");
    }

    @Test
    public void testFullJsonExtract()
            throws Exception
    {
        assertJsonExtract("{}", "$", "{}");
        assertJsonExtract("{\"fuu\": {\"bar\": 1}}", "$.fuu", "{\"bar\":1}");
        assertJsonExtract("{\"fuu\": 1}", "$.fuu", "1");
        assertJsonExtract("{\"fuu\": 1}", "$[fuu]", "1", false); //v2: Bracket notation requires quotes. $['fuu'] works.
        assertJsonExtract("{\"fuu\": 1}", "$[\"fuu\"]", "1");
        assertJsonExtract("{\"fuu\": null}", "$.fuu", "null");
        assertJsonExtract("{\"fuu\": 1}", "$.bar", null);
        assertJsonExtract("{\"fuu\": [\"\\u0001\"]}", "$.fuu[0]", "\"\\u0001\""); // Test escaped characters
        assertJsonExtract("{\"fuu\": 1, \"bar\": \"abc\"}", "$.bar", "\"abc\"");
        assertJsonExtract("{\"fuu\": [0.1, 1, 2]}", "$.fuu[0]", "0.1");
        assertJsonExtract("{\"fuu\": [0, [100, 101], 2]}", "$.fuu[1]", "[100,101]");
        assertJsonExtract("{\"fuu\": [0, [100, 101], 2]}", "$.fuu[1][1]", "101");

        // Test non-object extraction
        assertJsonExtract("[0, 1, 2]", "$[0]", "0");
        assertJsonExtract("\"abc\"", "$", "\"abc\"");
        assertJsonExtract("123", "$", "123");
        assertJsonExtract("null", "$", "null");

        // Test extraction using bracket json path
        assertJsonExtract("{\"fuu\": {\"bar\": 1}}", "$[\"fuu\"]", "{\"bar\":1}");
        assertJsonExtract("{\"fuu\": {\"bar\": 1}}", "$[\"fuu\"][\"bar\"]", "1");
        assertJsonExtract("{\"fuu\": 1}", "$[\"fuu\"]", "1");
        assertJsonExtract("{\"fuu\": null}", "$[\"fuu\"]", "null");
        assertJsonExtract("{\"fuu\": 1}", "$[\"bar\"]", null);
        assertJsonExtract("{\"fuu\": [\"\\u0001\"]}", "$[\"fuu\"][0]", "\"\\u0001\""); // Test escaped characters
        assertJsonExtract("{\"fuu\": 1, \"bar\": \"abc\"}", "$[\"bar\"]", "\"abc\"");
        assertJsonExtract("{\"fuu\": [0.1, 1, 2]}", "$[\"fuu\"][0]", "0.1");
        assertJsonExtract("{\"fuu\": [0, [100, 101], 2]}", "$[\"fuu\"][1]", "[100,101]");
        assertJsonExtract("{\"fuu\": [0, [100, 101], 2]}", "$[\"fuu\"][1][1]", "101");

        // Test extraction using bracket json path with special json characters in path
        assertJsonExtract("{\"@$fuu\": {\".b.ar\": 1}}", "$[\"@$fuu\"]", "{\".b.ar\":1}");
        assertJsonExtract("{\"fuu..\": 1}", "$[\"fuu..\"]", "1");
        assertJsonExtract("{\"fu*u\": null}", "$[\"fu*u\"]", "null");
        assertJsonExtract("{\",fuu\": 1}", "$[\"bar\"]", null);
        assertJsonExtract("{\",fuu\": [\"\\u0001\"]}", "$[\",fuu\"][0]", "\"\\u0001\"", false); // Test escaped characters -- v2 => need to escape ',': assertJsonExtract("{\",fuu\": [\"\\u0001\"]}", "$[\"\\,fuu\"][0]", "\"\\u0001\"")
        assertJsonExtract("{\":fu:u:\": 1, \":b:ar:\": \"abc\"}", "$[\":b:ar:\"]", "\"abc\"");
        assertJsonExtract("{\"?()fuu\": [0.1, 1, 2]}", "$[\"?()fuu\"][0]", "0.1");
        assertJsonExtract("{\"f?uu\": [0, [100, 101], 2]}", "$[\"f?uu\"][1]", "[100,101]");
        assertJsonExtract("{\"fuu()\": [0, [100, 101], 2]}", "$[\"fuu()\"][1][1]", "101");

        // Test extraction using mix of bracket and dot notation json path
        assertJsonExtract("{\"fuu\": {\"bar\": 1}}", "$[\"fuu\"].bar", "1");
        assertJsonExtract("{\"fuu\": {\"bar\": 1}}", "$.fuu[\"bar\"]", "1");
        assertJsonExtract("{\"fuu\": [\"\\u0001\"]}", "$[\"fuu\"][0]", "\"\\u0001\""); // Test escaped characters
        assertJsonExtract("{\"fuu\": [\"\\u0001\"]}", "$.fuu[0]", "\"\\u0001\""); // Test escaped characters

        // Test extraction using  mix of bracket and dot notation json path with special json characters in path
        assertJsonExtract("{\"@$fuu\": {\"bar\": 1}}", "$[\"@$fuu\"].bar", "1");
        assertJsonExtract("{\",fuu\": {\"bar\": [\"\\u0001\"]}}", "$[\",fuu\"].bar[0]", "\"\\u0001\"", false); // Test escaped characters -- v2 => need to escape ',': assertJsonExtract("{\",fuu\": {\"bar\": [\"\\u0001\"]}}", "$[\"\\,fuu\"].bar[0]", "\"\\u0001\"")

        // Test numeric path expression matches arrays and objects
        assertJsonExtract("[0, 1, 2]", "$.1", "1", false); // v2 => no such property. $.[1] works.
        assertJsonExtract("[0, 1, 2]", "$[1]", "1");
        assertJsonExtract("[0, 1, 2]", "$[\"1\"]", "1", false); //v2: invalid json path
        assertJsonExtract("{\"0\" : 0, \"1\" : 1, \"2\" : 2, }", "$.1", "1", false); //v2: invalid json, extra ',' in the end, so v2 returns null
        assertJsonExtract("{\"0\" : 0, \"1\" : 1, \"2\" : 2, }", "$[1]", "1", false); //v2: invalid json, extra ',' in the end, so v2 returns null
        assertJsonExtract("{\"0\" : 0, \"1\" : 1, \"2\" : 2, }", "$[\"1\"]", "1", false); //v2: invalid json, extra ',' in the end, so v2 returns null

        // Test fields starting with a digit
        assertJsonExtract("{\"15day\" : 0, \"30day\" : 1, \"90day\" : 2, }", "$.30day", "1", false); //v2: invalid json, extra ',' in the end, so v2 returns null
        assertJsonExtract("{\"15day\" : 0, \"30day\" : 1, \"90day\" : 2, }", "$[30day]", "1", false); //v2: invalid json, extra ',' in the end, so v2 returns null
        assertJsonExtract("{\"15day\" : 0, \"30day\" : 1, \"90day\" : 2, }", "$[\"30day\"]", "1", false); //v2: invalid json, extra ',' in the end, so v2 returns null
    }

    @Test
    public void testInvalidExtracts()
            throws IOException
    {
        assertInvalidExtract("", "", "Invalid JSON path: ''");
        assertInvalidExtract("{}", "$.bar[2][-1]", "Invalid JSON path: '$.bar[2][-1]'");
        assertInvalidExtract("{}", "$.fuu..bar", "Invalid JSON path: '$.fuu..bar'");
        assertInvalidExtract("{}", "$.", "Invalid JSON path: '$.'");
        assertInvalidExtract("", "$$", "Invalid JSON path: '$$'");
        assertInvalidExtract("", " ", "Invalid JSON path: ' '");
        assertInvalidExtract("", ".", "Invalid JSON path: '.'");
        assertInvalidExtract("{ \"store\": { \"book\": [{ \"title\": \"title\" }] } }", "$.store.book[", "Invalid JSON path: '$.store.book['");
    }

    private static String doExtract(JsonExtractor<Slice> jsonExtractor, String json)
            throws IOException
    {
        JsonFactory jsonFactory = new JsonFactory();
        JsonParser jsonParser = jsonFactory.createParser(json);
        jsonParser.nextToken(); // Advance to the first token
        Slice extract = jsonExtractor.extract(jsonParser);
        return (extract == null) ? null : extract.toStringUtf8();
    }

    private static String doScalarExtract(String inputJson, String jsonPath)
    {
        Slice value = JsonExtract.extract(utf8Slice(inputJson), generateExtractor(jsonPath, new ScalarValueJsonExtractor()));
        return (value == null) ? null : value.toStringUtf8();
    }

    private static void assertJsonExtract(String inputJson, String jsonPath, String expected)
            throws IOException
    {
        assertJsonExtract(inputJson, jsonPath, expected, true);
    }

    private static void assertJsonExtract(String inputJson, String jsonPath, String expected, boolean testV2)
            throws IOException
    {
        Slice value = JsonExtract.extract(utf8Slice(inputJson), generateExtractor(jsonPath, new JsonValueJsonExtractor()));
        assertEquals((value == null) ? null : value.toStringUtf8(), expected, "JsonExtract.extract() failed");
        if (testV2) {
            Slice valueV2 = JsonFunctions.varcharJsonExtract_v2(utf8Slice(inputJson), utf8Slice(jsonPath));
            assertEquals((valueV2 == null) ? null : valueV2.toStringUtf8(), expected, "JsonFunctions.varcharJsonExtract_v2 failed");
        }
    }

    private static List<String> tokenizePath(String path)
    {
        return ImmutableList.copyOf(new JsonPathTokenizer(path));
    }

    private static void assertInvalidExtract(String inputJson, String jsonPath, String message)
            throws IOException
    {
        try {
            assertJsonExtract(inputJson, jsonPath, null);
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), INVALID_FUNCTION_ARGUMENT.toErrorCode());
            assertEquals(e.getMessage(), message);
        }
    }
}
