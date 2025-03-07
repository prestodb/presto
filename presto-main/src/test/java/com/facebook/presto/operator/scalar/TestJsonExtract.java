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

import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;

import static com.facebook.presto.common.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.operator.scalar.JsonExtract.JsonExtractor;
import static com.facebook.presto.operator.scalar.JsonExtract.JsonValueJsonExtractor;
import static com.facebook.presto.operator.scalar.JsonExtract.ObjectFieldJsonExtractor;
import static com.facebook.presto.operator.scalar.JsonExtract.ScalarValueJsonExtractor;
import static com.facebook.presto.operator.scalar.JsonExtract.generateExtractor;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static java.util.Locale.ENGLISH;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestJsonExtract
        extends AbstractTestFunctions
{

    public static final SqlFunctionProperties PROPERTIES_LEGACY_EXTRACT_ENABLED = SqlFunctionProperties.builder().setTimeZoneKey(UTC_KEY).setLegacyTimestamp(true).setSessionStartTime(0).setSessionLocale(ENGLISH).setSessionUser("user").setLegacyJsonExtract(true).build();
    public static final SqlFunctionProperties PROPERTIES_LEGACY_EXTRACT_DISABLED = SqlFunctionProperties.builder().setTimeZoneKey(UTC_KEY).setLegacyTimestamp(true).setSessionStartTime(0).setSessionLocale(ENGLISH).setSessionUser("user").setLegacyJsonExtract(false).build();
    @BeforeClass
    public void setUp()
    {
        // for "utf8" function
        registerScalar(TestStringFunctions.class);
    }

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
        assertQuotedPathToken("ab\u0001c");
        assertQuotedPathToken("ab\0c");
        assertQuotedPathToken("ab\t\n\rc");
        assertQuotedPathToken(".");
        assertQuotedPathToken("$");
        assertQuotedPathToken("]");
        assertQuotedPathToken("[");
        assertQuotedPathToken("'");
        assertQuotedPathToken("!@#$%^&*(){}[]<>?/|.,`~\r\n\t \0");
        assertQuotedPathToken("a\\\\b\\\"", "a\\b\"");

        // backslash not followed by valid escape
        assertInvalidPath("$[\"a\\ \"]");

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
        assertQuotedPathToken(fieldName, fieldName);
    }

    private static void assertQuotedPathToken(String fieldName, String expectedTokenizedField)
    {
        assertPathTokenQuoting(fieldName, expectedTokenizedField);
        // without quoting we should get an error
        assertInvalidPath("$." + fieldName);
    }

    private static void assertPathTokenQuoting(String fieldName)
    {
        assertPathTokenQuoting(fieldName, fieldName);
    }

    private static void assertPathTokenQuoting(String fieldName, String expectedTokenizedField)
    {
        assertEquals(tokenizePath("$[\"" + fieldName + "\"]"), ImmutableList.of(expectedTokenizedField));
        assertEquals(tokenizePath("$.foo[\"" + fieldName + "\"].bar"), ImmutableList.of("foo", expectedTokenizedField, "bar"));
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
        assertEquals(doExtractLegacy(extractor, "123"), "123");
        assertEquals(doExtractLegacy(extractor, "-1"), "-1");
        assertEquals(doExtractLegacy(extractor, "0.01"), "0.01");
        assertEquals(doExtractLegacy(extractor, "\"abc\""), "abc");
        assertEquals(doExtractLegacy(extractor, "\"\""), "");
        assertNull(doExtractLegacy(extractor, "null"));
        assertEquals(doExtract(extractor, "123"), "123");
        assertEquals(doExtract(extractor, "-1"), "-1");
        assertEquals(doExtract(extractor, "0.01"), "0.01");
        assertEquals(doExtract(extractor, "\"abc\""), "abc");
        assertEquals(doExtract(extractor, "\"\""), "");
        assertNull(doExtract(extractor, "null"));

        // Test character escaped values
        assertEquals(doExtractLegacy(extractor, "\"ab\\u0001c\""), "ab\001c");
        assertEquals(doExtractLegacy(extractor, "\"ab\\u0002c\""), "ab\002c");
        assertEquals(doExtract(extractor, "\"ab\\u0001c\""), "ab\001c");
        assertEquals(doExtract(extractor, "\"ab\\u0002c\""), "ab\002c");

        // Complex types should return null
        assertNull(doExtractLegacy(extractor, "[1, 2, 3]"));
        assertNull(doExtractLegacy(extractor, "{\"a\": 1}"));
        assertNull(doExtract(extractor, "[1, 2, 3]"));
        assertNull(doExtract(extractor, "{\"a\": 1}"));
    }

    @Test
    public void testJsonValueJsonExtractor()
            throws Exception
    {
        JsonValueJsonExtractor extractor = new JsonValueJsonExtractor();

        // Check scalar values
        assertEquals(doExtractLegacy(extractor, "123"), "123");
        assertEquals(doExtractLegacy(extractor, "-1"), "-1");
        assertEquals(doExtractLegacy(extractor, "0.01"), "0.01");
        assertEquals(doExtractLegacy(extractor, "\"abc\""), "\"abc\"");
        assertEquals(doExtractLegacy(extractor, "\"\""), "\"\"");
        assertEquals(doExtractLegacy(extractor, "null"), "null");
        assertEquals(doExtract(extractor, "123"), "123");
        assertEquals(doExtract(extractor, "-1"), "-1");
        assertEquals(doExtract(extractor, "0.01"), "0.01");
        assertEquals(doExtract(extractor, "\"abc\""), "\"abc\"");
        assertEquals(doExtract(extractor, "\"\""), "\"\"");
        assertEquals(doExtract(extractor, "null"), "null");

        // Test character escaped values
        assertEquals(doExtractLegacy(extractor, "\"ab\\u0001c\""), "\"ab\\u0001c\"");
        assertEquals(doExtractLegacy(extractor, "\"ab\\u0002c\""), "\"ab\\u0002c\"");
        assertEquals(doExtract(extractor, "\"ab\\u0001c\""), "\"ab\\u0001c\"");
        assertEquals(doExtract(extractor, "\"ab\\u0002c\""), "\"ab\\u0002c\"");

        // Complex types should return json values
        assertEquals(doExtractLegacy(extractor, "[1, 2, 3]"), "[1,2,3]");
        assertEquals(doExtractLegacy(extractor, "{\"a\": 1}"), "{\"a\":1}");
        assertEquals(doExtract(extractor, "[1, 2, 3]"), "[1,2,3]");
        assertEquals(doExtract(extractor, "{\"a\": 1}"), "{\"a\":1}");
    }

    @Test
    public void testArrayElementJsonExtractor()
            throws Exception
    {
        ObjectFieldJsonExtractor<Slice> firstExtractor = new ObjectFieldJsonExtractor<>("0", new ScalarValueJsonExtractor());
        ObjectFieldJsonExtractor<Slice> secondExtractor = new ObjectFieldJsonExtractor<>("1", new ScalarValueJsonExtractor());

        assertNull(doExtractLegacy(firstExtractor, "[]"));
        assertEquals(doExtractLegacy(firstExtractor, "[1, 2, 3]"), "1");
        assertEquals(doExtractLegacy(secondExtractor, "[1, 2]"), "2");
        assertNull(doExtractLegacy(secondExtractor, "[1, null]"));
        assertNull(doExtract(firstExtractor, "[]"));
        assertEquals(doExtract(firstExtractor, "[1, 2, 3]"), "1");
        assertEquals(doExtract(secondExtractor, "[1, 2]"), "2");
        assertNull(doExtract(secondExtractor, "[1, null]"));
        // Out of bounds
        assertNull(doExtractLegacy(secondExtractor, "[1]"));
        assertNull(doExtract(secondExtractor, "[1]"));
        // Check skipping complex structures
        assertEquals(doExtractLegacy(secondExtractor, "[{\"a\": 1}, 2, 3]"), "2");
        assertEquals(doExtract(secondExtractor, "[{\"a\": 1}, 2, 3]"), "2");
    }

    @Test
    public void testObjectFieldJsonExtractor()
            throws Exception
    {
        ObjectFieldJsonExtractor<Slice> extractor = new ObjectFieldJsonExtractor<>("fuu", new ScalarValueJsonExtractor());

        assertNull(doExtractLegacy(extractor, "{}"));
        assertNull(doExtractLegacy(extractor, "{\"a\": 1}"));
        assertEquals(doExtractLegacy(extractor, "{\"fuu\": 1}"), "1");
        assertEquals(doExtractLegacy(extractor, "{\"a\": 0, \"fuu\": 1}"), "1");
        assertNull(doExtract(extractor, "{}"));
        assertNull(doExtract(extractor, "{\"a\": 1}"));
        assertEquals(doExtract(extractor, "{\"fuu\": 1}"), "1");
        assertEquals(doExtract(extractor, "{\"a\": 0, \"fuu\": 1}"), "1");
        // Check skipping complex structures
        assertEquals(doExtract(extractor, "{\"a\": [1, 2, 3], \"fuu\": 1}"), "1");
    }

    @Test
    public void testFullScalarExtract()
    {
        assertNull(doScalarExtractLegacy("{}", "$"));
        assertNull(doScalarExtractLegacy("{\"fuu\": {\"bar\": 1}}", "$.fuu")); // Null b/c value is complex type
        assertEquals(doScalarExtractLegacy("{\"fuu\": 1}", "$.fuu"), "1");
        assertEquals(doScalarExtractLegacy("{\"fuu\": 1}", "$[fuu]"), "1");
        assertEquals(doScalarExtractLegacy("{\"fuu\": 1}", "$[\"fuu\"]"), "1");
        assertNull(doScalarExtractLegacy("{\"fuu\": null}", "$.fuu"));
        assertNull(doScalarExtractLegacy("{\"fuu\": 1}", "$.bar"));
        assertEquals(doScalarExtractLegacy("{\"fuu\": [\"\\u0001\"]}", "$.fuu[0]"), "\001"); // Test escaped characters
        assertEquals(doScalarExtractLegacy("{\"fuu\": 1, \"bar\": \"abc\"}", "$.bar"), "abc");
        assertEquals(doScalarExtractLegacy("{\"fuu\": [0.1, 1, 2]}", "$.fuu[0]"), "0.1");
        assertNull(doScalarExtractLegacy("{\"fuu\": [0, [100, 101], 2]}", "$.fuu[1]")); // Null b/c value is complex type
        assertEquals(doScalarExtractLegacy("{\"fuu\": [0, [100, 101], 2]}", "$.fuu[1][1]"), "101");
        assertEquals(doScalarExtractLegacy("{\"fuu\": [0, {\"bar\": {\"key\" : [\"value\"]}}, 2]}", "$.fuu[1].bar.key[0]"), "value");

        // Test non-object extraction
        assertEquals(doScalarExtractLegacy("[0, 1, 2]", "$[0]"), "0");
        assertEquals(doScalarExtractLegacy("\"abc\"", "$"), "abc");
        assertEquals(doScalarExtractLegacy("123", "$"), "123");
        assertEquals(doScalarExtractLegacy("null", "$"), null);

        // Test numeric path expression matches arrays and objects
        assertEquals(doScalarExtractLegacy("[0, 1, 2]", "$.1"), "1");
        assertEquals(doScalarExtractLegacy("[0, 1, 2]", "$[1]"), "1");
        assertEquals(doScalarExtractLegacy("[0, 1, 2]", "$[\"1\"]"), "1");
        assertEquals(doScalarExtractLegacy("{\"0\" : 0, \"1\" : 1, \"2\" : 2, }", "$.1"), "1");
        assertEquals(doScalarExtractLegacy("{\"0\" : 0, \"1\" : 1, \"2\" : 2, }", "$[1]"), "1");
        assertEquals(doScalarExtractLegacy("{\"0\" : 0, \"1\" : 1, \"2\" : 2, }", "$[\"1\"]"), "1");

        // Test fields starting with a digit
        assertEquals(doScalarExtractLegacy("{\"15day\" : 0, \"30day\" : 1, \"90day\" : 2, }", "$.30day"), "1");
        assertEquals(doScalarExtractLegacy("{\"15day\" : 0, \"30day\" : 1, \"90day\" : 2, }", "$[30day]"), "1");
        assertEquals(doScalarExtractLegacy("{\"15day\" : 0, \"30day\" : 1, \"90day\" : 2, }", "$[\"30day\"]"), "1");
    }

    @Test
    public void testFullJsonExtract()
    {
        assertEquals(doJsonExtractLegacy("{}", "$"), "{}");
        assertEquals(doJsonExtractLegacy("{\"fuu\": {\"bar\": 1}}", "$.fuu"), "{\"bar\":1}");
        assertEquals(doJsonExtractLegacy("{\"fuu\": 1}", "$.fuu"), "1");
        assertEquals(doJsonExtractLegacy("{\"fuu\": 1}", "$[fuu]"), "1");
        assertEquals(doJsonExtractLegacy("{\"fuu\": 1}", "$[\"fuu\"]"), "1");
        assertEquals(doJsonExtractLegacy("{\"fuu\": null}", "$.fuu"), "null");
        assertNull(doJsonExtractLegacy("{\"fuu\": 1}", "$.bar"));
        assertEquals(doJsonExtractLegacy("{\"fuu\": [\"\\u0001\"]}", "$.fuu[0]"), "\"\\u0001\""); // Test escaped characters
        assertEquals(doJsonExtractLegacy("{\"fuu\": 1, \"bar\": \"abc\"}", "$.bar"), "\"abc\"");
        assertEquals(doJsonExtractLegacy("{\"fuu\": [0.1, 1, 2]}", "$.fuu[0]"), "0.1");
        assertEquals(doJsonExtractLegacy("{\"fuu\": [0, [100, 101], 2]}", "$.fuu[1]"), "[100,101]");
        assertEquals(doJsonExtractLegacy("{\"fuu\": [0, [100, 101], 2]}", "$.fuu[1][1]"), "101");

        // Test non-object extraction
        assertEquals(doJsonExtractLegacy("[0, 1, 2]", "$[0]"), "0");
        assertEquals(doJsonExtractLegacy("\"abc\"", "$"), "\"abc\"");
        assertEquals(doJsonExtractLegacy("123", "$"), "123");
        assertEquals(doJsonExtractLegacy("null", "$"), "null");

        // Test extraction using bracket json path
        assertEquals(doJsonExtractLegacy("{\"fuu\": {\"bar\": 1}}", "$[\"fuu\"]"), "{\"bar\":1}");
        assertEquals(doJsonExtractLegacy("{\"fuu\": {\"bar\": 1}}", "$[\"fuu\"][\"bar\"]"), "1");
        assertEquals(doJsonExtractLegacy("{\"fuu\": 1}", "$[\"fuu\"]"), "1");
        assertEquals(doJsonExtractLegacy("{\"fuu\": null}", "$[\"fuu\"]"), "null");
        assertNull(doJsonExtractLegacy("{\"fuu\": 1}", "$[\"bar\"]"));
        assertEquals(doJsonExtractLegacy("{\"fuu\": [\"\\u0001\"]}", "$[\"fuu\"][0]"), "\"\\u0001\""); // Test escaped characters
        assertEquals(doJsonExtractLegacy("{\"fuu\": 1, \"bar\": \"abc\"}", "$[\"bar\"]"), "\"abc\"");
        assertEquals(doJsonExtractLegacy("{\"fuu\": [0.1, 1, 2]}", "$[\"fuu\"][0]"), "0.1");
        assertEquals(doJsonExtractLegacy("{\"fuu\": [0, [100, 101], 2]}", "$[\"fuu\"][1]"), "[100,101]");
        assertEquals(doJsonExtractLegacy("{\"fuu\": [0, [100, 101], 2]}", "$[\"fuu\"][1][1]"), "101");

        // Test extraction using bracket json path with special json characters in path
        assertEquals(doJsonExtractLegacy("{\"@$fuu\": {\".b.ar\": 1}}", "$[\"@$fuu\"]"), "{\".b.ar\":1}");
        assertEquals(doJsonExtractLegacy("{\"fuu..\": 1}", "$[\"fuu..\"]"), "1");
        assertEquals(doJsonExtractLegacy("{\"fu*u\": null}", "$[\"fu*u\"]"), "null");
        assertNull(doJsonExtractLegacy("{\",fuu\": 1}", "$[\"bar\"]"));
        assertEquals(doJsonExtractLegacy("{\",fuu\": [\"\\u0001\"]}", "$[\",fuu\"][0]"), "\"\\u0001\""); // Test escaped characters
        assertEquals(doJsonExtractLegacy("{\":fu:u:\": 1, \":b:ar:\": \"abc\"}", "$[\":b:ar:\"]"), "\"abc\"");
        assertEquals(doJsonExtractLegacy("{\"?()fuu\": [0.1, 1, 2]}", "$[\"?()fuu\"][0]"), "0.1");
        assertEquals(doJsonExtractLegacy("{\"f?uu\": [0, [100, 101], 2]}", "$[\"f?uu\"][1]"), "[100,101]");
        assertEquals(doJsonExtractLegacy("{\"fuu()\": [0, [100, 101], 2]}", "$[\"fuu()\"][1][1]"), "101");

        // Test extraction using mix of bracket and dot notation json path
        assertEquals(doJsonExtractLegacy("{\"fuu\": {\"bar\": 1}}", "$[\"fuu\"].bar"), "1");
        assertEquals(doJsonExtractLegacy("{\"fuu\": {\"bar\": 1}}", "$.fuu[\"bar\"]"), "1");
        assertEquals(doJsonExtractLegacy("{\"fuu\": [\"\\u0001\"]}", "$[\"fuu\"][0]"), "\"\\u0001\""); // Test escaped characters
        assertEquals(doJsonExtractLegacy("{\"fuu\": [\"\\u0001\"]}", "$.fuu[0]"), "\"\\u0001\""); // Test escaped characters

        // Test extraction using  mix of bracket and dot notation json path with special json characters in path
        assertEquals(doJsonExtractLegacy("{\"@$fuu\": {\"bar\": 1}}", "$[\"@$fuu\"].bar"), "1");
        assertEquals(doJsonExtractLegacy("{\",fuu\": {\"bar\": [\"\\u0001\"]}}", "$[\",fuu\"].bar[0]"), "\"\\u0001\""); // Test escaped characters

        // Test numeric path expression matches arrays and objects
        assertEquals(doJsonExtractLegacy("[0, 1, 2]", "$.1"), "1");
        assertEquals(doJsonExtractLegacy("[0, 1, 2]", "$[1]"), "1");
        assertEquals(doJsonExtractLegacy("[0, 1, 2]", "$[\"1\"]"), "1");
        assertEquals(doJsonExtractLegacy("{\"0\" : 0, \"1\" : 1, \"2\" : 2, }", "$.1"), "1");
        assertEquals(doJsonExtractLegacy("{\"0\" : 0, \"1\" : 1, \"2\" : 2, }", "$[1]"), "1");
        assertEquals(doJsonExtractLegacy("{\"0\" : 0, \"1\" : 1, \"2\" : 2, }", "$[\"1\"]"), "1");

        // Test fields starting with a digit
        assertEquals(doJsonExtractLegacy("{\"15day\" : 0, \"30day\" : 1, \"90day\" : 2, }", "$.30day"), "1");
        assertEquals(doJsonExtractLegacy("{\"15day\" : 0, \"30day\" : 1, \"90day\" : 2, }", "$[30day]"), "1");
        assertEquals(doJsonExtractLegacy("{\"15day\" : 0, \"30day\" : 1, \"90day\" : 2, }", "$[\"30day\"]"), "1");
    }

    @Test
    public void testInvalidExtracts()
    {
        assertInvalidExtractLegacy("", "", "Invalid JSON path: ''");
        assertInvalidExtractLegacy("{}", "$.bar[2][-1]", "Invalid JSON path: '$.bar[2][-1]'");
        assertInvalidExtractLegacy("{}", "$.fuu..bar", "Invalid JSON path: '$.fuu..bar'");
        assertInvalidExtractLegacy("{}", "$.", "Invalid JSON path: '$.'");
        assertInvalidExtractLegacy("", "$$", "Invalid JSON path: '$$'");
        assertInvalidExtractLegacy("", " ", "Invalid JSON path: ' '");
        assertInvalidExtractLegacy("", ".", "Invalid JSON path: '.'");
        assertInvalidExtractLegacy("{ \"store\": { \"book\": [{ \"title\": \"title\" }] } }", "$.store.book[", "Invalid JSON path: '$.store.book['");
    }

    @Test
    public void testNoAutomaticEncodingDetection()
    {
        // Automatic encoding detection treats the following input as UTF-32
        assertFunction("JSON_EXTRACT_SCALAR(UTF8(X'00 00 00 00 7b 22 72 22'), '$.x')", VARCHAR, null);
    }

    private static String doExtractLegacy(JsonExtractor<Slice> jsonExtractor, String json)
            throws IOException
    {
        Slice extract = jsonExtractor.extract(Slices.utf8Slice(json).getInput(), PROPERTIES_LEGACY_EXTRACT_ENABLED);
        return (extract == null) ? null : extract.toStringUtf8();
    }

    private static String doExtract(JsonExtractor<Slice> jsonExtractor, String json)
            throws IOException
    {
        Slice extract = jsonExtractor.extract(Slices.utf8Slice(json).getInput(), PROPERTIES_LEGACY_EXTRACT_DISABLED);
        return (extract == null) ? null : extract.toStringUtf8();
    }

    private static String doScalarExtractLegacy(String inputJson, String jsonPath)
    {
        Slice value = JsonExtract.extract(Slices.utf8Slice(inputJson), generateExtractor(jsonPath, new ScalarValueJsonExtractor()), PROPERTIES_LEGACY_EXTRACT_ENABLED);
        return (value == null) ? null : value.toStringUtf8();
    }

    private static String doScalarExtract(String inputJson, String jsonPath)
    {
        Slice value = JsonExtract.extract(Slices.utf8Slice(inputJson), generateExtractor(jsonPath, new ScalarValueJsonExtractor()), PROPERTIES_LEGACY_EXTRACT_DISABLED);
        return (value == null) ? null : value.toStringUtf8();
    }

    private static String doJsonExtractLegacy(String inputJson, String jsonPath)
    {
        Slice value = JsonExtract.extract(Slices.utf8Slice(inputJson), generateExtractor(jsonPath, new JsonValueJsonExtractor()), PROPERTIES_LEGACY_EXTRACT_ENABLED);
        return (value == null) ? null : value.toStringUtf8();
    }

    private static String doJsonExtract(String inputJson, String jsonPath)
    {
        Slice value = JsonExtract.extract(Slices.utf8Slice(inputJson), generateExtractor(jsonPath, new JsonValueJsonExtractor()), PROPERTIES_LEGACY_EXTRACT_DISABLED);
        return (value == null) ? null : value.toStringUtf8();
    }

    private static List<String> tokenizePath(String path)
    {
        return ImmutableList.copyOf(new JsonPathTokenizer(path));
    }

    private static void assertInvalidExtractLegacy(String inputJson, String jsonPath, String message)
    {
        try {
            doJsonExtractLegacy(inputJson, jsonPath);
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), INVALID_FUNCTION_ARGUMENT.toErrorCode());
            assertEquals(e.getMessage(), message);
        }
    }
}
