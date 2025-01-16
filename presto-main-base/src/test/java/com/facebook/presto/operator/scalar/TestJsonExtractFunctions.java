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

import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.analyzer.FunctionsConfig;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.JsonType.JSON;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static java.lang.String.format;

public class TestJsonExtractFunctions
        extends AbstractTestFunctions
{
    private final String json = "{\n" +
            "    \"store\": {\n" +
            "        \"book\": [\n" +
            "            {\n" +
            "                \"category\": \"reference\",\n" +
            "                \"author\": \"Nigel Rees\",\n" +
            "                \"title\": \"Sayings of the Century\",\n" +
            "                \"price\": 8.95\n" +
            "            },\n" +
            "            {\n" +
            "                \"category\": \"fiction\",\n" +
            "                \"author\": \"Evelyn Waugh\",\n" +
            "                \"title\": \"Sword of Honour\",\n" +
            "                \"price\": 12.99\n" +
            "            },\n" +
            "            {\n" +
            "                \"category\": \"fiction\",\n" +
            "                \"author\": \"Herman Melville\",\n" +
            "                \"title\": \"Moby Dick\",\n" +
            "                \"isbn\": \"0-553-21311-3\",\n" +
            "                \"price\": 8.99\n" +
            "            },\n" +
            "            {\n" +
            "                \"category\": \"fiction\",\n" +
            "                \"author\": \"J. R. R. Tolkien\",\n" +
            "                \"title\": \"The Lord of the Rings\",\n" +
            "                \"isbn\": \"0-395-19395-8\",\n" +
            "                \"price\": 22.99\n" +
            "            }\n" +
            "        ],\n" +
            "        \"bicycle\": {\n" +
            "            \"color\": \"red\",\n" +
            "            \"price\": 19.95\n" +
            "        }\n" +
            "    },\n" +
            "    \"expensive\": 10\n" +
            "}";

    private static FunctionAssertions canonicalizedJsonExtractDisabled;
    private static FunctionAssertions canonicalizedJsonExtractEnabled;

    @BeforeClass
    public void setUp()
    {
        registerScalar(getClass());
        FunctionsConfig featuresConfigWithCanonicalizedJsonExtractDisabled = new FunctionsConfig()
                .setCanonicalizedJsonExtract(false);
        canonicalizedJsonExtractDisabled = new FunctionAssertions(session, new FeaturesConfig(), featuresConfigWithCanonicalizedJsonExtractDisabled, true);
        FunctionsConfig featuresConfigWithCanonicalizedJsonExtractEnabled = new FunctionsConfig()
                .setCanonicalizedJsonExtract(true);
        canonicalizedJsonExtractEnabled = new FunctionAssertions(session, new FeaturesConfig(), featuresConfigWithCanonicalizedJsonExtractEnabled, true);
    }

    @Test
    public void testJsonExtract()
    {
        // simple expressions (should run on Presto engine)
        assertFunction(format("JSON_EXTRACT('%s', '%s')", "{\"x\": {\"a\" : 1, \"b\" : 2} }", "$"), JSON, "{\"x\":{\"a\":1,\"b\":2}}");
        assertFunction(format("JSON_EXTRACT('%s', '%s')", "{\"x\": {\"a\" : 1, \"b\" : 2} }", "$.x"), JSON, "{\"a\":1,\"b\":2}");
        assertFunction(format("JSON_EXTRACT('%s', '%s')", "{\"x\": {\"a\" : 1, \"b\" : 2} }", "$.x.a"), JSON, "1");
        assertFunction(format("JSON_EXTRACT('%s', '%s')", "{\"x\": {\"a\" : 1, \"b\" : 2} }", "$.x.c"), JSON, null);
        assertFunction(format("JSON_EXTRACT('%s', '%s')", "{\"x\": {\"a\" : 1, \"b\" : [2, 3]} }", "$.x.b[1]"), JSON, "3");
        assertFunction(format("JSON_EXTRACT('%s', '%s')", "[1,2,3]", "$[1]"), JSON, "2");
        assertFunction(format("JSON_EXTRACT('%s', '%s')", "[1,null,3]", "$[1]"), JSON, "null");
        assertFunction(format("JSON_EXTRACT('%s', '%s')", "INVALID_JSON", "$"), JSON, null);
        assertInvalidFunction(format("JSON_EXTRACT('%s', '%s')", "{\"\":\"\"}", ""), "Invalid JSON path: ''");

        canonicalizedJsonExtractEnabled.assertFunction(format("JSON_EXTRACT('%s', '%s')", "{\"x\": {\"a\" : 1, \"b\" : 2} }", "$"), JSON, "{\"x\":{\"a\":1,\"b\":2}}");
        canonicalizedJsonExtractEnabled.assertFunction(format("JSON_EXTRACT('%s', '%s')", "{\"x\": {\"a\" : 1, \"b\" : 2} }", "$.x"), JSON, "{\"a\":1,\"b\":2}");
        canonicalizedJsonExtractEnabled.assertFunction(format("JSON_EXTRACT('%s', '%s')", "{\"x\": {\"a\" : 1, \"b\" : 2} }", "$.x.a"), JSON, "1");
        canonicalizedJsonExtractEnabled.assertFunction(format("JSON_EXTRACT('%s', '%s')", "{\"x\": {\"a\" : 1, \"b\" : 2} }", "$.x.c"), JSON, null);
        canonicalizedJsonExtractEnabled.assertFunction(format("JSON_EXTRACT('%s', '%s')", "{\"x\": {\"a\" : 1, \"b\" : [2, 3]} }", "$.x.b[1]"), JSON, "3");
        canonicalizedJsonExtractEnabled.assertFunction(format("JSON_EXTRACT('%s', '%s')", "[1,2,3]", "$[1]"), JSON, "2");
        canonicalizedJsonExtractEnabled.assertFunction(format("JSON_EXTRACT('%s', '%s')", "[1,null,3]", "$[1]"), JSON, "null");
        canonicalizedJsonExtractEnabled.assertFunction(format("JSON_EXTRACT('%s', '%s')", "INVALID_JSON", "$"), JSON, null);
        canonicalizedJsonExtractEnabled.assertInvalidFunction(format("JSON_EXTRACT('%s', '%s')", "{\"\":\"\"}", ""), "Invalid JSON path: ''");

        // complex expressions (should run on Jayway)
        assertFunction(format("JSON_EXTRACT('%s', '%s')", json, "$.store.book[*].isbn"), JSON, "[\"0-553-21311-3\",\"0-395-19395-8\"]");
        assertFunction(format("JSON_EXTRACT('%s', '%s')", json, "$..price"), JSON, "[8.95,12.99,8.99,22.99,19.95]");
        assertFunction(format("JSON_EXTRACT('%s', '%s')", json, "$.store.book[?(@.price < 10)].title"), JSON, "[\"Sayings of the Century\",\"Moby Dick\"]");
        assertFunction(format("JSON_EXTRACT('%s', '%s')", json, "max($..price)"), JSON, "22.99");
        assertFunction(format("JSON_EXTRACT('%s', '%s')", json, "concat($..category)"), JSON, "\"referencefictionfictionfiction\"");
        assertFunction(format("JSON_EXTRACT('%s', '%s')", json, "$.store.keys()"), JSON, "[\"book\",\"bicycle\"]");
        assertFunction(format("JSON_EXTRACT('%s', '%s')", json, "$.store.book[1].author"), JSON, "\"Evelyn Waugh\"");
        assertInvalidFunction(format("JSON_EXTRACT('%s', '%s')", json, "$...invalid"), "Invalid JSON path: '$...invalid'");
    }

    @Test
    public void testExtractJsonWithCanonicalOutput()
    {
        // Test with simple JSON object
        String json = "{\"key_2\": 2, \"key_3\": 3, \"key_1\": 1}";
        String path = "$";
        String expected = "{\"key_1\":1,\"key_2\":2,\"key_3\":3}";
        canonicalizedJsonExtractEnabled.assertFunction(format("JSON_EXTRACT('%s', '%s')", json, path), JSON, expected);

        // Test with nested JSON object
        json = "{\"key_1\": {\"nested_key_2\": \"value_2\", \"nested_key_1\": \"value_1\"}, \"key_2\": 2}";
        path = "$.key_1";
        expected = "{\"nested_key_1\":\"value_1\",\"nested_key_2\":\"value_2\"}";
        canonicalizedJsonExtractEnabled.assertFunction(format("JSON_EXTRACT('%s', '%s')", json, path), JSON, expected);

        // Test with Array of JSON objects
        json = "[{\"key_b\":\"v_b\",\"key_a\":\"v_a\"}, {\"key_2\": \"value_2\"}]";
        path = "$[0]";
        expected = "{\"key_a\":\"v_a\",\"key_b\":\"v_b\"}";
        canonicalizedJsonExtractEnabled.assertFunction(format("JSON_EXTRACT('%s', '%s')", json, path), JSON, expected);
    }

    @Test
    public void testInvalidFunctionIfJsonInvalid()
    {
        // Unbalanced quotes
        String json = "{ \"key_2\": 2, \"key_1\": \"z\"a1\" }";
        String path = "$.key_1";
        assertFunction(format("JSON_EXTRACT('%s', '%s')", json, path), JSON, "\"z\"");
        canonicalizedJsonExtractDisabled.assertFunction(format("JSON_EXTRACT('%s', '%s')", json, path), JSON, "\"z\"");
        canonicalizedJsonExtractEnabled.assertInvalidFunction(format("JSON_EXTRACT('%s', '%s')", json, path), INVALID_FUNCTION_ARGUMENT);

        // Extra comma
        json = "{ \"key_2\": 2, \"key_1\": \"value_1\", }";
        path = "$.key_1";
        assertFunction(format("JSON_EXTRACT('%s', '%s')", json, path), JSON, "\"value_1\"");
        canonicalizedJsonExtractDisabled.assertFunction(format("JSON_EXTRACT('%s', '%s')", json, path), JSON, "\"value_1\"");
        canonicalizedJsonExtractEnabled.assertInvalidFunction(format("JSON_EXTRACT('%s', '%s')", json, path), INVALID_FUNCTION_ARGUMENT);
    }

    @Test
    public void testJsonSize()
    {
        // simple expressions (should run on Presto engine)
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

        canonicalizedJsonExtractEnabled.assertFunction(format("JSON_SIZE('%s', '%s')", "{\"x\": {\"a\" : 1, \"b\" : 2} }", "$"), BIGINT, 1L);
        canonicalizedJsonExtractEnabled.assertFunction(format("JSON_SIZE('%s', '%s')", "{\"x\": {\"a\" : 1, \"b\" : 2} }", "$.x"), BIGINT, 2L);
        canonicalizedJsonExtractEnabled.assertFunction(format("JSON_SIZE('%s', '%s')", "{\"x\": {\"a\" : 1, \"b\" : [1,2,3], \"c\" : {\"w\":9}} }", "$.x"), BIGINT, 3L);
        canonicalizedJsonExtractEnabled.assertFunction(format("JSON_SIZE('%s', '%s')", "{\"x\": {\"a\" : 1, \"b\" : 2} }", "$.x.a"), BIGINT, 0L);
        canonicalizedJsonExtractEnabled.assertFunction(format("JSON_SIZE('%s', '%s')", "[1,2,3]", "$"), BIGINT, 3L);
        canonicalizedJsonExtractEnabled.assertFunction(format("JSON_SIZE('%s', CHAR '%s')", "[1,2,3]", "$"), BIGINT, 3L);
        canonicalizedJsonExtractEnabled.assertFunction(format("JSON_SIZE(null, '%s')", "$"), BIGINT, null);
        canonicalizedJsonExtractEnabled.assertFunction(format("JSON_SIZE('%s', '%s')", "INVALID_JSON", "$"), BIGINT, null);
        canonicalizedJsonExtractEnabled.assertFunction(format("JSON_SIZE('%s', null)", "[1,2,3]"), BIGINT, null);
        canonicalizedJsonExtractEnabled.assertFunction(format("JSON_SIZE(JSON '%s', '%s')", "{\"x\": {\"a\" : 1, \"b\" : 2} }", "$"), BIGINT, 1L);
        canonicalizedJsonExtractEnabled.assertFunction(format("JSON_SIZE(JSON '%s', '%s')", "{\"x\": {\"a\" : 1, \"b\" : 2} }", "$.x"), BIGINT, 2L);
        canonicalizedJsonExtractEnabled.assertFunction(format("JSON_SIZE(JSON '%s', '%s')", "{\"x\": {\"a\" : 1, \"b\" : [1,2,3], \"c\" : {\"w\":9}} }", "$.x"), BIGINT, 3L);
        canonicalizedJsonExtractEnabled.assertFunction(format("JSON_SIZE(JSON '%s', '%s')", "{\"x\": {\"a\" : 1, \"b\" : 2} }", "$.x.a"), BIGINT, 0L);
        canonicalizedJsonExtractEnabled.assertFunction(format("JSON_SIZE(JSON '%s', '%s')", "[1,2,3]", "$"), BIGINT, 3L);
        canonicalizedJsonExtractEnabled.assertFunction(format("JSON_SIZE(null, '%s')", "$"), BIGINT, null);
        canonicalizedJsonExtractEnabled.assertFunction(format("JSON_SIZE(JSON '%s', null)", "[1,2,3]"), BIGINT, null);

        assertInvalidFunction(format("JSON_SIZE('%s', '%s')", "{\"\":\"\"}", ""), "Invalid JSON path: ''");
        assertInvalidFunction(format("JSON_SIZE('%s', CHAR '%s')", "{\"\":\"\"}", " "), "Invalid JSON path: ' '");
        assertInvalidFunction(format("JSON_SIZE('%s', '%s')", "{\"\":\"\"}", "."), "Invalid JSON path: '.'");
        assertInvalidFunction(format("JSON_SIZE('%s', '%s')", "{\"\":\"\"}", "..."), "Invalid JSON path: '...'");

        canonicalizedJsonExtractEnabled.assertInvalidFunction(format("JSON_SIZE('%s', '%s')", "{\"\":\"\"}", ""), "Invalid JSON path: ''");
        canonicalizedJsonExtractEnabled.assertInvalidFunction(format("JSON_SIZE('%s', CHAR '%s')", "{\"\":\"\"}", " "), "Invalid JSON path: ' '");
        canonicalizedJsonExtractEnabled.assertInvalidFunction(format("JSON_SIZE('%s', '%s')", "{\"\":\"\"}", "."), "Invalid JSON path: '.'");
        canonicalizedJsonExtractEnabled.assertInvalidFunction(format("JSON_SIZE('%s', '%s')", "{\"\":\"\"}", "..."), "Invalid JSON path: '...'");

        // complex expressions (should run on Jayway)
        assertFunction(format("JSON_SIZE('%s', '%s')", json, "$.store.book[*].isbn"), BIGINT, 2L);
        assertFunction(format("JSON_SIZE('%s', '%s')", json, "$..price"), BIGINT, 5L);
        assertFunction(format("JSON_SIZE('%s', '%s')", json, "$.store.book[?(@.price < 10)].title"), BIGINT, 2L);
        assertFunction(format("JSON_SIZE('%s', '%s')", json, "max($..price)"), BIGINT, 0L);
        assertFunction(format("JSON_SIZE('%s', '%s')", json, "concat($..category)"), BIGINT, 0L);
        assertFunction(format("JSON_SIZE('%s', '%s')", json, "$.store.keys()"), BIGINT, 2L);
        assertFunction(format("JSON_SIZE('%s', '%s')", json, "$.store.book[1].author"), BIGINT, 0L);

        canonicalizedJsonExtractEnabled.assertFunction(format("JSON_SIZE('%s', '%s')", json, "$.store.book[*].isbn"), BIGINT, 2L);
        canonicalizedJsonExtractEnabled.assertFunction(format("JSON_SIZE('%s', '%s')", json, "$..price"), BIGINT, 5L);
        canonicalizedJsonExtractEnabled.assertFunction(format("JSON_SIZE('%s', '%s')", json, "$.store.book[?(@.price < 10)].title"), BIGINT, 2L);
        canonicalizedJsonExtractEnabled.assertFunction(format("JSON_SIZE('%s', '%s')", json, "max($..price)"), BIGINT, 0L);
        canonicalizedJsonExtractEnabled.assertFunction(format("JSON_SIZE('%s', '%s')", json, "concat($..category)"), BIGINT, 0L);
        canonicalizedJsonExtractEnabled.assertFunction(format("JSON_SIZE('%s', '%s')", json, "$.store.keys()"), BIGINT, 2L);
        canonicalizedJsonExtractEnabled.assertFunction(format("JSON_SIZE('%s', '%s')", json, "$.store.book[1].author"), BIGINT, 0L);

        assertInvalidFunction(format("JSON_SIZE('%s', '%s')", json, "$...invalid"), "Invalid JSON path: '$...invalid'");
        canonicalizedJsonExtractEnabled.assertInvalidFunction(format("JSON_SIZE('%s', '%s')", json, "$...invalid"), "Invalid JSON path: '$...invalid'");
    }

    @Test
    public void testJsonExtractScalar()
    {
        // simple expressions (should run on Presto)
        assertFunction(format("JSON_EXTRACT_SCALAR(JSON'%s', '%s')", "{\"x\": {\"a\" : 1, \"b\" : 2} }", "$"), VARCHAR, null);
        assertFunction(format("JSON_EXTRACT_SCALAR(JSON'%s', '%s')", "{\"x\": {\"a\" : 1, \"b\" : 2} }", "$.x"), VARCHAR, null);
        assertFunction(format("JSON_EXTRACT_SCALAR(JSON'%s', '%s')", "{\"x\": {\"a\" : 1, \"b\" : 2} }", "$.x.a"), VARCHAR, "1");
        assertFunction(format("JSON_EXTRACT_SCALAR(JSON'%s', '%s')", "{\"x\": {\"a\" : 1, \"b\" : [2, 3]} }", "$.x.b[1]"), VARCHAR, "3");
        assertFunction(format("JSON_EXTRACT_SCALAR(JSON'%s', '%s')", "[1,2,3]", "$[1]"), VARCHAR, "2");
        assertInvalidFunction(format("JSON_EXTRACT_SCALAR(JSON'%s', '%s')", "{\"\":\"\"}", ""), "Invalid JSON path: ''");

        canonicalizedJsonExtractEnabled.assertFunction(format("JSON_EXTRACT_SCALAR(JSON'%s', '%s')", "{\"x\": {\"a\" : 1, \"b\" : 2} }", "$"), VARCHAR, null);
        canonicalizedJsonExtractEnabled.assertFunction(format("JSON_EXTRACT_SCALAR(JSON'%s', '%s')", "{\"x\": {\"a\" : 1, \"b\" : 2} }", "$.x"), VARCHAR, null);
        canonicalizedJsonExtractEnabled.assertFunction(format("JSON_EXTRACT_SCALAR(JSON'%s', '%s')", "{\"x\": {\"a\" : 1, \"b\" : 2} }", "$.x.a"), VARCHAR, "1");
        canonicalizedJsonExtractEnabled.assertFunction(format("JSON_EXTRACT_SCALAR(JSON'%s', '%s')", "{\"x\": {\"a\" : 1, \"b\" : [2, 3]} }", "$.x.b[1]"), VARCHAR, "3");
        canonicalizedJsonExtractEnabled.assertFunction(format("JSON_EXTRACT_SCALAR(JSON'%s', '%s')", "[1,2,3]", "$[1]"), VARCHAR, "2");
        canonicalizedJsonExtractEnabled.assertInvalidFunction(format("JSON_EXTRACT_SCALAR(JSON'%s', '%s')", "{\"\":\"\"}", ""), "Invalid JSON path: ''");

        // complex expressions (should run on Jayway)
        assertFunction(format("JSON_EXTRACT_SCALAR(JSON'%s', '%s')", json, "$.store.book[*].isbn"), VARCHAR, null);
        assertFunction(format("JSON_EXTRACT_SCALAR(JSON'%s', '%s')", json, "$..price"), VARCHAR, null);
        assertFunction(format("JSON_EXTRACT_SCALAR(JSON'%s', '%s')", json, "$.store.book[?(@.price < 10)].title"), VARCHAR, null);
        assertFunction(format("JSON_EXTRACT_SCALAR(JSON'%s', '%s')", json, "max($..price)"), VARCHAR, "22.99");
        assertFunction(format("JSON_EXTRACT_SCALAR(JSON'%s', '%s')", json, "concat($..category)"), VARCHAR, "referencefictionfictionfiction");
        assertFunction(format("JSON_EXTRACT_SCALAR(JSON'%s', '%s')", json, "$.store.keys()"), VARCHAR, null);
        assertFunction(format("JSON_EXTRACT_SCALAR(JSON'%s', '%s')", json, "$.store.book[1].author"), VARCHAR, "Evelyn Waugh");

        canonicalizedJsonExtractEnabled.assertFunction(format("JSON_EXTRACT_SCALAR(JSON'%s', '%s')", json, "$.store.book[*].isbn"), VARCHAR, null);
        canonicalizedJsonExtractEnabled.assertFunction(format("JSON_EXTRACT_SCALAR(JSON'%s', '%s')", json, "$..price"), VARCHAR, null);
        canonicalizedJsonExtractEnabled.assertFunction(format("JSON_EXTRACT_SCALAR(JSON'%s', '%s')", json, "$.store.book[?(@.price < 10)].title"), VARCHAR, null);
        canonicalizedJsonExtractEnabled.assertFunction(format("JSON_EXTRACT_SCALAR(JSON'%s', '%s')", json, "max($..price)"), VARCHAR, "22.99");
        canonicalizedJsonExtractEnabled.assertFunction(format("JSON_EXTRACT_SCALAR(JSON'%s', '%s')", json, "concat($..category)"), VARCHAR, "referencefictionfictionfiction");
        canonicalizedJsonExtractEnabled.assertFunction(format("JSON_EXTRACT_SCALAR(JSON'%s', '%s')", json, "$.store.keys()"), VARCHAR, null);
        canonicalizedJsonExtractEnabled.assertFunction(format("JSON_EXTRACT_SCALAR(JSON'%s', '%s')", json, "$.store.book[1].author"), VARCHAR, "Evelyn Waugh");

        assertInvalidFunction(format("JSON_EXTRACT_SCALAR('%s', '%s')", json, "$...invalid"), "Invalid JSON path: '$...invalid'");
        canonicalizedJsonExtractEnabled.assertInvalidFunction(format("JSON_EXTRACT_SCALAR('%s', '%s')", json, "$...invalid"), "Invalid JSON path: '$...invalid'");
    }
}
