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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.google.common.base.Charsets;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.io.IOException;

import static com.facebook.presto.operator.scalar.JsonExtract.ArrayElementJsonExtractor;
import static com.facebook.presto.operator.scalar.JsonExtract.JsonExtractor;
import static com.facebook.presto.operator.scalar.JsonExtract.JsonValueJsonExtractor;
import static com.facebook.presto.operator.scalar.JsonExtract.ObjectFieldJsonExtractor;
import static com.facebook.presto.operator.scalar.JsonExtract.ScalarValueJsonExtractor;
import static com.facebook.presto.operator.scalar.JsonExtract.generateExtractor;
import static org.testng.Assert.assertEquals;

public class TestJsonExtract
{
    @Test
    public void testScalarVaueJsonExtractor()
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
    public void testJsonVaueJsonExtractor()
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
        ArrayElementJsonExtractor firstExtractor = new ArrayElementJsonExtractor(0, new ScalarValueJsonExtractor());
        ArrayElementJsonExtractor secondExtractor = new ArrayElementJsonExtractor(1, new ScalarValueJsonExtractor());

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
        ObjectFieldJsonExtractor extractor = new ObjectFieldJsonExtractor("fuu", new ScalarValueJsonExtractor());

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
    }

    @Test
    public void testFullJsonExtract()
            throws Exception
    {
        assertEquals(doJsonExtract("{}", "$"), "{}");
        assertEquals(doJsonExtract("{\"fuu\": {\"bar\": 1}}", "$.fuu"), "{\"bar\":1}");
        assertEquals(doJsonExtract("{\"fuu\": 1}", "$.fuu"), "1");
        assertEquals(doJsonExtract("{\"fuu\": null}", "$.fuu"), "null");
        assertEquals(doJsonExtract("{\"fuu\": 1}", "$.bar"), null);
        assertEquals(doJsonExtract("{\"fuu\": [\"\\u0001\"]}", "$.fuu[0]"), "\"\\u0001\""); // Test escaped characters
        assertEquals(doJsonExtract("{\"fuu\": 1, \"bar\": \"abc\"}", "$.bar"), "\"abc\"");
        assertEquals(doJsonExtract("{\"fuu\": [0.1, 1, 2]}", "$.fuu[0]"), "0.1");
        assertEquals(doJsonExtract("{\"fuu\": [0, [100, 101], 2]}", "$.fuu[1]"), "[100,101]");
        assertEquals(doJsonExtract("{\"fuu\": [0, [100, 101], 2]}", "$.fuu[1][1]"), "101");

        // Test non-object extraction
        assertEquals(doJsonExtract("[0, 1, 2]", "$[0]"), "0");
        assertEquals(doJsonExtract("\"abc\"", "$"), "\"abc\"");
        assertEquals(doJsonExtract("123", "$"), "123");
        assertEquals(doJsonExtract("null", "$"), "null");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testInvalidJsonPath1()
            throws Exception
    {
        doScalarExtract("{}", "$.");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testInvalidJsonPath2()
            throws Exception
    {
        doScalarExtract("{}", "$.fuu..bar");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testInvalidJsonPath3()
            throws Exception
    {
        doScalarExtract("{}", "$.bar[2][-1]");
    }

    private static String doExtract(JsonExtractor jsonExtractor, String json)
            throws IOException
    {
        JsonFactory jsonFactory = new JsonFactory();
        JsonParser jsonParser = jsonFactory.createJsonParser(json);
        jsonParser.nextToken(); // Advance to the first token
        Slice extract = jsonExtractor.extract(jsonParser);
        return (extract == null) ? null : extract.toString(Charsets.UTF_8);
    }

    private static String doScalarExtract(String inputJson, String jsonPath)
            throws IOException
    {
        Slice value = JsonExtract.extractInternal(Slices.wrappedBuffer(inputJson.getBytes(Charsets.UTF_8)), generateExtractor(jsonPath, true));
        return (value == null) ? null : value.toString(Charsets.UTF_8);
    }

    private static String doJsonExtract(String inputJson, String jsonPath)
            throws IOException
    {
        Slice value = JsonExtract.extractInternal(Slices.wrappedBuffer(inputJson.getBytes(Charsets.UTF_8)), generateExtractor(jsonPath, false));
        return (value == null) ? null : value.toString(Charsets.UTF_8);
    }
}
