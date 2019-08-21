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

import com.jayway.jsonpath.InvalidPathException;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestJsonExtractJayway
        extends AbstractTestFunctions
{
    @BeforeClass
    public void setUp()
    {
        // for "utf8" function
        registerScalar(TestStringFunctions.class);
    }

    @Test
    public void testArrayElementJsonExtractor()
            throws Exception
    {
        String firstElemJsonPath = "$[0]";
        String secondElemJsonPath = "$[1]";

        assertEquals(doJsonExtract("[]", firstElemJsonPath), null);
        assertEquals(doJsonExtract("[1, 2, 3]", firstElemJsonPath), "1");
        assertEquals(doJsonExtract("[1, 2]", secondElemJsonPath), "2");
        assertEquals(doJsonExtract("[1, null]", secondElemJsonPath), "null");
        // Out of bounds
        assertEquals(doJsonExtract("[1]", secondElemJsonPath), null);
        // Check skipping complex structures
        assertEquals(doJsonExtract("[{\"a\": 1}, 2, 3]", secondElemJsonPath), "2");

        // Use wildcard to get multiple values
        assertEquals(doJsonExtract("{name: Rose Kolodny, phoneNumbers: " +
                        "[{type: home, number: 954-555-1234}, " +
                        "{type: work, number: 754-555-5678}]}",
                "$.phoneNumbers[*].number"),
                "[\"954-555-1234\",\"754-555-5678\"]");

        // Select multiple array indices
        assertEquals(doJsonExtract("[0, 1, 2]", "$[1,2]"), "[1,2]");
    }

    @Test
    public void testObjectFieldJsonExtractor()
            throws Exception
    {
        String jsonPath = "$.fuu";

        assertEquals(doJsonExtract("{}", jsonPath), null);
        assertEquals(doJsonExtract("{\"a\": 1}", jsonPath), null);
        assertEquals(doJsonExtract("{\"fuu\": 1}", jsonPath), "1");
        assertEquals(doJsonExtract("{\"a\": 0, \"fuu\": 1}", jsonPath), "1");
        // Check skipping complex structures
        assertEquals(doJsonExtract("{\"a\": [1, 2, 3], \"fuu\": 1}", jsonPath), "1");
    }

    @Test
    public void testFullJsonExtract() throws IOException
    {
        assertEquals(doJsonExtract("{}", "$"), "{}");
        assertEquals(doJsonExtract("{\"fuu\": {\"bar\": 1}}", "$.fuu"), "{\"bar\":1}");
        assertEquals(doJsonExtract("{\"fuu\": 1}", "$.fuu"), "1");
        assertEquals(doJsonExtract("{\"fuu\": 1}", "$['fuu']"), "1");
        assertEquals(doJsonExtract("{\"fuu\": 1}", "$[\"fuu\"]"), "1");
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

        // Test extraction using bracket json path
        assertEquals(doJsonExtract("{\"fuu\": {\"bar\": 1}}", "$[\"fuu\"]"), "{\"bar\":1}");
        assertEquals(doJsonExtract("{\"fuu\": {\"bar\": 1}}", "$[\"fuu\"][\"bar\"]"), "1");
        assertEquals(doJsonExtract("{\"fuu\": 1}", "$[\"fuu\"]"), "1");
        assertEquals(doJsonExtract("{\"fuu\": null}", "$[\"fuu\"]"), "null");
        assertEquals(doJsonExtract("{\"fuu\": 1}", "$[\"bar\"]"), null);
        assertEquals(doJsonExtract("{\"fuu\": [\"\\u0001\"]}", "$[\"fuu\"][0]"), "\"\\u0001\""); // Test escaped characters
        assertEquals(doJsonExtract("{\"fuu\": 1, \"bar\": \"abc\"}", "$[\"bar\"]"), "\"abc\"");
        assertEquals(doJsonExtract("{\"fuu\": [0.1, 1, 2]}", "$[\"fuu\"][0]"), "0.1");
        assertEquals(doJsonExtract("{\"fuu\": [0, [100, 101], 2]}", "$[\"fuu\"][1]"), "[100,101]");
        assertEquals(doJsonExtract("{\"fuu\": [0, [100, 101], 2]}", "$[\"fuu\"][1][1]"), "101");

        // Test extraction using bracket json path with special json characters in path
        assertEquals(doJsonExtract("{\"@$fuu\": {\".b.ar\": 1}}", "$[\"@$fuu\"]"), "{\".b.ar\":1}");
        assertEquals(doJsonExtract("{\"fuu..\": 1}", "$[\"fuu..\"]"), "1");
        assertEquals(doJsonExtract("{\"fu*u\": null}", "$[\"fu*u\"]"), "null");
        assertEquals(doJsonExtract("{\",fuu\": 1}", "$[\"bar\"]"), null);
        // Test escaped characters
        assertEquals(doJsonExtract("{\",fuu\": [\"\\u0001\"]}", "$[\"\\,fuu\"][0]"), "\"\\u0001\"");
        assertEquals(doJsonExtract("{\":fu:u:\": 1, \":b:ar:\": \"abc\"}", "$[\":b:ar:\"]"), "\"abc\"");
        assertEquals(doJsonExtract("{\"?()fuu\": [0.1, 1, 2]}", "$[\"?()fuu\"][0]"), "0.1");
        assertEquals(doJsonExtract("{\"f?uu\": [0, [100, 101], 2]}", "$[\"f?uu\"][1]"), "[100,101]");
        assertEquals(doJsonExtract("{\"fuu()\": [0, [100, 101], 2]}", "$[\"fuu()\"][1][1]"), "101");

        // Test extraction using mix of bracket and dot notation json path
        assertEquals(doJsonExtract("{\"fuu\": {\"bar\": 1}}", "$[\"fuu\"].bar"), "1");
        assertEquals(doJsonExtract("{\"fuu\": {\"bar\": 1}}", "$.fuu[\"bar\"]"), "1");
        assertEquals(doJsonExtract("{\"fuu\": [\"\\u0001\"]}", "$[\"fuu\"][0]"), "\"\\u0001\""); // Test escaped characters
        assertEquals(doJsonExtract("{\"fuu\": [\"\\u0001\"]}", "$.fuu[0]"), "\"\\u0001\""); // Test escaped characters

        // Test extraction using  mix of bracket and dot notation json path with special json characters in path
        assertEquals(doJsonExtract("{\"@$fuu\": {\"bar\": 1}}", "$[\"@$fuu\"].bar"), "1");
        // Test escaped characters
        assertEquals(doJsonExtract("{\",fuu\": {\"bar\": [\"\\u0001\"]}}", "$[\"\\,fuu\"].bar[0]"), "\"\\u0001\"");

        // Test numeric path expression matches arrays and objects
        assertEquals(doJsonExtract("[0, 1, 2]", "$.1"), null);
        assertEquals(doJsonExtract("[0, 1, 2]", "$[1]"), "1");
        assertEquals(doJsonExtract("[0, 1, 2]", "$[0,1]"), "[0,1]");
        assertEquals(doJsonExtract("[0, 1, 2]", "$[\"1\"]"), null);
        assertEquals(doJsonExtract("{\"0\" : 0, \"1\" : 1, \"2\" : 2, }", "$.1"), "1");
        assertEquals(doJsonExtract("{\"0\" : 0, \"1\" : 1, \"2\" : 2, }", "$['1']"), "1");
        assertEquals(doJsonExtract("{\"0\" : 0, \"1\" : 1, \"2\" : 2, }", "$[\"1\"]"), "1");
        assertEquals(doJsonExtract("{\"0\" : 0, \"1\" : 1, \"2\" : 2, }", "$[1]"), null); // Looks at array position, not field name

        // Test fields starting with a digit
        assertEquals(doJsonExtract("{\"15day\" : 0, \"30day\" : 1, \"90day\" : 2, }", "$.30day"), "1");
        assertEquals(doJsonExtract("{\"15day\" : 0, \"30day\" : 1, \"90day\" : 2, }", "$['30day']"), "1");
        assertEquals(doJsonExtract("{\"15day\" : 0, \"30day\" : 1, \"90day\" : 2, }", "$[\"30day\"]"), "1");

        // Test selecting multiple fields at once
        assertEquals(doJsonExtract("{\"0\" : 0, \"1\" : 1, \"2\" : 2, }", "$['1','2']"), "{\"1\":1,\"2\":2}");

        // Test conditional in the jsonpath
        assertEquals(doJsonExtract("{ \"store\": {\n" +
                        "    \"book\": [ \n" +
                        "      { \"category\": \"reference\",\n" +
                        "        \"author\": \"Nigel Rees\",\n" +
                        "        \"title\": \"Sayings of the Century\",\n" +
                        "        \"price\": 8.95\n" +
                        "      },\n" +
                        "      { \"category\": \"fiction\",\n" +
                        "        \"author\": \"Evelyn Waugh\",\n" +
                        "        \"title\": \"Sword of Honour\",\n" +
                        "        \"price\": 12.99\n" +
                        "      },\n" +
                        "      { \"category\": \"fiction\",\n" +
                        "        \"author\": \"Herman Melville\",\n" +
                        "        \"title\": \"Moby Dick\",\n" +
                        "        \"isbn\": \"0-553-21311-3\",\n" +
                        "        \"price\": 8.99\n" +
                        "      }]}}",
                "$.store.book[?(@.price<10)]"), // filter all books cheapier than 10
                "[{\"author\":\"Nigel Rees\",\"category\":\"reference\",\"price\":8.95,\"title\":\"Sayings of the Century\"}," +
                        "{\"author\":\"Herman Melville\",\"category\":\"fiction\",\"isbn\":\"0-553-21311-3\",\"price\":8.99,\"title\":\"Moby Dick\"}]");
        assertEquals(doJsonExtract("{H:{1: a, 2: b}, I: {2: e, 3: f}}", "$.*.[?(@.3)]"), "[{\"2\":\"e\",\"3\":\"f\"}]");
    }

    @Test
    public void testInvalidExtracts() throws IOException
    {
        assertInvalidExtract("", "");
        assertInvalidExtract("{}", "$.");
        assertInvalidExtract("", "$$");
        assertInvalidExtract("", " ");
        assertInvalidExtract("", ".");
        assertInvalidExtract("{ \"store\": { \"book\": [{ \"title\": \"title\" }] } }", "$.store.book[]");
    }

    private static String doJsonExtract(String inputJson, String jsonPath)
            throws IOException
    {
        Slice value = JsonFunctions.varcharJsonExtractJayway(Slices.utf8Slice(inputJson), Slices.utf8Slice(jsonPath));
        return (value == null) ? null : value.toStringUtf8();
    }

    private static void assertInvalidExtract(String inputJson, String jsonPath)
            throws IOException
    {
        try {
            doJsonExtract(inputJson, jsonPath);
        }
        catch (IllegalArgumentException e) {
            return;
        }
        catch (InvalidPathException e) {
            return;
        }
        fail("Expected error for invalid json or jsonpath");
    }
}
