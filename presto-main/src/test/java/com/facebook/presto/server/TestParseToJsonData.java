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
package com.facebook.presto.server;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.airlift.json.JsonCodec.mapJsonCodec;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.server.protocol.QueryResourceUtil.parseToJson;
import static org.testng.Assert.assertEquals;

public class TestParseToJsonData
{
    @Test
    public void testParseData()
    {
        assertParsingResult("map(map(bigint,bigint),bigint)", ImmutableMap.of(ImmutableMap.of(1L, 2L, 3L, 4L, 5L, 6L), 3L), "{\n  \"{\\n  \\\"1\\\" : 2,\\n  \\\"5\\\" : 6,\\n  \\\"3\\\" : 4\\n}\" : 3\n}");
        assertParsingResult("map(row(foo bigint,bar double),bigint)", ImmutableMap.of(ImmutableList.of(1L, 2.0), 3L, ImmutableList.of(4L, 5.0), 6L), "{\n  \"[ 4, 5.0 ]\" : 6,\n  \"[ 1, 2.0 ]\" : 3\n}");
        assertParsingResult("map(array(bigint),bigint)", ImmutableMap.of(ImmutableList.of(1L, 3L, 2L, 3L), 3L), "{\n  \"[ 1, 3, 2, 3 ]\" : 3\n}");
        // test nested array structure
        assertParsingResult("array(array(bigint))", ImmutableList.of(ImmutableList.of(1L, 2L, 3L), ImmutableList.of(4L, 5L, 6L)), "[ \"[ 1, 2, 3 ]\", \"[ 4, 5, 6 ]\" ]");
        assertParsingResult("array(map(bigint,bigint))", ImmutableList.of(ImmutableMap.of(1L, 2L, 3L, 4L), ImmutableMap.of(5L, 6L, 7L, 8L)), "[ \"{\\n  \\\"1\\\" : 2,\\n  \\\"3\\\" : 4\\n}\", \"{\\n  \\\"5\\\" : 6,\\n  \\\"7\\\" : 8\\n}\" ]");
        assertParsingResult("array(row(foo bigint,bar double))", ImmutableList.of(ImmutableList.of(1L, 2.0), ImmutableList.of(3L, 4.0)), "[ \"[ 1, 2.0 ]\", \"[ 3, 4.0 ]\" ]");
        // test nested row structure
        assertParsingResult("row(foo array(bigint),bar double)", ImmutableList.of(ImmutableList.of(1L, 2L), 3.0), "[ \"[ 1, 2 ]\", 3.0 ]");
        assertParsingResult("row(foo map(bigint,bigint),bar double)", ImmutableList.of(ImmutableMap.of(1L, 2L), 3.0), "[ \"{\\n  \\\"1\\\" : 2\\n}\", 3.0 ]");
        assertParsingResult("row(foo row(x bigint,y double),bar double)", ImmutableList.of(ImmutableList.of(1L, 2.0), 3.0), "[ \"[ 1, 2.0 ]\", 3.0 ]");
        // complex json with escape character
        assertParsingResult("map(row(x map(bigint,bigint), y varchar),bigint)", ImmutableMap.of(ImmutableList.of(ImmutableMap.of(1L, 2L), "{123: 2245}"), 1L), "{\n  \"[ \\\"{\\\\n  \\\\\\\"1\\\\\\\" : 2\\\\n}\\\", \\\"{123: 2245}\\\" ]\" : 1\n}");
        // nested Json and varchar can be serialized and deserialized correctly
        assertNestedJson("map(json,bigint)", "{\"123\":\"456\"}");
        assertNestedJson("map(varchar,bigint)", "{\"12\":\"34\", \"56\":\"78\"}");
    }
    private void assertNestedJson(String type, String expected)
    {
        Object data = ImmutableMap.of(jsonCodec(String.class).toJson(expected), 1L);
        Object parsedData = parseToJson(parseTypeSignature(type), data);
        Map<String, String> fixedValue = mapJsonCodec(String.class, String.class).fromJson((String) parsedData);
        assertEquals(jsonCodec(String.class).fromJson(fixedValue.keySet().iterator().next()), expected);
    }
    private void assertParsingResult(String type, Object data, String expected)
    {
        assertEquals(parseToJson(parseTypeSignature(type), data), expected);
    }
}
