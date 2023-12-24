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
package com.facebook.presto.client;

import com.facebook.airlift.json.JsonCodec;
import org.testng.annotations.Test;

import java.util.Collections;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static org.testng.Assert.assertEquals;

public class TestQueryError
{
    private static final JsonCodec<QueryError> QUERY_ERROR_CODEC = jsonCodec(QueryError.class);

    @Test
    public void testToJson()
    {
        QueryError queryError = new QueryError(
                "message",
                "sqlState",
                1,
                "errorName",
                "errorType",
                true,
                new ErrorLocation(1, 2),
                new FailureInfo("type", "message", null, Collections.<FailureInfo>emptyList(), Collections.<String>emptyList(), null));

        String json = QUERY_ERROR_CODEC.toJson(queryError);

        String expected = "{\n" +
                "  \"message\" : \"message\",\n" +
                "  \"sqlState\" : \"sqlState\",\n" +
                "  \"errorCode\" : 1,\n" +
                "  \"errorName\" : \"errorName\",\n" +
                "  \"errorType\" : \"errorType\",\n" +
                "  \"retriable\" : true,\n" +
                "  \"errorLocation\" : {\n" +
                "    \"lineNumber\" : 1,\n" +
                "    \"columnNumber\" : 2\n" +
                "  },\n" +
                "  \"failureInfo\" : {\n" +
                "    \"type\" : \"type\",\n" +
                "    \"message\" : \"message\",\n" +
                "    \"suppressed\" : [ ],\n" +
                "    \"stack\" : [ ]\n" +
                "  }\n" +
                "}";
        assertEquals(json, expected);
    }
}
