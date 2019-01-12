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
package io.prestosql.client;

import io.airlift.json.JsonCodec;
import org.testng.annotations.Test;

import static io.airlift.json.JsonCodec.jsonCodec;
import static org.testng.Assert.assertEquals;

public class TestQueryResults
{
    private static final JsonCodec<QueryResults> QUERY_RESULTS_CODEC = jsonCodec(QueryResults.class);

    @Test
    public void testCompatibility()
    {
        String goldenValue = "{\n" +
                "  \"id\" : \"20160128_214710_00012_rk68b\",\n" +
                "  \"infoUri\" : \"http://localhost:54855/query.html?20160128_214710_00012_rk68b\",\n" +
                "  \"columns\" : [ {\n" +
                "    \"name\" : \"_col0\",\n" +
                "    \"type\" : \"bigint\",\n" +
                "    \"typeSignature\" : {\n" +
                "      \"rawType\" : \"bigint\",\n" +
                "      \"typeArguments\" : [ ],\n" +
                "      \"literalArguments\" : [ ],\n" +
                "      \"arguments\" : [ ]\n" +
                "    }\n" +
                "  } ],\n" +
                "  \"data\" : [ [ 123 ] ],\n" +
                "  \"stats\" : {\n" +
                "    \"state\" : \"FINISHED\",\n" +
                "    \"queued\" : false,\n" +
                "    \"scheduled\" : false,\n" +
                "    \"nodes\" : 0,\n" +
                "    \"totalSplits\" : 0,\n" +
                "    \"queuedSplits\" : 0,\n" +
                "    \"runningSplits\" : 0,\n" +
                "    \"completedSplits\" : 0,\n" +
                "    \"cpuTimeMillis\" : 0,\n" +
                "    \"wallTimeMillis\" : 0,\n" +
                "    \"queuedTimeMillis\" : 0,\n" +
                "    \"elapsedTimeMillis\" : 0,\n" +
                "    \"processedRows\" : 0,\n" +
                "    \"processedBytes\" : 0,\n" +
                "    \"peakMemoryBytes\" : 0\n" +
                "  }\n" +
                "}";

        QueryResults results = QUERY_RESULTS_CODEC.fromJson(goldenValue);
        assertEquals(results.getId(), "20160128_214710_00012_rk68b");
    }
}
