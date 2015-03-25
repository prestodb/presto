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

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;

public class QueryResultsTest
{
    @Test
    public void testRoundtrip() throws IOException
    {
        String id = "20150320_083317_00000_z792e";
        String infoUri = "http://127.0.0.1:52391/v1/query/20150320_083317_00000_z792e";
        String nextUri = "http://127.0.0.1:52391/v1/statement/20150320_083317_00000_z792e/1";
        ObjectMapper mapper = new ObjectMapper();
        QueryResults expected = new QueryResults(
                id,
                URI.create(infoUri),
                null,
                URI.create(nextUri),
                null,
                null,
                new StatementStats("QUEUED", false, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, null),
                null,
                null,
                null);

        String json = mapper.writeValueAsString(expected);
        QueryResults actual = mapper.readValue(json, QueryResults.class);

        assertEquals(actual.getId(), expected.getId());
        assertEquals(actual.getInfoUri(), expected.getInfoUri());
        assertEquals(actual.getNextUri(), expected.getNextUri());
        assertEquals(actual.getStats().toString(), expected.getStats().toString());
    }
}
