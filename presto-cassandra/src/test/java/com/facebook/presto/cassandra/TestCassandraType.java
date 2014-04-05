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
package com.facebook.presto.cassandra;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.testng.annotations.Test;
import com.google.common.collect.Lists;

import java.io.IOException;

import static org.testng.Assert.assertTrue;

public class TestCassandraType
{
    public void continueWhileNotNull(final JsonParser parser, final JsonToken token) throws IOException
    {
        if (token != null) {
            continueWhileNotNull(parser, parser.nextToken());
        }
    }
    public boolean isValidJson(final String json)
    {
        boolean valid = false;
        try {
            final JsonParser parser = new ObjectMapper().getFactory()
                    .createJsonParser(json);
            continueWhileNotNull(parser, parser.nextToken());
            valid = true;
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return valid;
    }

    @Test
    public void testJsonMapEncoding()
    {
        assertTrue(isValidJson(CassandraType.buildArrayValue(Lists.newArrayList("one", "two", "three\""), CassandraType.VARCHAR)));
        assertTrue(isValidJson(CassandraType.buildArrayValue(Lists.newArrayList(1, 2, 3), CassandraType.INT)));
        assertTrue(isValidJson(CassandraType.buildArrayValue(Lists.newArrayList(100000L, 200000000L, 3000000000L), CassandraType.BIGINT)));
        assertTrue(isValidJson(CassandraType.buildArrayValue(Lists.newArrayList(1.0, 2.0, 3.0), CassandraType.DOUBLE)));
    }
}
