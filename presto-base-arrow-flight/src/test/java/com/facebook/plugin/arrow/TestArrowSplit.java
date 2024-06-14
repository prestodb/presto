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
package com.facebook.plugin.arrow;

import com.facebook.airlift.json.JsonCodec;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.presto.spi.schedule.NodeSelectionStrategy.NO_PREFERENCE;
import static org.testng.Assert.assertEquals;

public class TestArrowSplit
{
    private final ArrowSplit split = new ArrowSplit("schemaName", "tableName",
            "ticket".getBytes(), "token", Arrays.asList("http://host"));

    @Test
    public void testNodes()
    {
        assertEquals(split.getNodeSelectionStrategy(), NO_PREFERENCE);
        assertEquals(split.getPreferredNodes(null), Collections.emptyList());
    }

    @Test
    public void testJsonRoundTrip()
    {
        JsonCodec<ArrowSplit> codec = jsonCodec(ArrowSplit.class);
        String json = codec.toJson(split);
        ArrowSplit copy = codec.fromJson(json);
        assertEquals(copy.getSchemaName(), split.getSchemaName());
        assertEquals(copy.getTableName(), split.getTableName());
        assertEquals(copy.getTicket(), split.getTicket());
        assertEquals(copy.getLocationUrls(), split.getLocationUrls());
    }
}
