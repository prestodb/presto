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

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.units.DataSize;
import com.facebook.airlift.units.Duration;
import com.facebook.presto.client.NodeVersion;
import com.facebook.presto.memory.MemoryInfo;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;

public class TestNodeStatus
{
    private static final JsonCodec<NodeStatus> CODEC = jsonCodec(NodeStatus.class);

    private static NodeStatus nodeStatus(long nonHeapUsed, long asyncDataCacheBytes, long queryMemoryBytes)
    {
        return new NodeStatus(
                "test-node",
                new NodeVersion("1"),
                "test-environment",
                false,
                new Duration(1, SECONDS),
                "http://externalAddress",
                "http://internalAddress",
                new MemoryInfo(new DataSize(1, MEGABYTE), ImmutableMap.of()),
                8,
                0.25,
                0.5,
                1024,
                8192,
                nonHeapUsed,
                asyncDataCacheBytes,
                queryMemoryBytes);
    }

    @Test
    public void testJsonRoundTrip()
    {
        // Use the native-worker shape: nonHeapUsed = -1 (not applicable) and
        // distinct, non-zero values for the two native memory fields so the
        // round-trip would catch a swapped or dropped field.
        NodeStatus expected = nodeStatus(-1L, 111L, 222L);

        NodeStatus actual = CODEC.fromJson(CODEC.toJson(expected));

        assertEquals(actual.getNonHeapUsed(), -1L);
        assertEquals(actual.getAsyncDataCacheBytes(), 111L);
        assertEquals(actual.getQueryMemoryBytes(), 222L);
        assertEquals(actual.getHeapUsed(), expected.getHeapUsed());
        assertEquals(actual.getHeapAvailable(), expected.getHeapAvailable());
        assertEquals(actual.getNodeId(), expected.getNodeId());
    }

    @Test
    public void testDeserializeLegacyStatusWithoutNativeFields()
    {
        // A pre-upgrade worker never emits the native memory fields. The new
        // coordinator must still deserialize its status, defaulting the
        // missing fields to 0 (mirrors the native from_json presence guard).
        String legacyJson = "{" +
                "\"nodeId\":\"test-node\"," +
                "\"nodeVersion\":{\"version\":\"1\"}," +
                "\"environment\":\"test-environment\"," +
                "\"coordinator\":false," +
                "\"uptime\":\"1.00s\"," +
                "\"externalAddress\":\"http://externalAddress\"," +
                "\"internalAddress\":\"http://internalAddress\"," +
                "\"memoryInfo\":{\"totalNodeMemory\":\"1MB\",\"pools\":{}}," +
                "\"processors\":8," +
                "\"processCpuLoad\":0.25," +
                "\"systemCpuLoad\":0.5," +
                "\"heapUsed\":1024," +
                "\"heapAvailable\":8192," +
                "\"nonHeapUsed\":42}";

        NodeStatus actual = CODEC.fromJson(legacyJson);

        assertEquals(actual.getNonHeapUsed(), 42L);
        assertEquals(actual.getAsyncDataCacheBytes(), 0L);
        assertEquals(actual.getQueryMemoryBytes(), 0L);
    }
}
