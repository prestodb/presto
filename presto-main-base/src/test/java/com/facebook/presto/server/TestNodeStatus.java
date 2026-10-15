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
import com.facebook.drift.codec.ThriftCodec;
import com.facebook.drift.codec.ThriftCodecManager;
import com.facebook.drift.codec.internal.compiler.CompilerThriftCodecFactory;
import com.facebook.drift.codec.metadata.ThriftCatalog;
import com.facebook.drift.codec.utils.DataSizeToBytesThriftCodec;
import com.facebook.drift.codec.utils.DurationToMillisThriftCodec;
import com.facebook.drift.protocol.TBinaryProtocol;
import com.facebook.drift.protocol.TMemoryBuffer;
import com.facebook.presto.client.NodeVersion;
import com.facebook.presto.memory.MemoryInfo;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;

public class TestNodeStatus
{
    private static final JsonCodec<NodeStatus> CODEC = jsonCodec(NodeStatus.class);

    private static NodeStatus nodeStatus(long nonHeapUsed, long asyncDataCacheBytes, long queryMemoryBytes, Optional<Long> systemMemoryBytes)
    {
        return new NodeStatus(
                "test-node",
                new NodeVersion("1"),
                "test-environment",
                false,
                new Duration(1, SECONDS),
                "http://externalAddress",
                "http://internalAddress",
                new MemoryInfo(new DataSize(1, MEGABYTE), systemMemoryBytes, ImmutableMap.of()),
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
        // Distinct values for every long field, including a negative one, so a
        // swapped, dropped, or sign-mangled field is caught on round-trip.
        NodeStatus expected = nodeStatus(-1L, 111L, 222L, Optional.of(333L));

        NodeStatus actual = CODEC.fromJson(CODEC.toJson(expected));

        assertEquals(actual.getNonHeapUsed(), -1L);
        assertEquals(actual.getAsyncDataCacheBytes(), 111L);
        assertEquals(actual.getQueryMemoryBytes(), 222L);
        assertEquals(actual.getMemoryInfo().getSystemMemoryBytes(), Optional.of(333L));
        assertEquals(actual.getHeapUsed(), expected.getHeapUsed());
        assertEquals(actual.getHeapAvailable(), expected.getHeapAvailable());
        assertEquals(actual.getNodeId(), expected.getNodeId());
    }

    @Test
    public void testJsonRoundTripEmptySystemMemoryBytes()
    {
        // Optional.empty() signals "not sampled" and must survive a JSON
        // round-trip as Optional.empty(), not as Optional.of(0L) — scrapers
        // and dashboards rely on that distinction.
        NodeStatus expected = nodeStatus(-1L, 111L, 222L, Optional.empty());

        NodeStatus actual = CODEC.fromJson(CODEC.toJson(expected));

        assertEquals(actual.getMemoryInfo().getSystemMemoryBytes(), Optional.empty());
    }

    @Test
    public void testDeserializeLegacyStatusWithoutNativeFields()
    {
        // A pre-upgrade worker never emits the native memory fields. The new
        // coordinator must still deserialize its status, defaulting the
        // missing fields to 0 (asyncDataCacheBytes, queryMemoryBytes) and
        // Optional.empty() (systemMemoryBytes).
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
        assertEquals(actual.getMemoryInfo().getSystemMemoryBytes(), Optional.empty());
    }

    @Test
    public void testThriftRoundTrip()
            throws Exception
    {
        // NodeStatus (and its nested MemoryInfo) travels over Thrift via
        // ResourceManagerClient.nodeHeartbeat, so a Thrift round-trip must
        // preserve systemMemoryBytes just like the JSON path does.
        ThriftCatalog catalog = new ThriftCatalog();
        ThriftCodecManager codecManager = new ThriftCodecManager(
                new CompilerThriftCodecFactory(false),
                catalog,
                ImmutableSet.of(
                        new DataSizeToBytesThriftCodec(catalog),
                        new DurationToMillisThriftCodec(catalog)));
        ThriftCodec<NodeStatus> codec = codecManager.getCodec(NodeStatus.class);

        NodeStatus expected = nodeStatus(-1L, 111L, 222L, Optional.of(333L));
        TMemoryBuffer transport = new TMemoryBuffer(1024);
        codec.write(expected, new TBinaryProtocol(transport));
        NodeStatus actual = codec.read(new TBinaryProtocol(transport));

        assertEquals(actual.getNonHeapUsed(), -1L);
        assertEquals(actual.getAsyncDataCacheBytes(), 111L);
        assertEquals(actual.getQueryMemoryBytes(), 222L);
        assertEquals(actual.getMemoryInfo().getSystemMemoryBytes(), Optional.of(333L));
    }

    @Test
    public void testThriftRoundTripEmptySystemMemoryBytes()
    {
        // Mirror of testJsonRoundTripEmptySystemMemoryBytes for the Thrift path.
        ThriftCatalog catalog = new ThriftCatalog();
        ThriftCodecManager codecManager = new ThriftCodecManager(
                new CompilerThriftCodecFactory(false),
                catalog,
                ImmutableSet.of(
                        new DataSizeToBytesThriftCodec(catalog),
                        new DurationToMillisThriftCodec(catalog)));

        NodeStatus expected = nodeStatus(-1L, 111L, 222L, Optional.empty());
        TMemoryBuffer transport = new TMemoryBuffer(1024);
        try {
            ThriftCodec<NodeStatus> codec = codecManager.getCodec(NodeStatus.class);
            codec.write(expected, new TBinaryProtocol(transport));
            NodeStatus actual = codec.read(new TBinaryProtocol(transport));
            assertEquals(actual.getMemoryInfo().getSystemMemoryBytes(), Optional.empty());
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
