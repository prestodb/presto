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

import com.facebook.drift.codec.ThriftCodec;
import com.facebook.drift.codec.ThriftCodecManager;
import com.facebook.drift.codec.internal.compiler.CompilerThriftCodecFactory;
import com.facebook.drift.codec.internal.reflection.ReflectionThriftCodecFactory;
import com.facebook.drift.protocol.TBinaryProtocol;
import com.facebook.drift.protocol.TCompactProtocol;
import com.facebook.drift.protocol.TFacebookCompactProtocol;
import com.facebook.drift.protocol.TMemoryBuffer;
import com.facebook.drift.protocol.TProtocol;
import com.facebook.drift.protocol.TTransport;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.function.Function;

import static com.facebook.presto.server.ClusterStatsResource.ClusterStats;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestThriftClusterStats
{
    public static final long RUNNING_QUERIES = 20;
    public static final long BLOCKED_QUERIES = 21;
    public static final long QUEUED_QUERIES = 22;
    public static final long ACTIVE_WORKERS = 12;
    public static final long RUNNING_DRIVERS = 13;
    public static final long RUNNING_TASKS = 101;
    public static final double RESERVED_MEMORY = 1001.5;
    public static final long TOTAL_INPUT_ROWS = 1002;
    public static final long TOTAL_INPUT_BYTES = 1003;
    public static final long TOTAL_CPU_TIME_SECS = 1004;
    public static final long ADJUSTED_QUEUE_SIZE = 1005;
    private static final ThriftCodecManager COMPILER_READ_CODEC_MANAGER = new ThriftCodecManager(new CompilerThriftCodecFactory(false));
    private static final ThriftCodecManager COMPILER_WRITE_CODEC_MANAGER = new ThriftCodecManager(new CompilerThriftCodecFactory(false));
    private static final ThriftCodec<ClusterStats> COMPILER_READ_CODEC = COMPILER_READ_CODEC_MANAGER.getCodec(ClusterStats.class);
    private static final ThriftCodec<ClusterStats> COMPILER_WRITE_CODEC = COMPILER_WRITE_CODEC_MANAGER.getCodec(ClusterStats.class);
    private static final ThriftCodecManager REFLECTION_READ_CODEC_MANAGER = new ThriftCodecManager(new ReflectionThriftCodecFactory());
    private static final ThriftCodecManager REFLECTION_WRITE_CODEC_MANAGER = new ThriftCodecManager(new ReflectionThriftCodecFactory());
    private static final ThriftCodec<ClusterStats> REFLECTION_READ_CODEC = REFLECTION_READ_CODEC_MANAGER.getCodec(ClusterStats.class);
    private static final ThriftCodec<ClusterStats> REFLECTION_WRITE_CODEC = REFLECTION_WRITE_CODEC_MANAGER.getCodec(ClusterStats.class);
    private static final TMemoryBuffer transport = new TMemoryBuffer(100 * 1024);
    private ClusterStats clusterStats;

    @BeforeMethod
    public void setUp()
    {
        clusterStats = getClusterStats();
    }

    @DataProvider
    public Object[][] codecCombinations()
    {
        return new Object[][] {
                {COMPILER_READ_CODEC, COMPILER_WRITE_CODEC},
                {COMPILER_READ_CODEC, REFLECTION_WRITE_CODEC},
                {REFLECTION_READ_CODEC, COMPILER_WRITE_CODEC},
                {REFLECTION_READ_CODEC, REFLECTION_WRITE_CODEC}
        };
    }

    @Test(dataProvider = "codecCombinations")
    public void testRoundTripSerializeBinaryProtocol(ThriftCodec<ClusterStats> readCodec, ThriftCodec<ClusterStats> writeCodec)
            throws Exception
    {
        ClusterStats clusterStats = getRoundTripSerialize(readCodec, writeCodec, TBinaryProtocol::new);
        assertSerde(clusterStats);
    }

    @Test(dataProvider = "codecCombinations")
    public void testRoundTripSerializeTCompactProtocol(ThriftCodec<ClusterStats> readCodec, ThriftCodec<ClusterStats> writeCodec)
            throws Exception
    {
        ClusterStats clusterStats = getRoundTripSerialize(readCodec, writeCodec, TCompactProtocol::new);
        assertSerde(clusterStats);
    }

    @Test(dataProvider = "codecCombinations")
    public void testRoundTripSerializeTFacebookCompactProtocol(ThriftCodec<ClusterStats> readCodec, ThriftCodec<ClusterStats> writeCodec)
            throws Exception
    {
        ClusterStats clusterStats = getRoundTripSerialize(readCodec, writeCodec, TFacebookCompactProtocol::new);
        assertSerde(clusterStats);
    }

    private void assertSerde(ClusterStats clusterStats)
    {
        assertEquals(clusterStats.getRunningQueries(), RUNNING_QUERIES);
        assertEquals(clusterStats.getBlockedQueries(), BLOCKED_QUERIES);
        assertEquals(clusterStats.getQueuedQueries(), QUEUED_QUERIES);
        assertEquals(clusterStats.getActiveWorkers(), ACTIVE_WORKERS);
        assertEquals(clusterStats.getRunningDrivers(), RUNNING_DRIVERS);
        assertEquals(clusterStats.getRunningTasks(), RUNNING_TASKS);
        assertEquals(clusterStats.getReservedMemory(), RESERVED_MEMORY);
        assertEquals(clusterStats.getTotalInputRows(), TOTAL_INPUT_ROWS);
        assertEquals(clusterStats.getTotalInputBytes(), TOTAL_INPUT_BYTES);
        assertEquals(clusterStats.getTotalCpuTimeSecs(), TOTAL_CPU_TIME_SECS);
        assertEquals(clusterStats.getAdjustedQueueSize(), ADJUSTED_QUEUE_SIZE);
    }

    private ClusterStats getRoundTripSerialize(ThriftCodec<ClusterStats> readCodec, ThriftCodec<ClusterStats> writeCodec, Function<TTransport, TProtocol> protocolFactory)
            throws Exception
    {
        TProtocol protocol = protocolFactory.apply(transport);
        writeCodec.write(clusterStats, protocol);
        return readCodec.read(protocol);
    }

    private ClusterStats getClusterStats()
    {
        return new ClusterStats(
                RUNNING_QUERIES,
                BLOCKED_QUERIES,
                QUEUED_QUERIES,
                ACTIVE_WORKERS,
                RUNNING_DRIVERS,
                RUNNING_TASKS,
                RESERVED_MEMORY,
                TOTAL_INPUT_ROWS,
                TOTAL_INPUT_BYTES,
                TOTAL_CPU_TIME_SECS,
                ADJUSTED_QUEUE_SIZE);
    }
}
