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
package com.facebook.presto.execution;

import com.facebook.drift.codec.ThriftCodec;
import com.facebook.drift.codec.ThriftCodecManager;
import com.facebook.drift.codec.internal.compiler.CompilerThriftCodecFactory;
import com.facebook.drift.codec.internal.reflection.ReflectionThriftCodecFactory;
import com.facebook.drift.codec.metadata.ThriftCatalog;
import com.facebook.drift.codec.utils.DataSizeToBytesThriftCodec;
import com.facebook.drift.codec.utils.JodaDateTimeToEpochMillisThriftCodec;
import com.facebook.drift.protocol.TBinaryProtocol;
import com.facebook.drift.protocol.TCompactProtocol;
import com.facebook.drift.protocol.TFacebookCompactProtocol;
import com.facebook.drift.protocol.TMemoryBuffer;
import com.facebook.drift.protocol.TProtocol;
import com.facebook.drift.protocol.TTransport;
import com.facebook.presto.operator.BlockedReason;
import com.facebook.presto.server.QueryProgressStats;
import com.facebook.presto.server.QueryStateInfo;
import com.facebook.presto.server.ResourceGroupInfo;
import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.spi.resourceGroups.ResourceGroupState;
import com.facebook.presto.spi.resourceGroups.SchedulingPolicy;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import org.joda.time.DateTime;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.Set;
import java.util.function.Function;

import static com.facebook.presto.operator.BlockedReason.WAITING_FOR_MEMORY;
import static com.facebook.presto.spi.ErrorType.EXTERNAL;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestThriftResourceGroupInfo
{
    private static final ThriftCatalog COMMON_CATALOG = new ThriftCatalog();
    private static final DataSizeToBytesThriftCodec DATA_SIZE_CODEC = new DataSizeToBytesThriftCodec(COMMON_CATALOG);
    private static final JodaDateTimeToEpochMillisThriftCodec DATE_TIME_CODEC = new JodaDateTimeToEpochMillisThriftCodec(COMMON_CATALOG);
    private static final ThriftCodecManager COMPILER_READ_CODEC_MANAGER = new ThriftCodecManager(new CompilerThriftCodecFactory(false), COMMON_CATALOG, ImmutableSet.of(DATA_SIZE_CODEC, DATE_TIME_CODEC));
    private static final ThriftCodecManager COMPILER_WRITE_CODEC_MANAGER = new ThriftCodecManager(new CompilerThriftCodecFactory(false), COMMON_CATALOG, ImmutableSet.of(DATA_SIZE_CODEC, DATE_TIME_CODEC));
    private static final ThriftCodec<ResourceGroupInfo> COMPILER_READ_CODEC = COMPILER_READ_CODEC_MANAGER.getCodec(ResourceGroupInfo.class);
    private static final ThriftCodec<ResourceGroupInfo> COMPILER_WRITE_CODEC = COMPILER_WRITE_CODEC_MANAGER.getCodec(ResourceGroupInfo.class);
    private static final ThriftCodecManager REFLECTION_READ_CODEC_MANAGER = new ThriftCodecManager(new ReflectionThriftCodecFactory(), COMMON_CATALOG, ImmutableSet.of(DATA_SIZE_CODEC, DATE_TIME_CODEC));
    private static final ThriftCodecManager REFLECTION_WRITE_CODEC_MANAGER = new ThriftCodecManager(new ReflectionThriftCodecFactory(), COMMON_CATALOG, ImmutableSet.of(DATA_SIZE_CODEC, DATE_TIME_CODEC));
    private static final ThriftCodec<ResourceGroupInfo> REFLECTION_READ_CODEC = REFLECTION_READ_CODEC_MANAGER.getCodec(ResourceGroupInfo.class);
    private static final ThriftCodec<ResourceGroupInfo> REFLECTION_WRITE_CODEC = REFLECTION_WRITE_CODEC_MANAGER.getCodec(ResourceGroupInfo.class);
    private static final TMemoryBuffer transport = new TMemoryBuffer(100 * 1024);

    // Dummy values for fake ResourceGroupInfo
    private static final ResourceGroupId FAKE_RESOURCE_GROUP_ID_PARENT = new ResourceGroupId(Arrays.asList("global", "pipeline", "ns"));
    private static final ResourceGroupId FAKE_SUB_RESOURCE_GROUP_ID_1 = new ResourceGroupId(Arrays.asList("global", "pipeline", "ns", "sub_rg_1"));
    private static final ResourceGroupId FAKE_SUB_RESOURCE_GROUP_ID_2 = new ResourceGroupId(Arrays.asList("global", "pipeline", "ns", "sub_rg_2"));
    private static final ResourceGroupId FAKE_SUB_RESOURCE_GROUP_ID_3 = new ResourceGroupId(Arrays.asList("global", "pipeline", "ns", "sub_rg_3"));
    private static final ResourceGroupState FAKE_RESOURCE_GROUP_STATE = ResourceGroupState.CAN_QUEUE;
    private static final SchedulingPolicy FAKE_SCHEDULING_POLICY = SchedulingPolicy.FAIR;
    private static final int FAKE_SCHEDULING_WEIGHT = 100;
    private static final long FAKE_SOFT_MEMORY_LIMIT_BYTES = Long.MAX_VALUE;
    private static final int FAKE_SOFT_CONCURRENCY_LIMIT = 10;
    private static final int FAKE_HARD_CONCURRENCY_LIMIT = 20;
    private static final int FAKE_MAX_QUEUED_QUERIES = 50;
    private static final long FAKE_CACHED_MEMORY_USAGE_BYTES = 20480;
    private static final int FAKE_QUEUED_QUERIES = 10;
    private static final int FAKE_RUNNING_QUERIES = 2;
    private static final int FAKE_ELIGIBLE_SUB_GROUPS = 3;

    // Dummy values for fake QueryStateInfo
    private static final QueryId FAKE_QUERY_ID_1 = new QueryId("20220202_215711_00938_kvz6g");
    private static final QueryId FAKE_QUERY_ID_2 = new QueryId("20220202_215711_00938_ks4fq");
    private static final ResourceGroupId FAKE_QUERY_RESOURCE_GROUP_ID = new ResourceGroupId(Arrays.asList("global", "pipeline", "ns", "sub_rg", "dummy_prefix", "dummy_query_task_id"));
    private static final QueryState FAKE_QUERY_STATE = QueryState.DISPATCHING;
    private static final String FAKE_QUERY = "SELECT something FROM somewhere";
    private static final DateTime FAKE_CREATE_TIME = new DateTime("2000-01-01T01:00:00Z");
    private static final String FAKE_QUERY_REQUESTER = "DummyUser";
    private static final String FAKE_QUERY_SOURCE = "DummySource";
    private static final String FAKE_QUERY_CLIENT_INFO = "DummyClientInfo";
    private static final String FAKE_QUERY_CATALOG = "DummyCatalog";
    private static final String FAKE_QUERY_SCHEMA = "DummySchema";
    private static final boolean FAKE_QUERY_TRUNCATED = true;
    private static final boolean FAKE_AUTHENTICATED = true;
    private static final List<String> FAKE_WARNING_CODES = ImmutableList.of("WARNING1");
    private static final ErrorCode FAKE_ERROR_CODE = new ErrorCode(1234, "DUMMY_ERROR", EXTERNAL, false);

    // Dummy values for fake QueryProgressStats
    private static final long FAKE_ELAPSED_TIME_MILLIS = 10000L;
    private static final long FAKE_QUEUED_TIME_MILLIS = 1000L;
    private static final long FAKE_CPU_TIME_MILLS = 1L;
    private static final long FAKE_SCHEDULED_TIME_MILLIS = 50L;
    private static final long FAKE_CURRENT_MEMORY_BYTES = 10240L;
    private static final long FAKE_PEAK_MEMORY_BYTES = 20480L;
    private static final long FAKE_PEAK_TOTAL_MEMORY_BYTES = 40960L;
    private static final long FAKE_PEAK_TASK_TOTAL_MEMORY_BYTES = 20480L;
    private static final long FAKE_INPUT_ROWS = 5L;
    private static final long FAKE_INPUT_BYTES = 1024L;
    private static final boolean FAKE_BLOCKED = true;
    private static final OptionalDouble FAKE_PROGRESS_PERCENTAGE_1 = OptionalDouble.of(65.23124);
    private static final OptionalDouble FAKE_PROGRESS_PERCENTAGE_2 = OptionalDouble.of(98.235);
    private static final long FAKE_EXECUTION_TIME_MILLIS = 124354L;
    private static final double FAKE_CUMULATIVE_USER_MEMORY = 1234.567;
    private static final double FAKE_CUMULATIVE_TOTAL_MEMORY = 2345.6789;
    private static final Set<BlockedReason> FAKE_BLOCKED_REASONS = ImmutableSet.of(WAITING_FOR_MEMORY);
    private static final int FAKE_QUEUED_DRIVERS = 123;
    private static final int FAKE_RUNNING_DRIVERS = 234;
    private static final int FAKE_COMPLETED_DRIVERS = 133435;

    private ResourceGroupInfo resourceGroupInfo;
    private List<QueryStateInfo> queryStateInfoList;
    private List<QueryProgressStats> queryProgressStats;

    @BeforeMethod
    public void setUp()
    {
        setUpQueryProgressStats();
        setUpQueryStateInfoList();
        List<ResourceGroupInfo> subGroups = new ArrayList<>();
        ResourceGroupInfo subResourceGroupInfo1 = getResourceGroupInfo(
                FAKE_SUB_RESOURCE_GROUP_ID_1,
                null,
                null);
        ResourceGroupInfo subResourceGroupInfo2 = getResourceGroupInfo(
                FAKE_SUB_RESOURCE_GROUP_ID_2,
                null,
                null);
        ResourceGroupInfo subResourceGroupInfo3 = getResourceGroupInfo(
                FAKE_SUB_RESOURCE_GROUP_ID_3,
                null,
                null);
        subGroups.add(subResourceGroupInfo1);
        subGroups.add(subResourceGroupInfo2);
        subGroups.add(subResourceGroupInfo3);
        resourceGroupInfo = getResourceGroupInfo(
                FAKE_RESOURCE_GROUP_ID_PARENT,
                subGroups,
                queryStateInfoList);
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
    public void testRoundTripSerializeBinaryProtocol(ThriftCodec<ResourceGroupInfo> readCodec, ThriftCodec<ResourceGroupInfo> writeCodec)
            throws Exception
    {
        ResourceGroupInfo groupInfo = getRoundTripSerialize(readCodec, writeCodec, TBinaryProtocol::new);
        assertSerde(groupInfo);
    }

    @Test(dataProvider = "codecCombinations")
    public void testRoundTripSerializeTCompactProtocol(ThriftCodec<ResourceGroupInfo> readCodec, ThriftCodec<ResourceGroupInfo> writeCodec)
            throws Exception
    {
        ResourceGroupInfo groupInfo = getRoundTripSerialize(readCodec, writeCodec, TCompactProtocol::new);
        assertSerde(groupInfo);
    }

    @Test(dataProvider = "codecCombinations")
    public void testRoundTripSerializeTFacebookCompactProtocol(ThriftCodec<ResourceGroupInfo> readCodec, ThriftCodec<ResourceGroupInfo> writeCodec)
            throws Exception
    {
        ResourceGroupInfo groupInfo = getRoundTripSerialize(readCodec, writeCodec, TFacebookCompactProtocol::new);
        assertSerde(groupInfo);
    }

    private void assertSerde(ResourceGroupInfo actualInfo)
    {
        assertEquals(actualInfo.getId(), FAKE_RESOURCE_GROUP_ID_PARENT);
        assertSerdeRGInfoCommonProperties(actualInfo);

        List<ResourceGroupInfo> actualSubGroupList = actualInfo.getSubGroups();
        assertNotNull(actualSubGroupList);
        assertEquals(actualSubGroupList.size(), 3);
        assertEquals(actualSubGroupList.get(0).getId(), FAKE_SUB_RESOURCE_GROUP_ID_1);
        assertSerdeRGInfoCommonProperties(actualSubGroupList.get(0));

        assertEquals(actualSubGroupList.get(1).getId(), FAKE_SUB_RESOURCE_GROUP_ID_2);
        assertSerdeRGInfoCommonProperties(actualSubGroupList.get(1));

        assertEquals(actualSubGroupList.get(2).getId(), FAKE_SUB_RESOURCE_GROUP_ID_3);
        assertSerdeRGInfoCommonProperties(actualSubGroupList.get(2));

        List<QueryStateInfo> actualQueryStateInfoList = actualInfo.getRunningQueries();
        assertNotNull(actualQueryStateInfoList);
        assertEquals(actualQueryStateInfoList.size(), 2);

        assertEquals(actualQueryStateInfoList.get(0).getQueryId(), FAKE_QUERY_ID_1);
        assertSerdeQueryState(actualQueryStateInfoList.get(0), FAKE_PROGRESS_PERCENTAGE_1);

        assertEquals(actualQueryStateInfoList.get(1).getQueryId(), FAKE_QUERY_ID_2);
        assertSerdeQueryState(actualQueryStateInfoList.get(1), FAKE_PROGRESS_PERCENTAGE_2);
    }

    private void assertSerdeQueryState(QueryStateInfo actualInfo, OptionalDouble expectedProgress)
    {
        assertEquals(actualInfo.getQueryState(), FAKE_QUERY_STATE);
        assertTrue(actualInfo.getResourceGroupId().isPresent());
        assertEquals(actualInfo.getResourceGroupId().get(), FAKE_QUERY_RESOURCE_GROUP_ID);
        assertEquals(actualInfo.getQuery(), FAKE_QUERY);
        assertEquals(actualInfo.getCreateTime(), FAKE_CREATE_TIME);
        assertEquals(actualInfo.getUser(), FAKE_QUERY_REQUESTER);
        assertTrue(actualInfo.getSource().isPresent());
        assertEquals(actualInfo.getSource().get(), FAKE_QUERY_SOURCE);
        assertTrue(actualInfo.getClientInfo().isPresent());
        assertEquals(actualInfo.getClientInfo().get(), FAKE_QUERY_CLIENT_INFO);
        assertTrue(actualInfo.getCatalog().isPresent());
        assertEquals(actualInfo.getCatalog().get(), FAKE_QUERY_CATALOG);
        assertTrue(actualInfo.getSchema().isPresent());
        assertEquals(actualInfo.getSchema().get(), FAKE_QUERY_SCHEMA);
        assertTrue(actualInfo.getPathToRoot().isPresent());
        assertEquals(actualInfo.getPathToRoot().get().size(), 0);

        assertTrue(actualInfo.getProgress().isPresent());
        QueryProgressStats queryProgressStats = actualInfo.getProgress().get();
        assertEquals(queryProgressStats.getElapsedTimeMillis(), FAKE_ELAPSED_TIME_MILLIS);
        assertEquals(queryProgressStats.getQueuedTimeMillis(), FAKE_QUEUED_TIME_MILLIS);
        assertEquals(queryProgressStats.getCpuTimeMillis(), FAKE_CPU_TIME_MILLS);
        assertEquals(queryProgressStats.getScheduledTimeMillis(), FAKE_SCHEDULED_TIME_MILLIS);
        assertEquals(queryProgressStats.getCurrentMemoryBytes(), FAKE_CURRENT_MEMORY_BYTES);
        assertEquals(queryProgressStats.getPeakMemoryBytes(), FAKE_PEAK_MEMORY_BYTES);
        assertEquals(queryProgressStats.getPeakTotalMemoryBytes(), FAKE_PEAK_TOTAL_MEMORY_BYTES);
        assertEquals(queryProgressStats.getPeakTaskTotalMemoryBytes(), FAKE_PEAK_TASK_TOTAL_MEMORY_BYTES);
        assertEquals(queryProgressStats.getInputRows(), FAKE_INPUT_ROWS);
        assertEquals(queryProgressStats.getInputBytes(), FAKE_INPUT_BYTES);
        assertEquals(queryProgressStats.getProgressPercentage(), expectedProgress);
    }

    private void assertSerdeRGInfoCommonProperties(ResourceGroupInfo actualInfo)
    {
        assertEquals(actualInfo.getState(), FAKE_RESOURCE_GROUP_STATE);
        assertEquals(actualInfo.getSchedulingPolicy(), FAKE_SCHEDULING_POLICY);
        assertEquals(actualInfo.getSchedulingWeight(), FAKE_SCHEDULING_WEIGHT);
        assertEquals(actualInfo.getSoftMemoryLimit().toBytes(), FAKE_SOFT_MEMORY_LIMIT_BYTES);
        assertEquals(actualInfo.getSoftConcurrencyLimit(), FAKE_SOFT_CONCURRENCY_LIMIT);
        assertEquals(actualInfo.getHardConcurrencyLimit(), FAKE_HARD_CONCURRENCY_LIMIT);
        assertEquals(actualInfo.getMaxQueuedQueries(), FAKE_MAX_QUEUED_QUERIES);
        assertEquals(actualInfo.getMemoryUsage().toBytes(), FAKE_CACHED_MEMORY_USAGE_BYTES);
        assertEquals(actualInfo.getNumQueuedQueries(), FAKE_QUEUED_QUERIES);
        assertEquals(actualInfo.getNumRunningQueries(), FAKE_RUNNING_QUERIES);
        assertEquals(actualInfo.getNumEligibleSubGroups(), FAKE_ELIGIBLE_SUB_GROUPS);
    }

    private ResourceGroupInfo getRoundTripSerialize(
            ThriftCodec<ResourceGroupInfo> readCodec,
            ThriftCodec<ResourceGroupInfo> writeCodec, Function<TTransport, TProtocol> protocolFactory)
            throws Exception
    {
        TProtocol protocol = protocolFactory.apply(transport);
        writeCodec.write(resourceGroupInfo, protocol);
        return readCodec.read(protocol);
    }

    private void setUpQueryStateInfoList()
    {
        queryStateInfoList = new ArrayList<>();
        queryStateInfoList.add(new QueryStateInfo(
                FAKE_QUERY_ID_1,
                FAKE_QUERY_STATE,
                Optional.of(FAKE_QUERY_RESOURCE_GROUP_ID),
                FAKE_QUERY,
                FAKE_QUERY_TRUNCATED,
                FAKE_CREATE_TIME,
                FAKE_QUERY_REQUESTER,
                FAKE_AUTHENTICATED,
                Optional.of(FAKE_QUERY_SOURCE),
                Optional.of(FAKE_QUERY_CLIENT_INFO),
                Optional.of(FAKE_QUERY_CATALOG),
                Optional.of(FAKE_QUERY_SCHEMA),
                Optional.of(new ArrayList<>()),
                Optional.of(queryProgressStats.get(0)),
                FAKE_WARNING_CODES,
                Optional.of(FAKE_ERROR_CODE)));
        queryStateInfoList.add(new QueryStateInfo(
                FAKE_QUERY_ID_2,
                FAKE_QUERY_STATE,
                Optional.of(FAKE_QUERY_RESOURCE_GROUP_ID),
                FAKE_QUERY,
                FAKE_QUERY_TRUNCATED,
                FAKE_CREATE_TIME,
                FAKE_QUERY_REQUESTER,
                FAKE_AUTHENTICATED,
                Optional.of(FAKE_QUERY_SOURCE),
                Optional.of(FAKE_QUERY_CLIENT_INFO),
                Optional.of(FAKE_QUERY_CATALOG),
                Optional.of(FAKE_QUERY_SCHEMA),
                Optional.of(new ArrayList<>()),
                Optional.of(queryProgressStats.get(1)),
                ImmutableList.of(),
                Optional.empty()));
    }

    private void setUpQueryProgressStats()
    {
        queryProgressStats = new ArrayList<>();
        queryProgressStats.add(new QueryProgressStats(
                FAKE_ELAPSED_TIME_MILLIS,
                FAKE_QUEUED_TIME_MILLIS,
                FAKE_EXECUTION_TIME_MILLIS,
                FAKE_CPU_TIME_MILLS,
                FAKE_SCHEDULED_TIME_MILLIS,
                FAKE_CURRENT_MEMORY_BYTES,
                FAKE_PEAK_MEMORY_BYTES,
                FAKE_PEAK_TOTAL_MEMORY_BYTES,
                FAKE_PEAK_TASK_TOTAL_MEMORY_BYTES,
                FAKE_CUMULATIVE_USER_MEMORY,
                FAKE_CUMULATIVE_TOTAL_MEMORY,
                FAKE_INPUT_ROWS,
                FAKE_INPUT_BYTES,
                FAKE_BLOCKED,
                Optional.of(FAKE_BLOCKED_REASONS),
                FAKE_PROGRESS_PERCENTAGE_1,
                FAKE_QUEUED_DRIVERS,
                FAKE_RUNNING_DRIVERS,
                FAKE_COMPLETED_DRIVERS));
        queryProgressStats.add(new QueryProgressStats(
                FAKE_ELAPSED_TIME_MILLIS,
                FAKE_QUEUED_TIME_MILLIS,
                FAKE_EXECUTION_TIME_MILLIS,
                FAKE_CPU_TIME_MILLS,
                FAKE_SCHEDULED_TIME_MILLIS,
                FAKE_CURRENT_MEMORY_BYTES,
                FAKE_PEAK_MEMORY_BYTES,
                FAKE_PEAK_TOTAL_MEMORY_BYTES,
                FAKE_PEAK_TASK_TOTAL_MEMORY_BYTES,
                FAKE_CUMULATIVE_USER_MEMORY,
                FAKE_CUMULATIVE_TOTAL_MEMORY,
                FAKE_INPUT_ROWS,
                FAKE_INPUT_BYTES,
                FAKE_BLOCKED,
                Optional.empty(),
                FAKE_PROGRESS_PERCENTAGE_2,
                FAKE_QUEUED_DRIVERS,
                FAKE_RUNNING_DRIVERS,
                FAKE_COMPLETED_DRIVERS));
    }

    private ResourceGroupInfo getResourceGroupInfo(
            ResourceGroupId rgID,
            List<ResourceGroupInfo> subGroups,
            List<QueryStateInfo> runningQueries)
    {
        return new ResourceGroupInfo(
                rgID,
                FAKE_RESOURCE_GROUP_STATE,
                FAKE_SCHEDULING_POLICY,
                FAKE_SCHEDULING_WEIGHT,
                DataSize.succinctBytes(FAKE_SOFT_MEMORY_LIMIT_BYTES),
                FAKE_SOFT_CONCURRENCY_LIMIT,
                FAKE_HARD_CONCURRENCY_LIMIT,
                FAKE_MAX_QUEUED_QUERIES,
                DataSize.succinctBytes(FAKE_CACHED_MEMORY_USAGE_BYTES),
                FAKE_QUEUED_QUERIES,
                FAKE_RUNNING_QUERIES,
                FAKE_ELIGIBLE_SUB_GROUPS,
                subGroups,
                runningQueries);
    }
}
