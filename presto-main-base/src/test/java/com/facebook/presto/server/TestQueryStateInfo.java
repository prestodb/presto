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

import com.facebook.airlift.units.DataSize;
import com.facebook.airlift.units.Duration;
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.cost.StatsAndCosts;
import com.facebook.presto.execution.ClusterOverloadConfig;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.execution.QueryStats;
import com.facebook.presto.execution.resourceGroups.InternalResourceGroup;
import com.facebook.presto.execution.scheduler.clusterOverload.ClusterOverloadPolicy;
import com.facebook.presto.execution.scheduler.clusterOverload.ClusterResourceChecker;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.spi.PrestoWarning;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.WarningCode;
import com.facebook.presto.spi.memory.MemoryPoolId;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.joda.time.DateTime;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.facebook.airlift.units.DataSize.Unit.MEGABYTE;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.execution.QueryState.FINISHED;
import static com.facebook.presto.execution.QueryState.QUEUED;
import static com.facebook.presto.execution.QueryState.RUNNING;
import static com.facebook.presto.operator.BlockedReason.WAITING_FOR_MEMORY;
import static com.facebook.presto.server.QueryStateInfo.createQueryStateInfo;
import static com.facebook.presto.spi.StandardErrorCode.EXCEEDED_GLOBAL_MEMORY_LIMIT;
import static com.facebook.presto.spi.resourceGroups.SchedulingPolicy.WEIGHTED;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestQueryStateInfo
{
    @Test
    public void testQueryStateInfo()
    {
        InternalResourceGroup.RootInternalResourceGroup root = new InternalResourceGroup.RootInternalResourceGroup("root", (group, export) -> {}, directExecutor(), ignored -> Optional.empty(), rg -> false, new InMemoryNodeManager(), createClusterResourceChecker());
        root.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        root.setMaxQueuedQueries(40);
        root.setHardConcurrencyLimit(0);
        root.setSchedulingPolicy(WEIGHTED);

        InternalResourceGroup rootA = root.getOrCreateSubGroup("a", true);
        rootA.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        rootA.setMaxQueuedQueries(20);
        rootA.setHardConcurrencyLimit(0);

        InternalResourceGroup rootAX = rootA.getOrCreateSubGroup("x", true);
        rootAX.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        rootAX.setMaxQueuedQueries(10);
        rootAX.setHardConcurrencyLimit(0);

        // Verify QueryStateInfo for query queued on resource group root.a.y
        QueryStateInfo query = createQueryStateInfo(
                new BasicQueryInfo(createQueryInfo("query_root_a_x", rootAX.getId(), QUEUED, "SELECT 1")),
                Optional.of(ImmutableList.of(rootAX.getInfo(), rootA.getInfo(), root.getInfo())),
                false,
                OptionalInt.empty());

        assertEquals(query.getQuery(), "SELECT 1");
        assertEquals(query.getQueryId().toString(), "query_root_a_x");
        assertEquals(query.getQueryState(), QUEUED);
        assertEquals(query.getProgress(), Optional.empty());

        List<ResourceGroupInfo> chainInfo = query.getPathToRoot().get();

        assertEquals(chainInfo.size(), 3);

        ResourceGroupInfo rootAInfo = chainInfo.get(1);
        ResourceGroupInfo expectedRootAInfo = rootA.getInfo();
        assertEquals(rootAInfo.getId(), expectedRootAInfo.getId());
        assertEquals(rootAInfo.getState(), expectedRootAInfo.getState());
        assertEquals(rootAInfo.getNumRunningQueries(), expectedRootAInfo.getNumRunningQueries());
        assertEquals(rootAInfo.getNumQueuedQueries(), expectedRootAInfo.getNumQueuedQueries());

        ResourceGroupInfo actualRootInfo = chainInfo.get(2);
        ResourceGroupInfo expectedRootInfo = root.getInfo();
        assertEquals(actualRootInfo.getId(), expectedRootInfo.getId());
        assertEquals(actualRootInfo.getState(), expectedRootInfo.getState());
        assertEquals(actualRootInfo.getNumRunningQueries(), expectedRootInfo.getNumRunningQueries());
        assertEquals(actualRootInfo.getNumQueuedQueries(), expectedRootInfo.getNumQueuedQueries());
    }

    @Test
    public void testQueryTextTruncation()
    {
        QueryInfo queryInfo = createQueryInfo("query_id_test", RUNNING, "SELECT * FROM foo");

        QueryStateInfo queryStateInfoNoLimit = createQueryStateInfo(new BasicQueryInfo(queryInfo), Optional.empty(), false, OptionalInt.empty());

        assertFalse(queryStateInfoNoLimit.isQueryTruncated());
        assertEquals(queryStateInfoNoLimit.getQuery(), queryInfo.getQuery());

        QueryStateInfo queryStateInfoLimitLower = createQueryStateInfo(new BasicQueryInfo(queryInfo), Optional.empty(), false, OptionalInt.of(5));

        assertTrue(queryStateInfoLimitLower.isQueryTruncated());
        assertEquals(queryStateInfoLimitLower.getQuery(), queryInfo.getQuery().substring(0, 5));
        assertEquals(queryStateInfoLimitLower.getQuery().length(), 5);

        QueryStateInfo queryStateInfoLimitHigher = createQueryStateInfo(new BasicQueryInfo(queryInfo), Optional.empty(), false, OptionalInt.of(500));

        assertFalse(queryStateInfoLimitHigher.isQueryTruncated());
        assertEquals(queryStateInfoLimitHigher.getQuery(), queryInfo.getQuery());
    }

    @Test
    public void testIncludeQueryProgress()
    {
        QueryInfo queuedQueryInfo = createQueryInfo("query_id_test", QUEUED, "SELECT * FROM foo");

        QueryStateInfo queuedStateInfoWithoutProgress = createQueryStateInfo(new BasicQueryInfo(queuedQueryInfo), Optional.empty(), false, OptionalInt.empty());
        assertFalse(queuedStateInfoWithoutProgress.getProgress().isPresent());

        QueryStateInfo queuedStateInfoWithProgress = createQueryStateInfo(new BasicQueryInfo(queuedQueryInfo), Optional.empty(), true, OptionalInt.empty());
        assertTrue(queuedStateInfoWithProgress.getProgress().isPresent());

        QueryInfo runningQueryInfo = createQueryInfo("query_id_test", RUNNING, "SELECT * FROM foo");

        QueryStateInfo runningStateInfoWithoutProgress = createQueryStateInfo(new BasicQueryInfo(runningQueryInfo), Optional.empty(), false, OptionalInt.empty());
        assertTrue(runningStateInfoWithoutProgress.getProgress().isPresent());

        QueryStateInfo runningStateInfoWithProgress = createQueryStateInfo(new BasicQueryInfo(runningQueryInfo), Optional.empty(), true, OptionalInt.empty());
        assertTrue(runningStateInfoWithProgress.getProgress().isPresent());

        QueryInfo finishedQueryInfo = createQueryInfo("query_id_test", FINISHED, "SELECT * FROM foo");

        QueryStateInfo finishedStateInfoWithoutProgress = createQueryStateInfo(new BasicQueryInfo(finishedQueryInfo), Optional.empty(), false, OptionalInt.empty());
        assertFalse(finishedStateInfoWithoutProgress.getProgress().isPresent());

        QueryStateInfo finishedStateInfoWithProgress = createQueryStateInfo(new BasicQueryInfo(finishedQueryInfo), Optional.empty(), true, OptionalInt.empty());
        assertTrue(finishedStateInfoWithProgress.getProgress().isPresent());
    }

    @Test
    public void testQueryStateInfoCreation()
    {
        QueryInfo queryInfo = createQueryInfo("query_id_test", RUNNING, "SELECT * FROM foo");
        QueryStateInfo queryStateInfo = createQueryStateInfo(new BasicQueryInfo(queryInfo));

        assertEquals(queryStateInfo.getQueryId(), queryInfo.getQueryId());
        assertEquals(queryStateInfo.getQueryState(), queryInfo.getState());
        assertEquals(queryStateInfo.getResourceGroupId(), queryInfo.getResourceGroupId());
        assertEquals(queryStateInfo.getQuery(), queryInfo.getQuery());
        assertEquals(queryStateInfo.getCreateTime().getMillis(), queryInfo.getQueryStats().getCreateTimeInMillis());
        assertEquals(queryStateInfo.getUser(), queryInfo.getSession().getUser());
        assertEquals(queryStateInfo.isAuthenticated(), queryInfo.getSession().getPrincipal().isPresent());
        assertEquals(queryStateInfo.getSource(), queryInfo.getSession().getSource());
        assertEquals(queryStateInfo.getClientInfo(), queryInfo.getSession().getClientInfo());
        assertEquals(queryStateInfo.getCatalog(), queryInfo.getSession().getCatalog());
        assertEquals(queryStateInfo.getSchema(), queryInfo.getSession().getSchema());
        assertEquals(queryStateInfo.getWarningCodes(), ImmutableList.of("WARNING_123"));
        assertTrue(queryStateInfo.getProgress().isPresent());
        assertEquals(queryStateInfo.getErrorCode(), Optional.ofNullable(queryInfo.getErrorCode()));

        QueryProgressStats progress = queryStateInfo.getProgress().get();
        QueryStats stats = queryInfo.getQueryStats();

        assertEquals(progress.getElapsedTimeMillis(), stats.getElapsedTime().toMillis());
        assertEquals(progress.getQueuedTimeMillis(), stats.getQueuedTime().toMillis());
        assertEquals(progress.getExecutionTimeMillis(), stats.getExecutionTime().toMillis());
        assertEquals(progress.getCpuTimeMillis(), stats.getTotalCpuTime().toMillis());
        assertEquals(progress.getScheduledTimeMillis(), stats.getTotalScheduledTime().toMillis());
        assertEquals(progress.getCurrentMemoryBytes(), stats.getUserMemoryReservation().toBytes());
        assertEquals(progress.getPeakMemoryBytes(), stats.getPeakUserMemoryReservation().toBytes());
        assertEquals(progress.getPeakTotalMemoryBytes(), stats.getPeakTotalMemoryReservation().toBytes());
        assertEquals(progress.getCumulativeUserMemory(), stats.getCumulativeUserMemory());
        assertEquals(progress.getCumulativeTotalMemory(), stats.getCumulativeTotalMemory());
        assertEquals(progress.getInputRows(), stats.getRawInputPositions());
        assertEquals(progress.getInputBytes(), stats.getRawInputDataSize().toBytes());
        assertEquals(progress.isBlocked(), stats.isFullyBlocked());
        assertEquals(progress.getBlockedReasons(), Optional.of(stats.getBlockedReasons()));
        assertEquals(progress.getProgressPercentage(), stats.getProgressPercentage());
        assertEquals(progress.getQueuedDrivers(), stats.getQueuedDrivers());
        assertEquals(progress.getRunningDrivers(), stats.getRunningDrivers());
        assertEquals(progress.getCompletedDrivers(), stats.getCompletedDrivers());
    }

    private QueryInfo createQueryInfo(String queryId, QueryState state, String query)
    {
        return createQueryInfo(queryId, new ResourceGroupId("global"), state, query);
    }

    private QueryInfo createQueryInfo(String queryId, ResourceGroupId resourceGroupId, QueryState state, String query)
    {
        return new QueryInfo(
                new QueryId(queryId),
                TEST_SESSION.toSessionRepresentation(),
                state,
                new MemoryPoolId("reserved"),
                true,
                URI.create("1"),
                ImmutableList.of("2", "3"),
                query,
                Optional.empty(),
                Optional.empty(),
                new QueryStats(
                        new DateTime("1991-09-06T05:00").getMillis(),
                        new DateTime("1991-09-06T05:01").getMillis(),
                        new DateTime("1991-09-06T05:02").getMillis(),
                        new DateTime("1991-09-06T06:00").getMillis(),
                        Duration.valueOf("8m"),
                        Duration.valueOf("5m"),
                        Duration.valueOf("7m"),
                        Duration.valueOf("34m"),
                        Duration.valueOf("5m"),
                        Duration.valueOf("6m"),
                        Duration.valueOf("35m"),
                        Duration.valueOf("44m"),
                        Duration.valueOf("9m"),
                        Duration.valueOf("10m"),
                        Duration.valueOf("11m"),
                        13,
                        14,
                        15,
                        16,
                        100,
                        17,
                        18,
                        34,
                        19,
                        100,
                        17,
                        18,
                        19,
                        100,
                        17,
                        18,
                        19,
                        20.0,
                        43.0,
                        DataSize.valueOf("21GB"),
                        DataSize.valueOf("22GB"),
                        DataSize.valueOf("23GB"),
                        DataSize.valueOf("24GB"),
                        DataSize.valueOf("25GB"),
                        DataSize.valueOf("26GB"),
                        DataSize.valueOf("42GB"),
                        true,
                        Duration.valueOf("23m"),
                        Duration.valueOf("24m"),
                        Duration.valueOf("0m"),
                        Duration.valueOf("26m"),
                        true,
                        ImmutableSet.of(WAITING_FOR_MEMORY),
                        DataSize.valueOf("123MB"),
                        DataSize.valueOf("27GB"),
                        28,
                        DataSize.valueOf("29GB"),
                        30,
                        DataSize.valueOf("32GB"),
                        40,
                        DataSize.valueOf("31GB"),
                        32,
                        33,
                        DataSize.valueOf("34GB"),
                        DataSize.valueOf("35GB"),
                        DataSize.valueOf("36GB"),
                        ImmutableList.of(),
                        ImmutableList.of(),
                        new RuntimeStats()),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of(),
                ImmutableSet.of(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableSet.of(),
                Optional.empty(),
                false,
                "33",
                Optional.empty(),
                null,
                EXCEEDED_GLOBAL_MEMORY_LIMIT.toErrorCode(),
                ImmutableList.of(
                        new PrestoWarning(
                                new WarningCode(123, "WARNING_123"),
                                "warning message")),
                ImmutableSet.of(),
                Optional.empty(),
                false,
                Optional.of(resourceGroupId),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of(),
                ImmutableSet.of(),
                StatsAndCosts.empty(),
                ImmutableList.of(),
                ImmutableList.of(),
                ImmutableSet.of(),
                ImmutableSet.of(),
                ImmutableSet.of(),
                ImmutableList.of(),
                ImmutableMap.of(),
                Optional.empty());
    }

    private ClusterResourceChecker createClusterResourceChecker()
    {
        // Create a mock cluster overload policy that never reports overload
        ClusterOverloadPolicy mockPolicy = new ClusterOverloadPolicy()
        {
            @Override
            public boolean isClusterOverloaded(InternalNodeManager nodeManager)
            {
                return false; // Never overloaded for tests
            }

            @Override
            public String getName()
            {
                return "test-policy";
            }
        };

        // Create a config with throttling disabled for tests
        ClusterOverloadConfig config = new ClusterOverloadConfig()
                .setClusterOverloadThrottlingEnabled(false);

        return new ClusterResourceChecker(mockPolicy, config, new InMemoryNodeManager());
    }
}
