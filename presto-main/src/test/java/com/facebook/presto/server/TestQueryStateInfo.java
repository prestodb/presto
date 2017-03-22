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

import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.execution.QueryStats;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.memory.MemoryPoolId;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.spi.resourceGroups.ResourceGroupInfo;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.joda.time.DateTime;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.execution.QueryState.QUEUED;
import static com.facebook.presto.execution.QueryState.RUNNING;
import static com.facebook.presto.operator.BlockedReason.WAITING_FOR_MEMORY;
import static com.facebook.presto.server.QueryStateInfo.createQueryStateInfo;
import static com.facebook.presto.spi.resourceGroups.ResourceGroupState.CAN_QUEUE;
import static com.facebook.presto.spi.resourceGroups.ResourceGroupState.CAN_RUN;
import static io.airlift.units.DataSize.Unit.BYTE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestQueryStateInfo
{
    @Test
    public void testQueryStateInfo()
    {
        ResourceGroupId groupRoot = new ResourceGroupId("root");
        ResourceGroupId groupRootA = new ResourceGroupId(groupRoot, "a");
        ResourceGroupId groupRootAX = new ResourceGroupId(groupRootA, "x");
        ResourceGroupId groupRootAY = new ResourceGroupId(groupRootA, "y");
        ResourceGroupId groupRootB = new ResourceGroupId(groupRoot, "b");

        ResourceGroupInfo rootAXInfo = new ResourceGroupInfo(
                groupRootAX,
                new DataSize(6000, BYTE),
                1,
                10,
                CAN_QUEUE,
                0,
                new DataSize(4000, BYTE),
                1,
                1,
                ImmutableList.of());

        ResourceGroupInfo rootAYInfo = new ResourceGroupInfo(
                groupRootAY,
                new DataSize(8000, BYTE),
                1,
                10,
                CAN_RUN,
                0,
                new DataSize(0, BYTE),
                0,
                1,
                ImmutableList.of());

        ResourceGroupInfo rootAInfo = new ResourceGroupInfo(
                groupRootA,
                new DataSize(8000, BYTE),
                1,
                10,
                CAN_QUEUE,
                1,
                new DataSize(4000, BYTE),
                1,
                2,
                ImmutableList.of(rootAXInfo, rootAYInfo));

        ResourceGroupInfo rootBInfo = new ResourceGroupInfo(
                groupRootB,
                new DataSize(8000, BYTE),
                1,
                10,
                CAN_QUEUE,
                0,
                new DataSize(4000, BYTE),
                1,
                1,
                ImmutableList.of());

        ResourceGroupInfo rootInfo = new ResourceGroupInfo(
                new ResourceGroupId("root"),
                new DataSize(10000, BYTE),
                2,
                20,
                CAN_QUEUE,
                0,
                new DataSize(6000, BYTE),
                2,
                3,
                ImmutableList.of(rootAInfo, rootBInfo));

        // Verify QueryStateInfo for query queued on resource group root.a.y
        QueryStateInfo infoForQueryQueuedOnRootAY = createQueryStateInfo(
                createQueryInfo("query_root_a_y", QUEUED, "SELECT 1"),
                Optional.of(groupRootAY),
                Optional.of(rootInfo));
        assertEquals(infoForQueryQueuedOnRootAY.getQuery(), "SELECT 1");
        assertEquals(infoForQueryQueuedOnRootAY.getQueryId().toString(), "query_root_a_y");
        assertEquals(infoForQueryQueuedOnRootAY.getQueryState(), QUEUED);
        assertEquals(infoForQueryQueuedOnRootAY.getProgress(), Optional.empty());

        Optional<List<ResourceGroupInfo>> optionalRootAYResourceGroupChainInfo = infoForQueryQueuedOnRootAY.getResourceGroupChain();
        assertTrue(optionalRootAYResourceGroupChainInfo.isPresent());
        List<ResourceGroupInfo> rootAYResourceGroupChainInfo = optionalRootAYResourceGroupChainInfo.isPresent() ? optionalRootAYResourceGroupChainInfo.get() : ImmutableList.of();
        assertEquals(rootAYResourceGroupChainInfo.size(), 3);
        ResourceGroupInfo actualRootAYInfo = rootAYResourceGroupChainInfo.get(0);
        assertEquals(actualRootAYInfo.getId().toString(), groupRootAY.toString());
        assertEquals(actualRootAYInfo.getState(), rootAYInfo.getState());
        assertEquals(actualRootAYInfo.getNumAggregatedRunningQueries(), rootAYInfo.getNumAggregatedRunningQueries());
        assertEquals(actualRootAYInfo.getNumAggregatedQueuedQueries(), rootAYInfo.getNumAggregatedQueuedQueries());
        ResourceGroupInfo actualRootAInfo = rootAYResourceGroupChainInfo.get(1);
        assertEquals(actualRootAInfo.getId().toString(), groupRootA.toString());
        assertEquals(actualRootAInfo.getState(), rootAInfo.getState());
        assertEquals(actualRootAInfo.getNumAggregatedRunningQueries(), rootAInfo.getNumAggregatedRunningQueries());
        assertEquals(actualRootAInfo.getNumAggregatedQueuedQueries(), rootAInfo.getNumAggregatedQueuedQueries());

        ResourceGroupInfo actualRootInfo = rootAYResourceGroupChainInfo.get(2);
        assertEquals(actualRootInfo.getId().toString(), groupRoot.toString());
        assertEquals(actualRootInfo.getState(), rootInfo.getState());
        assertEquals(actualRootInfo.getNumAggregatedRunningQueries(), rootInfo.getNumAggregatedRunningQueries());
        assertEquals(actualRootInfo.getNumAggregatedQueuedQueries(), rootInfo.getNumAggregatedQueuedQueries());

        // Verify QueryStateInfo for query queued on resource group root.b
        QueryStateInfo infoForQueryQueuedOnRootB = createQueryStateInfo(
                createQueryInfo("query_root_b", QUEUED, "SELECT count(*) FROM t"),
                Optional.of(groupRootB),
                Optional.of(rootInfo));
        assertEquals(infoForQueryQueuedOnRootB.getQuery(), "SELECT count(*) FROM t");
        assertEquals(infoForQueryQueuedOnRootB.getQueryId().toString(), "query_root_b");
        assertEquals(infoForQueryQueuedOnRootB.getQueryState(), QUEUED);
        assertEquals(infoForQueryQueuedOnRootB.getProgress(), Optional.empty());

        Optional<List<ResourceGroupInfo>> optionalRootBResourceGroupChainInfo = infoForQueryQueuedOnRootB.getResourceGroupChain();
        assertTrue(optionalRootBResourceGroupChainInfo.isPresent());
        List<ResourceGroupInfo> rootBResourceGroupChainInfo = optionalRootBResourceGroupChainInfo.isPresent() ? optionalRootBResourceGroupChainInfo.get() : ImmutableList.of();
        assertEquals(rootBResourceGroupChainInfo.size(), 2);

        ResourceGroupInfo actualRootBInfo = rootBResourceGroupChainInfo.get(0);
        assertEquals(actualRootBInfo.getId().toString(), groupRootB.toString());
        assertEquals(actualRootBInfo.getState(), rootBInfo.getState());
        assertEquals(actualRootBInfo.getNumAggregatedRunningQueries(), rootBInfo.getNumAggregatedRunningQueries());
        assertEquals(actualRootBInfo.getNumAggregatedQueuedQueries(), rootBInfo.getNumAggregatedQueuedQueries());

        actualRootInfo = rootBResourceGroupChainInfo.get(1);
        assertEquals(actualRootInfo.getId().toString(), groupRoot.toString());
        assertEquals(actualRootInfo.getState(), rootInfo.getState());
        assertEquals(actualRootInfo.getNumAggregatedRunningQueries(), rootInfo.getNumAggregatedRunningQueries());
        assertEquals(actualRootInfo.getNumAggregatedQueuedQueries(), rootInfo.getNumAggregatedQueuedQueries());

        // Verify QueryStateInfo for query running on resource group root.a.x
        QueryStateInfo infoForQueryRunningOnRootAX = createQueryStateInfo(
                createQueryInfo("query_root_a_x", RUNNING, "SELECT sum(a) FROM t"),
                Optional.of(groupRootAX),
                Optional.of(rootInfo));

        assertEquals(infoForQueryRunningOnRootAX.getQuery(), "SELECT sum(a) FROM t");
        assertEquals(infoForQueryRunningOnRootAX.getQueryId().toString(), "query_root_a_x");
        assertEquals(infoForQueryRunningOnRootAX.getQueryState(), RUNNING);
        assertEquals(infoForQueryRunningOnRootAX.getResourceGroupChain(), Optional.empty());

        QueryProgressStats progress = infoForQueryRunningOnRootAX.getProgress().get();
        assertTrue(progress.isBlocked());
        assertEquals(progress.getProgressPercentage(), OptionalDouble.of(19));
        assertEquals(progress.getExecutionStartTime(), DateTime.parse("1991-09-06T05:01-05:30"));
        assertEquals(progress.getElapsedTimeMillis(), Duration.valueOf("8m").toMillis());
        assertEquals(progress.getQueuedTimeMillis(), Duration.valueOf("7m").toMillis());
        assertEquals(progress.getCpuTimeMillis(), Duration.valueOf("24m").toMillis());
        assertEquals(progress.getScheduledTimeMillis(), Duration.valueOf("23m").toMillis());
        assertEquals(progress.getBlockedTimeMillis(), Duration.valueOf("26m").toMillis());
        assertEquals(progress.getPeakMemoryBytes(), DataSize.valueOf("22GB").toBytes());
        assertEquals(progress.getInputRows(), 28);
        assertEquals(progress.getInputBytes(), DataSize.valueOf("27GB").toBytes());
    }

    private QueryInfo createQueryInfo(String queryId, QueryState state, String query)
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
                new QueryStats(
                        DateTime.parse("1991-09-06T05:00-05:30"),
                        DateTime.parse("1991-09-06T05:01-05:30"),
                        DateTime.parse("1991-09-06T05:02-05:30"),
                        DateTime.parse("1991-09-06T06:00-05:30"),
                        Duration.valueOf("8m"),
                        Duration.valueOf("7m"),
                        Duration.valueOf("9m"),
                        Duration.valueOf("10m"),
                        Duration.valueOf("11m"),
                        Duration.valueOf("12m"),
                        13,
                        14,
                        15,
                        100,
                        17,
                        18,
                        19,
                        20.0,
                        DataSize.valueOf("21GB"),
                        DataSize.valueOf("22GB"),
                        true,
                        Duration.valueOf("23m"),
                        Duration.valueOf("24m"),
                        Duration.valueOf("25m"),
                        Duration.valueOf("26m"),
                        true,
                        ImmutableSet.of(WAITING_FOR_MEMORY),
                        DataSize.valueOf("27GB"),
                        28,
                        DataSize.valueOf("29GB"),
                        30,
                        DataSize.valueOf("31GB"),
                        32,
                        ImmutableList.of()),
                ImmutableMap.of(),
                ImmutableSet.of(),
                ImmutableMap.of(),
                ImmutableSet.of(),
                Optional.empty(),
                false,
                "33",
                Optional.empty(),
                null,
                null,
                ImmutableSet.of(),
                Optional.empty(),
                false,
                Optional.empty());
    }
}
