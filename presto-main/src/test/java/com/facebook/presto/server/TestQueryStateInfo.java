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

import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.execution.QueryStats;
import com.facebook.presto.execution.resourceGroups.InternalResourceGroup;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.memory.MemoryPoolId;
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

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.execution.QueryState.QUEUED;
import static com.facebook.presto.operator.BlockedReason.WAITING_FOR_MEMORY;
import static com.facebook.presto.server.QueryStateInfo.createQueuedQueryStateInfo;
import static com.facebook.presto.spi.resourceGroups.SchedulingPolicy.WEIGHTED;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static org.testng.Assert.assertEquals;

public class TestQueryStateInfo
{
    @Test
    public void testQueryStateInfo()
    {
        InternalResourceGroup.RootInternalResourceGroup root = new InternalResourceGroup.RootInternalResourceGroup("root", (group, export) -> {}, directExecutor(), ignored -> Optional.empty(), rg -> false);
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
        QueryStateInfo query = createQueuedQueryStateInfo(
                new BasicQueryInfo(createQueryInfo("query_root_a_x", QUEUED, "SELECT 1")),
                Optional.of(rootAX.getId()),
                Optional.of(ImmutableList.of(rootAX.getInfo(), rootA.getInfo(), root.getInfo())));

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
                Optional.empty(),
                new QueryStats(
                        DateTime.parse("1991-09-06T05:00-05:30"),
                        DateTime.parse("1991-09-06T05:01-05:30"),
                        DateTime.parse("1991-09-06T05:02-05:30"),
                        DateTime.parse("1991-09-06T06:00-05:30"),
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
                null,
                ImmutableList.of(),
                ImmutableSet.of(),
                Optional.empty(),
                false,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of(),
                ImmutableSet.of());
    }
}
