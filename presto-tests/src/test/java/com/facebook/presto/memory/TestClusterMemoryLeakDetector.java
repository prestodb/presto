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
package com.facebook.presto.memory;

import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.execution.QueryStats;
import com.facebook.presto.spi.QueryId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.joda.time.DateTime;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Optional;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.execution.QueryState.FINISHED;
import static com.facebook.presto.execution.QueryState.RUNNING;
import static com.facebook.presto.memory.LocalMemoryManager.GENERAL_POOL;
import static com.facebook.presto.operator.BlockedReason.WAITING_FOR_MEMORY;
import static org.testng.Assert.assertEquals;

@Test
public class TestClusterMemoryLeakDetector
{
    @Test
    public void testLeakDetector()
    {
        QueryId testQuery = new QueryId("test");
        ClusterMemoryLeakDetector leakDetector = new ClusterMemoryLeakDetector();

        leakDetector.checkForMemoryLeaks(() -> ImmutableList.of(), ImmutableMap.of());
        assertEquals(leakDetector.getNumberOfLeakedQueries(), 0);

        // the leak detector should report no leaked queries as the query is still running
        leakDetector.checkForMemoryLeaks(() -> ImmutableList.of(createQueryInfo(testQuery.getId(), RUNNING)), ImmutableMap.of(testQuery, 1L));
        assertEquals(leakDetector.getNumberOfLeakedQueries(), 0);

        // the leak detector should report exactly one leaked query since the query is finished, and its end time is way in the past
        leakDetector.checkForMemoryLeaks(() -> ImmutableList.of(createQueryInfo(testQuery.getId(), FINISHED)), ImmutableMap.of(testQuery, 1L));
        assertEquals(leakDetector.getNumberOfLeakedQueries(), 1);

        // the leak detector should report no leaked queries as the query doesn't have any memory reservation
        leakDetector.checkForMemoryLeaks(() -> ImmutableList.of(createQueryInfo(testQuery.getId(), FINISHED)), ImmutableMap.of(testQuery, 0L));
        assertEquals(leakDetector.getNumberOfLeakedQueries(), 0);

        // the leak detector should report exactly one leaked query since the coordinator doesn't know of any query
        leakDetector.checkForMemoryLeaks(() -> ImmutableList.of(), ImmutableMap.of(testQuery, 1L));
        assertEquals(leakDetector.getNumberOfLeakedQueries(), 1);
    }

    private QueryInfo createQueryInfo(String queryId, QueryState state)
    {
        return new QueryInfo(
                new QueryId(queryId),
                TEST_SESSION.toSessionRepresentation(),
                state,
                GENERAL_POOL,
                true,
                URI.create("1"),
                ImmutableList.of("2", "3"),
                "",
                new QueryStats(
                        DateTime.parse("1991-09-06T05:00-05:30"),
                        DateTime.parse("1991-09-06T05:01-05:30"),
                        DateTime.parse("1991-09-06T05:02-05:30"),
                        DateTime.parse("1991-09-06T06:00-05:30"),
                        Duration.valueOf("8m"),
                        Duration.valueOf("7m"),
                        Duration.valueOf("34m"),
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
                        34,
                        19,
                        20.0,
                        DataSize.valueOf("21GB"),
                        DataSize.valueOf("22GB"),
                        DataSize.valueOf("23GB"),
                        DataSize.valueOf("24GB"),
                        DataSize.valueOf("25GB"),
                        true,
                        Duration.valueOf("23m"),
                        Duration.valueOf("24m"),
                        Duration.valueOf("26m"),
                        true,
                        ImmutableSet.of(WAITING_FOR_MEMORY),
                        DataSize.valueOf("27GB"),
                        28,
                        DataSize.valueOf("29GB"),
                        30,
                        DataSize.valueOf("31GB"),
                        32,
                        DataSize.valueOf("33GB"),
                        ImmutableList.of(),
                        ImmutableList.of()),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
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
