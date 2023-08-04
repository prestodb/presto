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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.spi.QueryId;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import org.joda.time.DateTime;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.execution.QueryState.RUNNING;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static org.joda.time.DateTime.now;
import static org.joda.time.Seconds.secondsBetween;

@ThreadSafe
public class ClusterMemoryLeakDetector
{
    private static final Logger log = Logger.get(ClusterMemoryLeakDetector.class);

    // It may take some time to remove a query's memory reservations from the worker nodes, that's why
    // we check to see whether some time has passed after the query finishes to claim that it is leaked.
    private static final int DEFAULT_LEAK_CLAIM_DELTA_SEC = 60;

    @GuardedBy("this")
    private Set<QueryId> leakedQueries;

    @GuardedBy("this")
    private long leakedBytes;

    /**
     * @param queryIdToInfo All queries that the coordinator knows about, along with their optional query info.
     * @param queryMemoryReservations The memory reservations of queries in the GENERAL cluster memory pool.
     */
    void checkForMemoryLeaks(Map<QueryId, Optional<BasicQueryInfo>> queryIdToInfo, Map<QueryId, Long> queryMemoryReservations)
    {
        Map<QueryId, Long> leakedQueryReservations = queryMemoryReservations.entrySet()
                .stream()
                .filter(entry -> entry.getValue() > 0)
                .filter(entry -> isLeaked(queryIdToInfo, entry.getKey()))
                .collect(toImmutableMap(Entry::getKey, Entry::getValue));

        long leakedBytesThisTime = leakedQueryReservations.values().stream().reduce(0L, Long::sum);
        if (!leakedQueryReservations.isEmpty()) {
            log.warn("Memory leak of %s detected. The following queries are already finished, " +
                    "but they have memory reservations on some worker node(s): %s",
                    DataSize.succinctBytes(leakedBytes), leakedQueryReservations);
        }

        synchronized (this) {
            leakedQueries = ImmutableSet.copyOf(leakedQueryReservations.keySet());
            leakedBytes = leakedBytesThisTime;
        }
    }

    private static boolean isLeaked(Map<QueryId, Optional<BasicQueryInfo>> queryIdToInfo, QueryId queryId)
    {
        Optional<BasicQueryInfo> queryInfo = queryIdToInfo.get(queryId);

        // if the query is not even found then it is definitely leaked
        if (queryInfo == null) {
            return true;
        }

        Optional<DateTime> queryEndTime = queryInfo.flatMap(qi -> Optional.ofNullable(qi.getState() == RUNNING ? null : qi.getQueryStats().getEndTime()));

        return queryEndTime.map(ts -> secondsBetween(ts, now()).getSeconds() >= DEFAULT_LEAK_CLAIM_DELTA_SEC).orElse(false);
    }

    synchronized boolean wasQueryPossiblyLeaked(QueryId queryId)
    {
        return leakedQueries.contains(queryId);
    }

    synchronized int getNumberOfLeakedQueries()
    {
        return leakedQueries.size();
    }

    synchronized long getLeakedBytes()
    {
        return leakedBytes;
    }
}
