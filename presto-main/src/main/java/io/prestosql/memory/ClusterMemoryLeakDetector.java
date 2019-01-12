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
package io.prestosql.memory;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import io.airlift.log.Logger;
import io.prestosql.server.BasicQueryInfo;
import io.prestosql.spi.QueryId;
import org.joda.time.DateTime;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Supplier;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.prestosql.execution.QueryState.RUNNING;
import static java.util.Objects.requireNonNull;
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

    /**
     * @param queryInfoSupplier All queries that the coordinator knows about.
     * @param queryMemoryReservations The memory reservations of queries in the GENERAL cluster memory pool.
     */
    void checkForMemoryLeaks(Supplier<List<BasicQueryInfo>> queryInfoSupplier, Map<QueryId, Long> queryMemoryReservations)
    {
        requireNonNull(queryInfoSupplier);
        requireNonNull(queryMemoryReservations);

        Map<QueryId, BasicQueryInfo> queryIdToInfo = Maps.uniqueIndex(queryInfoSupplier.get(), BasicQueryInfo::getQueryId);

        Map<QueryId, Long> leakedQueryReservations = queryMemoryReservations.entrySet()
                .stream()
                .filter(entry -> entry.getValue() > 0)
                .filter(entry -> isLeaked(queryIdToInfo, entry.getKey()))
                .collect(toImmutableMap(Entry::getKey, Entry::getValue));

        if (!leakedQueryReservations.isEmpty()) {
            log.debug("Memory leak detected. The following queries are already finished, " +
                    "but they have memory reservations on some worker node(s): %s", leakedQueryReservations);
        }

        synchronized (this) {
            leakedQueries = ImmutableSet.copyOf(leakedQueryReservations.keySet());
        }
    }

    private static boolean isLeaked(Map<QueryId, BasicQueryInfo> queryIdToInfo, QueryId queryId)
    {
        BasicQueryInfo queryInfo = queryIdToInfo.get(queryId);

        if (queryInfo == null) {
            return true;
        }

        DateTime queryEndTime = queryInfo.getQueryStats().getEndTime();

        if (queryInfo.getState() == RUNNING || queryEndTime == null) {
            return false;
        }

        return secondsBetween(queryEndTime, now()).getSeconds() >= DEFAULT_LEAK_CLAIM_DELTA_SEC;
    }

    synchronized boolean wasQueryPossiblyLeaked(QueryId queryId)
    {
        return leakedQueries.contains(queryId);
    }

    synchronized int getNumberOfLeakedQueries()
    {
        return leakedQueries.size();
    }
}
