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
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.spi.QueryId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import org.joda.time.DateTime;
import org.weakref.jmx.Managed;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static com.facebook.presto.execution.QueryState.RUNNING;
import static com.facebook.presto.memory.LocalMemoryManager.GENERAL_POOL;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.joda.time.Seconds.secondsBetween;

public class ClusterMemoryLeakDetector
{
    private static final Logger log = Logger.get(ClusterMemoryLeakDetector.class);

    // It may take some time to remove a query's memory reservations from the worker nodes,
    // that's why we check to see whether some time has passed after the query end time to claim that
    // a query is leaked.
    private static final long DEFAULT_LEAK_CLAIM_DELTA_SECONDS = 60;

    private final ClusterMemoryManager clusterMemoryManager;
    private final QueryManager queryManager;
    private final AtomicInteger numberOfLeakedQueries = new AtomicInteger();
    private final ScheduledExecutorService leakDetectorScheduler = newSingleThreadScheduledExecutor(daemonThreadsNamed("cluster-leak-detector"));

    @Inject
    public ClusterMemoryLeakDetector(ClusterMemoryManager clusterMemoryManager, QueryManager queryManager)
    {
        this.clusterMemoryManager = requireNonNull(clusterMemoryManager, "clusterMemoryManager is null");
        this.queryManager = requireNonNull(queryManager, "queryManager is null");
    }

    @PostConstruct
    public void start()
    {
        leakDetectorScheduler.scheduleWithFixedDelay(this::run, 0, 60, SECONDS);
    }

    private void run()
    {
        try {
            checkForMemoryLeaks(DEFAULT_LEAK_CLAIM_DELTA_SECONDS);
        }
        catch (Throwable e) {
            log.error(e, "Error checking for memory leaks");
        }
    }

    @VisibleForTesting
    void checkForMemoryLeaks(long leakClaimDeltaSeconds)
    {
        // A query is considered leaked if it has a memory reservation on some worker and it is currently not running.
        ClusterMemoryPool generalPool = clusterMemoryManager.getPools().get(GENERAL_POOL);
        Map<QueryId, Long> queryMemoryReservations = generalPool.getQueryMemoryReservations();
        ImmutableMap.Builder<QueryId, Long> leakedQueriesBuilder = ImmutableMap.builder();
        for (Entry<QueryId, Long> reservation : queryMemoryReservations.entrySet()) {
            if (isQueryLeaked(reservation.getKey(), leakClaimDeltaSeconds)) {
                leakedQueriesBuilder.put(reservation.getKey(), reservation.getValue());
            }
        }
        Map<QueryId, Long> leakedQueries = leakedQueriesBuilder.build();
        numberOfLeakedQueries.set(leakedQueries.size());
        if (!leakedQueries.isEmpty()) {
            log.warn("Memory leak detected. The following queries have already finished, but they still have memory reservations on some worker node(s): %s", leakedQueries);
        }
    }

    private boolean isQueryLeaked(QueryId queryId, long leakClaimDeltaSeconds)
    {
        List<QueryInfo> allQueries = queryManager.getAllQueryInfo();
        boolean queryExists = false;
        QueryState queryState = null;
        DateTime queryEndTime = null;
        for (QueryInfo queryInfo : allQueries) {
            if (queryInfo.getQueryId().equals(queryId)) {
                queryExists = true;
                queryState = queryInfo.getState();
                queryEndTime = queryInfo.getQueryStats().getEndTime();
            }
        }

        // If the query doesn't exist anymore on the coordinator we just claim that it's leaked.
        if (!queryExists) {
            return true;
        }

        // If the query is still running it's not leaked.
        if (queryState.equals(RUNNING)) {
            return false;
        }

        // If the query exists and it's not running anymore we check the query end time.
        if (secondsBetween(queryEndTime, DateTime.now()).getSeconds() >= leakClaimDeltaSeconds) {
            return true;
        }

        return false;
    }

    /**
     * When a query is completed it may take some time for the query to be removed from all the worker
     * memory reservations, so what really matters is the steady state value of this counter.
     */
    @Managed
    public int getNumberOfLeakedQueries()
    {
        return numberOfLeakedQueries.get();
    }
}
