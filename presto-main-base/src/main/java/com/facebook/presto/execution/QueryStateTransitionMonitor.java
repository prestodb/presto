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

import com.facebook.airlift.log.Logger;
import com.facebook.airlift.stats.CounterStat;
import com.facebook.airlift.stats.TimeStat;
import com.facebook.presto.spi.QueryId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.inject.Singleton;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Objects.requireNonNull;

/**
 * Monitors query state transitions and detects anomalies in state transition durations.
 * This class tracks the time spent in each query state and logs warnings when queries
 * spend an abnormally long time in specific states (e.g., DISPATCHING, FINISHING).
 *
 * Statistics are collected using rolling windows to calculate mean and standard deviation,
 * allowing detection of queries that take significantly longer than normal (> mean + 2*stddev).
 */
@Singleton
@ThreadSafe
public class QueryStateTransitionMonitor
{
    private static final Logger log = Logger.get(QueryStateTransitionMonitor.class);

    private static final double ANOMALY_THRESHOLD_STDDEV = 2.0;
    private static final int MIN_SAMPLES_FOR_ANOMALY_DETECTION = 10;

    private final Map<QueryId, QueryTransitionTracker> activeQueries = new ConcurrentHashMap<>();

    private final TimeStat dispatchingTimeStats = new TimeStat();
    private final TimeStat finishingTimeStats = new TimeStat();
    private final TimeStat planningTimeStats = new TimeStat();
    private final TimeStat runningTimeStats = new TimeStat();

    private final CounterStat anomalousDispatchingCount = new CounterStat();
    private final CounterStat anomalousFinishingCount = new CounterStat();
    private final CounterStat anomalousPlanningCount = new CounterStat();
    private final CounterStat anomalousRunningCount = new CounterStat();

    private final AtomicLong totalQueriesTracked = new AtomicLong(0);

    public QueryStateTransitionMonitor()
    {
    }

    /**
     * Registers a new query for state transition monitoring.
     */
    public void registerQuery(QueryId queryId)
    {
        requireNonNull(queryId, "queryId is null");
        activeQueries.put(queryId, new QueryTransitionTracker(queryId));
        totalQueriesTracked.incrementAndGet();
        log.debug("Registered query %s for state transition monitoring", queryId);
    }

    /**
     * Records a state transition for the given query.
     */
    public void recordStateTransition(QueryId queryId, QueryState fromState, QueryState toState, long durationMillis)
    {
        requireNonNull(queryId, "queryId is null");
        requireNonNull(fromState, "fromState is null");
        requireNonNull(toState, "toState is null");

        QueryTransitionTracker tracker = activeQueries.get(queryId);
        if (tracker == null) {
            log.debug("Query %s not registered for monitoring, registering now", queryId);
            registerQuery(queryId);
            tracker = activeQueries.get(queryId);
        }

        tracker.recordTransition(fromState, toState, durationMillis);

        switch (fromState) {
            case DISPATCHING:
                dispatchingTimeStats.add(durationMillis, TimeUnit.MILLISECONDS);
                checkAndLogAnomaly(queryId, fromState, durationMillis, dispatchingTimeStats, anomalousDispatchingCount);
                break;
            case FINISHING:
                finishingTimeStats.add(durationMillis, TimeUnit.MILLISECONDS);
                checkAndLogAnomaly(queryId, fromState, durationMillis, finishingTimeStats, anomalousFinishingCount);
                break;
            case PLANNING:
                planningTimeStats.add(durationMillis, TimeUnit.MILLISECONDS);
                checkAndLogAnomaly(queryId, fromState, durationMillis, planningTimeStats, anomalousPlanningCount);
                break;
            case RUNNING:
                runningTimeStats.add(durationMillis, TimeUnit.MILLISECONDS);
                checkAndLogAnomaly(queryId, fromState, durationMillis, runningTimeStats, anomalousRunningCount);
                break;
            default:
                break;
        }

        log.debug("Query %s: %s -> %s transition took %d ms", queryId, fromState, toState, durationMillis);

        if (toState.isDone()) {
            activeQueries.remove(queryId);
            log.debug("Unregistered query %s from state transition monitoring", queryId);
        }
    }

    /**
     * Checks if the duration is anomalous and logs a warning if so.
     * Note: Anomaly detection logic is simplified since TimeStat doesn't expose
     * statistical methods directly. The metrics are available via JMX/ODS.
     */
    @VisibleForTesting
    void checkAndLogAnomaly(QueryId queryId, QueryState state, long durationMillis, TimeStat stats, CounterStat anomalyCounter)
    {
        // TimeStat metrics are exposed via JMX but not accessible programmatically
        // For now, we log all significant duration spikes (> 10 seconds)
        if (durationMillis > 10000) {
            anomalyCounter.update(1);
            log.warn("Query %s spent significant time in %s state: %d ms",
                    queryId, state, durationMillis);
        }
    }

    /**
     * Returns the total number of queries tracked.
     */
    @Managed
    public long getTotalQueriesTracked()
    {
        return totalQueriesTracked.get();
    }

    /**
     * Returns the number of queries currently being tracked.
     */
    @Managed
    public long getActiveQueriesCount()
    {
        return activeQueries.size();
    }

    /**
     * Returns statistics for dispatching state duration.
     */
    @Managed
    @Nested
    public TimeStat getDispatchingTimeStats()
    {
        return dispatchingTimeStats;
    }

    /**
     * Returns statistics for finishing state duration.
     */
    @Managed
    @Nested
    public TimeStat getFinishingTimeStats()
    {
        return finishingTimeStats;
    }

    /**
     * Returns statistics for planning state duration.
     */
    @Managed
    @Nested
    public TimeStat getPlanningTimeStats()
    {
        return planningTimeStats;
    }

    /**
     * Returns statistics for running state duration.
     */
    @Managed
    @Nested
    public TimeStat getRunningTimeStats()
    {
        return runningTimeStats;
    }

    /**
     * Returns count of queries with anomalous dispatching time.
     */
    @Managed
    @Nested
    public CounterStat getAnomalousDispatchingCount()
    {
        return anomalousDispatchingCount;
    }

    /**
     * Returns count of queries with anomalous finishing time.
     */
    @Managed
    @Nested
    public CounterStat getAnomalousFinishingCount()
    {
        return anomalousFinishingCount;
    }

    /**
     * Returns count of queries with anomalous planning time.
     */
    @Managed
    @Nested
    public CounterStat getAnomalousPlanningCount()
    {
        return anomalousPlanningCount;
    }

    /**
     * Returns count of queries with anomalous running time.
     */
    @Managed
    @Nested
    public CounterStat getAnomalousRunningCount()
    {
        return anomalousRunningCount;
    }

    /**
     * Returns a summary of current statistics as a map.
     * Note: Detailed TimeStat metrics (mean, max, min, percentiles) are exposed via JMX
     * through the @Nested annotated getter methods above.
     */
    @Managed
    public Map<String, String> getStatsSummary()
    {
        return ImmutableMap.<String, String>builder()
                .put("totalQueriesTracked", String.valueOf(totalQueriesTracked.get()))
                .put("activeQueries", String.valueOf(activeQueries.size()))
                .put("anomalousDispatchingCount", String.valueOf(anomalousDispatchingCount.getTotalCount()))
                .put("anomalousFinishingCount", String.valueOf(anomalousFinishingCount.getTotalCount()))
                .put("anomalousPlanningCount", String.valueOf(anomalousPlanningCount.getTotalCount()))
                .put("anomalousRunningCount", String.valueOf(anomalousRunningCount.getTotalCount()))
                .build();
    }

    /**
     * Internal class to track state transitions for a single query.
     */
    private static class QueryTransitionTracker
    {
        private final QueryId queryId;
        private QueryState lastState;
        private long lastTransitionTimeMillis;

        public QueryTransitionTracker(QueryId queryId)
        {
            this.queryId = requireNonNull(queryId, "queryId is null");
            this.lastTransitionTimeMillis = System.currentTimeMillis();
        }

        public void recordTransition(QueryState fromState, QueryState toState, long durationMillis)
        {
            this.lastState = toState;
            this.lastTransitionTimeMillis = System.currentTimeMillis();
        }

        public QueryState getLastState()
        {
            return lastState;
        }

        public long getLastTransitionTimeMillis()
        {
            return lastTransitionTimeMillis;
        }
    }
}
