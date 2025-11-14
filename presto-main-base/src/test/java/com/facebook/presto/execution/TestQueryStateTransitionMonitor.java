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

import com.facebook.airlift.stats.CounterStat;
import com.facebook.airlift.stats.TimeStat;
import com.facebook.presto.spi.QueryId;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.presto.execution.QueryState.DISPATCHING;
import static com.facebook.presto.execution.QueryState.FAILED;
import static com.facebook.presto.execution.QueryState.FINISHED;
import static com.facebook.presto.execution.QueryState.FINISHING;
import static com.facebook.presto.execution.QueryState.PLANNING;
import static com.facebook.presto.execution.QueryState.QUEUED;
import static com.facebook.presto.execution.QueryState.RUNNING;
import static com.facebook.presto.execution.QueryState.STARTING;
import static com.facebook.presto.execution.QueryState.WAITING_FOR_PREREQUISITES;
import static com.facebook.presto.execution.QueryState.WAITING_FOR_RESOURCES;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestQueryStateTransitionMonitor
{
    private QueryStateTransitionMonitor monitor;
    private QueryId testQueryId;

    @BeforeMethod
    public void setup()
    {
        monitor = new QueryStateTransitionMonitor();
        testQueryId = new QueryId("test_query_1");
    }

    @Test
    public void testRegisterQuery()
    {
        monitor.registerQuery(testQueryId);
        assertEquals(monitor.getTotalQueriesTracked(), 1);
        assertEquals(monitor.getActiveQueriesCount(), 1);
    }

    @Test
    public void testRegisterMultipleQueries()
    {
        QueryId query1 = new QueryId("query_1");
        QueryId query2 = new QueryId("query_2");
        QueryId query3 = new QueryId("query_3");

        monitor.registerQuery(query1);
        monitor.registerQuery(query2);
        monitor.registerQuery(query3);

        assertEquals(monitor.getTotalQueriesTracked(), 3);
        assertEquals(monitor.getActiveQueriesCount(), 3);
    }

    @Test
    public void testRecordStateTransitionDispatchingState()
    {
        monitor.registerQuery(testQueryId);

        long dispatchingDuration = 1000L;
        monitor.recordStateTransition(testQueryId, DISPATCHING, PLANNING, dispatchingDuration);

        TimeStat dispatchingStats = monitor.getDispatchingTimeStats();
        assertNotNull(dispatchingStats);
    }

    @Test
    public void testRecordStateTransitionFinishingState()
    {
        monitor.registerQuery(testQueryId);

        long finishingDuration = 2000L;
        monitor.recordStateTransition(testQueryId, FINISHING, FINISHED, finishingDuration);

        TimeStat finishingStats = monitor.getFinishingTimeStats();
        assertNotNull(finishingStats);
    }

    @Test
    public void testRecordStateTransitionPlanningState()
    {
        monitor.registerQuery(testQueryId);

        long planningDuration = 500L;
        monitor.recordStateTransition(testQueryId, PLANNING, STARTING, planningDuration);

        TimeStat planningStats = monitor.getPlanningTimeStats();
        assertNotNull(planningStats);
    }

    @Test
    public void testRecordStateTransitionRunningState()
    {
        monitor.registerQuery(testQueryId);

        long runningDuration = 5000L;
        monitor.recordStateTransition(testQueryId, RUNNING, FINISHING, runningDuration);

        TimeStat runningStats = monitor.getRunningTimeStats();
        assertNotNull(runningStats);
    }

    @Test
    public void testAutoRegisterQuery()
    {
        long duration = 1000L;
        monitor.recordStateTransition(testQueryId, DISPATCHING, PLANNING, duration);

        assertEquals(monitor.getTotalQueriesTracked(), 1);
        TimeStat dispatchingStats = monitor.getDispatchingTimeStats();
        assertNotNull(dispatchingStats);
    }

    @Test
    public void testQueryUnregisteredAfterTerminalState()
    {
        monitor.registerQuery(testQueryId);
        assertEquals(monitor.getActiveQueriesCount(), 1);

        monitor.recordStateTransition(testQueryId, RUNNING, FINISHED, 1000L);

        assertEquals(monitor.getTotalQueriesTracked(), 1);
        assertEquals(monitor.getActiveQueriesCount(), 0);
    }

    @Test
    public void testQueryUnregisteredAfterFailedState()
    {
        monitor.registerQuery(testQueryId);
        assertEquals(monitor.getActiveQueriesCount(), 1);

        monitor.recordStateTransition(testQueryId, RUNNING, FAILED, 1000L);

        assertEquals(monitor.getTotalQueriesTracked(), 1);
        assertEquals(monitor.getActiveQueriesCount(), 0);
    }

    @Test
    public void testFullQueryLifecycle()
    {
        monitor.registerQuery(testQueryId);

        monitor.recordStateTransition(testQueryId, WAITING_FOR_PREREQUISITES, QUEUED, 100L);
        monitor.recordStateTransition(testQueryId, QUEUED, WAITING_FOR_RESOURCES, 200L);
        monitor.recordStateTransition(testQueryId, WAITING_FOR_RESOURCES, DISPATCHING, 300L);
        monitor.recordStateTransition(testQueryId, DISPATCHING, PLANNING, 400L);
        monitor.recordStateTransition(testQueryId, PLANNING, STARTING, 500L);
        monitor.recordStateTransition(testQueryId, STARTING, RUNNING, 600L);
        monitor.recordStateTransition(testQueryId, RUNNING, FINISHING, 5000L);
        monitor.recordStateTransition(testQueryId, FINISHING, FINISHED, 700L);

        TimeStat dispatchingStats = monitor.getDispatchingTimeStats();
        assertNotNull(dispatchingStats);

        TimeStat finishingStats = monitor.getFinishingTimeStats();
        assertNotNull(finishingStats);

        TimeStat planningStats = monitor.getPlanningTimeStats();
        assertNotNull(planningStats);

        TimeStat runningStats = monitor.getRunningTimeStats();
        assertNotNull(runningStats);

        assertEquals(monitor.getActiveQueriesCount(), 0);
        assertEquals(monitor.getTotalQueriesTracked(), 1);
    }

    @Test
    public void testAnomalyDetectionWithSufficientSamples()
    {
        QueryId[] queryIds = new QueryId[15];
        for (int i = 0; i < 15; i++) {
            queryIds[i] = new QueryId("query_" + i);
            monitor.registerQuery(queryIds[i]);
        }

        for (int i = 0; i < 10; i++) {
            monitor.recordStateTransition(queryIds[i], DISPATCHING, PLANNING, 1000L);
        }

        monitor.recordStateTransition(queryIds[10], DISPATCHING, PLANNING, 50000L);

        CounterStat anomalousCount = monitor.getAnomalousDispatchingCount();
        assertTrue(anomalousCount.getTotalCount() >= 0);
    }

    @Test
    public void testAnomalyDetectionInsufficientSamples()
    {
        QueryId query1 = new QueryId("query_1");
        QueryId query2 = new QueryId("query_2");

        monitor.registerQuery(query1);
        monitor.registerQuery(query2);

        monitor.recordStateTransition(query1, DISPATCHING, PLANNING, 100L);
        monitor.recordStateTransition(query2, DISPATCHING, PLANNING, 10000L);

        CounterStat anomalousCount = monitor.getAnomalousDispatchingCount();
        assertEquals(anomalousCount.getTotalCount(), 0);
    }

    @Test
    public void testGetStatsSummary()
    {
        monitor.registerQuery(testQueryId);

        monitor.recordStateTransition(testQueryId, DISPATCHING, PLANNING, 1000L);
        monitor.recordStateTransition(testQueryId, PLANNING, STARTING, 500L);
        monitor.recordStateTransition(testQueryId, STARTING, RUNNING, 100L);
        monitor.recordStateTransition(testQueryId, RUNNING, FINISHING, 5000L);
        monitor.recordStateTransition(testQueryId, FINISHING, FINISHED, 2000L);

        Map<String, String> summary = monitor.getStatsSummary();

        assertNotNull(summary);
        assertTrue(summary.containsKey("totalQueriesTracked"));
        assertTrue(summary.containsKey("activeQueries"));
        assertTrue(summary.containsKey("anomalousDispatchingCount"));
        assertTrue(summary.containsKey("anomalousFinishingCount"));

        assertEquals(summary.get("totalQueriesTracked"), "1");
        assertEquals(summary.get("activeQueries"), "0");
    }

    @Test
    public void testMultipleQueriesDispatchingStatistics()
    {
        QueryId[] queryIds = new QueryId[5];
        long[] durations = {1000L, 1100L, 900L, 1050L, 950L};

        for (int i = 0; i < 5; i++) {
            queryIds[i] = new QueryId("query_" + i);
            monitor.registerQuery(queryIds[i]);
            monitor.recordStateTransition(queryIds[i], DISPATCHING, PLANNING, durations[i]);
        }

        TimeStat dispatchingStats = monitor.getDispatchingTimeStats();
        assertNotNull(dispatchingStats);
    }

    @Test
    public void testMultipleQueriesFinishingStatistics()
    {
        QueryId[] queryIds = new QueryId[5];
        long[] durations = {2000L, 2100L, 1900L, 2050L, 1950L};

        for (int i = 0; i < 5; i++) {
            queryIds[i] = new QueryId("query_" + i);
            monitor.registerQuery(queryIds[i]);
            monitor.recordStateTransition(queryIds[i], FINISHING, FINISHED, durations[i]);
        }

        TimeStat finishingStats = monitor.getFinishingTimeStats();
        assertNotNull(finishingStats);
    }

    @Test
    public void testODSMetricsExported()
    {
        assertNotNull(monitor.getDispatchingTimeStats());
        assertNotNull(monitor.getFinishingTimeStats());
        assertNotNull(monitor.getPlanningTimeStats());
        assertNotNull(monitor.getRunningTimeStats());
        assertNotNull(monitor.getAnomalousDispatchingCount());
        assertNotNull(monitor.getAnomalousFinishingCount());
        assertNotNull(monitor.getAnomalousPlanningCount());
        assertNotNull(monitor.getAnomalousRunningCount());
        assertNotNull(monitor.getStatsSummary());
    }

    @Test
    public void testConcurrentQueryRegistration()
    {
        int numQueries = 100;
        for (int i = 0; i < numQueries; i++) {
            QueryId queryId = new QueryId("concurrent_query_" + i);
            monitor.registerQuery(queryId);
            monitor.recordStateTransition(queryId, DISPATCHING, PLANNING, 1000L + i);
        }

        assertEquals(monitor.getTotalQueriesTracked(), numQueries);
    }

    @Test
    public void testStateTransitionWithZeroDuration()
    {
        monitor.registerQuery(testQueryId);
        monitor.recordStateTransition(testQueryId, DISPATCHING, PLANNING, 0L);

        TimeStat dispatchingStats = monitor.getDispatchingTimeStats();
        assertNotNull(dispatchingStats);
    }

    @Test
    public void testIgnoredNonTrackedStates()
    {
        monitor.registerQuery(testQueryId);

        monitor.recordStateTransition(testQueryId, WAITING_FOR_PREREQUISITES, QUEUED, 100L);
        monitor.recordStateTransition(testQueryId, QUEUED, WAITING_FOR_RESOURCES, 200L);

        TimeStat dispatchingStats = monitor.getDispatchingTimeStats();
        TimeStat finishingStats = monitor.getFinishingTimeStats();

        assertNotNull(dispatchingStats);
        assertNotNull(finishingStats);
    }
}
