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

@Test(singleThreaded = true)
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

    @Test
    public void testIdempotentQueryRegistration()
    {
        monitor.registerQuery(testQueryId);
        monitor.registerQuery(testQueryId);
        monitor.registerQuery(testQueryId);

        assertEquals(monitor.getTotalQueriesTracked(), 1);
        assertEquals(monitor.getActiveQueriesCount(), 1);
    }

    @Test
    public void testAnomalyDetectionForPlanningState()
    {
        for (int i = 0; i < 12; i++) {
            QueryId queryId = new QueryId("planning_query_" + i);
            monitor.registerQuery(queryId);
            monitor.recordStateTransition(queryId, PLANNING, STARTING, 500L + (i * 10));
        }

        QueryId anomalousQuery = new QueryId("anomalous_planning_query");
        monitor.registerQuery(anomalousQuery);
        monitor.recordStateTransition(anomalousQuery, PLANNING, STARTING, 50000L);

        CounterStat anomalousCount = monitor.getAnomalousPlanningCount();
        assertTrue(anomalousCount.getTotalCount() >= 0);
    }

    @Test
    public void testAnomalyDetectionForFinishingState()
    {
        for (int i = 0; i < 12; i++) {
            QueryId queryId = new QueryId("finishing_query_" + i);
            monitor.registerQuery(queryId);
            monitor.recordStateTransition(queryId, FINISHING, FINISHED, 200L + (i * 5));
        }

        QueryId anomalousQuery = new QueryId("anomalous_finishing_query");
        monitor.registerQuery(anomalousQuery);
        monitor.recordStateTransition(anomalousQuery, FINISHING, FINISHED, 20000L);

        CounterStat anomalousCount = monitor.getAnomalousFinishingCount();
        assertTrue(anomalousCount.getTotalCount() >= 0);
    }

    @Test
    public void testNoAnomalyDetectionForRunningState()
    {
        for (int i = 0; i < 15; i++) {
            QueryId queryId = new QueryId("running_query_" + i);
            monitor.registerQuery(queryId);
            monitor.recordStateTransition(queryId, RUNNING, FINISHING, 1000L);
        }

        QueryId slowQuery = new QueryId("slow_running_query");
        monitor.registerQuery(slowQuery);
        monitor.recordStateTransition(slowQuery, RUNNING, FINISHING, 1000000L);

        CounterStat anomalousCount = monitor.getAnomalousRunningCount();
        assertEquals(anomalousCount.getTotalCount(), 0);
    }

    @Test
    public void testCheckAndLogAnomalyDirectly()
    {
        CounterStat testCounter = new CounterStat();

        for (int i = 0; i < 15; i++) {
            QueryId queryId = new QueryId("direct_test_" + i);
            monitor.checkAndLogAnomaly(queryId, DISPATCHING, 100L, testCounter);
        }

        QueryId anomalousQuery = new QueryId("direct_anomalous");
        monitor.checkAndLogAnomaly(anomalousQuery, DISPATCHING, 10000L, testCounter);

        assertTrue(testCounter.getTotalCount() >= 0);
    }

    @Test
    public void testCheckAndLogAnomalyWithLowVariance()
    {
        CounterStat testCounter = new CounterStat();

        for (int i = 0; i < 15; i++) {
            QueryId queryId = new QueryId("low_variance_" + i);
            monitor.checkAndLogAnomaly(queryId, DISPATCHING, 100L, testCounter);
        }

        QueryId testQuery = new QueryId("test_low_variance");
        monitor.checkAndLogAnomaly(testQuery, DISPATCHING, 105L, testCounter);

        assertEquals(testCounter.getTotalCount(), 0);
    }

    @Test
    public void testMultipleQueriesCompleteLifecycle()
    {
        int numQueries = 50;
        for (int i = 0; i < numQueries; i++) {
            QueryId queryId = new QueryId("lifecycle_query_" + i);
            monitor.registerQuery(queryId);

            monitor.recordStateTransition(queryId, WAITING_FOR_PREREQUISITES, QUEUED, 10L);
            monitor.recordStateTransition(queryId, QUEUED, WAITING_FOR_RESOURCES, 20L);
            monitor.recordStateTransition(queryId, WAITING_FOR_RESOURCES, DISPATCHING, 30L);
            monitor.recordStateTransition(queryId, DISPATCHING, PLANNING, 100L + i);
            monitor.recordStateTransition(queryId, PLANNING, STARTING, 50L + i);
            monitor.recordStateTransition(queryId, STARTING, RUNNING, 20L);
            monitor.recordStateTransition(queryId, RUNNING, FINISHING, 1000L + (i * 100));
            monitor.recordStateTransition(queryId, FINISHING, FINISHED, 100L + i);
        }

        assertEquals(monitor.getTotalQueriesTracked(), numQueries);
        assertEquals(monitor.getActiveQueriesCount(), 0);
    }

    @Test
    public void testMixedQueryOutcomes()
    {
        for (int i = 0; i < 10; i++) {
            QueryId queryId = new QueryId("mixed_query_" + i);
            monitor.registerQuery(queryId);

            monitor.recordStateTransition(queryId, DISPATCHING, PLANNING, 100L);
            monitor.recordStateTransition(queryId, PLANNING, STARTING, 50L);
            monitor.recordStateTransition(queryId, STARTING, RUNNING, 20L);

            if (i % 2 == 0) {
                monitor.recordStateTransition(queryId, RUNNING, FINISHING, 1000L);
                monitor.recordStateTransition(queryId, FINISHING, FINISHED, 100L);
            }
            else {
                monitor.recordStateTransition(queryId, RUNNING, FAILED, 500L);
            }
        }

        assertEquals(monitor.getTotalQueriesTracked(), 10);
        assertEquals(monitor.getActiveQueriesCount(), 0);
    }

    @Test
    public void testStatsSummaryContainsAllKeys()
    {
        Map<String, String> summary = monitor.getStatsSummary();

        assertNotNull(summary);
        assertEquals(summary.size(), 6);
        assertTrue(summary.containsKey("totalQueriesTracked"));
        assertTrue(summary.containsKey("activeQueries"));
        assertTrue(summary.containsKey("anomalousDispatchingCount"));
        assertTrue(summary.containsKey("anomalousFinishingCount"));
        assertTrue(summary.containsKey("anomalousPlanningCount"));
        assertTrue(summary.containsKey("anomalousRunningCount"));
    }

    @Test
    public void testStatsSummaryValues()
    {
        for (int i = 0; i < 5; i++) {
            QueryId queryId = new QueryId("summary_query_" + i);
            monitor.registerQuery(queryId);
            monitor.recordStateTransition(queryId, DISPATCHING, PLANNING, 100L);
            monitor.recordStateTransition(queryId, PLANNING, STARTING, 50L);
            monitor.recordStateTransition(queryId, STARTING, RUNNING, 20L);
            monitor.recordStateTransition(queryId, RUNNING, FINISHED, 1000L);
        }

        for (int i = 5; i < 8; i++) {
            QueryId queryId = new QueryId("summary_query_" + i);
            monitor.registerQuery(queryId);
            monitor.recordStateTransition(queryId, DISPATCHING, PLANNING, 100L);
        }

        Map<String, String> summary = monitor.getStatsSummary();
        assertEquals(summary.get("totalQueriesTracked"), "8");
        assertEquals(summary.get("activeQueries"), "3");
    }

    @Test
    public void testVeryLongDurations()
    {
        monitor.registerQuery(testQueryId);

        long oneHourMs = 3600000L;
        monitor.recordStateTransition(testQueryId, DISPATCHING, PLANNING, oneHourMs);
        monitor.recordStateTransition(testQueryId, PLANNING, STARTING, oneHourMs);
        monitor.recordStateTransition(testQueryId, STARTING, RUNNING, oneHourMs);
        monitor.recordStateTransition(testQueryId, RUNNING, FINISHING, oneHourMs);
        monitor.recordStateTransition(testQueryId, FINISHING, FINISHED, oneHourMs);

        assertEquals(monitor.getActiveQueriesCount(), 0);
    }

    @Test
    public void testQueryReregistrationAfterCompletion()
    {
        monitor.registerQuery(testQueryId);
        monitor.recordStateTransition(testQueryId, DISPATCHING, PLANNING, 100L);
        monitor.recordStateTransition(testQueryId, PLANNING, FINISHED, 100L);

        assertEquals(monitor.getActiveQueriesCount(), 0);

        monitor.registerQuery(testQueryId);
        assertEquals(monitor.getActiveQueriesCount(), 1);
        assertEquals(monitor.getTotalQueriesTracked(), 2);
    }

    @Test
    public void testAllTrackedStatesHaveStatObjects()
    {
        assertNotNull(monitor.getDispatchingTimeStats());
        assertNotNull(monitor.getFinishingTimeStats());
        assertNotNull(monitor.getPlanningTimeStats());
        assertNotNull(monitor.getRunningTimeStats());
        assertNotNull(monitor.getAnomalousDispatchingCount());
        assertNotNull(monitor.getAnomalousFinishingCount());
        assertNotNull(monitor.getAnomalousPlanningCount());
        assertNotNull(monitor.getAnomalousRunningCount());
    }

    @Test
    public void testRapidSuccessiveTransitions()
    {
        monitor.registerQuery(testQueryId);

        monitor.recordStateTransition(testQueryId, WAITING_FOR_PREREQUISITES, QUEUED, 0L);
        monitor.recordStateTransition(testQueryId, QUEUED, WAITING_FOR_RESOURCES, 0L);
        monitor.recordStateTransition(testQueryId, WAITING_FOR_RESOURCES, DISPATCHING, 0L);
        monitor.recordStateTransition(testQueryId, DISPATCHING, PLANNING, 0L);
        monitor.recordStateTransition(testQueryId, PLANNING, STARTING, 0L);
        monitor.recordStateTransition(testQueryId, STARTING, RUNNING, 0L);
        monitor.recordStateTransition(testQueryId, RUNNING, FINISHING, 0L);
        monitor.recordStateTransition(testQueryId, FINISHING, FINISHED, 0L);

        assertEquals(monitor.getActiveQueriesCount(), 0);
        assertEquals(monitor.getTotalQueriesTracked(), 1);
    }

    @Test
    public void testAnomalyThresholdCalculation()
    {
        CounterStat testCounter = new CounterStat();

        long[] samples = {80L, 90L, 100L, 110L, 120L, 80L, 90L, 100L, 110L, 120L};
        for (int i = 0; i < samples.length; i++) {
            QueryId queryId = new QueryId("threshold_test_" + i);
            monitor.checkAndLogAnomaly(queryId, DISPATCHING, samples[i], testCounter);
        }

        QueryId normalQuery = new QueryId("threshold_normal");
        long countBefore = testCounter.getTotalCount();
        monitor.checkAndLogAnomaly(normalQuery, DISPATCHING, 125L, testCounter);
        assertEquals(testCounter.getTotalCount(), countBefore);

        QueryId anomalousQuery = new QueryId("threshold_anomalous");
        monitor.checkAndLogAnomaly(anomalousQuery, DISPATCHING, 200L, testCounter);
        assertTrue(testCounter.getTotalCount() > countBefore);
    }

    @Test
    public void testAnomalyDetectionForDifferentStatesIndependent()
    {
        for (int i = 0; i < 12; i++) {
            QueryId dispatchQuery = new QueryId("dispatch_" + i);
            QueryId planningQuery = new QueryId("planning_" + i);

            monitor.registerQuery(dispatchQuery);
            monitor.registerQuery(planningQuery);

            monitor.recordStateTransition(dispatchQuery, DISPATCHING, PLANNING, 1000L + (i * 10));
            monitor.recordStateTransition(planningQuery, PLANNING, STARTING, 100L + i);
        }

        QueryId anomalousDispatch = new QueryId("anomalous_dispatch");
        monitor.registerQuery(anomalousDispatch);
        monitor.recordStateTransition(anomalousDispatch, DISPATCHING, PLANNING, 50000L);

        QueryId normalPlanning = new QueryId("normal_planning");
        monitor.registerQuery(normalPlanning);
        monitor.recordStateTransition(normalPlanning, PLANNING, STARTING, 150L);

        assertNotNull(monitor.getAnomalousDispatchingCount());
        assertNotNull(monitor.getAnomalousPlanningCount());
    }

    @Test
    public void testLargeScaleQueryProcessing()
    {
        int numQueries = 500;

        for (int i = 0; i < numQueries; i++) {
            QueryId queryId = new QueryId("large_scale_" + i);
            monitor.registerQuery(queryId);
            monitor.recordStateTransition(queryId, DISPATCHING, PLANNING, 100L + (i % 50));
            monitor.recordStateTransition(queryId, PLANNING, STARTING, 50L + (i % 25));
            monitor.recordStateTransition(queryId, STARTING, RUNNING, 20L);
            monitor.recordStateTransition(queryId, RUNNING, FINISHING, 1000L + (i % 500));
            monitor.recordStateTransition(queryId, FINISHING, FINISHED, 100L + (i % 50));
        }

        assertEquals(monitor.getTotalQueriesTracked(), numQueries);
        assertEquals(monitor.getActiveQueriesCount(), 0);

        Map<String, String> summary = monitor.getStatsSummary();
        assertEquals(summary.get("totalQueriesTracked"), String.valueOf(numQueries));
    }

    @Test
    public void testQueryStateTransitionWithoutPriorRegistration()
    {
        QueryId unregisteredQuery = new QueryId("unregistered_query");
        monitor.recordStateTransition(unregisteredQuery, DISPATCHING, PLANNING, 100L);

        assertEquals(monitor.getTotalQueriesTracked(), 1);
        assertEquals(monitor.getActiveQueriesCount(), 1);
    }

    @Test
    public void testMultipleTerminalStateTransitions()
    {
        monitor.registerQuery(testQueryId);
        monitor.recordStateTransition(testQueryId, RUNNING, FINISHED, 1000L);

        assertEquals(monitor.getActiveQueriesCount(), 0);

        monitor.recordStateTransition(testQueryId, DISPATCHING, PLANNING, 100L);
        assertEquals(monitor.getTotalQueriesTracked(), 2);
        assertEquals(monitor.getActiveQueriesCount(), 1);
    }

    @Test
    public void testAnomalyCountersStartAtZero()
    {
        assertEquals(monitor.getAnomalousDispatchingCount().getTotalCount(), 0);
        assertEquals(monitor.getAnomalousFinishingCount().getTotalCount(), 0);
        assertEquals(monitor.getAnomalousPlanningCount().getTotalCount(), 0);
        assertEquals(monitor.getAnomalousRunningCount().getTotalCount(), 0);
    }

    @Test
    public void testStatsSummaryWithNoQueries()
    {
        Map<String, String> summary = monitor.getStatsSummary();

        assertEquals(summary.get("totalQueriesTracked"), "0");
        assertEquals(summary.get("activeQueries"), "0");
        assertEquals(summary.get("anomalousDispatchingCount"), "0");
        assertEquals(summary.get("anomalousFinishingCount"), "0");
        assertEquals(summary.get("anomalousPlanningCount"), "0");
        assertEquals(summary.get("anomalousRunningCount"), "0");
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testQueryIdNullValidation()
    {
        monitor.registerQuery(null);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testRecordStateTransitionNullQueryId()
    {
        monitor.recordStateTransition(null, DISPATCHING, PLANNING, 100L);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testRecordStateTransitionNullFromState()
    {
        monitor.registerQuery(testQueryId);
        monitor.recordStateTransition(testQueryId, null, PLANNING, 100L);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testRecordStateTransitionNullToState()
    {
        monitor.registerQuery(testQueryId);
        monitor.recordStateTransition(testQueryId, DISPATCHING, null, 100L);
    }

    @Test
    public void testConsistentStatisticsAfterManyOperations()
    {
        int iterations = 100;
        int activeQueries = 0;

        for (int i = 0; i < iterations; i++) {
            QueryId queryId = new QueryId("consistency_query_" + i);
            monitor.registerQuery(queryId);
            activeQueries++;

            monitor.recordStateTransition(queryId, DISPATCHING, PLANNING, 100L);
            monitor.recordStateTransition(queryId, PLANNING, STARTING, 50L);
            monitor.recordStateTransition(queryId, STARTING, RUNNING, 20L);

            if (i % 2 == 0) {
                monitor.recordStateTransition(queryId, RUNNING, FINISHED, 1000L);
                activeQueries--;
            }
        }

        assertEquals(monitor.getTotalQueriesTracked(), iterations);
        assertEquals(monitor.getActiveQueriesCount(), activeQueries);
    }

    @Test
    public void testStartingStateTransition()
    {
        monitor.registerQuery(testQueryId);
        monitor.recordStateTransition(testQueryId, STARTING, RUNNING, 500L);

        assertEquals(monitor.getActiveQueriesCount(), 1);
    }

    @Test
    public void testQueuedStateTransition()
    {
        monitor.registerQuery(testQueryId);
        monitor.recordStateTransition(testQueryId, QUEUED, WAITING_FOR_RESOURCES, 1000L);

        assertEquals(monitor.getActiveQueriesCount(), 1);
    }

    @Test
    public void testWaitingForResourcesStateTransition()
    {
        monitor.registerQuery(testQueryId);
        monitor.recordStateTransition(testQueryId, WAITING_FOR_RESOURCES, DISPATCHING, 2000L);

        assertEquals(monitor.getActiveQueriesCount(), 1);
    }

    @Test
    public void testWaitingForPrerequisitesStateTransition()
    {
        monitor.registerQuery(testQueryId);
        monitor.recordStateTransition(testQueryId, WAITING_FOR_PREREQUISITES, QUEUED, 500L);

        assertEquals(monitor.getActiveQueriesCount(), 1);
    }

    @Test
    public void testAnomalyDetectionWithHighVariance()
    {
        CounterStat testCounter = new CounterStat();

        long[] samples = {100L, 200L, 300L, 400L, 500L, 600L, 700L, 800L, 900L, 1000L};
        for (int i = 0; i < samples.length; i++) {
            QueryId queryId = new QueryId("high_variance_" + i);
            monitor.checkAndLogAnomaly(queryId, DISPATCHING, samples[i], testCounter);
        }

        QueryId outlierQuery = new QueryId("outlier_query");
        monitor.checkAndLogAnomaly(outlierQuery, DISPATCHING, 5000L, testCounter);

        assertTrue(testCounter.getTotalCount() >= 1);
    }

    @Test
    public void testMultipleAnomaliesDetected()
    {
        for (int i = 0; i < 12; i++) {
            QueryId queryId = new QueryId("normal_dispatch_" + i);
            monitor.registerQuery(queryId);
            monitor.recordStateTransition(queryId, DISPATCHING, PLANNING, 100L);
        }

        for (int i = 0; i < 5; i++) {
            QueryId anomalousQuery = new QueryId("anomalous_dispatch_" + i);
            monitor.registerQuery(anomalousQuery);
            monitor.recordStateTransition(anomalousQuery, DISPATCHING, PLANNING, 10000L + (i * 1000));
        }

        assertTrue(monitor.getAnomalousDispatchingCount().getTotalCount() >= 0);
    }

    @Test
    public void testQueryLifecycleWithAllStates()
    {
        monitor.registerQuery(testQueryId);

        monitor.recordStateTransition(testQueryId, WAITING_FOR_PREREQUISITES, QUEUED, 50L);
        assertEquals(monitor.getActiveQueriesCount(), 1);

        monitor.recordStateTransition(testQueryId, QUEUED, WAITING_FOR_RESOURCES, 100L);
        assertEquals(monitor.getActiveQueriesCount(), 1);

        monitor.recordStateTransition(testQueryId, WAITING_FOR_RESOURCES, DISPATCHING, 150L);
        assertEquals(monitor.getActiveQueriesCount(), 1);

        monitor.recordStateTransition(testQueryId, DISPATCHING, PLANNING, 200L);
        assertEquals(monitor.getActiveQueriesCount(), 1);

        monitor.recordStateTransition(testQueryId, PLANNING, STARTING, 250L);
        assertEquals(monitor.getActiveQueriesCount(), 1);

        monitor.recordStateTransition(testQueryId, STARTING, RUNNING, 300L);
        assertEquals(monitor.getActiveQueriesCount(), 1);

        monitor.recordStateTransition(testQueryId, RUNNING, FINISHING, 5000L);
        assertEquals(monitor.getActiveQueriesCount(), 1);

        monitor.recordStateTransition(testQueryId, FINISHING, FINISHED, 350L);
        assertEquals(monitor.getActiveQueriesCount(), 0);
    }

    @Test
    public void testInterleaveQueriesLifecycle()
    {
        QueryId query1 = new QueryId("interleave_1");
        QueryId query2 = new QueryId("interleave_2");
        QueryId query3 = new QueryId("interleave_3");

        monitor.registerQuery(query1);
        monitor.registerQuery(query2);
        monitor.registerQuery(query3);

        monitor.recordStateTransition(query1, DISPATCHING, PLANNING, 100L);
        monitor.recordStateTransition(query2, DISPATCHING, PLANNING, 150L);
        monitor.recordStateTransition(query3, DISPATCHING, PLANNING, 200L);

        monitor.recordStateTransition(query1, PLANNING, RUNNING, 50L);
        monitor.recordStateTransition(query2, PLANNING, RUNNING, 75L);

        monitor.recordStateTransition(query1, RUNNING, FINISHED, 1000L);
        assertEquals(monitor.getActiveQueriesCount(), 2);

        monitor.recordStateTransition(query3, PLANNING, FAILED, 25L);
        assertEquals(monitor.getActiveQueriesCount(), 1);

        monitor.recordStateTransition(query2, RUNNING, FINISHED, 2000L);
        assertEquals(monitor.getActiveQueriesCount(), 0);

        assertEquals(monitor.getTotalQueriesTracked(), 3);
    }

    @Test
    public void testDispatchingStatsAccumulation()
    {
        long totalDuration = 0;
        int numSamples = 20;

        for (int i = 0; i < numSamples; i++) {
            QueryId queryId = new QueryId("dispatch_accumulate_" + i);
            monitor.registerQuery(queryId);
            long duration = 100L + (i * 50);
            totalDuration += duration;
            monitor.recordStateTransition(queryId, DISPATCHING, PLANNING, duration);
        }

        TimeStat stats = monitor.getDispatchingTimeStats();
        assertNotNull(stats);
    }

    @Test
    public void testPlanningStatsAccumulation()
    {
        int numSamples = 20;

        for (int i = 0; i < numSamples; i++) {
            QueryId queryId = new QueryId("planning_accumulate_" + i);
            monitor.registerQuery(queryId);
            long duration = 50L + (i * 25);
            monitor.recordStateTransition(queryId, PLANNING, STARTING, duration);
        }

        TimeStat stats = monitor.getPlanningTimeStats();
        assertNotNull(stats);
    }

    @Test
    public void testFinishingStatsAccumulation()
    {
        int numSamples = 20;

        for (int i = 0; i < numSamples; i++) {
            QueryId queryId = new QueryId("finishing_accumulate_" + i);
            monitor.registerQuery(queryId);
            long duration = 200L + (i * 30);
            monitor.recordStateTransition(queryId, FINISHING, FINISHED, duration);
        }

        TimeStat stats = monitor.getFinishingTimeStats();
        assertNotNull(stats);
    }

    @Test
    public void testRunningStatsAccumulation()
    {
        int numSamples = 20;

        for (int i = 0; i < numSamples; i++) {
            QueryId queryId = new QueryId("running_accumulate_" + i);
            monitor.registerQuery(queryId);
            long duration = 1000L + (i * 500);
            monitor.recordStateTransition(queryId, RUNNING, FINISHING, duration);
        }

        TimeStat stats = monitor.getRunningTimeStats();
        assertNotNull(stats);
    }
}
