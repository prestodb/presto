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
package com.facebook.presto.spark;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.Session;
import com.facebook.presto.common.RuntimeMetric;
import com.facebook.presto.spark.execution.TaskInfoAggregationMode;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.eventlistener.EventListener;
import com.facebook.presto.spi.eventlistener.EventListenerFactory;
import com.facebook.presto.spi.eventlistener.OperatorStatistics;
import com.facebook.presto.spi.eventlistener.QueryCompletedEvent;
import com.facebook.presto.spi.eventlistener.QueryStatistics;
import com.facebook.presto.spi.eventlistener.StageStatistics;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import static com.facebook.airlift.json.JsonCodec.mapJsonCodec;
import static com.facebook.presto.spark.PrestoSparkQueryRunner.createHivePrestoSparkQueryRunner;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.SPARK_MAX_TASK_INFOS_IN_QUERY_COMPLETED_EVENT;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.SPARK_TASK_INFO_AGGREGATION_DRAIN_INTERVAL;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.SPARK_TASK_INFO_AGGREGATION_MODE;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.SPARK_TASK_INFO_AGGREGATION_SEAL_TIMEOUT;
import static com.facebook.presto.spark.execution.AbstractPrestoSparkQueryExecution.EXECUTOR_TASK_INFOS_DROPPED_METRIC;
import static com.facebook.presto.spark.execution.TaskInfoAggregationMode.INCREMENTAL;
import static com.facebook.presto.spark.execution.TaskInfoAggregationMode.LEGACY;
import static com.facebook.presto.spark.execution.TaskInfoAggregationResult.METRIC_PREFIX;
import static com.facebook.presto.spi.StandardErrorCode.DIVISION_BY_ZERO;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.tpch.TpchTable.NATION;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestPrestoSparkQueryCompletedEvent
        extends AbstractTestQueryFramework
{
    private static final String EVENT_LISTENER_NAME = "query-completed-event-collector";
    private static final JsonCodec<Map<String, Object>> HIVE_OUTPUT_INFO_CODEC = mapJsonCodec(String.class, Object.class);
    private static final Set<String> EXPECTED_PARTITIONS = ImmutableSet.of("regionkey=0", "regionkey=1", "regionkey=2", "regionkey=3", "regionkey=4");
    private static final String PARTITIONED_TABLE_QUERY = "CREATE TABLE %s WITH (partitioned_by = ARRAY['regionkey']) AS SELECT count(*) nations, regionkey FROM nation GROUP BY regionkey";
    private static final String JOIN_AGGREGATION_QUERY = "SELECT a.regionkey, count(*), sum(b.nationkey) FROM nation a JOIN nation b ON a.regionkey = b.regionkey GROUP BY a.regionkey";

    private final List<QueryCompletedEvent> queryCompletedEvents = new CopyOnWriteArrayList<>();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        PrestoSparkQueryRunner queryRunner = createHivePrestoSparkQueryRunner(ImmutableList.of(NATION));
        queryRunner.installPlugin(new Plugin()
        {
            @Override
            public Iterable<EventListenerFactory> getEventListenerFactories()
            {
                return ImmutableList.of(new QueryCompletedEventCollectorFactory());
            }
        });
        queryRunner.getEventListenerManager().loadConfiguredEventListener(ImmutableMap.of("event-listener.name", EVENT_LISTENER_NAME));
        return queryRunner;
    }

    @DataProvider
    public Object[][] taskInfoAggregationModes()
    {
        return new Object[][] {{LEGACY}, {INCREMENTAL}};
    }

    @Test(dataProvider = "taskInfoAggregationModes")
    public void testWrittenPartitions(TaskInfoAggregationMode mode)
    {
        QueryCompletedEvent event = createPartitionedTable(getSession(mode), "test_written_partitions_" + mode.name().toLowerCase(ENGLISH));
        assertTaskInfoAggregation(event, mode);
        assertTrue(getStageIds(event).size() > 1);
        assertNull(event.getStatistics().getRuntimeStats().getMetric(EXECUTOR_TASK_INFOS_DROPPED_METRIC));
        assertEquals(getWrittenPartitions(event), EXPECTED_PARTITIONS);
    }

    @Test(dataProvider = "taskInfoAggregationModes")
    public void testWrittenPartitionsWhenTaskInfoLimitExceeded(TaskInfoAggregationMode mode)
    {
        Session session = Session.builder(getSession(mode))
                .setSystemProperty(SPARK_MAX_TASK_INFOS_IN_QUERY_COMPLETED_EVENT, "0")
                .build();
        String tableName = "test_written_partitions_task_info_limit_exceeded_" + mode.name().toLowerCase(ENGLISH);
        QueryCompletedEvent event = createPartitionedTable(session, tableName);
        assertTaskInfoAggregation(event, mode);
        if (mode == LEGACY) {
            // Only the root stage, which ran on the driver, keeps its statistics
            assertEquals(getStageIds(event).size(), 1);
            assertTrue(event.getStatistics().getRuntimeStats().getMetric(EXECUTOR_TASK_INFOS_DROPPED_METRIC).getSum() > 0);
        }
        else {
            // the limit does not apply
            assertEquals(getStageIds(event), getStageIds(createPartitionedTable(getSession(LEGACY), tableName + "_without_limit")));
            assertNull(event.getStatistics().getRuntimeStats().getMetric(EXECUTOR_TASK_INFOS_DROPPED_METRIC));
        }
        assertEquals(getWrittenPartitions(event), EXPECTED_PARTITIONS);
    }

    @Test
    public void testWrittenPartitionsWhenIncrementalAggregationTimesOut()
    {
        Session session = Session.builder(getSession(INCREMENTAL))
                .setSystemProperty(SPARK_TASK_INFO_AGGREGATION_SEAL_TIMEOUT, "0ms")
                .build();
        QueryCompletedEvent event = createPartitionedTable(session, "test_written_partitions_aggregation_timed_out");
        assertEquals(event.getStatistics().getRuntimeStats().getMetric(METRIC_PREFIX + "timedOut").getSum(), 1);
        assertEquals(event.getStatistics().getRuntimeStats().getMetric(METRIC_PREFIX + "partial").getSum(), 1);
        // as without incremental aggregation when the task info limit is exceeded, only the stage of the driver task
        assertEquals(getStageIds(event).size(), 1);
        assertEquals(getWrittenPartitions(event), EXPECTED_PARTITIONS);
    }

    @Test(dataProvider = "taskInfoAggregationModes")
    public void testReadOnlyQueryWhenTaskInfoLimitExceeded(TaskInfoAggregationMode mode)
    {
        Session session = Session.builder(getSession(mode))
                .setSystemProperty(SPARK_MAX_TASK_INFOS_IN_QUERY_COMPLETED_EVENT, "0")
                .build();
        queryCompletedEvents.clear();
        assertEquals(computeActual(session, "SELECT count(*) FROM nation").getOnlyValue(), 25L);
        QueryCompletedEvent event = getOnlyElement(queryCompletedEvents);
        assertTaskInfoAggregation(event, mode);
        if (mode == LEGACY) {
            // No stage ran on the driver, so no statistics are kept
            assertEquals(getStageIds(event), ImmutableSet.of());
            assertTrue(event.getStatistics().getRuntimeStats().getMetric(EXECUTOR_TASK_INFOS_DROPPED_METRIC).getSum() > 0);
        }
        else {
            assertFalse(getStageIds(event).isEmpty());
            assertNull(event.getStatistics().getRuntimeStats().getMetric(EXECUTOR_TASK_INFOS_DROPPED_METRIC));
        }
        assertFalse(event.getIoMetadata().getOutput().isPresent());
    }

    @Test
    public void testIncrementalAggregationPublishesSameEventAsLegacy()
    {
        String tableName = "test_same_event";
        QueryCompletedEvent legacyEvent = createPartitionedTable(getSession(LEGACY), tableName);
        getQueryRunner().execute(getSession(), "DROP TABLE " + tableName);
        QueryCompletedEvent incrementalEvent = createPartitionedTable(getSession(INCREMENTAL), tableName);
        assertTaskInfoAggregation(incrementalEvent, INCREMENTAL);
        assertEquals(getComparableContent(incrementalEvent, false), getComparableContent(legacyEvent, false));
        assertEquals(getWrittenPartitions(incrementalEvent), EXPECTED_PARTITIONS);

        legacyEvent = runQuery(getSession(LEGACY), JOIN_AGGREGATION_QUERY);
        incrementalEvent = runQuery(getSession(INCREMENTAL), JOIN_AGGREGATION_QUERY);
        assertTaskInfoAggregation(incrementalEvent, INCREMENTAL);
        assertEquals(getComparableContent(incrementalEvent, true), getComparableContent(legacyEvent, true));
    }

    @Test(dataProvider = "taskInfoAggregationModes")
    public void testFailedQuery(TaskInfoAggregationMode mode)
    {
        queryCompletedEvents.clear();
        assertQueryFails(getSession(mode), "SELECT nationkey / (regionkey - regionkey) FROM nation", "(?s)(/ by zero|Division by zero).*");
        QueryCompletedEvent event = getOnlyElement(queryCompletedEvents);
        assertTaskInfoAggregation(event, mode);
        assertTrue(event.getFailureInfo().isPresent());
        assertEquals(event.getFailureInfo().get().getErrorCode(), DIVISION_BY_ZERO.toErrorCode());
        assertFalse(event.getStageStatistics().isEmpty());
    }

    private Session getSession(TaskInfoAggregationMode mode)
    {
        return Session.builder(getSession())
                .setSystemProperty(SPARK_TASK_INFO_AGGREGATION_MODE, mode.name())
                // drain while the tasks of the query are still being merged
                .setSystemProperty(SPARK_TASK_INFO_AGGREGATION_DRAIN_INTERVAL, "1ms")
                .build();
    }

    private QueryCompletedEvent createPartitionedTable(Session session, String tableName)
    {
        queryCompletedEvents.clear();
        // The aggregation adds a hash partitioned stage, so the query has more than one executor task
        assertUpdate(session, format(PARTITIONED_TABLE_QUERY, tableName), 5);
        return getOnlyElement(queryCompletedEvents);
    }

    private QueryCompletedEvent runQuery(Session session, String sql)
    {
        queryCompletedEvents.clear();
        computeActual(session, sql);
        return getOnlyElement(queryCompletedEvents);
    }

    private static void assertTaskInfoAggregation(QueryCompletedEvent event, TaskInfoAggregationMode mode)
    {
        Map<String, RuntimeMetric> metrics = event.getStatistics().getRuntimeStats().getMetrics();
        if (mode == LEGACY) {
            assertTrue(metrics.keySet().stream().noneMatch(name -> name.startsWith(METRIC_PREFIX)), metrics.keySet().toString());
            return;
        }
        assertTrue(metrics.get(METRIC_PREFIX + "received").getSum() > 0, metrics.keySet().toString());
        assertEquals(metrics.get(METRIC_PREFIX + "degraded").getSum(), 0);
        assertEquals(metrics.get(METRIC_PREFIX + "timedOut").getSum(), 0);
        assertEquals(metrics.get(METRIC_PREFIX + "partial").getSum(), 0);
        assertEquals(metrics.get(METRIC_PREFIX + "dropped").getSum(), 0);
        assertEquals(metrics.get(METRIC_PREFIX + "foldFailures").getSum(), 0);
    }

    /**
     * The content of the event that does not depend on timing. The sizes of the data written by a CTAS vary from run
     * to run, because the commit fragments carry generated file names, so they are only compared on request.
     */
    private static Map<String, Object> getComparableContent(QueryCompletedEvent event, boolean compareDataSizes)
    {
        QueryStatistics statistics = event.getStatistics();
        ImmutableMap.Builder<String, Object> content = ImmutableMap.<String, Object>builder()
                .put("totalRows", statistics.getTotalRows())
                .put("totalBytes", statistics.getTotalBytes())
                .put("outputRows", statistics.getOutputRows())
                .put("outputBytes", statistics.getOutputBytes())
                .put("writtenOutputRows", statistics.getWrittenOutputRows())
                .put("writtenOutputBytes", statistics.getWrittenOutputBytes())
                .put("shuffledRows", statistics.getShuffledRows())
                .put("peakRunningTasks", statistics.getPeakRunningTasks())
                .put("completedSplits", statistics.getCompletedSplits())
                .put("stages", event.getStageStatistics().stream()
                        .sorted(Comparator.comparingInt(StageStatistics::getStageId))
                        .map(stage -> compareDataSizes
                                ? ImmutableList.of(stage.getStageId(), stage.getTasks(), stage.getRawInputDataSize(), stage.getProcessedInputDataSize())
                                : ImmutableList.of(stage.getStageId(), stage.getTasks()))
                        .collect(toImmutableList()))
                .put("operators", event.getOperatorStatistics().stream()
                        .sorted(Comparator.comparingInt(OperatorStatistics::getStageId)
                                .thenComparingInt(OperatorStatistics::getPipelineId)
                                .thenComparingInt(OperatorStatistics::getOperatorId))
                        .map(operator -> getComparableContent(operator, compareDataSizes))
                        .collect(toImmutableList()));
        Optional<String> plan = event.getMetadata().getPlan().map(TestPrestoSparkQueryCompletedEvent::removeTimings);
        if (compareDataSizes) {
            content.put("shuffledBytes", statistics.getShuffledBytes());
        }
        else {
            plan = plan.map(text -> text.replaceAll("\\d+(\\.\\d+)?[kMGTP]?B\\b", "<size>"));
        }
        return content.put("plan", plan.orElse("")).build();
    }

    private static List<Object> getComparableContent(OperatorStatistics operator, boolean compareDataSizes)
    {
        ImmutableList.Builder<Object> content = ImmutableList.builder()
                .add(operator.getStageId())
                .add(operator.getPipelineId())
                .add(operator.getOperatorId())
                .add(operator.getPlanNodeId())
                .add(operator.getOperatorType())
                .add(operator.getTotalDrivers())
                .add(operator.getRawInputPositions())
                .add(operator.getInputPositions())
                .add(operator.getOutputPositions());
        if (compareDataSizes) {
            content.add(operator.getRawInputDataSize())
                    .add(operator.getInputDataSize())
                    .add(operator.getOutputDataSize());
        }
        return content.build();
    }

    // the plan node stats in the plan text, without the CPU, scheduled and blocked times
    private static String removeTimings(String plan)
    {
        return plan.replaceAll("\\d+(\\.\\d+)?(ns|us|ms|s|m|h|d)\\b", "<duration>")
                .replaceAll("\\(\\d+(\\.\\d+)?%\\)", "(<percent>)");
    }

    private static Set<Integer> getStageIds(QueryCompletedEvent event)
    {
        return event.getOperatorStatistics().stream()
                .map(OperatorStatistics::getStageId)
                .collect(toImmutableSet());
    }

    private static Set<Object> getWrittenPartitions(QueryCompletedEvent event)
    {
        assertTrue(event.getIoMetadata().getOutput().isPresent());
        Optional<String> connectorOutputMetadata = event.getIoMetadata().getOutput().get().getConnectorOutputMetadata();
        assertTrue(connectorOutputMetadata.isPresent());
        return ImmutableSet.copyOf((List<?>) HIVE_OUTPUT_INFO_CODEC.fromJson(connectorOutputMetadata.get()).get("partitionNames"));
    }

    private class QueryCompletedEventCollectorFactory
            implements EventListenerFactory
    {
        @Override
        public String getName()
        {
            return EVENT_LISTENER_NAME;
        }

        @Override
        public EventListener create(Map<String, String> config)
        {
            return new EventListener()
            {
                @Override
                public void queryCompleted(QueryCompletedEvent queryCompletedEvent)
                {
                    queryCompletedEvents.add(queryCompletedEvent);
                }
            };
        }
    }
}
