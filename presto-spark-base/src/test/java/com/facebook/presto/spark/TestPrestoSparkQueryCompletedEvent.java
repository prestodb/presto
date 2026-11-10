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
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.eventlistener.EventListener;
import com.facebook.presto.spi.eventlistener.EventListenerFactory;
import com.facebook.presto.spi.eventlistener.OperatorStatistics;
import com.facebook.presto.spi.eventlistener.QueryCompletedEvent;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import static com.facebook.airlift.json.JsonCodec.mapJsonCodec;
import static com.facebook.presto.spark.PrestoSparkQueryRunner.createHivePrestoSparkQueryRunner;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.SPARK_MAX_TASK_INFOS_IN_QUERY_COMPLETED_EVENT;
import static com.facebook.presto.spark.execution.AbstractPrestoSparkQueryExecution.EXECUTOR_TASK_INFOS_DROPPED_METRIC;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.tpch.TpchTable.NATION;
import static java.lang.String.format;
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

    @Test
    public void testWrittenPartitions()
    {
        QueryCompletedEvent event = createPartitionedTable(getSession(), "test_written_partitions");
        assertTrue(getStageIds(event).size() > 1);
        assertNull(event.getStatistics().getRuntimeStats().getMetric(EXECUTOR_TASK_INFOS_DROPPED_METRIC));
        assertEquals(getWrittenPartitions(event), EXPECTED_PARTITIONS);
    }

    @Test
    public void testWrittenPartitionsWhenTaskInfoLimitExceeded()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(SPARK_MAX_TASK_INFOS_IN_QUERY_COMPLETED_EVENT, "0")
                .build();
        QueryCompletedEvent event = createPartitionedTable(session, "test_written_partitions_task_info_limit_exceeded");
        // Only the root stage, which ran on the driver, keeps its statistics
        assertEquals(getStageIds(event).size(), 1);
        assertTrue(event.getStatistics().getRuntimeStats().getMetric(EXECUTOR_TASK_INFOS_DROPPED_METRIC).getSum() > 0);
        assertEquals(getWrittenPartitions(event), EXPECTED_PARTITIONS);
    }

    @Test
    public void testReadOnlyQueryWhenTaskInfoLimitExceeded()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(SPARK_MAX_TASK_INFOS_IN_QUERY_COMPLETED_EVENT, "0")
                .build();
        queryCompletedEvents.clear();
        assertEquals(computeActual(session, "SELECT count(*) FROM nation").getOnlyValue(), 25L);
        QueryCompletedEvent event = getOnlyElement(queryCompletedEvents);
        // No stage ran on the driver, so no statistics are kept
        assertEquals(getStageIds(event), ImmutableSet.of());
        assertTrue(event.getStatistics().getRuntimeStats().getMetric(EXECUTOR_TASK_INFOS_DROPPED_METRIC).getSum() > 0);
        assertFalse(event.getIoMetadata().getOutput().isPresent());
    }

    private QueryCompletedEvent createPartitionedTable(Session session, String tableName)
    {
        queryCompletedEvents.clear();
        // The aggregation adds a hash partitioned stage, so the query has more than one executor task
        assertUpdate(
                session,
                format("CREATE TABLE %s WITH (partitioned_by = ARRAY['regionkey']) AS SELECT count(*) nations, regionkey FROM nation GROUP BY regionkey", tableName),
                5);
        return getOnlyElement(queryCompletedEvents);
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
