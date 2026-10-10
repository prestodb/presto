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

import com.facebook.presto.Session;
import com.facebook.presto.spark.execution.TaskInfoAggregationMode;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.eventlistener.EventListener;
import com.facebook.presto.spi.eventlistener.EventListenerFactory;
import com.facebook.presto.spi.eventlistener.QueryCompletedEvent;
import com.facebook.presto.testing.QueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static com.facebook.presto.spark.PrestoSparkSessionProperties.SPARK_TASK_INFO_AGGREGATION_MODE;
import static com.facebook.presto.spark.execution.TaskInfoAggregationMode.INCREMENTAL;
import static com.facebook.presto.spark.execution.TaskInfoAggregationMode.LEGACY;
import static com.facebook.presto.spark.execution.TaskInfoAggregationResult.METRIC_PREFIX;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Runs the history based tracking tests with incremental task info aggregation, and checks that it gives the history
 * based optimizer the same statistics as the legacy aggregation.
 */
@Test(singleThreaded = true)
public class TestPrestoSparkHistoryBasedTrackingWithIncrementalTaskInfoAggregation
        extends TestPrestoSparkHistoryBasedTracking
{
    private static final String EVENT_LISTENER_NAME = "plan-statistics-collector";

    private final List<QueryCompletedEvent> queryCompletedEvents = new CopyOnWriteArrayList<>();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        PrestoSparkQueryRunner queryRunner = (PrestoSparkQueryRunner) super.createQueryRunner();
        queryRunner.installPlugin(new Plugin()
        {
            @Override
            public Iterable<EventListenerFactory> getEventListenerFactories()
            {
                return ImmutableList.of(new EventListenerFactory()
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
                });
            }
        });
        queryRunner.getEventListenerManager().loadConfiguredEventListener(ImmutableMap.of("event-listener.name", EVENT_LISTENER_NAME));
        return queryRunner;
    }

    @Override
    protected Session getSession()
    {
        return getSession(INCREMENTAL);
    }

    @Test
    public void testRecordsSameStatisticsAsLegacy()
    {
        try {
            for (String sql : ImmutableList.of(
                    "SELECT regionkey, count(*) FROM nation WHERE nationkey > 5 GROUP BY regionkey",
                    "SELECT n.regionkey, count(*) FROM orders o JOIN nation n ON o.custkey % 25 = n.nationkey GROUP BY n.regionkey")) {
                QueryCompletedEvent legacyEvent = execute(LEGACY, sql);
                QueryCompletedEvent incrementalEvent = execute(INCREMENTAL, sql);
                assertTrue(incrementalEvent.getStatistics().getRuntimeStats().getMetrics().containsKey(METRIC_PREFIX + "received"));
                assertFalse(getPlanStatisticsWritten(incrementalEvent).isEmpty());
                assertEquals(getPlanStatisticsWritten(incrementalEvent), getPlanStatisticsWritten(legacyEvent));
            }
        }
        finally {
            getHistoryProvider().clearCache();
        }
    }

    private Session getSession(TaskInfoAggregationMode mode)
    {
        return Session.builder(super.getSession())
                .setSystemProperty(SPARK_TASK_INFO_AGGREGATION_MODE, mode.name())
                .build();
    }

    private QueryCompletedEvent execute(TaskInfoAggregationMode mode, String sql)
    {
        queryCompletedEvents.clear();
        getQueryRunner().execute(getSession(mode), sql);
        getHistoryProvider().waitProcessQueryEvents();
        return getOnlyElement(queryCompletedEvents);
    }

    private static List<String> getPlanStatisticsWritten(QueryCompletedEvent event)
    {
        return event.getPlanStatisticsWritten().stream()
                .map(statistics -> statistics.getId() + " " + statistics.getPlanStatistics())
                .sorted()
                .collect(toImmutableList());
    }
}
