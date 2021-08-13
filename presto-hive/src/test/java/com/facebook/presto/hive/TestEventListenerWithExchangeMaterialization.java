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
package com.facebook.presto.hive;

import com.facebook.presto.Session;
import com.facebook.presto.execution.StageId;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.eventlistener.EventListener;
import com.facebook.presto.spi.eventlistener.EventListenerFactory;
import com.facebook.presto.spi.eventlistener.QueryCompletedEvent;
import com.facebook.presto.spi.eventlistener.QueryCreatedEvent;
import com.facebook.presto.spi.eventlistener.QueryMetadata;
import com.facebook.presto.spi.eventlistener.SplitCompletedEvent;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.hive.HiveQueryRunner.createQueryRunner;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.tpch.TpchTable.getTables;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestEventListenerWithExchangeMaterialization
{
    private EventsBuilder generatedEvents = new EventsBuilder();
    private DistributedQueryRunner queryRunner;
    private Session session;

    @BeforeClass
    private void setUp()
            throws Exception
    {
        queryRunner = createQueryRunner(
                getTables(),
                new ImmutableMap.Builder<String, String>()
                        .put("query.partitioning-provider-catalog", "hive")
                        .put("query.exchange-materialization-strategy", "ALL")
                        .put("experimental.runtime-optimizer-enabled", "true")
                        .put("experimental.enable-stats-collection-for-temporary-table", "true")
                        .put("join-distribution-type", "PARTITIONED")
                        .put("optimizer.join-reordering-strategy", "ELIMINATE_CROSS_JOINS")
                        .put("query.hash-partition-count", "11")
                        .put("colocated-joins-enabled", "true")
                        .put("grouped-execution-enabled", "true")
                        .build(),
                Optional.empty());
        queryRunner.installPlugin(new TestingEventListenerPlugin(generatedEvents));
        session = queryRunner.getDefaultSession();
        generatedEvents.initialize(16);
        // Wait until all tpch table copying queries finish populating completedEvents.
        generatedEvents.waitForEvents(180);
    }

    @AfterClass(alwaysRun = true)
    private void tearDown()
    {
        queryRunner.close();
        queryRunner = null;
        session = null;
        generatedEvents = null;
    }

    private QueryId runQueryAndWaitForEvents(@Language("SQL") String sql, int numEventsExpected)
            throws Exception
    {
        generatedEvents.initialize(numEventsExpected);
        QueryId resultId = queryRunner.executeWithQueryId(session, sql).getQueryId();
        generatedEvents.waitForEvents(600);
        return resultId;
    }

    @Test
    public void testRuntimeOptimizedStagesCorrectness()
            throws Exception
    {
        // We expect one runtime optimized stage: 1.
        int expectedEvents = 2;
        QueryId queryId = runQueryAndWaitForEvents("SELECT phone, regionkey FROM nation INNER JOIN supplier ON supplier.nationkey=nation.nationkey", expectedEvents);
        QueryCreatedEvent queryCreatedEvent = getOnlyElement(generatedEvents.getQueryCreatedEvents());
        QueryCompletedEvent queryCompletedEvent = getOnlyElement(generatedEvents.getQueryCompletedEvents());
        QueryMetadata queryMetadata = queryCompletedEvent.getMetadata();
        Optional<List<StageId>> runtimeOptimizedStages = queryRunner.getCoordinator().getQueryManager().getFullQueryInfo(new QueryId(queryCreatedEvent.getMetadata().getQueryId())).getRuntimeOptimizedStages();

        assertEquals(queryMetadata.getQueryId(), queryId.toString());
        assertEquals(queryMetadata.getRuntimeOptimizedStages().size(), 1);
        assertEquals(queryMetadata.getRuntimeOptimizedStages().get(0), "1");
        assertTrue(runtimeOptimizedStages.isPresent());
        assertEquals(runtimeOptimizedStages.get().size(), 1);
        assertEquals(queryMetadata.getRuntimeOptimizedStages(), runtimeOptimizedStages.get().stream()
                .map(stageId -> String.valueOf(stageId.getId()))
                .collect(toImmutableList()));

        // Now, the following query should not trigger runtime optimizations, so should have empty list of runtime optimized stages.
        runQueryAndWaitForEvents("SELECT phone, regionkey FROM supplier INNER JOIN nation ON supplier.nationkey=nation.nationkey", expectedEvents);
        queryCreatedEvent = getOnlyElement(generatedEvents.getQueryCreatedEvents());
        queryCompletedEvent = getOnlyElement(generatedEvents.getQueryCompletedEvents());
        runtimeOptimizedStages = queryRunner.getCoordinator().getQueryManager().getFullQueryInfo(new QueryId(queryCreatedEvent.getMetadata().getQueryId())).getRuntimeOptimizedStages();

        assertTrue(queryCompletedEvent.getMetadata().getRuntimeOptimizedStages().isEmpty());
        assertFalse(runtimeOptimizedStages.isPresent());

        // Now, the following query should have two optimized joins in a single stage (both on the same nationkey), therefore expect only one optimized stage: 1.
        runQueryAndWaitForEvents("SELECT supplier.phone, regionkey, custkey FROM nation INNER JOIN supplier ON supplier.nationkey=nation.nationkey INNER JOIN customer ON nation.nationkey=customer.nationkey", expectedEvents);
        queryCreatedEvent = getOnlyElement(generatedEvents.getQueryCreatedEvents());
        queryCompletedEvent = getOnlyElement(generatedEvents.getQueryCompletedEvents());
        queryMetadata = queryCompletedEvent.getMetadata();
        runtimeOptimizedStages = queryRunner.getCoordinator().getQueryManager().getFullQueryInfo(new QueryId(queryCreatedEvent.getMetadata().getQueryId())).getRuntimeOptimizedStages();

        assertEquals(queryMetadata.getRuntimeOptimizedStages().size(), 1);
        assertEquals(queryMetadata.getRuntimeOptimizedStages().get(0), "1");
        assertTrue(runtimeOptimizedStages.isPresent());
        assertEquals(runtimeOptimizedStages.get().size(), 1);
        assertEquals(queryMetadata.getRuntimeOptimizedStages(), runtimeOptimizedStages.get().stream()
                .map(stageId -> String.valueOf(stageId.getId()))
                .collect(toImmutableList()));

        // Now, the following query should have two runtime optimized stages: 1 and 4, corresponding to the two join operations (on regionkey and nationkey respectively).
        runQueryAndWaitForEvents("WITH natreg AS (SELECT nation.regionkey, nationkey, region.name FROM region INNER JOIN nation ON nation.regionkey=region.regionkey) SELECT phone, regionkey FROM natreg INNER JOIN supplier ON supplier.nationkey=natreg.nationkey", expectedEvents);
        queryCreatedEvent = getOnlyElement(generatedEvents.getQueryCreatedEvents());
        queryCompletedEvent = getOnlyElement(generatedEvents.getQueryCompletedEvents());
        queryMetadata = queryCompletedEvent.getMetadata();
        runtimeOptimizedStages = queryRunner.getCoordinator().getQueryManager().getFullQueryInfo(new QueryId(queryCreatedEvent.getMetadata().getQueryId())).getRuntimeOptimizedStages();

        assertEquals(queryMetadata.getRuntimeOptimizedStages().size(), 2);
        assertEquals(ImmutableSet.copyOf(queryMetadata.getRuntimeOptimizedStages()), ImmutableSet.of("1", "4"));
        assertTrue(runtimeOptimizedStages.isPresent());
        assertEquals(runtimeOptimizedStages.get().size(), 2);
        assertEquals(queryMetadata.getRuntimeOptimizedStages(), runtimeOptimizedStages.get().stream()
                .map(stageId -> String.valueOf(stageId.getId()))
                .collect(toImmutableList()));
    }

    static class TestingEventListenerPlugin
            implements Plugin
    {
        private final EventsBuilder eventsBuilder;

        public TestingEventListenerPlugin(EventsBuilder eventsBuilder)
        {
            this.eventsBuilder = requireNonNull(eventsBuilder, "eventsBuilder is null");
        }

        @Override
        public Iterable<EventListenerFactory> getEventListenerFactories()
        {
            return ImmutableList.of(new TestingEventListenerFactory(eventsBuilder));
        }
    }

    private static class TestingEventListenerFactory
            implements EventListenerFactory
    {
        private final EventsBuilder eventsBuilder;

        public TestingEventListenerFactory(EventsBuilder eventsBuilder)
        {
            this.eventsBuilder = eventsBuilder;
        }

        @Override
        public String getName()
        {
            return "test";
        }

        @Override
        public EventListener create(Map<String, String> config)
        {
            return new TestingEventListener(eventsBuilder);
        }
    }

    private static class TestingEventListener
            implements EventListener
    {
        private final EventsBuilder eventsBuilder;

        public TestingEventListener(EventsBuilder eventsBuilder)
        {
            this.eventsBuilder = eventsBuilder;
        }

        @Override
        public void queryCreated(QueryCreatedEvent queryCreatedEvent)
        {
            eventsBuilder.addQueryCreated(queryCreatedEvent);
        }

        @Override
        public void queryCompleted(QueryCompletedEvent queryCompletedEvent)
        {
            eventsBuilder.addQueryCompleted(queryCompletedEvent);
        }

        @Override
        public void splitCompleted(SplitCompletedEvent splitCompletedEvent)
        {
        }
    }

    static class EventsBuilder
    {
        private ImmutableList.Builder<QueryCreatedEvent> queryCreatedEvents;
        private ImmutableList.Builder<QueryCompletedEvent> queryCompletedEvents;

        private CountDownLatch eventsLatch;

        public synchronized void initialize(int numEvents)
        {
            queryCreatedEvents = ImmutableList.builder();
            queryCompletedEvents = ImmutableList.builder();

            eventsLatch = new CountDownLatch(numEvents);
        }

        public void waitForEvents(int timeoutSeconds)
                throws InterruptedException
        {
            eventsLatch.await(timeoutSeconds, TimeUnit.SECONDS);
        }

        public synchronized void addQueryCreated(QueryCreatedEvent event)
        {
            queryCreatedEvents.add(event);
            eventsLatch.countDown();
        }

        public synchronized void addQueryCompleted(QueryCompletedEvent event)
        {
            queryCompletedEvents.add(event);
            eventsLatch.countDown();
        }

        public List<QueryCreatedEvent> getQueryCreatedEvents()
        {
            return queryCreatedEvents.build();
        }

        public List<QueryCompletedEvent> getQueryCompletedEvents()
        {
            return queryCompletedEvents.build();
        }
    }
}
