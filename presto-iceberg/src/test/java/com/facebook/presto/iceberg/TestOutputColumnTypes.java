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

package com.facebook.presto.iceberg;

import com.facebook.presto.Session;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.eventlistener.Column;
import com.facebook.presto.spi.eventlistener.EventListener;
import com.facebook.presto.spi.eventlistener.EventListenerFactory;
import com.facebook.presto.spi.eventlistener.QueryCompletedEvent;
import com.facebook.presto.spi.eventlistener.QueryCreatedEvent;
import com.facebook.presto.spi.eventlistener.SplitCompletedEvent;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableList;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.assertj.core.api.Assertions.assertThat;

@Test(singleThreaded = true)
public class TestOutputColumnTypes
        extends AbstractTestQueryFramework
{
    public static final String ICEBERG_CATALOG = "iceberg";
    private static final Duration EVENT_TIMEOUT = Duration.ofSeconds(10);

    private QueryRunner queryRunner;
    private final CatalogType catalogType;
    private final EventsBuilder generatedEvents = new EventsBuilder();
    private final Session session;

    public TestOutputColumnTypes()
            throws Exception
    {
        this.catalogType = CatalogType.HIVE;
        this.queryRunner = createQueryRunner();
        this.queryRunner.installPlugin(new TestingEventListenerPlugin(generatedEvents));
        session = testSessionBuilder()
                .setCatalog(ICEBERG_CATALOG)
                .setSchema("tpch")
                .build();
    }

    @AfterClass(alwaysRun = true)
    private void tearDown()
    {
        queryRunner.close();
        queryRunner = null;
    }

    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder().build().getQueryRunner();
    }

    private MaterializedResult runQueryAndWaitForEvents(@Language("SQL") String sql, int numEventsExpected)
            throws Exception
    {
        return runQueryAndWaitForEvents(sql, numEventsExpected, session);
    }

    private MaterializedResult runQueryAndWaitForEvents(@Language("SQL") String sql, int numEventsExpected, Session session)
            throws Exception
    {
        generatedEvents.initialize(numEventsExpected);
        MaterializedResult result = queryRunner.execute(session, sql);
        generatedEvents.waitForEvents(EVENT_TIMEOUT);
        return result;
    }

    @Test
    public void testOutputColumnsForInsertAsSelect()
            throws Exception
    {
        runQueryAndWaitForEvents("CREATE TABLE create_insert_table1 AS SELECT clerk, orderkey, totalprice FROM orders", 2);
        runQueryAndWaitForEvents("INSERT INTO create_insert_table1 SELECT clerk, orderkey, totalprice FROM orders", 2);
        QueryCompletedEvent event = getOnlyElement(generatedEvents.getQueryCompletedEvents());

        assertThat(event.getIoMetadata().getOutput().get().getCatalogName()).isEqualTo("iceberg");
        assertThat(event.getIoMetadata().getOutput().get().getSchema()).isEqualTo("tpch");
        assertThat(event.getIoMetadata().getOutput().get().getTable()).isEqualTo("create_insert_table1");
        assertThat(event.getMetadata().getUpdateQueryType().get()).isEqualTo("INSERT");

        assertThat(event.getIoMetadata().getOutput().get().getColumns().get())
                .containsExactly(
                        new Column("clerk", "varchar"),
                        new Column("orderkey", "bigint"),
                        new Column("totalprice", "double"));
    }

    @Test
    public void testOutputColumnsForCreateTableAS()
            throws Exception
    {
        runQueryAndWaitForEvents("CREATE TABLE create_update_table AS SELECT * FROM orders ", 2);
        QueryCompletedEvent event = getOnlyElement(generatedEvents.getQueryCompletedEvents());

        assertThat(event.getIoMetadata().getOutput().get().getCatalogName()).isEqualTo("iceberg");
        assertThat(event.getIoMetadata().getOutput().get().getSchema()).isEqualTo("tpch");
        assertThat(event.getIoMetadata().getOutput().get().getTable()).isEqualTo("create_update_table");
        assertThat(event.getMetadata().getUpdateQueryType().get()).isEqualTo("CREATE TABLE");

        assertThat(event.getIoMetadata().getOutput().get().getColumns().get())
                .containsExactly(
                        new Column("orderkey", "bigint"),
                        new Column("custkey", "bigint"),
                        new Column("orderstatus", "varchar"),
                        new Column("totalprice", "double"),
                        new Column("orderdate", "date"),
                        new Column("orderpriority", "varchar"),
                        new Column("clerk", "varchar"),
                        new Column("shippriority", "integer"),
                        new Column("comment", "varchar"));
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
            eventsBuilder.addSplitCompleted(splitCompletedEvent);
        }
    }

    static class EventsBuilder
    {
        private ImmutableList.Builder<QueryCreatedEvent> queryCreatedEvents;
        private ImmutableList.Builder<QueryCompletedEvent> queryCompletedEvents;
        private ImmutableList.Builder<SplitCompletedEvent> splitCompletedEvents;

        private CountDownLatch eventsLatch;

        public synchronized void initialize(int numEvents)
        {
            queryCreatedEvents = ImmutableList.builder();
            queryCompletedEvents = ImmutableList.builder();
            splitCompletedEvents = ImmutableList.builder();

            eventsLatch = new CountDownLatch(numEvents);
        }
        public void waitForEvents(Duration duration)
                throws InterruptedException
        {
            eventsLatch.await(duration.toNanos(), NANOSECONDS);
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

        public synchronized void addSplitCompleted(SplitCompletedEvent event)
        {
            splitCompletedEvents.add(event);
        }

        public List<QueryCompletedEvent> getQueryCompletedEvents()
        {
            return queryCompletedEvents.build();
        }
    }
}
