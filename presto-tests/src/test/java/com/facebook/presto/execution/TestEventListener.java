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

import com.facebook.presto.Session;
import com.facebook.presto.execution.TestEventListenerPlugin.TestingEventListenerPlugin;
import com.facebook.presto.spi.eventlistener.QueryCompletedEvent;
import com.facebook.presto.spi.eventlistener.QueryCreatedEvent;
import com.facebook.presto.spi.eventlistener.SplitCompletedEvent;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.google.common.collect.Iterables.getOnlyElement;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestEventListener
{
    private QueryRunner queryRunner;
    private Session session;

    private EventsBuilder generateEvents(@Language("SQL") String sql)
            throws Exception
    {
        EventsBuilder generatedEvents = new EventsBuilder();

        try {
            queryRunner = new DistributedQueryRunner(testSessionBuilder().build(), 1);
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.installPlugin(new TestingEventListenerPlugin(generatedEvents));
            queryRunner.createCatalog("tpch", "tpch", ImmutableMap.of());
        }
        catch (Exception e) {
            queryRunner.close();
            throw e;
        }

        session = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema("tiny")
                .build();

        queryRunner.execute(session, sql);
        queryRunner.close();

        return generatedEvents;
    }

    @Test
    public void testConstantQuery()
            throws Exception
    {
        EventsBuilder events = generateEvents("SELECT 1");

        assertEquals(events.getQueryCreatedEvents().size(), 1);
        QueryCreatedEvent queryCreatedEvent = getOnlyElement(events.getQueryCreatedEvents());
        assertEquals(queryCreatedEvent.getContext().getEnvironment(), "testing");
        assertEquals(queryCreatedEvent.getMetadata().getQuery(), "SELECT 1");

        assertEquals(events.getQueryCompletedEvents().size(), 1);
        QueryCompletedEvent queryCompletedEvent = getOnlyElement(events.getQueryCompletedEvents());
        assertEquals(queryCompletedEvent.getStatistics().getTotalRows(), 0L);

        // TODO: make this an equality check after we fix final statistics collection
        assertTrue(events.getSplitCompletedEvents().size() >= queryCompletedEvent.getStatistics().getCompletedSplits());
    }

    @Test
    public void testNormalQuery()
            throws Exception
    {
        EventsBuilder events = generateEvents("SELECT sum(linenumber) FROM lineitem");

        assertEquals(events.getQueryCreatedEvents().size(), 1);
        QueryCreatedEvent queryCreatedEvent = getOnlyElement(events.getQueryCreatedEvents());
        assertEquals(queryCreatedEvent.getContext().getEnvironment(), "testing");
        assertEquals(queryCreatedEvent.getMetadata().getQuery(), "SELECT sum(linenumber) FROM lineitem");

        assertEquals(events.getQueryCompletedEvents().size(), 1);
        QueryCompletedEvent queryCompletedEvent = getOnlyElement(events.getQueryCompletedEvents());
        assertEquals(queryCompletedEvent.getIoMetadata().getOutput(), Optional.empty());
        assertEquals(queryCompletedEvent.getIoMetadata().getInputs().size(), 1);
        assertEquals(getOnlyElement(queryCompletedEvent.getIoMetadata().getInputs()).getConnectorId(), "tpch");

        // TODO: make this an equality check after we fix final statistics collection
        assertTrue(events.getSplitCompletedEvents().size() >= queryCompletedEvent.getStatistics().getCompletedSplits());
    }

    static class EventsBuilder
    {
        private final ImmutableList.Builder<QueryCreatedEvent> queryCreatedEvents = ImmutableList.builder();
        private final ImmutableList.Builder<QueryCompletedEvent> queryCompletedEvents = ImmutableList.builder();
        private final ImmutableList.Builder<SplitCompletedEvent> splitCompletedEvents = ImmutableList.builder();

        public void addQueryCreated(QueryCreatedEvent event)
        {
            queryCreatedEvents.add(event);
        }

        public void addQueryCompleted(QueryCompletedEvent event)
        {
            queryCompletedEvents.add(event);
        }

        public void addSplitCompleted(SplitCompletedEvent event)
        {
            splitCompletedEvents.add(event);
        }

        public List<QueryCreatedEvent> getQueryCreatedEvents()
        {
            return queryCreatedEvents.build();
        }

        public List<QueryCompletedEvent> getQueryCompletedEvents()
        {
            return queryCompletedEvents.build();
        }

        public List<SplitCompletedEvent> getSplitCompletedEvents()
        {
            return splitCompletedEvents.build();
        }
    }
}
