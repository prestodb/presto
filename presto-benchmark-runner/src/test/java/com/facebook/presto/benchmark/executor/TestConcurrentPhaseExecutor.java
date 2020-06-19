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
package com.facebook.presto.benchmark.executor;

import com.facebook.airlift.event.client.AbstractEventClient;
import com.facebook.presto.benchmark.event.BenchmarkPhaseEvent;
import com.facebook.presto.benchmark.event.BenchmarkPhaseEvent.Status;
import com.facebook.presto.benchmark.event.BenchmarkQueryEvent;
import com.facebook.presto.benchmark.framework.BenchmarkQuery;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.benchmark.BenchmarkTestUtil.CATALOG;
import static com.facebook.presto.benchmark.BenchmarkTestUtil.SCHEMA;
import static com.facebook.presto.benchmark.event.BenchmarkPhaseEvent.Status.COMPLETED_WITH_FAILURES;
import static com.facebook.presto.benchmark.event.BenchmarkPhaseEvent.Status.FAILED;
import static com.facebook.presto.benchmark.event.BenchmarkPhaseEvent.Status.SUCCEEDED;
import static com.google.common.base.Preconditions.checkArgument;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class TestConcurrentPhaseExecutor
{
    private static final String TEST_ID = "test-id";
    private static final String PHASE_NAME = "test=phase";
    private static final List<BenchmarkQuery> ALL_QUERIES = ImmutableList.of(
            new BenchmarkQuery("Q1", "SELECT 1", CATALOG, SCHEMA, Optional.empty()),
            new BenchmarkQuery("Q2", "SELECT 2", CATALOG, SCHEMA, Optional.empty()),
            new BenchmarkQuery("Q3", "SELECT 3", CATALOG, SCHEMA, Optional.empty()));
    private static final List<String> QUERY_NAMES = ImmutableList.of("Q1", "Q2", "Q3");

    private static class MockQueryExecutor
            implements QueryExecutor
    {
        private final boolean failQueries;

        public MockQueryExecutor(boolean failQueries)
        {
            this.failQueries = failQueries;
        }

        @Override
        public BenchmarkQueryEvent run(BenchmarkQuery benchmarkQuery, Map<String, String> sessionProperties)
        {
            if (failQueries) {
                return new BenchmarkQueryEvent(
                        TEST_ID,
                        benchmarkQuery.getName(),
                        BenchmarkQueryEvent.Status.FAILED,
                        benchmarkQuery.getCatalog(),
                        benchmarkQuery.getSchema(),
                        benchmarkQuery.getQuery(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty());
            }
            else {
                return new BenchmarkQueryEvent(
                        TEST_ID,
                        benchmarkQuery.getName(),
                        BenchmarkQueryEvent.Status.SUCCEEDED,
                        benchmarkQuery.getCatalog(),
                        benchmarkQuery.getSchema(),
                        benchmarkQuery.getQuery(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty());
            }
        }
    }

    private static class MockEventClient
            extends AbstractEventClient
    {
        private final List<BenchmarkPhaseEvent> events = new ArrayList<>();

        @Override
        protected <T> void postEvent(T event)
        {
            checkArgument(event instanceof BenchmarkPhaseEvent);
            this.events.add((BenchmarkPhaseEvent) event);
        }

        public List<BenchmarkPhaseEvent> getEvents()
        {
            return events;
        }
    }

    private ConcurrentPhaseExecutor createConcurrentPhaseExecutor(boolean failQueries, MockEventClient eventClient)
    {
        return new ConcurrentPhaseExecutor(PHASE_NAME, new MockQueryExecutor(failQueries), ALL_QUERIES, ImmutableSet.of(eventClient), new HashMap<>(), 50);
    }

    private void assertBenchmarkPhaseEvent(BenchmarkPhaseEvent event, Status expectedStatus)
    {
        assertEquals(event.getName(), PHASE_NAME);
        assertEquals(event.getStatus(), expectedStatus.name());
    }

    @Test
    public void testSuccess()
    {
        MockEventClient eventClient = new MockEventClient();
        BenchmarkPhaseEvent phaseEvent = createConcurrentPhaseExecutor(false, eventClient).run(false);
        assertNotNull(phaseEvent);
        assertBenchmarkPhaseEvent(phaseEvent, SUCCEEDED);
        List<BenchmarkPhaseEvent> postedEvents = eventClient.getEvents();
        assertEquals(postedEvents.size(), 1);
        assertBenchmarkPhaseEvent(postedEvents.get(0), SUCCEEDED);
    }

    @Test
    public void testFailOnFailure()
    {
        MockEventClient eventClient = new MockEventClient();
        BenchmarkPhaseEvent phaseEvent = createConcurrentPhaseExecutor(true, eventClient).run(false);
        assertNotNull(phaseEvent);
        assertBenchmarkPhaseEvent(phaseEvent, FAILED);
        List<BenchmarkPhaseEvent> postedEvents = eventClient.getEvents();
        assertEquals(postedEvents.size(), 1);
        assertBenchmarkPhaseEvent(postedEvents.get(0), FAILED);
    }

    @Test
    public void testContinueOnFailure()
    {
        MockEventClient eventClient = new MockEventClient();
        BenchmarkPhaseEvent phaseEvent = createConcurrentPhaseExecutor(true, eventClient).run(true);
        assertNotNull(phaseEvent);
        assertBenchmarkPhaseEvent(phaseEvent, COMPLETED_WITH_FAILURES);
        List<BenchmarkPhaseEvent> postedEvents = eventClient.getEvents();
        assertEquals(postedEvents.size(), 1);
        assertBenchmarkPhaseEvent(postedEvents.get(0), COMPLETED_WITH_FAILURES);
    }
}
