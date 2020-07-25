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
package com.facebook.presto.benchmark.framework;

import com.facebook.airlift.event.client.AbstractEventClient;
import com.facebook.presto.benchmark.event.BenchmarkPhaseEvent;
import com.facebook.presto.benchmark.event.BenchmarkQueryEvent;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ListMultimap;
import org.testng.annotations.BeforeMethod;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.benchmark.BenchmarkTestUtil.CATALOG;
import static com.facebook.presto.benchmark.BenchmarkTestUtil.SCHEMA;
import static java.lang.String.format;
import static java.util.function.Function.identity;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public abstract class AbstractExecutorTest
{
    protected static final String TEST_ID = "test-id";

    private MockEventClient eventClient;

    @BeforeMethod
    public void setup()
    {
        this.eventClient = new MockEventClient();
    }

    public MockEventClient getEventClient()
    {
        return eventClient;
    }

    protected static void assertEvent(BenchmarkQueryEvent event, String name, BenchmarkQueryEvent.Status status)
    {
        assertEquals(event.getTestId(), TEST_ID);
        assertEquals(event.getName(), name);
        assertEquals(event.getStatus(), status.name());
        assertEquals(event.getCatalog(), CATALOG);
        assertEquals(event.getSchema(), SCHEMA);
        assertNotNull(event.getQuery());
        assertNotNull(event.getQueryId());
        assertNotNull(event.getCpuTime());
        assertNotNull(event.getWallTime());

        assertEquals(event.getErrorCode() == null, status == BenchmarkQueryEvent.Status.SUCCEEDED);
        assertEquals(event.getErrorMessage() == null, status == BenchmarkQueryEvent.Status.SUCCEEDED);
        assertEquals(event.getStackTrace() == null, status == BenchmarkQueryEvent.Status.SUCCEEDED);
    }

    protected static void assertEvent(BenchmarkPhaseEvent event, String phaseName, BenchmarkPhaseEvent.Status status)
    {
        assertEquals(event.getName(), phaseName);
        assertEquals(event.getStatus(), status.name());
    }

    protected static class MockEventClient
            extends AbstractEventClient
    {
        private final List<BenchmarkQueryEvent> queryEvents = new ArrayList<>();
        private final List<BenchmarkPhaseEvent> phaseEvents = new ArrayList<>();

        @Override
        public <T> void postEvent(T event)
        {
            if (event instanceof BenchmarkQueryEvent) {
                queryEvents.add((BenchmarkQueryEvent) event);
            }
            else if (event instanceof BenchmarkPhaseEvent) {
                phaseEvents.add((BenchmarkPhaseEvent) event);
            }
            else {
                throw new IllegalArgumentException(format("Unsupported event type %s", event.getClass()));
            }
        }

        public ListMultimap<String, BenchmarkQueryEvent> getQueryEventsByName()
        {
            return queryEvents.stream()
                    .collect(ImmutableListMultimap.toImmutableListMultimap(BenchmarkQueryEvent::getName, identity()));
        }

        public List<BenchmarkPhaseEvent> getPhaseEvents()
        {
            return phaseEvents;
        }
    }
}
