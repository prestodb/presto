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

import com.facebook.presto.benchmark.event.BenchmarkPhaseEvent;
import com.facebook.presto.benchmark.event.BenchmarkPhaseEvent.Status;
import com.facebook.presto.benchmark.event.BenchmarkQueryEvent;
import com.facebook.presto.benchmark.framework.BenchmarkQuery;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.benchmark.BenchmarkTestUtil.CATALOG;
import static com.facebook.presto.benchmark.BenchmarkTestUtil.SCHEMA;
import static com.facebook.presto.benchmark.event.BenchmarkPhaseEvent.Status.FAILED;
import static com.facebook.presto.benchmark.event.BenchmarkPhaseEvent.Status.SUCCEEDED;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class TestConcurrentPhaseExecutor
{
    private static final String TEST_ID = "test-id";
    private static final String PHASE_NAME = "test=phase";
    private static final List<BenchmarkQuery> ALL_QUERIES = ImmutableList.of(
            new BenchmarkQuery("Q1", "SELECT 1", CATALOG, SCHEMA),
            new BenchmarkQuery("Q2", "SELECT 2", CATALOG, SCHEMA),
            new BenchmarkQuery("Q3", "SELECT 3", CATALOG, SCHEMA));

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

    private ConcurrentPhaseExecutor createConcurrentPhaseExecutor(boolean failQueries)
    {
        return new ConcurrentPhaseExecutor(PHASE_NAME, new MockQueryExecutor(failQueries), ALL_QUERIES, new HashMap<>(), 50);
    }

    private void assertBenchmarkPhaseEvent(BenchmarkPhaseEvent event, Status expectedStatus)
    {
        assertEquals(event.getName(), PHASE_NAME);
        assertEquals(event.getStatus(), expectedStatus.name());
    }

    @Test
    public void testSuccess()
    {
        BenchmarkPhaseEvent phaseEvent = createConcurrentPhaseExecutor(false).run();
        assertNotNull(phaseEvent);
        assertBenchmarkPhaseEvent(phaseEvent, SUCCEEDED);
    }

    @Test
    public void testFailure()
    {
        BenchmarkPhaseEvent phaseEvent = createConcurrentPhaseExecutor(true).run();
        assertNotNull(phaseEvent);
        assertBenchmarkPhaseEvent(phaseEvent, FAILED);
    }
}
