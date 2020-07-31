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

import com.facebook.presto.benchmark.event.BenchmarkPhaseEvent;
import com.facebook.presto.benchmark.event.BenchmarkQueryEvent;
import com.facebook.presto.benchmark.prestoaction.BenchmarkPrestoActionFactory;
import com.facebook.presto.benchmark.prestoaction.PrestoActionFactory;
import com.facebook.presto.benchmark.prestoaction.PrestoClusterConfig;
import com.facebook.presto.benchmark.prestoaction.PrestoExceptionClassifier;
import com.facebook.presto.benchmark.retry.RetryConfig;
import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.parser.SqlParserOptions;
import com.facebook.presto.tests.StandaloneQueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.airlift.testing.Closeables.closeQuietly;
import static com.facebook.presto.benchmark.BenchmarkTestUtil.CATALOG;
import static com.facebook.presto.benchmark.BenchmarkTestUtil.SCHEMA;
import static com.facebook.presto.benchmark.BenchmarkTestUtil.setupPresto;
import static com.facebook.presto.sql.parser.IdentifierSymbol.AT_SIGN;
import static com.facebook.presto.sql.parser.IdentifierSymbol.COLON;
import static com.facebook.presto.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestConcurrentExecutor
        extends AbstractExecutorTest
{
    private static final int MAX_CONCURRENCY = 1;

    private static final BenchmarkQuery QUERY_1 = new BenchmarkQuery("Q1", "SELECT 1", CATALOG, SCHEMA, Optional.empty());
    private static final BenchmarkQuery QUERY_2 = new BenchmarkQuery("Q2", "SELECT 2", CATALOG, SCHEMA, Optional.empty());
    private static final BenchmarkQuery QUERY_BAD = new BenchmarkQuery("QBad", "SELECT a", CATALOG, SCHEMA, Optional.empty());
    private static final ConcurrentExecutionPhase PHASE_GOOD = new ConcurrentExecutionPhase("good", ImmutableList.of("Q1", "Q2", "Q1"), Optional.of(MAX_CONCURRENCY));
    private static final ConcurrentExecutionPhase PHASE_BAD = new ConcurrentExecutionPhase("bad", ImmutableList.of("Q1", "QBad", "Q2"), Optional.of(MAX_CONCURRENCY));
    private static final BenchmarkSuite SUITE = new BenchmarkSuite(
            "test-suite",
            "test-query-set",
            ImmutableList.of(PHASE_GOOD, PHASE_BAD),
            ImmutableMap.of(),
            ImmutableList.of(QUERY_1, QUERY_2, QUERY_BAD));

    private static StandaloneQueryRunner queryRunner;

    @BeforeClass
    public void setupClass()
            throws Exception
    {
        queryRunner = setupPresto();
    }

    @AfterClass
    public void destroy()
    {
        closeQuietly(queryRunner);
    }

    @Test
    public void testSucceeded()
    {
        BenchmarkPhaseEvent phaseEvent = createExecutor(new BenchmarkRunnerConfig().setMaxConcurrency(10)).runPhase(PHASE_GOOD, SUITE);

        // check phase events
        assertEvent(phaseEvent, "good", BenchmarkPhaseEvent.Status.SUCCEEDED);
        assertTrue(getEventClient().getPhaseEvents().isEmpty());

        // check query events
        ListMultimap<String, BenchmarkQueryEvent> queryEvents = getEventClient().getQueryEventsByName();
        assertEquals(queryEvents.size(), 3);
        assertEquals(queryEvents.get("Q1").size(), 2);
        assertEvent(queryEvents.get("Q1").get(0), "Q1", BenchmarkQueryEvent.Status.SUCCEEDED);
        assertEvent(queryEvents.get("Q1").get(1), "Q1", BenchmarkQueryEvent.Status.SUCCEEDED);
        assertEvent(getOnlyElement(queryEvents.get("Q2")), "Q2", BenchmarkQueryEvent.Status.SUCCEEDED);
    }

    @Test
    public void testFailed()
    {
        BenchmarkPhaseEvent phaseEvent = createExecutor().runPhase(PHASE_BAD, SUITE);

        // check phase events
        assertEvent(phaseEvent, "bad", BenchmarkPhaseEvent.Status.FAILED);
        assertTrue(getEventClient().getPhaseEvents().isEmpty());

        // check query events
        ListMultimap<String, BenchmarkQueryEvent> queryEvents = getEventClient().getQueryEventsByName();
        assertEvent(getOnlyElement(queryEvents.get("Q1")), "Q1", BenchmarkQueryEvent.Status.SUCCEEDED);
        assertEvent(getOnlyElement(queryEvents.get("QBad")), "QBad", BenchmarkQueryEvent.Status.FAILED);
    }

    @Test
    public void testCompletedWithFailure()
    {
        BenchmarkPhaseEvent phaseEvent = createExecutor(new BenchmarkRunnerConfig().setContinueOnFailure(true)).runPhase(PHASE_BAD, SUITE);

        // check phase events
        assertEvent(phaseEvent, "bad", BenchmarkPhaseEvent.Status.COMPLETED_WITH_FAILURES);
        assertTrue(getEventClient().getPhaseEvents().isEmpty());

        // check query events
        ListMultimap<String, BenchmarkQueryEvent> queryEvents = getEventClient().getQueryEventsByName();
        assertEvent(getOnlyElement(queryEvents.get("Q1")), "Q1", BenchmarkQueryEvent.Status.SUCCEEDED);
        assertEvent(getOnlyElement(queryEvents.get("Q2")), "Q2", BenchmarkQueryEvent.Status.SUCCEEDED);
        assertEvent(getOnlyElement(queryEvents.get("QBad")), "QBad", BenchmarkQueryEvent.Status.FAILED);
    }

    private ConcurrentPhaseExecutor createExecutor()
    {
        return createExecutor(new BenchmarkRunnerConfig());
    }

    private ConcurrentPhaseExecutor createExecutor(BenchmarkRunnerConfig config)
    {
        SqlParser sqlParser = new SqlParser(new SqlParserOptions().allowIdentifierSymbol(COLON, AT_SIGN));
        ParsingOptions parsingOptions = ParsingOptions.builder().setDecimalLiteralTreatment(AS_DOUBLE).build();
        PrestoActionFactory prestoActionFactory = new BenchmarkPrestoActionFactory(
                new PrestoExceptionClassifier(ImmutableSet.of()),
                new PrestoClusterConfig().setJdbcUrl(format(
                        "jdbc:presto://%s:%s",
                        queryRunner.getServer().getAddress().getHost(),
                        queryRunner.getServer().getAddress().getPort())),
                new RetryConfig());

        return new ConcurrentPhaseExecutor(
                sqlParser,
                parsingOptions,
                prestoActionFactory,
                ImmutableSet.of(getEventClient()),
                config.setTestId(TEST_ID));
    }
}
