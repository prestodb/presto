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
package com.facebook.presto.verifier.framework;

import com.facebook.presto.verifier.event.DeterminismAnalysisRun;
import com.facebook.presto.verifier.event.QueryInfo;
import com.facebook.presto.verifier.event.VerifierQueryEvent;
import com.facebook.presto.verifier.event.VerifierQueryEvent.EventStatus;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

import static com.facebook.presto.SystemSessionProperties.QUERY_MAX_EXECUTION_TIME;
import static com.facebook.presto.verifier.VerifierTestUtil.CATALOG;
import static com.facebook.presto.verifier.VerifierTestUtil.SCHEMA;
import static com.facebook.presto.verifier.event.VerifierQueryEvent.EventStatus.FAILED;
import static com.facebook.presto.verifier.event.VerifierQueryEvent.EventStatus.FAILED_RESOLVED;
import static com.facebook.presto.verifier.event.VerifierQueryEvent.EventStatus.SKIPPED;
import static com.facebook.presto.verifier.event.VerifierQueryEvent.EventStatus.SUCCEEDED;
import static com.facebook.presto.verifier.framework.DeterminismAnalysis.DETERMINISTIC;
import static com.facebook.presto.verifier.framework.DeterminismAnalysis.NON_DETERMINISTIC_COLUMNS;
import static com.facebook.presto.verifier.framework.SkippedReason.CONTROL_SETUP_QUERY_FAILED;
import static com.facebook.presto.verifier.framework.SkippedReason.FAILED_BEFORE_CONTROL_QUERY;
import static com.facebook.presto.verifier.framework.SkippedReason.NON_DETERMINISTIC;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.regex.Pattern.DOTALL;
import static java.util.stream.Collectors.joining;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestDataVerification
        extends AbstractVerificationTest
{
    public TestDataVerification()
            throws Exception
    {
    }

    @Test
    public void testSuccess()
    {
        Optional<VerifierQueryEvent> event = runVerification("SELECT 1.0", "SELECT 1.00001");
        assertTrue(event.isPresent());
        assertEvent(event.get(), SUCCEEDED, Optional.empty(), Optional.empty(), Optional.empty());

        getQueryRunner().execute("CREATE TABLE success_test (x double)");
        event = runVerification("INSERT INTO success_test SELECT 1.0", "INSERT INTO success_test SELECT 1.00001");
        assertTrue(event.isPresent());
        assertEvent(event.get(), SUCCEEDED, Optional.empty(), Optional.empty(), Optional.empty());
    }

    @Test
    public void testSchemaMismatch()
    {
        Optional<VerifierQueryEvent> event = runVerification("SELECT 1", "SELECT 1.00001");
        assertTrue(event.isPresent());
        assertEvent(
                event.get(),
                FAILED,
                Optional.empty(),
                Optional.of("SCHEMA_MISMATCH"),
                Optional.of("Test state SUCCEEDED, Control state SUCCEEDED.\n\n" +
                        "SCHEMA MISMATCH\n"));
    }

    @Test
    public void testRowCountMismatch()
    {
        Optional<VerifierQueryEvent> event = runVerification(
                "SELECT 1 x",
                "SELECT 1 x UNION ALL SELECT 1 x");
        assertTrue(event.isPresent());
        assertEvent(
                event.get(),
                FAILED,
                Optional.of(DETERMINISTIC),
                Optional.of("ROW_COUNT_MISMATCH"),
                Optional.of("Test state SUCCEEDED, Control state SUCCEEDED.\n\n" +
                        "ROW COUNT MISMATCH\n" +
                        "Control 1 rows, Test 2 rows\n"));
    }

    @Test
    public void testColumnMismatch()
    {
        Optional<VerifierQueryEvent> event = runVerification("SELECT 1.0", "SELECT 1.001");
        assertTrue(event.isPresent());
        assertEvent(
                event.get(),
                FAILED,
                Optional.of(DETERMINISTIC),
                Optional.of("COLUMN_MISMATCH"),
                Optional.of("Test state SUCCEEDED, Control state SUCCEEDED.\n\n" +
                        "COLUMN MISMATCH\n" +
                        "Control 1 rows, Test 1 rows\n" +
                        "Mismatched Columns:\n" +
                        "  _col0 \\(double\\) relative error: 9\\.995002498749525E-4\n" +
                        "    control\t\\(sum: 1\\.0, NaN: 0, \\+infinity: 0, -infinity: 0, mean: 1\\.0\\)\n" +
                        "    test\t\\(sum: 1\\.001, NaN: 0, \\+infinity: 0, -infinity: 0, mean: 1\\.001\\)\n"));
    }

    @Test
    public void testRewriteFailed()
    {
        Optional<VerifierQueryEvent> event = runVerification("SELECT * FROM test", "SELECT 1");
        assertTrue(event.isPresent());
        assertEquals(event.get().getSkippedReason(), FAILED_BEFORE_CONTROL_QUERY.name());
        assertEvent(
                event.get(),
                SKIPPED,
                Optional.empty(),
                Optional.of("PRESTO(SYNTAX_ERROR)"),
                Optional.of("Test state NOT_RUN, Control state NOT_RUN.\n\n" +
                        "REWRITE query failed on CONTROL cluster:\n.*"));
    }

    @Test
    public void testControlFailed()
    {
        Optional<VerifierQueryEvent> event = runVerification("INSERT INTO dest SELECT * FROM test", "SELECT 1");
        assertTrue(event.isPresent());
        assertEquals(event.get().getSkippedReason(), CONTROL_SETUP_QUERY_FAILED.name());
        assertEvent(
                event.get(),
                SKIPPED,
                Optional.empty(),
                Optional.of("PRESTO(SYNTAX_ERROR)"),
                Optional.of("Test state NOT_RUN, Control state FAILED_TO_SETUP.\n\n" +
                        "CONTROL SETUP query failed on CONTROL cluster:\n.*"));
    }

    @Test
    public void testNonDeterministic()
    {
        // Select
        Optional<VerifierQueryEvent> event = runVerification("SELECT rand()", "SELECT 2.0");
        assertTrue(event.isPresent());
        assertEquals(event.get().getSkippedReason(), NON_DETERMINISTIC.name());
        assertEvent(
                event.get(),
                SKIPPED,
                Optional.of(NON_DETERMINISTIC_COLUMNS),
                Optional.of("COLUMN_MISMATCH"),
                Optional.of("Test state SUCCEEDED, Control state SUCCEEDED.\n\n" +
                        "COLUMN MISMATCH\n" +
                        "Control 1 rows, Test 1 rows\n" +
                        "Mismatched Columns:\n" +
                        "  _col0 \\(double\\) relative error: .*\n" +
                        "    control\t\\(sum: .*, NaN: 0, \\+infinity: 0, -infinity: 0, mean: .*\\)\n" +
                        "    test\t\\(sum: 2\\.0, NaN: 0, \\+infinity: 0, -infinity: 0, mean: 2.0\\)\n"));

        List<DeterminismAnalysisRun> runs = event.get().getDeterminismAnalysisDetails().getRuns();
        assertEquals(runs.size(), 1);
        assertDeterminismAnalysisRun(runs.get(0), false);

        // Insert
        getQueryRunner().execute("CREATE TABLE non_deterministic_test (x double)");
        event = runVerification("INSERT INTO non_deterministic_test SELECT rand()", "INSERT INTO non_deterministic_test SELECT 2.0");
        assertTrue(event.isPresent());
        assertEquals(event.get().getSkippedReason(), NON_DETERMINISTIC.name());

        runs = event.get().getDeterminismAnalysisDetails().getRuns();
        assertEquals(runs.size(), 1);
        assertDeterminismAnalysisRun(runs.get(0), true);
    }

    @Test
    public void testArrayOfRow()
    {
        Optional<VerifierQueryEvent> event = runVerification("SELECT ARRAY[ROW(1, 'a'), ROW(2, null)]", "SELECT ARRAY[ROW(1, 'a'), ROW(2, null)]");
        assertTrue(event.isPresent());
        assertEvent(event.get(), SUCCEEDED, Optional.empty(), Optional.empty(), Optional.empty());

        event = runVerification("SELECT ARRAY[ROW(1, 'a'), ROW(2, 'b')]", "SELECT ARRAY[ROW(1, 'a'), ROW(2, null)]");
        assertTrue(event.isPresent());
        assertEvent(
                event.get(),
                FAILED,
                Optional.of(DETERMINISTIC),
                Optional.of("COLUMN_MISMATCH"),
                Optional.of("Test state SUCCEEDED, Control state SUCCEEDED.\n\n" +
                        "COLUMN MISMATCH\n" +
                        "Control 1 rows, Test 1 rows\n" +
                        "Mismatched Columns:\n" +
                        "  _col0 \\(array\\(row\\(integer, varchar\\(1\\)\\)\\)\\)\n" +
                        "    control\t\\(checksum: 71 b5 2f 7f 1e 9b a6 a4, cardinality_checksum: ad 20 38 f3 85 7c ba 56, cardinality_sum: 2\\)\n" +
                        "    test\t\\(checksum: b4 3c 7d 02 2b 14 77 12, cardinality_checksum: ad 20 38 f3 85 7c ba 56, cardinality_sum: 2\\)\n"));

        List<DeterminismAnalysisRun> runs = event.get().getDeterminismAnalysisDetails().getRuns();
        assertEquals(runs.size(), 3);
        assertDeterminismAnalysisRun(runs.get(0), false);
        assertDeterminismAnalysisRun(runs.get(1), false);
        assertDeterminismAnalysisRun(runs.get(2), false);
    }

    @Test
    public void testSelectDate()
    {
        Optional<VerifierQueryEvent> event = runVerification("SELECT date '2020-01-01', date(now()) today", "SELECT date '2020-01-01', date(now()) today");
        assertTrue(event.isPresent());
        assertEvent(event.get(), SUCCEEDED, Optional.empty(), Optional.empty(), Optional.empty());
    }

    @Test
    public void testSelectTime()
    {
        String query = "SELECT time '12:34:56'";
        Optional<VerifierQueryEvent> event = runVerification(query, query);
        assertTrue(event.isPresent());
        assertEvent(event.get(), SUCCEEDED, Optional.empty(), Optional.empty(), Optional.empty());
    }

    @Test
    public void testSelectTimestampWithTimeZone()
    {
        String query = "SELECT cast(timestamp '2020-04-01 12:34:56' AS timestamp with time zone)";
        Optional<VerifierQueryEvent> event = runVerification(query, query);
        assertTrue(event.isPresent());
        assertEvent(event.get(), SUCCEEDED, Optional.empty(), Optional.empty(), Optional.empty());
    }

    @Test
    public void testSelectUnknown()
    {
        Optional<VerifierQueryEvent> event = runVerification("SELECT null, null unknown", "SELECT null, null unknown");
        assertTrue(event.isPresent());
        assertEvent(event.get(), SUCCEEDED, Optional.empty(), Optional.empty(), Optional.empty());
    }

    @Test
    public void testSelectDecimal()
    {
        String query = "SELECT decimal '1.2'";
        Optional<VerifierQueryEvent> event = runVerification(query, query);
        assertTrue(event.isPresent());
        assertEvent(event.get(), SUCCEEDED, Optional.empty(), Optional.empty(), Optional.empty());
    }

    @Test
    public void testSelectNonStorableStructuredColumns()
    {
        String query = "SELECT\n" +
                "    ARRAY[DATE '2020-01-01'],\n" +
                "    ARRAY[NULL],\n" +
                "    MAP(\n" +
                "        ARRAY[DATE '2020-01-01'], ARRAY[\n" +
                "            CAST(ROW(1, 'a', DATE '2020-01-01') AS ROW(x int, y VARCHAR, z date))\n" +
                "        ]\n" +
                "    ),\n" +
                "    ROW(NULL)";
        Optional<VerifierQueryEvent> event = runVerification(query, query);
        assertTrue(event.isPresent());
        assertEvent(event.get(), SUCCEEDED, Optional.empty(), Optional.empty(), Optional.empty());
    }

    @Test
    public void testChecksumQueryCompilerError()
    {
        List<String> columns = IntStream.range(0, 1000).mapToObj(i -> "c" + i).collect(toImmutableList());
        getQueryRunner().execute(format("CREATE TABLE checksum_test (%s)", columns.stream().map(column -> column + " double").collect(joining(","))));

        String query = format("SELECT %s FROM checksum_test", Joiner.on(",").join(columns));
        Optional<VerifierQueryEvent> event = runVerification(query, query);

        assertTrue(event.isPresent());
        assertEquals(event.get().getStatus(), FAILED_RESOLVED.name());
        assertEquals(event.get().getErrorCode(), "PRESTO(GENERATED_BYTECODE_TOO_LARGE)");
        assertNotNull(event.get().getControlQueryInfo().getChecksumQuery());
        assertNotNull(event.get().getControlQueryInfo().getChecksumQueryId());
        assertNotNull(event.get().getTestQueryInfo().getChecksumQuery());
    }

    @Test
    public void testExecutionTimeSessionProperty()
    {
        QueryConfiguration configuration = new QueryConfiguration(CATALOG, SCHEMA, Optional.of("user"), Optional.empty(), Optional.of(ImmutableMap.of(QUERY_MAX_EXECUTION_TIME, "20m")));
        SourceQuery sourceQuery = new SourceQuery(SUITE, NAME, "SELECT 1.0", "SELECT 1.00001", configuration, configuration);
        Optional<VerifierQueryEvent> event = verify(sourceQuery, false);
        assertTrue(event.isPresent());
        assertEvent(event.get(), SUCCEEDED, Optional.empty(), Optional.empty(), Optional.empty());
    }

    private void assertEvent(
            VerifierQueryEvent event,
            EventStatus expectedStatus,
            Optional<DeterminismAnalysis> expectedDeterminismAnalysis,
            Optional<String> expectedErrorCode,
            Optional<String> expectedErrorMessageRegex)
    {
        assertEquals(event.getSuite(), SUITE);
        assertEquals(event.getTestId(), TEST_ID);
        assertEquals(event.getName(), NAME);
        assertEquals(event.getStatus(), expectedStatus.name());
        assertEquals(event.getDeterminismAnalysis(), expectedDeterminismAnalysis.map(DeterminismAnalysis::name).orElse(null));
        assertEquals(event.getErrorCode(), expectedErrorCode.orElse(null));
        if (event.getErrorMessage() == null) {
            assertFalse(expectedErrorMessageRegex.isPresent());
        }
        else {
            assertTrue(expectedErrorMessageRegex.isPresent());
            assertTrue(Pattern.compile(expectedErrorMessageRegex.get(), DOTALL).matcher(event.getErrorMessage()).matches());
        }

        if (event.getStatus().equals(SUCCEEDED.name())) {
            QueryType queryType = QueryType.of(getSqlParser().createStatement(event.getControlQueryInfo().getOriginalQuery(), PARSING_OPTIONS));
            assertSuccessQueryInfo(queryType, event.getControlQueryInfo());
            assertSuccessQueryInfo(queryType, event.getTestQueryInfo());
        }
    }

    private void assertDeterminismAnalysisRun(DeterminismAnalysisRun run, boolean hasSetup)
    {
        assertNotNull(run.getTableName());
        assertNotNull(run.getQueryId());
        assertNotNull(run.getSetupQueryIds());
        assertNotNull(run.getTeardownQueryIds());
        assertNotNull(run.getChecksumQueryId());

        if (hasSetup) {
            assertEquals(run.getSetupQueryIds().size(), 1);
        }
        assertEquals(run.getTeardownQueryIds().size(), 1);
    }

    private static void assertSuccessQueryInfo(QueryType queryType, QueryInfo queryInfo)
    {
        assertNotNull(queryInfo.getQuery());
        assertNotNull(queryInfo.getSessionProperties());
        assertNotNull(queryInfo.getSetupQueries());
        assertNotNull(queryInfo.getTeardownQueries());
        assertEquals(queryInfo.getTeardownQueries().size(), 1);

        assertNotNull(queryInfo.getQueryId());
        assertNotNull(queryInfo.getSetupQueryIds());
        assertNotNull(queryInfo.getTeardownQueryIds());
        assertEquals(queryInfo.getTeardownQueryIds().size(), 1);

        if (queryType == QueryType.INSERT) {
            assertEquals(queryInfo.getSetupQueries().size(), 1);
            assertEquals(queryInfo.getSetupQueryIds().size(), 1);
        }

        assertNotNull(queryInfo.getOutputTableName());

        assertNotNull(queryInfo.getCpuTimeSecs());
        assertNotNull(queryInfo.getWallTimeSecs());
        assertNotNull(queryInfo.getPeakTotalMemoryBytes());
        assertNotNull(queryInfo.getPeakTaskTotalMemoryBytes());
    }
}
