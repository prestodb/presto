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

import com.facebook.presto.verifier.event.VerifierQueryEvent;
import com.facebook.presto.verifier.event.VerifierQueryEvent.EventStatus;
import com.google.common.base.Strings;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.verifier.event.VerifierQueryEvent.EventStatus.FAILED;
import static com.facebook.presto.verifier.event.VerifierQueryEvent.EventStatus.SUCCEEDED;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestExplainVerification
        extends AbstractVerificationTest
{
    public TestExplainVerification()
            throws Exception
    {
    }

    @Test
    public void testSuccess()
    {
        Optional<VerifierQueryEvent> event = runExplain("SHOW FUNCTIONS", "SHOW FUNCTIONS");
        assertTrue(event.isPresent());

        assertEvent(event.get(), SUCCEEDED);
        assertEquals(event.get().getMatchType(), "MATCH");
        assertEquals(event.get().getControlQueryInfo().getQuery().trim(), "EXPLAIN (FORMAT JSON)\nSHOW FUNCTIONS");
        assertEquals(event.get().getTestQueryInfo().getQuery().trim(), "EXPLAIN (FORMAT JSON)\nSHOW FUNCTIONS");
        assertNotNull(event.get().getControlQueryInfo().getJsonPlan());
        assertNotNull(event.get().getTestQueryInfo().getJsonPlan());
    }

    @Test
    public void testStructureMismatch()
    {
        getQueryRunner().execute("CREATE TABLE structure_mismatch (x int, ds varchar)");
        Optional<VerifierQueryEvent> event = runExplain(
                "SELECT count(*) FROM structure_mismatch",
                "SELECT count(*) FROM structure_mismatch CROSS JOIN structure_mismatch");
        assertTrue(event.isPresent());

        // Explain verification do not fail in case of plan changes.
        assertEvent(event.get(), FAILED);
        assertEquals(event.get().getMatchType(), "STRUCTURE_MISMATCH");
        assertEquals(event.get().getErrorCode(), "STRUCTURE_MISMATCH");
        assertTrue(
                event.get().getErrorMessage().endsWith(
                        "STRUCTURE_MISMATCH\n"
                                + "Differing Plan Node:\n"
                                + "  Output > Aggregate(FINAL) > LocalExchange > RemoteStreamingExchange > Aggregate(PARTIAL): TableScan -> CrossJoin"),
                event.get().getErrorMessage());
        assertEquals(event.get().getControlQueryInfo().getQuery().trim(), "EXPLAIN (FORMAT JSON)\nSELECT \"count\"(*)\nFROM\n  structure_mismatch");
        assertEquals(event.get().getTestQueryInfo().getQuery().trim(), "EXPLAIN (FORMAT JSON)\nSELECT \"count\"(*)\nFROM\n  (structure_mismatch\nCROSS JOIN structure_mismatch)");
        assertNotNull(event.get().getControlQueryInfo().getJsonPlan());
        assertNotNull(event.get().getTestQueryInfo().getJsonPlan());
    }

    @Test
    public void testDetailsMismatch()
    {
        Optional<VerifierQueryEvent> event = runExplain("SELECT 1", "SELECT 2");
        assertTrue(event.isPresent());

        // Explain verification do not fail in case of plan changes.
        assertEvent(event.get(), SUCCEEDED);
        assertEquals(event.get().getMatchType(), "DETAILS_MISMATCH");
        assertEquals(event.get().getControlQueryInfo().getQuery().trim(), "EXPLAIN (FORMAT JSON)\nSELECT 1");
        assertEquals(event.get().getTestQueryInfo().getQuery().trim(), "EXPLAIN (FORMAT JSON)\nSELECT 2");
        assertNotNull(event.get().getControlQueryInfo().getJsonPlan());
        assertNotNull(event.get().getTestQueryInfo().getJsonPlan());
    }

    @Test
    public void testFailure()
    {
        Optional<VerifierQueryEvent> event = runExplain("SELECT 1", "SELECT x");
        assertTrue(event.isPresent());

        assertEvent(event.get(), FAILED);
        assertNull(event.get().getMatchType());
        assertEquals(event.get().getControlQueryInfo().getQuery().trim(), "EXPLAIN (FORMAT JSON)\nSELECT 1");
        assertEquals(event.get().getTestQueryInfo().getQuery().trim(), "EXPLAIN (FORMAT JSON)\nSELECT x");
        assertNotNull(event.get().getControlQueryInfo().getJsonPlan());
        assertNull(event.get().getTestQueryInfo().getJsonPlan());
    }

    private void assertEvent(
            VerifierQueryEvent event,
            EventStatus expectedStatus)
    {
        assertEquals(event.getSuite(), SUITE);
        assertEquals(event.getTestId(), TEST_ID);
        assertEquals(event.getName(), NAME);
        assertEquals(event.getStatus(), expectedStatus.name());
    }

    @Test
    public void testRunningInQueryBankMode()
    {
        Optional<VerifierQueryEvent> event = runExplain("SELECT 1", "SELECT 2", saveSnapshotSettings);
        assertTrue(event.isPresent());
        assertEvent(event.get(), SUCCEEDED);

        event = runExplain("SELECT 1", "SELECT 2", queryBankModeSettings);
        assertTrue(event.isPresent());
        assertEvent(event.get(), SUCCEEDED);
    }

    @Test
    public void testExplainTypeIoPassthroughMatch()
    {
        getQueryRunner().execute("CREATE TABLE explain_io_match (x int, ds varchar)");
        Optional<VerifierQueryEvent> event = runExplain(
                "EXPLAIN (TYPE IO) SELECT * FROM explain_io_match",
                "EXPLAIN (TYPE IO) SELECT * FROM explain_io_match");
        assertTrue(event.isPresent());

        assertEvent(event.get(), SUCCEEDED);
        assertEquals(event.get().getMatchType(), "MATCH");
        // A source query that is already an EXPLAIN is run verbatim, not re-wrapped in EXPLAIN (FORMAT JSON).
        assertTrue(event.get().getControlQueryInfo().getQuery().trim().startsWith("EXPLAIN (TYPE IO)"));
        assertTrue(event.get().getTestQueryInfo().getQuery().trim().startsWith("EXPLAIN (TYPE IO)"));
        assertFalse(event.get().getControlQueryInfo().getQuery().contains("FORMAT JSON"));
        assertNotNull(event.get().getControlQueryInfo().getJsonPlan());
        assertNotNull(event.get().getTestQueryInfo().getJsonPlan());
    }

    @Test
    public void testExplainTypeIoPassthroughMismatch()
    {
        getQueryRunner().execute("CREATE TABLE explain_io_a (x int, ds varchar)");
        getQueryRunner().execute("CREATE TABLE explain_io_b (y int, ds varchar)");
        Optional<VerifierQueryEvent> event = runExplain(
                "EXPLAIN (TYPE IO) SELECT * FROM explain_io_a",
                "EXPLAIN (TYPE IO) SELECT * FROM explain_io_b");
        assertTrue(event.isPresent());

        assertEvent(event.get(), FAILED);
        assertEquals(event.get().getMatchType(), "STRUCTURE_MISMATCH");
        assertEquals(event.get().getErrorCode(), "STRUCTURE_MISMATCH");
        assertTrue(
                event.get().getErrorMessage().endsWith(
                        "STRUCTURE_MISMATCH\n"
                                + "Differing Fields:\n"
                                + "  inputTableColumnInfos[].table.schemaTable.table: \"explain_io_a\" -> \"explain_io_b\""),
                event.get().getErrorMessage());
        assertTrue(event.get().getControlQueryInfo().getQuery().trim().startsWith("EXPLAIN (TYPE IO)"));
        assertNotNull(event.get().getControlQueryInfo().getJsonPlan());
        assertNotNull(event.get().getTestQueryInfo().getJsonPlan());
    }

    @Test
    public void testExplainTypeIoPassthroughArrayLengthMismatch()
    {
        getQueryRunner().execute("CREATE TABLE explain_io_left (x int)");
        getQueryRunner().execute("CREATE TABLE explain_io_right (y int)");
        Optional<VerifierQueryEvent> event = runExplain(
                "EXPLAIN (TYPE IO) SELECT * FROM explain_io_left",
                "EXPLAIN (TYPE IO) SELECT * FROM explain_io_left CROSS JOIN explain_io_right");
        assertTrue(event.isPresent());

        assertEvent(event.get(), FAILED);
        assertEquals(event.get().getErrorCode(), "STRUCTURE_MISMATCH");
        assertTrue(
                event.get().getErrorMessage().endsWith(
                        "STRUCTURE_MISMATCH\n"
                                + "Differing Fields:\n"
                                + "  inputTableColumnInfos: 1 elements -> 2 elements"),
                event.get().getErrorMessage());
    }

    @Test
    public void testExplainTypeIoReportCaps()
    {
        StringBuilder control = new StringBuilder("{");
        StringBuilder test = new StringBuilder("{");
        for (int i = 0; i < 12; i++) {
            control.append(format("%s\"field%02d\": \"a\"", i == 0 ? "" : ", ", i));
            test.append(format("%s\"field%02d\": \"b\"", i == 0 ? "" : ", ", i));
        }
        String longValue = Strings.repeat("x", 300);
        control.append(format(", \"long\": \"%s\"}", longValue));
        test.append(", \"long\": \"y\"}");

        ExplainMatchResult result = ExplainVerification.matchRaw(control.toString(), test.toString());

        assertFalse(result.isMatched());
        assertEquals(
                result.getReport(),
                "STRUCTURE_MISMATCH\n"
                        + "Differing Fields:\n"
                        + "  field00: \"a\" -> \"b\"\n"
                        + "  field01: \"a\" -> \"b\"\n"
                        + "  field02: \"a\" -> \"b\"\n"
                        + "  field03: \"a\" -> \"b\"\n"
                        + "  field04: \"a\" -> \"b\"\n"
                        + "  field05: \"a\" -> \"b\"\n"
                        + "  field06: \"a\" -> \"b\"\n"
                        + "  field07: \"a\" -> \"b\"\n"
                        + "  field08: \"a\" -> \"b\"\n"
                        + "  field09: \"a\" -> \"b\"\n"
                        + "  ... and 3 more");
    }

    @Test
    public void testExplainTypeIoReportTruncatesLongValues()
    {
        String longValue = Strings.repeat("x", 300);
        ExplainMatchResult result = ExplainVerification.matchRaw(
                format("{\"long\": \"%s\"}", longValue),
                "{\"long\": \"y\"}");

        assertEquals(
                result.getReport(),
                format("STRUCTURE_MISMATCH\nDiffering Fields:\n  long: \"%s... -> \"y\"", Strings.repeat("x", 199)));
    }
}
