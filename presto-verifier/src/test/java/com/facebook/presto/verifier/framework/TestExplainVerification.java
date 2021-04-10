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
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.verifier.event.VerifierQueryEvent.EventStatus.FAILED;
import static com.facebook.presto.verifier.event.VerifierQueryEvent.EventStatus.SUCCEEDED;
import static org.testng.Assert.assertEquals;
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
        assertEvent(event.get(), SUCCEEDED);
        assertEquals(event.get().getMatchType(), "STRUCTURE_MISMATCH");
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
}
