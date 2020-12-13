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
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.verifier.event.VerifierQueryEvent.EventStatus.FAILED;
import static com.facebook.presto.verifier.event.VerifierQueryEvent.EventStatus.SKIPPED;
import static com.facebook.presto.verifier.event.VerifierQueryEvent.EventStatus.SUCCEEDED;
import static com.facebook.presto.verifier.framework.DdlMatchResult.MatchType.CONTROL_NOT_PARSABLE;
import static com.facebook.presto.verifier.framework.DdlMatchResult.MatchType.MATCH;
import static com.facebook.presto.verifier.framework.DdlMatchResult.MatchType.MISMATCH;
import static com.facebook.presto.verifier.framework.DdlMatchResult.MatchType.TEST_NOT_PARSABLE;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestCreateTableVerification
        extends AbstractDdlVerificationTest
{
    public TestCreateTableVerification()
            throws Exception
    {
    }

    @Test
    public void testSucceeded()
    {
        String query = "CREATE TABLE succeeded (x int, ds varchar) COMMENT 'test table'";

        Optional<VerifierQueryEvent> event = runVerification(query, query);
        assertTrue(event.isPresent());
        assertEvent(event.get(), SUCCEEDED, Optional.of(MATCH), false);

        getQueryRunner().execute("CREATE TABLE like_table (x int, ds varchar)");
        query = "CREATE TABLE succeeded (LIKE like_table INCLUDING PROPERTIES)";

        event = runVerification(query, query);
        assertTrue(event.isPresent());
        System.out.println(event.get().getErrorMessage());
        assertEvent(event.get(), SUCCEEDED, Optional.of(MATCH), false);

        getQueryRunner().execute("DROP TABLE like_table");
    }

    @Test
    public void testSucceededExists()
    {
        getQueryRunner().execute("CREATE TABLE succeeded_exists (x int, ds varchar)");
        String query = "CREATE TABLE IF NOT EXISTS succeeded_exists (x int, ds varchar)";

        Optional<VerifierQueryEvent> event = runVerification(query, query);
        assertTrue(event.isPresent());
        assertEvent(event.get(), SUCCEEDED, Optional.of(MATCH), false);

        getQueryRunner().execute("DROP TABLE succeeded_exists");
    }

    @Test
    public void testControlNotParsable()
    {
        String query = "CREATE TABLE control_not_parsable (x int, ds varchar) COMMENT 'test table'";
        MockPrestoAction prestoAction = new MockPrestoAction(ImmutableMap.of(1, "CREATE TABLE succeeded (x int, ds varchar) 'test table'"));

        Optional<VerifierQueryEvent> event = verify(getSourceQuery(query, query), false, prestoAction);
        assertTrue(event.isPresent());
        System.out.println(event.get().getErrorMessage());
        assertEvent(event.get(), FAILED, Optional.of(CONTROL_NOT_PARSABLE), false);
    }

    @Test
    public void testTestNotParsable()
    {
        String query = "CREATE TABLE test_not_parsable (x int, ds varchar) COMMENT 'test table'";
        MockPrestoAction prestoAction = new MockPrestoAction(ImmutableMap.of(2, "CREATE TABLE test_not_parsable (x int, ds varchar) 'test table'"));

        Optional<VerifierQueryEvent> event = verify(getSourceQuery(query, query), false, prestoAction);
        assertTrue(event.isPresent());
        assertEvent(event.get(), FAILED, Optional.of(TEST_NOT_PARSABLE), false);
    }

    @Test
    public void testMismatched()
    {
        String query = "CREATE TABLE mismatched (x int, ds varchar)";
        MockPrestoAction prestoAction = new MockPrestoAction(ImmutableMap.of(
                1, "CREATE TABLE mismatched (x int, ds varchar)",
                2, "CREATE TABLE mismatched (ds varchar)"));

        Optional<VerifierQueryEvent> event = verify(getSourceQuery(query, query), false, prestoAction);
        assertTrue(event.isPresent());
        assertEvent(event.get(), FAILED, Optional.of(MISMATCH), false);
    }

    @Test
    public void testSkipped()
    {
        Optional<VerifierQueryEvent> event = runVerification(
                "CREATE TABLE failed (LIKE non_existing)",
                "CREATE TABLE failed (LIKE non_existing)");
        assertTrue(event.isPresent());
        assertEvent(event.get(), SKIPPED, Optional.empty(), false);
    }

    @Test
    public void testFailed()
    {
        Optional<VerifierQueryEvent> event = runVerification(
                "CREATE TABLE failed (x int, ds varchar)",
                "CREATE TABLE failed (LIKE non_existing)");
        assertTrue(event.isPresent());
        assertEvent(event.get(), FAILED, Optional.empty(), false);
    }
}
