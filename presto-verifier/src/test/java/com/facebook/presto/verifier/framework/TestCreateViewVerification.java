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

import com.facebook.presto.tpch.TpchPlugin;
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
public class TestCreateViewVerification
        extends AbstractDdlVerificationTest
{
    public TestCreateViewVerification()
            throws Exception
    {
        getQueryRunner().installPlugin(new TpchPlugin());
        getQueryRunner().createCatalog("tpch", "tpch");
    }

    @Test
    public void testSucceededNotExists()
    {
        String query = "CREATE VIEW succeeded_not_exists AS SELECT * FROM tpch.tiny.nation";

        Optional<VerifierQueryEvent> event = runVerification(query, query);
        assertTrue(event.isPresent());
        assertEvent(event.get(), SUCCEEDED, Optional.of(MATCH), false);
    }

    @Test
    public void testSucceededExists()
    {
        getQueryRunner().execute("CREATE VIEW succeeded_exists SECURITY INVOKER AS SELECT * FROM tpch.tiny.customer");
        String query = "CREATE OR REPLACE VIEW succeeded_exists SECURITY DEFINER AS SELECT * FROM tpch.tiny.nation";

        Optional<VerifierQueryEvent> event = runVerification(query, query);
        assertTrue(event.isPresent());
        assertEvent(event.get(), SUCCEEDED, Optional.of(MATCH), true);

        getQueryRunner().execute("DROP VIEW succeeded_exists");
    }

    @Test
    public void testSkippedExists()
    {
        getQueryRunner().execute("CREATE VIEW skipped_exists AS SELECT * FROM tpch.tiny.customer");
        String query = "CREATE VIEW skipped_exists AS SELECT * FROM tpch.tiny.nation";

        Optional<VerifierQueryEvent> event = runVerification(query, query);
        assertTrue(event.isPresent());
        assertEvent(event.get(), SKIPPED, Optional.empty(), true);

        getQueryRunner().execute("DROP VIEW skipped_exists");
    }

    @Test
    public void testControlNotParsable()
    {
        String query = "CREATE VIEW control_not_parsable AS SELECT * FROM tpch.tiny.nation";
        MockPrestoAction prestoAction = new MockPrestoAction(ImmutableMap.of(3, "CREATE VIEW control_not_parsable SELECT * FROM tpch.tiny.nation"));

        Optional<VerifierQueryEvent> event = verify(getSourceQuery(query, query), false, prestoAction);
        assertTrue(event.isPresent());
        assertEvent(event.get(), FAILED, Optional.of(CONTROL_NOT_PARSABLE), false);
    }

    @Test
    public void testTestNotParsable()
    {
        String query = "CREATE VIEW test_not_parsable AS SELECT * FROM tpch.tiny.nation";
        MockPrestoAction prestoAction = new MockPrestoAction(ImmutableMap.of(4, "CREATE VIEW test_not_parsable SELECT * FROM tpch.tiny.nation"));

        Optional<VerifierQueryEvent> event = verify(getSourceQuery(query, query), false, prestoAction);
        assertTrue(event.isPresent());
        assertEvent(event.get(), FAILED, Optional.of(TEST_NOT_PARSABLE), false);
    }

    @Test
    public void testMismatched()
    {
        String query = "CREATE VIEW mismatch AS SELECT * FROM tpch.tiny.nation";
        MockPrestoAction prestoAction = new MockPrestoAction(ImmutableMap.of(
                3, "CREATE VIEW mismatch AS SELECT * FROM tpch.tiny.nation",
                4, "CREATE VIEW mismatch SECURITY DEFINER AS SELECT * FROM tpch.tiny.nation"));

        Optional<VerifierQueryEvent> event = verify(getSourceQuery(query, query), false, prestoAction);
        assertTrue(event.isPresent());
        assertEvent(event.get(), FAILED, Optional.of(MISMATCH), false);
    }

    @Test
    public void testColumnMismatched()
    {
        getQueryRunner().execute("CREATE VIEW column_mismatched AS SELECT * FROM tpch.tiny.customer");
        Optional<VerifierQueryEvent> event = runVerification(
                "CREATE OR REPLACE VIEW column_mismatched AS SELECT name FROM tpch.tiny.nation",
                "CREATE OR REPLACE VIEW column_mismatched AS SELECT concat(name, name) name FROM tpch.tiny.nation");
        assertTrue(event.isPresent());
        assertEvent(event.get(), FAILED, Optional.of(MISMATCH), true);

        getQueryRunner().execute("DROP VIEW column_mismatched");
    }

    @Test
    public void testFailed()
    {
        getQueryRunner().execute("CREATE VIEW failed AS SELECT * FROM tpch.tiny.customer");
        Optional<VerifierQueryEvent> event = runVerification(
                "CREATE OR REPLACE VIEW failed AS SELECT name FROM tpch.tiny.nation",
                "CREATE OR REPLACE VIEW failed AS SELECT nonexistent_column FROM tpch.tiny.nation");
        assertTrue(event.isPresent());
        assertEvent(event.get(), FAILED, Optional.empty(), true);

        getQueryRunner().execute("DROP VIEW failed");
    }
}
