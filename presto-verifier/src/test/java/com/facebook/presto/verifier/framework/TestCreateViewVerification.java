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
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.verifier.event.VerifierQueryEvent.EventStatus.SKIPPED;
import static com.facebook.presto.verifier.event.VerifierQueryEvent.EventStatus.SUCCEEDED;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestCreateViewVerification
        extends AbstractTestVerification
{
    public TestCreateViewVerification()
            throws Exception
    {
    }

    @Test
    public void testCreateView()
    {
        getQueryRunner().execute("CREATE TABLE test_create_view (x int, y double)");
        String query = "CREATE VIEW v AS SELECT x + y as s FROM test_create_view";
        Optional<VerifierQueryEvent> event = runVerification(query, query);

        assertTrue(event.isPresent());
        assertEquals(event.get().getStatus(), SUCCEEDED.name());
        assertEvent(
                event.get(),
                SKIPPED,
                Optional.empty(),
                Optional.of("PRESTO(SYNTAX_ERROR)"),
                Optional.of("Test state NOT_RUN, Control state NOT_RUN.\n\n" +
                        "REWRITE query failed on CONTROL cluster:\n.*"));
    }
}
