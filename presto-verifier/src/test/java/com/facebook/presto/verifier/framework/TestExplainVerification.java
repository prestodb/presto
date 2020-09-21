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

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.tests.StandaloneQueryRunner;
import com.facebook.presto.verifier.event.VerifierQueryEvent;
import com.facebook.presto.verifier.event.VerifierQueryEvent.EventStatus;
import com.facebook.presto.verifier.prestoaction.JdbcPrestoAction;
import com.facebook.presto.verifier.prestoaction.PrestoAction;
import com.facebook.presto.verifier.prestoaction.PrestoActionConfig;
import com.facebook.presto.verifier.prestoaction.PrestoExceptionClassifier;
import com.facebook.presto.verifier.prestoaction.QueryActions;
import com.facebook.presto.verifier.prestoaction.QueryActionsConfig;
import com.facebook.presto.verifier.retry.RetryConfig;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.verifier.VerifierTestUtil.CATALOG;
import static com.facebook.presto.verifier.VerifierTestUtil.SCHEMA;
import static com.facebook.presto.verifier.VerifierTestUtil.setupPresto;
import static com.facebook.presto.verifier.event.VerifierQueryEvent.EventStatus.FAILED;
import static com.facebook.presto.verifier.event.VerifierQueryEvent.EventStatus.SUCCEEDED;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestExplainVerification
{
    private static final String SUITE = "test-suite";
    private static final String NAME = "test-query";
    private static final String TEST_ID = "test-id";
    private static final QueryConfiguration QUERY_CONFIGURATION = new QueryConfiguration(CATALOG, SCHEMA, Optional.of("user"), Optional.empty(), Optional.empty());
    private static final SqlParser sqlParser = new SqlParser();

    private static StandaloneQueryRunner queryRunner;

    @BeforeClass
    public void setupClass()
            throws Exception
    {
        queryRunner = setupPresto();
    }

    @Test
    public void testSuccess()
    {
        Optional<VerifierQueryEvent> event = runVerification("SHOW FUNCTIONS");
        assertTrue(event.isPresent());

        assertEvent(event.get(), SUCCEEDED);
        assertEquals(event.get().getMatchType(), "MATCH");
        assertEquals(event.get().getControlQueryInfo().getQuery().trim(), "EXPLAIN (FORMAT JSON)\nSHOW FUNCTIONS");
        assertEquals(event.get().getTestQueryInfo().getQuery().trim(), "EXPLAIN (FORMAT JSON)\nSHOW FUNCTIONS");
        assertNotNull(event.get().getControlQueryInfo().getJsonPlan());
        assertNotNull(event.get().getTestQueryInfo().getJsonPlan());
    }

    @Test
    public void testPlanChanged()
    {
        Optional<VerifierQueryEvent> event = runVerification("SELECT 1", "SELECT 2");
        assertTrue(event.isPresent());

        // Explain verification do not fail in case of plan changes.
        assertEvent(event.get(), SUCCEEDED);
        assertEquals(event.get().getMatchType(), "PLAN_CHANGED");
        assertEquals(event.get().getControlQueryInfo().getQuery().trim(), "EXPLAIN (FORMAT JSON)\nSELECT 1");
        assertEquals(event.get().getTestQueryInfo().getQuery().trim(), "EXPLAIN (FORMAT JSON)\nSELECT 2");
        assertNotNull(event.get().getControlQueryInfo().getJsonPlan());
        assertNotNull(event.get().getTestQueryInfo().getJsonPlan());
    }

    @Test
    public void testFailure()
    {
        Optional<VerifierQueryEvent> event = runVerification("SELECT 1", "SELECT x");
        assertTrue(event.isPresent());

        assertEvent(event.get(), FAILED);
        assertNull(event.get().getMatchType());
        assertEquals(event.get().getControlQueryInfo().getQuery().trim(), "EXPLAIN (FORMAT JSON)\nSELECT 1");
        assertEquals(event.get().getTestQueryInfo().getQuery().trim(), "EXPLAIN (FORMAT JSON)\nSELECT x");
        assertNotNull(event.get().getControlQueryInfo().getJsonPlan());
        assertNull(event.get().getTestQueryInfo().getJsonPlan());
    }

    private Optional<VerifierQueryEvent> runVerification(String query)
    {
        return runVerification(query, query);
    }

    private Optional<VerifierQueryEvent> runVerification(String controlQuery, String testQuery)
    {
        SourceQuery sourceQuery = new SourceQuery(SUITE, NAME, controlQuery, testQuery, QUERY_CONFIGURATION, QUERY_CONFIGURATION);
        PrestoExceptionClassifier exceptionClassifier = PrestoExceptionClassifier.defaultBuilder().build();
        VerificationContext verificationContext = VerificationContext.create(NAME, SUITE);
        VerifierConfig verifierConfig = new VerifierConfig().setTestId(TEST_ID);
        RetryConfig retryConfig = new RetryConfig();
        QueryActionsConfig queryActionsConfig = new QueryActionsConfig();
        PrestoAction prestoAction = new JdbcPrestoAction(
                exceptionClassifier,
                sourceQuery.getControlConfiguration(),
                verificationContext,
                new PrestoActionConfig()
                        .setHosts(queryRunner.getServer().getAddress().getHost())
                        .setJdbcPort(queryRunner.getServer().getAddress().getPort()),
                queryActionsConfig.getMetadataTimeout(),
                queryActionsConfig.getChecksumTimeout(),
                retryConfig,
                retryConfig,
                verifierConfig);
        return new ExplainVerification(
                new QueryActions(prestoAction, prestoAction, prestoAction),
                sourceQuery,
                exceptionClassifier,
                verificationContext,
                verifierConfig,
                sqlParser).run().getEvent();
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
