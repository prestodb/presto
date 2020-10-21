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
package com.facebook.presto.verifier.prestoaction;

import com.facebook.presto.jdbc.QueryStats;
import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.parser.SqlParserOptions;
import com.facebook.presto.tests.StandaloneQueryRunner;
import com.facebook.presto.verifier.event.QueryFailure;
import com.facebook.presto.verifier.framework.PrestoQueryException;
import com.facebook.presto.verifier.framework.QueryConfiguration;
import com.facebook.presto.verifier.framework.QueryResult;
import com.facebook.presto.verifier.framework.QueryStage;
import com.facebook.presto.verifier.framework.VerificationContext;
import com.facebook.presto.verifier.framework.VerifierConfig;
import com.facebook.presto.verifier.retry.RetryConfig;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.execution.QueryState.FAILED;
import static com.facebook.presto.execution.QueryState.FINISHED;
import static com.facebook.presto.sql.parser.IdentifierSymbol.AT_SIGN;
import static com.facebook.presto.sql.parser.IdentifierSymbol.COLON;
import static com.facebook.presto.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL;
import static com.facebook.presto.verifier.VerifierTestUtil.CATALOG;
import static com.facebook.presto.verifier.VerifierTestUtil.SCHEMA;
import static com.facebook.presto.verifier.VerifierTestUtil.setupPresto;
import static com.facebook.presto.verifier.framework.ClusterType.CONTROL;
import static com.facebook.presto.verifier.framework.QueryStage.CONTROL_MAIN;
import static com.google.common.collect.Iterables.getOnlyElement;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestJdbcPrestoAction
{
    private static final String SUITE = "test-suite";
    private static final String NAME = "test-query";
    private static final QueryStage QUERY_STAGE = CONTROL_MAIN;
    private static final QueryConfiguration CONFIGURATION = new QueryConfiguration(CATALOG, SCHEMA, Optional.of("user"), Optional.empty(), Optional.empty());
    private static final SqlParser sqlParser = new SqlParser(new SqlParserOptions().allowIdentifierSymbol(COLON, AT_SIGN));
    private static final ParsingOptions PARSING_OPTIONS = ParsingOptions.builder().setDecimalLiteralTreatment(AS_DECIMAL).build();

    private static StandaloneQueryRunner queryRunner;

    private VerificationContext verificationContext;
    private PrestoAction prestoAction;

    @BeforeClass
    public void setupQueryRunner()
            throws Exception
    {
        queryRunner = setupPresto();
    }

    @BeforeMethod
    public void setup()
    {
        QueryActionsConfig queryActionsConfig = new QueryActionsConfig();
        verificationContext = VerificationContext.create(SUITE, NAME);
        PrestoActionConfig prestoActionConfig = new PrestoActionConfig()
                .setHosts(queryRunner.getServer().getAddress().getHost())
                .setJdbcPort(queryRunner.getServer().getAddress().getPort());
        prestoAction = new JdbcPrestoAction(
                PrestoExceptionClassifier.defaultBuilder().build(),
                CONFIGURATION,
                verificationContext,
                new JdbcUrlSelector(prestoActionConfig.getJdbcUrls()),
                prestoActionConfig,
                queryActionsConfig.getMetadataTimeout(),
                queryActionsConfig.getChecksumTimeout(),
                new RetryConfig(),
                new RetryConfig(),
                new VerifierConfig().setTestId("test"));
    }

    @Test
    public void testQuerySucceeded()
    {
        assertEquals(
                prestoAction.execute(
                        sqlParser.createStatement("SELECT 1", PARSING_OPTIONS),
                        QUERY_STAGE).getQueryStats().map(QueryStats::getState).orElse(null),
                FINISHED.name());

        assertEquals(
                prestoAction.execute(
                        sqlParser.createStatement("CREATE TABLE test_table (x int)", PARSING_OPTIONS),
                        QUERY_STAGE).getQueryStats().map(QueryStats::getState).orElse(null),
                FINISHED.name());
    }

    @Test
    public void testQuerySucceededWithConverter()
    {
        QueryResult<Integer> result = prestoAction.execute(
                sqlParser.createStatement("SELECT x FROM (VALUES (1), (2), (3)) t(x)", PARSING_OPTIONS),
                QUERY_STAGE,
                resultSet -> Optional.of(resultSet.getInt("x") * resultSet.getInt("x")));
        assertEquals(result.getQueryActionStats().getQueryStats().map(QueryStats::getState).orElse(null), FINISHED.name());
        assertEquals(result.getResults(), ImmutableList.of(1, 4, 9));
    }

    @Test
    public void testQueryFailed()
    {
        try {
            prestoAction.execute(
                    sqlParser.createStatement("SELECT * FROM test_table", PARSING_OPTIONS),
                    QUERY_STAGE);
            fail("Expect QueryException");
        }
        catch (PrestoQueryException e) {
            assertFalse(e.isRetryable());
            assertEquals(e.getErrorCodeName(), "PRESTO(SYNTAX_ERROR)");
            assertTrue(e.getQueryActionStats().getQueryStats().isPresent());
            assertEquals(e.getQueryActionStats().getQueryStats().map(QueryStats::getState).orElse(null), FAILED.name());

            QueryFailure queryFailure = getOnlyElement(verificationContext.getQueryFailures());
            assertEquals(queryFailure.getClusterType(), CONTROL.name());
            assertEquals(queryFailure.getQueryStage(), QUERY_STAGE.name());
            assertEquals(queryFailure.getErrorCode(), "PRESTO(SYNTAX_ERROR)");
            assertFalse(queryFailure.isRetryable());
            assertNotNull(queryFailure.getPrestoQueryId());
        }
    }
}
