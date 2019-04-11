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

import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.parser.SqlParserOptions;
import com.facebook.presto.tests.StandaloneQueryRunner;
import com.facebook.presto.verifier.event.FailureInfo;
import com.facebook.presto.verifier.retry.RetryConfig;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
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
import static com.facebook.presto.verifier.VerifierTestUtil.getJdbcUrl;
import static com.facebook.presto.verifier.VerifierTestUtil.setupPresto;
import static com.facebook.presto.verifier.framework.QueryException.Type.PRESTO;
import static com.facebook.presto.verifier.framework.QueryOrigin.QueryStage.MAIN;
import static com.facebook.presto.verifier.framework.QueryOrigin.TargetCluster.CONTROL;
import static com.facebook.presto.verifier.framework.QueryOrigin.forMain;
import static com.google.common.collect.Iterables.getOnlyElement;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestPrestoAction
{
    private static final QueryOrigin QUERY_ORIGIN = forMain(CONTROL);
    private static final SqlParser sqlParser = new SqlParser(new SqlParserOptions().allowIdentifierSymbol(COLON, AT_SIGN));
    private static StandaloneQueryRunner queryRunner;

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
        String jdbcUrl = getJdbcUrl(queryRunner);
        prestoAction = new PrestoAction(
                new PrestoExceptionClassifier(ImmutableSet.of(), ImmutableSet.of()),
                new VerifierConfig()
                        .setControlJdbcUrl(jdbcUrl)
                        .setTestJdbcUrl(jdbcUrl),
                new RetryConfig(),
                new RetryConfig());
    }

    @Test
    public void testQuerySucceeded()
    {
        assertEquals(
                prestoAction.execute(
                        sqlParser.createStatement("SELECT 1", new ParsingOptions(AS_DECIMAL)),
                        new QueryConfiguration(CATALOG, SCHEMA, "user", Optional.empty(), ImmutableMap.of()),
                        QUERY_ORIGIN,
                        new VerificationContext()).getState(),
                FINISHED.name());

        assertEquals(
                prestoAction.execute(
                        sqlParser.createStatement("CREATE TABLE test_table (x int)", new ParsingOptions(AS_DECIMAL)),
                        new QueryConfiguration(CATALOG, SCHEMA, "user", Optional.empty(), ImmutableMap.of()),
                        QUERY_ORIGIN,
                        new VerificationContext()).getState(),
                FINISHED.name());
    }

    @Test
    public void testQuerySucceededWithConverter()
    {
        QueryResult<Integer> result = prestoAction.execute(
                sqlParser.createStatement("SELECT x FROM (VALUES (1), (2), (3)) t(x)", new ParsingOptions(AS_DECIMAL)),
                new QueryConfiguration(CATALOG, SCHEMA, "user", Optional.empty(), ImmutableMap.of()),
                QUERY_ORIGIN,
                new VerificationContext(),
                resultSet -> resultSet.getInt("x") * resultSet.getInt("x"));
        assertEquals(result.getQueryStats().getState(), FINISHED.name());
        assertEquals(result.getResults(), ImmutableList.of(1, 4, 9));
    }

    @Test
    public void testQueryFailed()
    {
        VerificationContext context = new VerificationContext();
        try {
            prestoAction.execute(
                    sqlParser.createStatement("SELECT * FROM test_table", new ParsingOptions(AS_DECIMAL)),
                    new QueryConfiguration(CATALOG, SCHEMA, "user", Optional.empty(), ImmutableMap.of()),
                    QUERY_ORIGIN,
                    context);
            fail("Expect QueryException");
        }
        catch (QueryException qe) {
            assertEquals(qe.getType(), PRESTO);
            assertFalse(qe.isRetryable());
            assertEquals(qe.getErrorCode(), "PRESTO(SYNTAX_ERROR)");
            assertTrue(qe.getQueryStats().isPresent());
            assertEquals(qe.getQueryStats().get().getState(), FAILED.name());

            FailureInfo failureInfo = getOnlyElement(context.getAllFailures(CONTROL));
            assertEquals(failureInfo.getQueryStage(), MAIN.name());
            assertEquals(failureInfo.getErrorCode(), "PRESTO(SYNTAX_ERROR)");
            assertNotNull(failureInfo.getPrestoQueryId());
        }
    }
}
