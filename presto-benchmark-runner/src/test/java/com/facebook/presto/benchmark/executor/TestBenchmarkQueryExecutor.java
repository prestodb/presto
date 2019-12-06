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
package com.facebook.presto.benchmark.executor;

import com.facebook.presto.benchmark.event.BenchmarkQueryEvent;
import com.facebook.presto.benchmark.event.BenchmarkQueryEvent.Status;
import com.facebook.presto.benchmark.framework.BenchmarkQuery;
import com.facebook.presto.benchmark.framework.BenchmarkRunnerConfig;
import com.facebook.presto.benchmark.framework.QueryException;
import com.facebook.presto.benchmark.framework.QueryResult;
import com.facebook.presto.benchmark.prestoaction.JdbcPrestoAction;
import com.facebook.presto.benchmark.prestoaction.PrestoAction;
import com.facebook.presto.benchmark.prestoaction.PrestoClusterConfig;
import com.facebook.presto.benchmark.prestoaction.PrestoExceptionClassifier;
import com.facebook.presto.benchmark.retry.RetryConfig;
import com.facebook.presto.jdbc.QueryStats;
import com.facebook.presto.spi.ErrorCodeSupplier;
import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.parser.SqlParserOptions;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.tests.StandaloneQueryRunner;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Optional;

import static com.facebook.presto.benchmark.BenchmarkTestUtil.CATALOG;
import static com.facebook.presto.benchmark.BenchmarkTestUtil.SCHEMA;
import static com.facebook.presto.benchmark.BenchmarkTestUtil.setupPresto;
import static com.facebook.presto.benchmark.event.BenchmarkQueryEvent.Status.FAILED;
import static com.facebook.presto.benchmark.event.BenchmarkQueryEvent.Status.SUCCEEDED;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.sql.parser.IdentifierSymbol.AT_SIGN;
import static com.facebook.presto.sql.parser.IdentifierSymbol.COLON;
import static com.facebook.presto.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class TestBenchmarkQueryExecutor
{
    private static final String NAME = "test-query";
    private static final String TEST_ID = "test-id";
    private final SqlParser sqlParser = new SqlParser(new SqlParserOptions().allowIdentifierSymbol(COLON, AT_SIGN));
    private final ParsingOptions parsingOptions = ParsingOptions.builder().setDecimalLiteralTreatment(AS_DOUBLE).build();
    private StandaloneQueryRunner queryRunner;

    private static class MockPrestoAction
            implements PrestoAction
    {
        private final ErrorCodeSupplier errorCode;

        public MockPrestoAction(ErrorCodeSupplier errorCode)
        {
            this.errorCode = requireNonNull(errorCode, "errorCode is null");
        }

        @Override
        public QueryStats execute(Statement statement)
        {
            throw QueryException.forPresto(new RuntimeException(), Optional.of(errorCode), Optional.empty());
        }

        @Override
        public <R> QueryResult<R> execute(
                Statement statement,
                ResultSetConverter<R> converter)
        {
            throw QueryException.forPresto(new RuntimeException(), Optional.of(errorCode), Optional.empty());
        }
    }

    @BeforeClass
    public void setupClass()
            throws Exception
    {
        queryRunner = setupPresto();
    }

    private BenchmarkQueryExecutor createBenchmarkQueryExecutor(boolean useMockPrestoAction)
    {
        BenchmarkRunnerConfig benchmarkRunnerConfig = new BenchmarkRunnerConfig().setTestId(TEST_ID);

        if (useMockPrestoAction) {
            return new BenchmarkQueryExecutor((query, sessionProperties) -> new MockPrestoAction(GENERIC_INTERNAL_ERROR), sqlParser, parsingOptions, benchmarkRunnerConfig);
        }
        else {
            JdbcPrestoAction jdbcPrestoAction = new JdbcPrestoAction(
                    new PrestoExceptionClassifier(ImmutableSet.of()),
                    new BenchmarkQuery("Test-Query", "SELECT 1", CATALOG, SCHEMA),
                    new PrestoClusterConfig()
                            .setJdbcUrl(queryRunner.getServer().getBaseUrl().toString().replace("http", "jdbc:presto")),
                    new HashMap<>(),
                    new RetryConfig());

            return new BenchmarkQueryExecutor((query, sessionProperties) -> jdbcPrestoAction, sqlParser, parsingOptions, benchmarkRunnerConfig);
        }
    }

    @Test
    public void testSuccess()
    {
        BenchmarkQueryEvent event = createBenchmarkQueryExecutor(false)
                .run(new BenchmarkQuery(NAME, "SELECT 1", CATALOG, SCHEMA), new HashMap<>());
        assertNotNull(event);
        assertQueryEvent(event, SUCCEEDED, Optional.empty());
    }

    @Test
    public void testFailure()
    {
        BenchmarkQueryEvent event = createBenchmarkQueryExecutor(true)
                .run(new BenchmarkQuery(NAME, "SELECT 1", CATALOG, SCHEMA), new HashMap<>());
        assertNotNull(event);
        assertQueryEvent(event, FAILED, Optional.of(GENERIC_INTERNAL_ERROR.toString()));
    }

    private void assertQueryEvent(
            BenchmarkQueryEvent event,
            Status expectedStatus,
            Optional<String> expectedErrorCode)
    {
        assertEquals(event.getTestId(), TEST_ID);
        assertEquals(event.getName(), NAME);
        assertEquals(event.getEventStatus(), expectedStatus);
        assertEquals(event.getErrorCode(), expectedErrorCode.orElse(null));
    }
}
