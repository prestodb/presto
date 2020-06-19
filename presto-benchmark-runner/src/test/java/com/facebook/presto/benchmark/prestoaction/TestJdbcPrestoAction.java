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
package com.facebook.presto.benchmark.prestoaction;

import com.facebook.presto.benchmark.framework.BenchmarkQuery;
import com.facebook.presto.benchmark.framework.QueryResult;
import com.facebook.presto.benchmark.retry.RetryConfig;
import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.parser.SqlParserOptions;
import com.facebook.presto.tests.StandaloneQueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.benchmark.BenchmarkTestUtil.CATALOG;
import static com.facebook.presto.benchmark.BenchmarkTestUtil.SCHEMA;
import static com.facebook.presto.benchmark.BenchmarkTestUtil.setupPresto;
import static com.facebook.presto.execution.QueryState.FINISHED;
import static com.facebook.presto.sql.parser.IdentifierSymbol.AT_SIGN;
import static com.facebook.presto.sql.parser.IdentifierSymbol.COLON;
import static com.facebook.presto.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestJdbcPrestoAction
{
    private static final SqlParser SQL_PARSER = new SqlParser(new SqlParserOptions().allowIdentifierSymbol(COLON, AT_SIGN));
    private static final ParsingOptions PARSING_OPTIONS = ParsingOptions.builder().setDecimalLiteralTreatment(AS_DECIMAL).build();

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
        prestoAction = new JdbcPrestoAction(
                new PrestoExceptionClassifier(ImmutableSet.of()),
                new BenchmarkQuery("Test-Query", "SELECT 1", CATALOG, SCHEMA, Optional.empty()),
                new PrestoClusterConfig()
                        .setJdbcUrl(queryRunner.getServer().getBaseUrl().toString().replace("http", "jdbc:presto")),
                new RetryConfig());
    }

    @Test
    public void testQuerySucceeded()
    {
        assertEquals(
                prestoAction.execute(
                        SQL_PARSER.createStatement("SELECT 1", PARSING_OPTIONS)).getState(),
                FINISHED.name());
    }

    @Test
    public void testQuerySucceededWithConverter()
    {
        QueryResult<Integer> result = prestoAction.execute(
                SQL_PARSER.createStatement("SELECT x FROM (VALUES (1), (2), (3)) t(x)", PARSING_OPTIONS),
                resultSet -> resultSet.getInt("x") * resultSet.getInt("x"));
        assertEquals(result.getQueryStats().getState(), FINISHED.name());
        assertEquals(result.getResults(), ImmutableList.of(1, 4, 9));
    }
}
