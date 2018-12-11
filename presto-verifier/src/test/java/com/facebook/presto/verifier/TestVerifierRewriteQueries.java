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
package com.facebook.presto.verifier;

import com.facebook.presto.sql.parser.SqlParser;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.verifier.VerifyCommand.rewriteQueries;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestVerifierRewriteQueries
{
    private static final String CATALOG = "TEST_VERIFIER_REWRITE_QUERIES";
    private static final String SCHEMA = "PUBLIC";
    private static final String URL = "jdbc:h2:mem:" + CATALOG;
    private static final String QUERY_SUITE = "TEST_SUITE";
    private static final String QUERY_NAME = "TEST_QUERY";
    private static final String[] QUERY_STRINGS = {
            "INSERT INTO test_table (a, b, c, d) values (0, 0.1, 'a', (1.0, 2.0, 3.0))",
            "INSERT INTO test_table (a, b, c, d) values (1, 1.1, 'b', (1.1, 2.1, 3.1))",
            "INSERT INTO test_table (a, b, c, d) values (2, 2.2, 'c', (1.2, 2.2, 3.2))",
            "INSERT INTO test_table (a, b, c, d) values (3, 3.3, 'd', (1.3, 2.3, 3.3))",
            "INSERT INTO test_table (a, b, c, d) values (4, 4.4, 'e', (1.4, 2.4, 3.4))",
            "INSERT INTO test_table (a, b, c, d) values (5, 5.5, 'f', (1.5, 2.5, 3.5))",
            "INSERT INTO test_table (a, b, c, d) values (6, 6.6, 'g', (1.6, 2.6, 3.6))",
            "INSERT INTO test_table (a, b, c, d) values (7, 7.7, 'h', (1.7, 2.7, 3.7))",
            "INSERT INTO test_table (a, b, c, d) values (8, 8.8, 'i', (1.8, 2.8, 3.8))",
            "INSERT INTO test_table (a, b, c, d) values (9, 9.9, 'j', (1.9, 2.9, 3.9))",
    };

    private final Handle handle;
    private final SqlParser parser;
    private final VerifierConfig config;
    private final ImmutableList<QueryPair> queryPairs;

    public TestVerifierRewriteQueries()
    {
        handle = Jdbi.open(URL);
        handle.execute("CREATE TABLE \"test_table\" (a BIGINT, b DOUBLE, c VARCHAR, d ARRAY)");
        parser = new SqlParser();

        config = new VerifierConfig();
        config.setTestGateway(URL);
        config.setTestTimeout(new Duration(10, TimeUnit.SECONDS));
        config.setControlTimeout(new Duration(10, TimeUnit.SECONDS));
        config.setShadowTestTablePrefix("tmp_verifier_test_");
        config.setShadowControlTablePrefix("tmp_verifier_control_");

        ImmutableList.Builder<QueryPair> builder = ImmutableList.builder();
        for (String queryString : QUERY_STRINGS) {
            Query query = new Query(
                    CATALOG,
                    SCHEMA,
                    ImmutableList.of(),
                    queryString,
                    ImmutableList.of(),
                    null,
                    null,
                    ImmutableMap.of());
            builder.add(new QueryPair(QUERY_SUITE, QUERY_NAME, query, query));
        }
        queryPairs = builder.build();
    }

    @AfterClass(alwaysRun = true)
    public void close()
    {
        handle.close();
    }

    @Test
    public void testSingleThread()
    {
        config.setControlGateway(URL);
        config.setThreadCount(1);
        List<QueryPair> rewrittenQueries = rewriteQueries(parser, config, queryPairs);
        assertEquals(rewrittenQueries.size(), queryPairs.size());
    }

    @Test
    public void testMultipleThreads()
    {
        config.setControlGateway(URL);
        config.setThreadCount(5);
        List<QueryPair> rewrittenQueries = rewriteQueries(parser, config, queryPairs);
        assertEquals(rewrittenQueries.size(), queryPairs.size());
    }

    @Test
    public void testQueryRewriteException()
    {
        config.setControlGateway(URL);
        Query invalidQuery = new Query(
                CATALOG,
                SCHEMA,
                ImmutableList.of("INSERT INTO test_table (a, b, c) values (10, 10.11, 'k')"),
                "INSERT INTO test_table (a, b, c) values (11, 11.12, 'l')",
                ImmutableList.of(),
                null,
                null,
                ImmutableMap.of());
        List<QueryPair> rewrittenQueries = rewriteQueries(parser, config, ImmutableList.<QueryPair>builder()
                .addAll(queryPairs)
                .add(new QueryPair(QUERY_SUITE, QUERY_NAME, invalidQuery, invalidQuery))
                .build());
        assertEquals(rewrittenQueries.size(), queryPairs.size());
    }

    @Test
    public void testSQLException()
    {
        config.setControlGateway("invalid:url");
        List<QueryPair> rewrittenQueries = rewriteQueries(parser, config, queryPairs);
        assertEquals(rewrittenQueries.size(), 0);
    }
}
