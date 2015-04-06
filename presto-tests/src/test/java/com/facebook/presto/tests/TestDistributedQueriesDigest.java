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
package com.facebook.presto.tests;

import com.facebook.presto.Session;
import com.facebook.presto.client.QueryResults;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.tpch.TpchPlugin;
import io.airlift.log.Logger;
import io.airlift.testing.Closeables;
import io.airlift.units.Duration;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.airlift.units.Duration.nanosSince;
import static java.util.Locale.ENGLISH;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestDistributedQueriesDigest
        extends AbstractTestQueryFramework
{
    private static final Logger log = Logger.get(TestDistributedQueriesDigest.class);

    protected TestDistributedQueriesDigest()
                throws Exception
    {
        super(createQueryRunner());
    }

    @AfterClass
    public void destroy()
            throws Exception
    {
        Closeables.closeQuietly(queryRunner);
    }

    private static DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        Session session = Session.builder()
                .setUser("user")
                .setSource("test")
                .setCatalog("tpch")
                .setSchema(TINY_SCHEMA_NAME)
                .setTimeZoneKey(UTC_KEY)
                .setLocale(ENGLISH)
                .build();

        DistributedQueryRunner queryRunner = new DistributedQueryRunner(session, 2);

        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");

        return queryRunner;
    }

    protected void assertQuery(@Language("SQL") String sql, boolean shouldDigestMatch)
            throws Exception
    {
        long start = System.nanoTime();
        MaterializedResult results = queryRunner.execute(sql);
        Duration executionTime = nanosSince(start);

        assertTrue(results.getFinalQueryResults().isPresent());
        QueryResults queryResults = results.getFinalQueryResults().get();
        log.info("FINISHED in presto: %s, time: %s, digest: %s, rows: %d [sql: %s]",
                queryResults.getStats().getState(), executionTime, queryResults.getQueryDigest(), results.getRowCount(), sql);

        assertNull(queryResults.getError(), "Error in query result.");
        assertFalse(queryResults.isDigestMatched(), "Query digest matched.");

        // send the same query with the digest
        start = System.nanoTime();
        results = queryRunner.execute(queryRunner.getDefaultSession(), sql, queryResults.getQueryDigest());
        executionTime = nanosSince(start);

        assertTrue(results.getFinalQueryResults().isPresent());
        queryResults = results.getFinalQueryResults().get();
        log.info("FINISHED in presto: %s, time: %s, digest: %s, rows: %d [sql: %s]",
                queryResults.getStats().getState(), executionTime, queryResults.getQueryDigest(), results.getRowCount(), sql);

        assertNull(queryResults.getError(), "Error in query result.");
        if (shouldDigestMatch) {
            assertTrue(queryResults.isDigestMatched(), "Query digest not matched.");
            assertEquals(results.getRowCount(), 0, "No rows expected in the query results but found: " + results.getRowCount());
        }
        else {
            assertFalse(queryResults.isDigestMatched(), "Query digest matched.");
        }
    }

    @Override
    protected void assertQuery(@Language("SQL") String sql)
            throws Exception
    {
        assertQuery(sql, true);
    }
        @Test
    public void testQueryDigestMatched()
            throws Exception
    {
        // test a sample of queries that should return the same digest every time
        assertQuery("SELECT COUNT(true) FROM orders");
        assertQuery("SELECT orderstatus FROM orders ORDER BY orderstatus");
        assertQuery("SELECT * FROM orders UNION ALL SELECT * FROM orders");
        assertQuery("SELECT COUNT(*) FROM ORDERS");
        assertQuery("SELECT COUNT(42) FROM ORDERS");
        assertQuery("SELECT COUNT(42 + 42) FROM ORDERS");
        assertQuery("SELECT COUNT(null) FROM ORDERS");
        assertQuery("SELECT *, orders.*, orderkey FROM orders");
        assertQuery("SELECT *, orderkey NOT IN (SELECT orderkey FROM lineitem WHERE orderkey % 3 = 0) FROM orders");
        assertQuery("SELECT sum(IF(orderstatus = 'F', totalprice, 0.0)) FROM orders");
        assertQuery("SELECT orderkey FROM orders WHERE orderkey IN (1.5, 2.3)");
        assertQuery("SELECT * FROM (SELECT orderkey X FROM orders)");
        assertQuery("SELECT COUNT(DISTINCT clerk) as count, orderdate FROM orders GROUP BY orderdate ORDER BY count, orderdate");

        assertQuery("" +
                "SELECT a.custkey, b.orderkey " +
                "FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 5) a " +
                "CROSS JOIN (SELECT * FROM lineitem ORDER BY orderkey LIMIT 5) b");

        assertQuery("SELECT RANK() OVER (PARTITION BY orderdate ORDER BY COUNT(DISTINCT clerk)) rnk " +
                "FROM orders " +
                "GROUP BY orderdate, custkey " +
                "ORDER BY rnk " +
                "LIMIT 1");

        assertQuery("SELECT RANK() OVER (PARTITION BY orderdate ORDER BY COUNT(DISTINCT clerk)) rnk " +
                "FROM orders " +
                "GROUP BY orderdate, custkey " +
                "ORDER BY rnk " +
                "LIMIT 1");

        assertQuery("SELECT orderkey, orderstatus\n" +
                ", row_number() OVER (ORDER BY orderkey * 2) *\n" +
                "  row_number() OVER (ORDER BY orderkey DESC) + 100\n" +
                "FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 10) x\n" +
                "ORDER BY orderkey LIMIT 5");
    }

    @Test
    public void testQueryDigestNotMatched()
            throws Exception
    {
        // now() should result in different digest every time
        assertQuery("SELECT EXTRACT(YEAR FROM now()), count(*) FROM orders GROUP BY now()", false);

        // information_schema not in catalog, the connector should return a random digest
        assertQuery("SELECT table_name FROM information_schema.tables WHERE table_name = 'orders' LIMIT 1", false);
    }
}
