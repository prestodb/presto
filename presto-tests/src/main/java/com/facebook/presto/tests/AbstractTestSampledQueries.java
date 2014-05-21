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

import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public abstract class AbstractTestSampledQueries
        extends AbstractTestQueries
{
    @Test
    public void testApproximateQueryCount()
            throws Exception
    {
        assertApproximateQuery("SELECT COUNT(*) FROM orders APPROXIMATE AT 99.999 CONFIDENCE", "SELECT 2 * COUNT(*) FROM orders");
    }

    @Test
    public void testApproximateQueryCountCustkey()
            throws Exception
    {
        assertApproximateQuery("SELECT COUNT(custkey) FROM orders APPROXIMATE AT 99.999 CONFIDENCE", "SELECT 2 * COUNT(custkey) FROM orders");
    }

    @Test
    public void testApproximateQuerySum()
            throws Exception
    {
        assertApproximateQuery("SELECT SUM(totalprice) FROM orders APPROXIMATE AT 99.999 CONFIDENCE", "SELECT 2 * SUM(totalprice) FROM orders");
    }

    @Test
    public void testApproximateQueryAverage()
            throws Exception
    {
        assertApproximateQuery("SELECT AVG(totalprice) FROM orders APPROXIMATE AT 99.999 CONFIDENCE", "SELECT AVG(totalprice) FROM orders");
    }

    @Test
    public void testSampledRightOuterJoin()
            throws Exception
    {
        assertSampledQuery("SELECT COUNT(*) FROM orders a RIGHT OUTER JOIN orders b ON a.custkey = b.orderkey",
                "SELECT COUNT(*) FROM (SELECT * FROM orders UNION ALL SELECT * FROM orders) a RIGHT OUTER JOIN (SELECT * FROM orders UNION ALL SELECT * FROM orders) b ON a.custkey = b.orderkey");
    }

    @Test
    public void testSampledLeftOuterJoin()
            throws Exception
    {
        assertSampledQuery("SELECT COUNT(*) FROM orders a LEFT OUTER JOIN orders b ON a.orderkey = b.custkey",
                "SELECT COUNT(*) FROM (SELECT * FROM orders UNION ALL SELECT * FROM orders) a LEFT OUTER JOIN (SELECT * FROM orders UNION ALL SELECT * FROM orders) b ON a.orderkey = b.custkey");
    }

    @Test
    public void testSampledDistinctLimit()
            throws Exception
    {
        assertSampledQuery("SELECT DISTINCT custkey FROM orders ORDER BY custkey LIMIT 5", "SELECT DISTINCT custkey FROM orders ORDER BY custkey LIMIT 5");
    }

    @Test
    public void testSampledCountStar()
            throws Exception
    {
        assertSampledQuery("SELECT COUNT(*) FROM orders", "SELECT 2 * COUNT(*) FROM orders");
    }

    @Test
    public void testSampledCount()
            throws Exception
    {
        assertSampledQuery("SELECT COUNT(custkey), COUNT(DISTINCT custkey) FROM orders", "SELECT 2 * COUNT(custkey), COUNT(DISTINCT custkey) FROM orders");
    }

    @Test
    public void testSampledCountIf()
            throws Exception
    {
        assertSampledQuery("SELECT COUNT_IF(custkey > 100) FROM orders", "SELECT 2 * COUNT(custkey) FROM orders WHERE custkey > 100");
    }

    @Test
    public void testSampledAvg()
            throws Exception
    {
        assertSampledQuery("SELECT AVG(totalprice) FROM orders", "SELECT AVG(totalprice) FROM orders");
    }

    @Test
    public void testSampledVariance()
            throws Exception
    {
        assertSampledQuery("SELECT CAST(VARIANCE(totalprice) / 100000000 AS BIGINT) FROM orders", "SELECT CAST(VARIANCE(totalprice) / 100000000 AS BIGINT) FROM orders");
    }

    @Test
    public void testSampledSum()
            throws Exception
    {
        assertSampledQuery("SELECT SUM(custkey), SUM(totalprice) FROM orders",
                "SELECT 2 * SUM(custkey), 2 * SUM(totalprice) FROM orders");
    }

    @Test
    public void testSampledMin()
            throws Exception
    {
        assertSampledQuery("SELECT MIN(custkey), MIN(totalprice), MIN(clerk), MIN(CAST(custkey AS BOOLEAN)) FROM orders",
                "SELECT MIN(custkey), MIN(totalprice), MIN(clerk), MIN(CAST(custkey AS BOOLEAN)) FROM orders");
    }

    @Test
    public void testSampledMax()
            throws Exception
    {
        assertSampledQuery("SELECT MAX(custkey), MAX(totalprice), MAX(clerk), MAX(CAST(custkey AS BOOLEAN)) FROM orders",
                "SELECT MAX(custkey), MAX(totalprice), MAX(clerk), MAX(CAST(custkey AS BOOLEAN)) FROM orders");
    }

    @Test
    public void testSampledGroupBy()
            throws Exception
    {
        assertSampledQuery("SELECT MAX(custkey), AVG(totalprice), COUNT(custkey), SUM(totalprice), clerk FROM orders GROUP BY clerk",
                "SELECT MAX(custkey), AVG(totalprice), 2 * COUNT(custkey), 2 * SUM(totalprice), clerk FROM orders GROUP BY clerk");
    }

    @Test
    public void testSampledJoin()
            throws Exception
    {
        assertSampledQuery("SELECT SUM(quantity), clerk FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey GROUP BY clerk ORDER BY clerk",
                "SELECT 4 * SUM(quantity), clerk FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey GROUP BY clerk ORDER BY clerk");
    }

    @Test
    public void testSampledUnion()
            throws Exception
    {
        assertSampledQuery("SELECT COUNT(*) FROM (SELECT orderkey FROM lineitem UNION SELECT orderkey FROM orders)",
                "SELECT COUNT(*) FROM (SELECT orderkey FROM lineitem UNION SELECT orderkey FROM orders)");
    }

    @Test
    public void testSampledUnionAll()
            throws Exception
    {
        assertSampledQuery("SELECT COUNT(*) FROM (SELECT orderkey FROM lineitem UNION ALL SELECT orderkey FROM orders)",
                "SELECT 2 * COUNT(*) FROM (SELECT orderkey FROM lineitem UNION ALL SELECT orderkey FROM orders)");
    }

    @Test
    public void testSampledDistinctGroupBy()
            throws Exception
    {
        assertSampledQuery("SELECT COUNT(DISTINCT clerk) as count, orderdate FROM orders GROUP BY orderdate ORDER BY count",
                "SELECT COUNT(DISTINCT clerk) as count, orderdate FROM orders GROUP BY orderdate ORDER BY count");
    }

    @Test
    public void testSampledSelect()
            throws Exception
    {
        assertSampledQuery("SELECT orderkey, orderkey + 1, sqrt(orderkey) FROM orders WHERE orderkey > 2 ORDER BY orderkey LIMIT 5",
                "SELECT orderkey, orderkey + 1, sqrt(orderkey) FROM (SELECT orderkey FROM orders UNION ALL SELECT orderkey FROM orders) t WHERE orderkey > 2 ORDER BY orderkey LIMIT 5");
    }

    @Test
    public void testSampledSort()
            throws Exception
    {
        assertSampledQuery("SELECT orderkey FROM orders ORDER BY orderkey",
                "SELECT orderkey FROM (SELECT orderkey FROM orders UNION ALL SELECT orderkey FROM orders) t ORDER BY orderkey");
    }

    @Test
    public void testSampledSemiJoin()
            throws Exception
    {
        assertSampledQuery("SELECT partkey FROM lineitem WHERE orderkey IN (SELECT DISTINCT(orderkey) FROM orders WHERE custkey > 10)",
                "SELECT partkey FROM (SELECT partkey, orderkey FROM lineitem UNION ALL SELECT partkey, orderkey FROM lineitem) t WHERE orderkey IN " +
                        "(SELECT orderkey FROM orders WHERE custkey > 10)");
    }

    @Test
    public void testSampledLimit()
            throws Exception
    {
        assertSampledQuery("SELECT COUNT(*) FROM (SELECT orderkey FROM orders LIMIT 5) t",
                "SELECT COUNT(*) FROM (SELECT orderkey FROM orders LIMIT 5) t");
    }

    @Test
    public void testSampledDistinct()
            throws Exception
    {
        assertSampledQuery("SELECT COUNT(DISTINCT clerk) FROM orders", "SELECT COUNT(DISTINCT clerk) FROM orders");
    }

    private static final Logger log = Logger.get(AbstractTestSampledQueries.class);

    protected void assertSampledQuery(@Language("SQL") String actual, @Language("SQL") String expected)
            throws Exception
    {
        assertSampledQuery(actual, expected, false);
    }

    private void assertSampledQuery(@Language("SQL") String actual, @Language("SQL") String expected, boolean ensureOrdering)
            throws Exception
    {
        long start = System.nanoTime();
        MaterializedResult actualResults = computeActualSampled(actual);
        Duration actualTime = Duration.nanosSince(start);

        long expectedStart = System.nanoTime();
        MaterializedResult expectedResults = computeExpected(expected, actualResults.getTypes());
        log.info("FINISHED in presto: %s, h2: %s, total: %s", actualTime, Duration.nanosSince(expectedStart), Duration.nanosSince(start));

        if (ensureOrdering) {
            assertEquals(actualResults.getMaterializedRows(), expectedResults.getMaterializedRows());
        }
        else {
            assertEqualsIgnoreOrder(actualResults.getMaterializedRows(), expectedResults.getMaterializedRows());
        }
    }

    private void assertApproximateQuery(@Language("SQL") String actual, @Language("SQL") String expected)
            throws Exception
    {
        long start = System.nanoTime();
        MaterializedResult actualResults = computeActualSampled(actual);
        log.info("FINISHED in %s", Duration.nanosSince(start));

        MaterializedResult expectedResults = computeExpected(expected, actualResults.getTypes());
        assertApproximatelyEqual(actualResults.getMaterializedRows(), expectedResults.getMaterializedRows());
    }

    private void assertApproximatelyEqual(List<MaterializedRow> actual, List<MaterializedRow> expected)
            throws Exception
    {
        // TODO: support GROUP BY queries
        assertEquals(actual.size(), 1, "approximate query returned more than one row");

        MaterializedRow actualRow = actual.get(0);
        MaterializedRow expectedRow = expected.get(0);

        for (int i = 0; i < actualRow.getFieldCount(); i++) {
            String actualField = (String) actualRow.getField(i);
            double actualValue = Double.parseDouble(actualField.split(" ")[0]);
            double error = Double.parseDouble(actualField.split(" ")[2]);
            Object expectedField = expectedRow.getField(i);
            assertTrue(expectedField instanceof String || expectedField instanceof Number);
            double expectedValue;
            if (expectedField instanceof String) {
                expectedValue = Double.parseDouble((String) expectedField);
            }
            else {
                expectedValue = ((Number) expectedField).doubleValue();
            }
            assertTrue(Math.abs(actualValue - expectedValue) < error);
        }
    }

    protected abstract MaterializedResult computeActualSampled(@Language("SQL") String sql);
}
