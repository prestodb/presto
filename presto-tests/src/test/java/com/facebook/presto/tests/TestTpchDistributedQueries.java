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

import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import static com.facebook.presto.tests.tpch.TpchQueryRunner.createQueryRunner;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static org.testng.Assert.assertTrue;

public class TestTpchDistributedQueries
        extends AbstractTestQueries
{
    public TestTpchDistributedQueries()
            throws Exception
    {
        super(createQueryRunner());
    }

    @Test
    public void testExplainAnalyze()
    {
        assertExplainAnalyze("EXPLAIN ANALYZE SELECT * FROM orders");
        assertExplainAnalyze("EXPLAIN ANALYZE SELECT count(*), clerk FROM orders GROUP BY clerk");
        assertExplainAnalyze(
                "EXPLAIN ANALYZE SELECT x + y FROM (" +
                        "   SELECT orderdate, COUNT(*) x FROM orders GROUP BY orderdate) a JOIN (" +
                        "   SELECT orderdate, COUNT(*) y FROM orders GROUP BY orderdate) b ON a.orderdate = b.orderdate");
        assertExplainAnalyze("" +
                "EXPLAIN ANALYZE SELECT *, o2.custkey\n" +
                "  IN (\n" +
                "    SELECT orderkey\n" +
                "    FROM lineitem\n" +
                "    WHERE orderkey % 5 = 0)\n" +
                "FROM (SELECT * FROM orders WHERE custkey % 256 = 0) o1\n" +
                "JOIN (SELECT * FROM orders WHERE custkey % 256 = 0) o2\n" +
                "  ON (o1.orderkey IN (SELECT orderkey FROM lineitem WHERE orderkey % 4 = 0)) = (o2.orderkey IN (SELECT orderkey FROM lineitem WHERE orderkey % 4 = 0))\n" +
                "WHERE o1.orderkey\n" +
                "  IN (\n" +
                "    SELECT orderkey\n" +
                "    FROM lineitem\n" +
                "    WHERE orderkey % 4 = 0)\n" +
                "ORDER BY o1.orderkey\n" +
                "  IN (\n" +
                "    SELECT orderkey\n" +
                "    FROM lineitem\n" +
                "    WHERE orderkey % 7 = 0)");
        assertExplainAnalyze("EXPLAIN ANALYZE SELECT count(*), clerk FROM orders GROUP BY clerk UNION ALL SELECT sum(orderkey), clerk FROM orders GROUP BY clerk");
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "EXPLAIN ANALYZE only supported for statements that are queries")
    public void testExplainAnalyzeDDL()
    {
        computeActual("EXPLAIN ANALYZE DROP TABLE orders");
    }

    private void assertExplainAnalyze(@Language("SQL") String query)
    {
        String value = getOnlyElement(computeActual(query).getOnlyColumnAsSet());
        // TODO: check that rendered plan is as expected, once stats are collected in a consistent way
        assertTrue(value.contains("Cost: "), format("Expected output to contain \"Cost: \", but it is %s", value));
    }
}
