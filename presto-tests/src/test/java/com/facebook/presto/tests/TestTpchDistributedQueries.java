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
    public void testExplainAnalyzeSimple()
    {
        assertExplainAnalyze("EXPLAIN ANALYZE SELECT * FROM orders",
                "Fragment 1 \\[SOURCE\\]\n" +
                        "    Cost: CPU .*, Input: 15000 lines \\(.*\\), Output: 15000 lines \\(.*\\)\n" +
                        "    Output layout: \\[orderkey, custkey, orderstatus, totalprice, orderdate, orderpriority, clerk, shippriority, comment\\]\n" +
                        "    Output partitioning: SINGLE \\[\\]\n" +
                        "    - TableScan\\[tpch:tpch:orders:sf0\\.01, originalConstraint = true] => \\[orderkey:bigint, custkey:bigint, orderstatus:varchar, totalprice:double, orderdate:date, orderpriority:varchar, clerk:varchar, shippriority:bigint, comment:varchar\\]\n" +
                        "            Cost: .*%, Output: 15000 lines \\(.*\\)\n" +
                        "            TableScanOperator := Drivers: .*, Input avg\\.: .* lines, Input std\\.dev\\.: .*\n" +
                        "            TaskOutputOperator := Drivers: .*, Input avg\\.: .* lines, Input std\\.dev\\.: .*\n" +
                        "            orderkey := tpch:orderkey\n" +
                        "            custkey := tpch:custkey\n" +
                        "            orderstatus := tpch:orderstatus\n" +
                        "            totalprice := tpch:totalprice\n" +
                        "            orderdate := tpch:orderdate\n" +
                        "            orderpriority := tpch:orderpriority\n" +
                        "            clerk := tpch:clerk\n" +
                        "            shippriority := tpch:shippriority\n" +
                        "            comment := tpch:comment\n\n");
    }

    @Test
    public void testExplainAnalyzeAggregation()
    {
        assertExplainAnalyze("EXPLAIN ANALYZE SELECT count(*), clerk FROM orders GROUP BY clerk",
                "Fragment 1 \\[HASH\\]\n" +
                        "    Cost: CPU .*, Input: .* lines \\(.*\\), Output: 1000 lines \\(.*\\)\n" +
                        "    Output layout: \\[count, clerk\\]\n" +
                        "    Output partitioning: SINGLE \\[\\]\n" +
                        "    - ScanFilterAndProject\\[\\] => \\[count:bigint, clerk:varchar\\]\n" +
                        "            Cost: .*, Input: 1000 lines \\(.*\\), Output: 1000 lines \\(.*\\), Filtered: 0\\.00%\n" +
                        "            TaskOutputOperator := Drivers: .*, Input avg\\.: .* lines, Input std\\.dev\\.: .*\n" +
                        "            FilterAndProjectOperator := Drivers: .*, Input avg\\.: .* lines, Input std\\.dev\\.: .*\n" +
                        "        - Aggregate\\(FINAL\\)\\[clerk\\] => \\[clerk:varchar, \\$hashvalue:bigint, count:bigint\\]\n" +
                        "                Cost: .*, Output: 1000 lines \\(.*\\)\n" +
                        "                HashAggregationOperator := Drivers: .*, Input avg\\.: .* lines, Input std\\.dev\\.: .*\n" +
                        "                HashAggregationOperator := Collisions avg\\.: .*, Collisions std\\.dev\\.: .*\n" +
                        "                InMemoryExchangeSinkOperator := Drivers: .*, Input avg\\.: .* lines, Input std\\.dev\\.: .*\n" +
                        "                InMemoryExchangeSourceOperator := Drivers: .*, Input avg\\.: .* lines, Input std\\.dev\\.: .*\n" +
                        "                MarkDistinctOperator := Drivers: .*, Input avg\\.: .* lines, Input std\\.dev\\.: .*\n" +
                        "                count := \"count\"\\(\"count_8\"\\)\n" +
                        "            - RemoteSource\\[2\\] => \\[clerk:varchar, \\$hashvalue:bigint, count_8:bigint\\]\n" +
                        "                    Cost: .*, Output: .* lines \\(.*\\)\n" +
                        "                    ExchangeOperator := Drivers: .*, Input avg\\.: .* lines, Input std\\.dev\\.: .*\n" +
                        "\n" +
                        "Fragment 2 \\[SOURCE\\]\n" +
                        "    Cost: CPU .*, Input: 15000 lines \\(.*\\), Output: .* lines \\(.*\\)\n" +
                        "    Output layout: \\[clerk, \\$hashvalue, count_8\\]\n" +
                        "    Output partitioning: HASH \\[clerk\\]\n" +
                        "    - Aggregate\\(PARTIAL\\)\\[clerk\\] => \\[clerk:varchar, \\$hashvalue:bigint, count_8:bigint\\]\n" +
                        "            Cost: .*, Output: .* lines \\(.*\\)\n" +
                        "            HashAggregationOperator := Drivers: .*, Input avg\\.: .* lines, Input std\\.dev\\.: .*\n" +
                        "            HashAggregationOperator := Collisions avg\\.: .*, Collisions std\\.dev\\.: .*\n" +
                        "            PartitionedOutputOperator := Drivers: .*, Input avg\\.: .* lines, Input std\\.dev\\.: .*\n" +
                        "            count_8 := \"count\"\\(\\*\\)\n" +
                        "        - ScanFilterAndProject\\[table = tpch:tpch:orders:sf0\\.01, originalConstraint = true\\] => \\[clerk:varchar, \\$hashvalue:bigint\\]\n" +
                        "                Cost: .*, Input: 15000 lines \\(.*\\), Output: 15000 lines \\(.*\\), Filtered: 0\\.00%\n" +
                        "                ScanFilterAndProjectOperator := Drivers: .*, Input avg\\.: .* lines, Input std\\.dev\\.: .*\n" +
                        "                \\$hashvalue := \"combine_hash\"\\(0, COALESCE\\(\"\\$operator\\$hash_code\"\\(\"clerk\"\\), 0\\)\\)\n" +
                        "                clerk := tpch:clerk\n\n");
    }

    @Test
    public void testExplainAnalyzeJoinAggregation()
    {
        assertExplainAnalyze(
                "EXPLAIN ANALYZE SELECT x + y FROM (" +
                        "   SELECT orderdate, COUNT(*) x FROM orders GROUP BY orderdate) a JOIN (" +
                        "   SELECT orderdate, COUNT(*) y FROM orders GROUP BY orderdate) b ON a.orderdate = b.orderdate",
                "Fragment 1 \\[HASH\\]\n" +
                        "    Cost: CPU .*, Input: .* lines \\(.*\\), Output: 2401 lines \\(.*\\)\n" +
                        "    Output layout: \\[expr_28\\]\n" +
                        "    Output partitioning: SINGLE \\[\\]\n" +
                        "    - ScanFilterAndProject\\[\\] => \\[expr_28:bigint\\]\n" +
                        "            Cost: .*, Input: 2401 lines \\(.*\\), Output: 2401 lines \\(.*\\), Filtered: 0\\.00%\n" +
                        "            TaskOutputOperator := Drivers: .*, Input avg\\.: .* lines, Input std\\.dev\\.: .*\n" +
                        "            FilterAndProjectOperator := Drivers: .*, Input avg\\.: .* lines, Input std\\.dev\\.: .*\n" +
                        "            expr_28 := \\(\"count\" \\+ \"count_19\"\\)\n" +
                        "        - InnerJoin\\[.*\\]\n" +
                        "                Cost: .*, Output: 2401 lines \\(.*\\)\n" +
                        "                LookupJoinOperator := Drivers: .*, Input avg\\.: .* lines, Input std\\.dev\\.: .*\n" +
                        "                ParallelHashCollectOperator := Drivers: .*, Input avg\\.: .* lines, Input std\\.dev\\.: .*\n" +
                        "                ParallelHashBuilderOperator := Drivers: .*, Input avg\\.: .* lines, Input std\\.dev\\.: .*\n" +
                        "                ParallelHashBuilderOperator := Collisions avg\\.: .*, Collisions std\\.dev\\.: .*\n" +
                        "                InMemoryExchangeSinkOperator := Drivers: .*, Input avg\\.: .* lines, Input std\\.dev\\.: .*\n" +
                        "                InMemoryExchangeSourceOperator := Drivers: .*, Input avg\\.: .* lines, Input std\\.dev\\.: .*\n" +
                        "            - ScanFilterAndProject\\[\\] => \\[\\$hashvalue_32:bigint, count:bigint, orderdate:date\\]\n" +
                        "                    Cost: .*, Input: 2401 lines \\(.*\\), Output: 2401 lines \\(.*\\), Filtered: 0\\.00%\n" +
                        "                    InMemoryExchangeSinkOperator := Drivers: .*, Input avg\\.: .* lines, Input std\\.dev\\.: .*\n" +
                        "                    FilterAndProjectOperator := Drivers: .*, Input avg\\.: .* lines, Input std\\.dev\\.: .*\n" +
                        "                    InMemoryExchangeSourceOperator := Drivers: .*, Input avg\\.: .* lines, Input std\\.dev\\.: .*\n" +
                        "                    \\$hashvalue_32 := \"combine_hash\"\\(0, COALESCE\\(\"\\$operator\\$hash_code\"\\(\"orderdate\"\\), 0\\)\\)\n" +
                        "                - Aggregate\\(FINAL\\)\\[orderdate\\] => \\[orderdate:date, \\$hashvalue:bigint, count:bigint\\]\n" +
                        "                        Cost: .*, Output: 2401 lines \\(.*\\)\n" +
                        "                        HashAggregationOperator := Drivers: .*, Input avg\\.: .* lines, Input std\\.dev\\.: .*\n" +
                        "                        HashAggregationOperator := Collisions avg\\.: .*, Collisions std\\.dev\\.: .*\n" +
                        "                        InMemoryExchangeSinkOperator := Drivers: .*, Input avg\\.: .* lines, Input std\\.dev\\.: .*\n" +
                        "                        InMemoryExchangeSourceOperator := Drivers: .*, Input avg\\.: .* lines, Input std\\.dev\\.: .*\n" +
                        "                        MarkDistinctOperator := Drivers: .*, Input avg\\.: .* lines, Input std\\.dev\\.: .*\n" +
                        "                        count := \"count\"\\(\"count_34\"\\)\n" +
                        "                    - RemoteSource\\[2\\] => \\[orderdate:date, \\$hashvalue:bigint, count_34:bigint\\]\n" +
                        "                            Cost: .*, Output: .* lines \\(.*\\)\n" +
                        "                            ExchangeOperator := Drivers: .*, Input avg\\.: .* lines, Input std\\.dev\\.: .*" +
                        "            - ScanFilterAndProject\\[\\] => \\[count_19:bigint, \\$hashvalue_33:bigint, orderdate_12:date\\]\n" +
                        "                    Cost: .*, Input: 2401 lines \\(.*\\), Output: 2401 lines \\(.*\\), Filtered: 0\\.00%\n" +
                        "                    FilterAndProjectOperator := Drivers: .*, Input avg\\.: .* lines, Input std\\.dev\\.: .*\n" +
                        "                    \\$hashvalue_33 := \"combine_hash\"\\(0, COALESCE\\(\"\\$operator\\$hash_code\"\\(\"orderdate_12\"\\), 0\\)\\)\n" +
                        "                - Aggregate\\(FINAL\\)\\[orderdate_12\\] => \\[orderdate_12:date, \\$hashvalue_31:bigint, count_19:bigint\\]\n" +
                        "                        Cost: .*, Output: 2401 lines \\(.*\\)\n" +
                        "                        HashAggregationOperator := Drivers: .*, Input avg\\.: .* lines, Input std\\.dev\\.: .*\n" +
                        "                        HashAggregationOperator := Collisions avg\\.: .*, Collisions std\\.dev\\.: .*\n" +
                        "                        InMemoryExchangeSinkOperator := Drivers: .*, Input avg\\.: .* lines, Input std\\.dev\\.: .*\n" +
                        "                        InMemoryExchangeSourceOperator := Drivers: .*, Input avg\\.: .* lines, Input std\\.dev\\.: .*\n" +
                        "                        MarkDistinctOperator := Drivers: .*, Input avg\\.: .* lines, Input std\\.dev\\.: .*\n" +
                        "                        count_19 := \"count\"\\(\"count_35\"\\)\n" +
                        "                    - RemoteSource\\[3\\] => \\[orderdate_12:date, \\$hashvalue_31:bigint, count_35:bigint\\]\n" +
                        "                            Cost: .*, Output: .* lines \\(.*\\)\n" +
                        "                            ExchangeOperator := Drivers: .*, Input avg\\.: .* lines, Input std\\.dev\\.: .*\n" +
                        "\n" +
                        "Fragment 2 \\[SOURCE\\]\n" +
                        "    Cost: .*, Input: 15000 lines \\(.*\\), Output: .* lines \\(.*\\)\n" +
                        "    Output layout: \\[orderdate, \\$hashvalue, count_34\\]\n" +
                        "    Output partitioning: HASH \\[orderdate\\]\n" +
                        "    - Aggregate\\(PARTIAL\\)\\[orderdate\\] => \\[orderdate:date, \\$hashvalue:bigint, count_34:bigint\\]\n" +
                        "            Cost: .*, Output: .* lines \\(.*\\)\n" +
                        "            HashAggregationOperator := Drivers: .*, Input avg\\.: .* lines, Input std\\.dev\\.: .*\n" +
                        "            HashAggregationOperator := Collisions avg\\.: .*, Collisions std\\.dev\\.: .*\n" +
                        "            PartitionedOutputOperator := Drivers: .*, Input avg\\.: .* lines, Input std\\.dev\\.: .*\n" +
                        "            count_34 := \"count\"\\(\\*\\)\n" +
                        "        - ScanFilterAndProject\\[table = tpch:tpch:orders:sf0\\.01, originalConstraint = true\\] => \\[orderdate:date, \\$hashvalue:bigint\\]\n" +
                        "                Cost: .*, Input: 15000 lines \\(.*\\), Output: 15000 lines \\(.*\\), Filtered: 0\\.00%\n" +
                        "                ScanFilterAndProjectOperator := Drivers: .*, Input avg\\.: .* lines, Input std\\.dev\\.: .*\n" +
                        "                \\$hashvalue := \"combine_hash\"\\(0, COALESCE\\(\"\\$operator\\$hash_code\"\\(\"orderdate\"\\), 0\\)\\)\n" +
                        "                orderdate := tpch:orderdate\n" +
                        "\n" +
                        "Fragment 3 \\[SOURCE\\]\n" +
                        "    Cost: CPU .*, Input: 15000 lines \\(.*\\), Output: .* lines \\(.*\\)\n" +
                        "    Output layout: \\[orderdate_12, \\$hashvalue_31, count_35\\]\n" +
                        "    Output partitioning: HASH \\[orderdate_12\\]\n" +
                        "    - Aggregate\\(PARTIAL\\)\\[orderdate_12\\] => \\[orderdate_12:date, \\$hashvalue_31:bigint, count_35:bigint\\]\n" +
                        "            Cost: .*, Output: .* lines \\(.*\\)\n" +
                        "            HashAggregationOperator := Drivers: .*, Input avg\\.: .* lines, Input std\\.dev\\.: .*\n" +
                        "            HashAggregationOperator := Collisions avg\\.: .*, Collisions std\\.dev\\.: .*\n" +
                        "            PartitionedOutputOperator := Drivers: .*, Input avg\\.: .* lines, Input std\\.dev\\.: .*\n" +
                        "            count_35 := \"count\"\\(\\*\\)\n" +
                        "        - ScanFilterAndProject\\[table = tpch:tpch:orders:sf0\\.01, originalConstraint = true\\] => \\[orderdate_12:date, \\$hashvalue_31:bigint\\]\n" +
                        "                Cost: .*, Input: 15000 lines \\(.*\\), Output: 15000 lines \\(.*\\), Filtered: 0\\.00%\n" +
                        "                ScanFilterAndProjectOperator := Drivers: .*, Input avg\\.: .* lines, Input std\\.dev\\.: .*\n" +
                        "                \\$hashvalue_31 := \"combine_hash\"\\(0, COALESCE\\(\"\\$operator\\$hash_code\"\\(\"orderdate_12\"\\), 0\\)\\)\n" +
                        "                orderdate_12 := tpch:orderdate\n\n");
    }

    @Test
    public void testExplainAnalyzeOrder()
    {
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
    }

    @Test
    public void testExplainAnalyzeUnion()
    {
        assertExplainAnalyze("EXPLAIN ANALYZE SELECT count(*), clerk FROM orders GROUP BY clerk UNION ALL SELECT sum(orderkey), clerk FROM orders GROUP BY clerk");
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "EXPLAIN ANALYZE only supported for statements that are queries")
    public void testExplainAnalyzeDDL()
    {
        computeActual("EXPLAIN ANALYZE DROP TABLE orders");
    }

    // TODO: use assertExplainAnalyze with expected regex
    private void assertExplainAnalyze(@Language("SQL") String query)
    {
        String value = getOnlyElement(computeActual(query).getOnlyColumnAsSet());
        assertTrue(value.contains("Cost: "), format("Expected output to contain \"Cost: \", but it is %s", value));
    }

    private void assertExplainAnalyze(@Language("SQL") String query, String expected)
    {
        String value = getOnlyElement(computeActual(query).getOnlyColumnAsSet());
        assertTrue(value.matches("(?s)" + expected), format("Expected output to match: %s, but it is: %s", expected, value));
    }
}
