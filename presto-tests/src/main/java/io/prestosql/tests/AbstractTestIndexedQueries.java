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
package io.prestosql.tests;

import com.google.common.collect.ImmutableSet;
import io.prestosql.plugin.tpch.TpchMetadata;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.tests.tpch.TpchIndexSpec;
import io.prestosql.tests.tpch.TpchIndexSpec.Builder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public abstract class AbstractTestIndexedQueries
        extends AbstractTestQueryFramework
{
    // Generate the indexed data sets
    public static final TpchIndexSpec INDEX_SPEC = new Builder()
            .addIndex("orders", TpchMetadata.TINY_SCALE_FACTOR, ImmutableSet.of("orderkey"))
            .addIndex("orders", TpchMetadata.TINY_SCALE_FACTOR, ImmutableSet.of("orderkey", "orderstatus"))
            .addIndex("orders", TpchMetadata.TINY_SCALE_FACTOR, ImmutableSet.of("orderkey", "custkey"))
            .addIndex("orders", TpchMetadata.TINY_SCALE_FACTOR, ImmutableSet.of("orderstatus", "shippriority"))
            .build();

    protected AbstractTestIndexedQueries(QueryRunnerSupplier supplier)
    {
        super(supplier);
    }

    @Test
    public void testExampleSystemTable()
    {
        assertQuery("SELECT name FROM sys.example", "SELECT 'test' AS name");

        MaterializedResult result = computeActual("SHOW SCHEMAS");
        assertTrue(result.getOnlyColumnAsSet().containsAll(ImmutableSet.of("sf100", "tiny", "sys")));

        result = computeActual("SHOW TABLES FROM sys");
        assertEquals(result.getOnlyColumnAsSet(), ImmutableSet.of("example"));
    }

    @Test
    public void testExplainAnalyzeIndexJoin()
    {
        assertQuerySucceeds(getSession(), "EXPLAIN ANALYZE " +
                " SELECT *\n" +
                "FROM (\n" +
                "  SELECT *\n" +
                "  FROM lineitem\n" +
                "  WHERE partkey % 8 = 0) l\n" +
                "JOIN orders o\n" +
                "  ON l.orderkey = o.orderkey");
    }

    @Test
    public void testBasicIndexJoin()
    {
        assertQuery("" +
                "SELECT *\n" +
                "FROM (\n" +
                "  SELECT *\n" +
                "  FROM lineitem\n" +
                "  WHERE partkey % 8 = 0) l\n" +
                "JOIN orders o\n" +
                "  ON l.orderkey = o.orderkey");
    }

    @Test
    public void testBasicIndexJoinReverseCandidates()
    {
        assertQuery("" +
                "SELECT *\n" +
                "FROM orders o " +
                "JOIN (\n" +
                "  SELECT *\n" +
                "  FROM lineitem\n" +
                "  WHERE partkey % 8 = 0) l\n" +
                "  ON o.orderkey = l.orderkey");
    }

    @Test
    public void testBasicIndexJoinWithNullKeys()
    {
        assertQuery("" +
                "SELECT *\n" +
                "FROM (\n" +
                "  SELECT CASE WHEN suppkey % 2 = 0 THEN orderkey ELSE NULL END AS orderkey\n" +
                "  FROM lineitem\n" +
                "  WHERE partkey % 8 = 0) l\n" +
                "JOIN orders o\n" +
                "  ON l.orderkey = o.orderkey");
    }

    @Test
    public void testMultiKeyIndexJoinAligned()
    {
        assertQuery("" +
                "SELECT *\n" +
                "FROM (\n" +
                "  SELECT orderkey, CASE WHEN suppkey % 2 = 0 THEN 'F' ELSE 'O' END AS orderstatus\n" +
                "  FROM lineitem\n" +
                "  WHERE partkey % 8 = 0) l\n" +
                "JOIN orders o\n" +
                "  ON l.orderkey = o.orderkey AND l.orderstatus = o.orderstatus");
    }

    @Test
    public void testMultiKeyIndexJoinUnaligned()
    {
        // This test a join order that is different from the inner select column ordering
        assertQuery("" +
                "SELECT *\n" +
                "FROM (\n" +
                "  SELECT orderkey, CASE WHEN suppkey % 2 = 0 THEN 'F' ELSE 'O' END AS orderstatus\n" +
                "  FROM lineitem\n" +
                "  WHERE partkey % 8 = 0) l\n" +
                "JOIN orders o\n" +
                "  ON l.orderstatus = o.orderstatus AND l.orderkey = o.orderkey");
    }

    @Test
    public void testJoinWithNonJoinExpression()
    {
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.custkey = 1");
    }

    @Test
    public void testPredicateDerivedKey()
    {
        assertQuery("" +
                "SELECT *\n" +
                "FROM (\n" +
                "  SELECT orderkey\n" +
                "  FROM lineitem\n" +
                "  WHERE partkey % 8 = 0) l\n" +
                "JOIN orders o\n" +
                "  ON l.orderkey = o.orderkey\n" +
                "WHERE o.orderstatus = 'F'");
    }

    @Test
    public void testCompoundPredicateDerivedKey()
    {
        assertQuery("" +
                "SELECT *\n" +
                "FROM (\n" +
                "  SELECT orderkey\n" +
                "  FROM lineitem\n" +
                "  WHERE partkey % 8 = 0) l\n" +
                "JOIN orders o\n" +
                "  ON l.orderkey = o.orderkey\n" +
                "WHERE o.orderstatus = 'F'\n" +
                "  AND o.custkey % 2 = 0");
    }

    @Test
    public void testChainedIndexJoin()
    {
        assertQuery("" +
                "SELECT *\n" +
                "FROM (\n" +
                "  SELECT orderkey, CASE WHEN suppkey % 2 = 0 THEN 'F' ELSE 'O' END AS orderstatus\n" +
                "  FROM lineitem\n" +
                "  WHERE partkey % 8 = 0) l\n" +
                "JOIN orders o1\n" +
                "  ON l.orderkey = o1.orderkey AND l.orderstatus = o1.orderstatus\n" +
                "JOIN orders o2\n" +
                "  ON o1.custkey % 1024 = o2.orderkey");
    }

    @Test
    public void testBasicLeftIndexJoin()
    {
        assertQuery("" +
                "SELECT *\n" +
                "FROM (\n" +
                "  SELECT *\n" +
                "  FROM lineitem\n" +
                "  WHERE partkey % 8 = 0) l\n" +
                "LEFT JOIN orders o\n" +
                "  ON l.orderkey = o.orderkey");
    }

    @Test
    public void testNonIndexLeftJoin()
    {
        assertQuery("" +
                "SELECT *\n" +
                "FROM orders o " +
                "LEFT JOIN (\n" +
                "  SELECT *\n" +
                "  FROM lineitem\n" +
                "  WHERE partkey % 8 = 0) l\n" +
                "  ON o.orderkey = l.orderkey");
    }

    @Test
    public void testBasicRightIndexJoin()
    {
        assertQuery("" +
                "SELECT COUNT(*)\n" +
                "FROM orders o " +
                "RIGHT JOIN (\n" +
                "  SELECT *\n" +
                "  FROM lineitem\n" +
                "  WHERE partkey % 8 = 0) l\n" +
                "  ON o.orderkey = l.orderkey");
    }

    @Test
    public void testNonIndexRightJoin()
    {
        assertQuery("" +
                "SELECT COUNT(*)\n" +
                "FROM (\n" +
                "  SELECT *\n" +
                "  FROM lineitem\n" +
                "  WHERE partkey % 8 = 0) l\n" +
                "RIGHT JOIN orders o\n" +
                "  ON l.orderkey = o.orderkey");
    }

    @Test
    public void testIndexJoinThroughAggregation()
    {
        assertQuery("" +
                "SELECT *\n" +
                "FROM (\n" +
                "  SELECT *\n" +
                "  FROM lineitem\n" +
                "  WHERE partkey % 8 = 0) l\n" +
                "JOIN (\n" +
                "  SELECT orderkey, COUNT(*)\n" +
                "  FROM orders\n" +
                "  WHERE custkey % 8 = 0\n" +
                "  GROUP BY orderkey\n" +
                "  ORDER BY orderkey) o\n" +
                "  ON l.orderkey = o.orderkey");
    }

    @Test
    public void testIndexJoinThroughMultiKeyAggregation()
    {
        assertQuery("" +
                "SELECT *\n" +
                "FROM (\n" +
                "  SELECT *\n" +
                "  FROM lineitem\n" +
                "  WHERE partkey % 8 = 0) l\n" +
                "JOIN (\n" +
                "  SELECT shippriority, orderkey, COUNT(*)\n" +
                "  FROM orders\n" +
                "  WHERE custkey % 8 = 0\n" +
                "  GROUP BY shippriority, orderkey\n" +
                "  ORDER BY orderkey) o\n" +
                "  ON l.orderkey = o.orderkey");
    }

    @Test
    public void testNonIndexableKeys()
    {
        assertQuery("" +
                "SELECT *\n" +
                "FROM (\n" +
                "  SELECT *\n" +
                "  FROM lineitem\n" +
                "  WHERE partkey % 8 = 0) l\n" +
                "JOIN (\n" +
                "  SELECT orderkey % 2 as orderkey\n" +
                "  FROM orders) o\n" +
                "  ON l.orderkey = o.orderkey");
    }

    @Test
    public void testComposableIndexJoins()
    {
        assertQuery("" +
                "SELECT *\n" +
                "FROM (\n" +
                "  SELECT *\n" +
                "  FROM lineitem\n" +
                "  WHERE partkey % 8 = 0) x\n" +
                "JOIN (\n" +
                "  SELECT o1.orderkey as orderkey, o2.custkey as custkey\n" +
                "  FROM orders o1\n" +
                "  JOIN orders o2\n" +
                "    ON o1.orderkey = o2.orderkey) y\n" +
                "  ON x.orderkey = y.orderkey\n");
    }

    @Test
    public void testNonComposableIndexJoins()
    {
        assertQuery("" +
                "SELECT *\n" +
                "FROM (\n" +
                "  SELECT *\n" +
                "  FROM lineitem\n" +
                "  WHERE partkey % 8 = 0) x\n" +
                "JOIN (\n" +
                "  SELECT l.orderkey as orderkey, o.custkey as custkey\n" +
                "  FROM lineitem l\n" +
                "  JOIN orders o\n" +
                "    ON l.orderkey = o.orderkey) y\n" +
                "  ON x.orderkey = y.orderkey\n");
    }

    @Test
    public void testOverlappingIndexJoinLookupSymbol()
    {
        assertQuery("" +
                "SELECT *\n" +
                "FROM (\n" +
                "  SELECT *\n" +
                "  FROM lineitem\n" +
                "  WHERE partkey % 8 = 0) l\n" +
                "JOIN orders o\n" +
                "  ON l.orderkey % 1024 = o.orderkey AND l.partkey % 1024 = o.orderkey");
    }

    @Test
    public void testOverlappingSourceOuterIndexJoinLookupSymbol()
    {
        assertQuery("" +
                "SELECT *\n" +
                "FROM (\n" +
                "  SELECT *\n" +
                "  FROM lineitem\n" +
                "  WHERE partkey % 8 = 0) l\n" +
                "LEFT JOIN orders o\n" +
                "  ON l.orderkey % 1024 = o.orderkey AND l.partkey % 1024 = o.orderkey");
    }

    @Test
    public void testOverlappingIndexJoinProbeSymbol()
    {
        assertQuery("" +
                "SELECT *\n" +
                "FROM (\n" +
                "  SELECT *\n" +
                "  FROM lineitem\n" +
                "  WHERE partkey % 8 = 0) l\n" +
                "JOIN orders o\n" +
                "  ON l.orderkey = o.orderkey AND l.orderkey = o.custkey");
    }

    @Test
    public void testOverlappingSourceOuterIndexJoinProbeSymbol()
    {
        assertQuery("" +
                "SELECT *\n" +
                "FROM (\n" +
                "  SELECT *\n" +
                "  FROM lineitem\n" +
                "  WHERE partkey % 8 = 0) l\n" +
                "LEFT JOIN orders o\n" +
                "  ON l.orderkey = o.orderkey AND l.orderkey = o.custkey");
    }

    @Test
    public void testRepeatedIndexJoinClause()
    {
        assertQuery("" +
                "SELECT *\n" +
                "FROM (\n" +
                "  SELECT *\n" +
                "  FROM lineitem\n" +
                "  WHERE partkey % 8 = 0) l\n" +
                "JOIN orders o\n" +
                "  ON l.orderkey = o.orderkey AND l.orderkey = o.orderkey");
    }

    /**
     * Assure nulls in probe readahead does not leak into connectors.
     */
    @Test
    public void testProbeNullInReadahead()
    {
        assertQuery(
                "select count(*) from (values (1), (cast(null as bigint))) x(orderkey) join orders using (orderkey)",
                "select count(*) from orders where orderkey = 1");
    }

    @Test
    public void testHighCardinalityIndexJoinResult()
    {
        assertQuery("" +
                "SELECT *\n" +
                "FROM (\n" +
                "  SELECT *\n" +
                "  FROM orders\n" +
                "  WHERE orderkey % 10000 = 0) o1\n" +
                "JOIN (\n" +
                "  SELECT *\n" +
                "  FROM orders\n" +
                "  WHERE orderkey % 4 = 0) o2\n" +
                "  ON o1.orderstatus = o2.orderstatus AND o1.shippriority = o2.shippriority");
    }

    @Test
    public void testReducedIndexProbeKey()
    {
        assertQuery("" +
                "SELECT *\n" +
                "FROM (\n" +
                "  SELECT orderkey % 64 AS a, suppkey % 2 AS b\n" +
                "  FROM lineitem\n" +
                "  WHERE partkey % 8 = 0) l\n" +
                "JOIN (\n" +
                "  SELECT orderkey AS a, SUM(LENGTH(comment)) % 2 AS b\n" +
                "  FROM orders\n" +
                "  GROUP BY orderkey) o\n" +
                "  ON l.a = o.a AND l.b = o.b");
    }

    @Test
    public void testReducedIndexProbeKeyNegativeCaching()
    {
        // Not every column 'b' can be matched through the join
        assertQuery("" +
                "SELECT *\n" +
                "FROM (\n" +
                "  SELECT orderkey % 64 AS a, (suppkey % 2) + 1 AS b\n" +
                "  FROM lineitem\n" +
                "  WHERE partkey % 8 = 0) l\n" +
                "JOIN (\n" +
                "  SELECT orderkey AS a, SUM(LENGTH(comment)) % 2 AS b\n" +
                "  FROM orders\n" +
                "  GROUP BY orderkey) o\n" +
                "  ON l.a = o.a AND l.b = o.b");
    }

    @Test
    public void testHighCardinalityReducedIndexProbeKey()
    {
        assertQuery("" +
                "SELECT *\n" +
                "FROM (\n" +
                "  SELECT *, custkey % 4 AS x, custkey % 2 AS y\n" +
                "  FROM orders\n" +
                "  WHERE orderkey % 10000 = 0) o1\n" +
                "JOIN (\n" +
                "  SELECT *, custkey % 5 AS x, custkey % 3 AS y\n" +
                "  FROM orders\n" +
                "  WHERE orderkey % 4 = 0) o2\n" +
                "  ON o1.orderstatus = o2.orderstatus AND o1.shippriority = o2.shippriority AND o1.x = o2.x AND o1.y = o2.y");
    }

    @Test
    public void testReducedIndexProbeKeyComplexQueryShapes()
    {
        // Reduce the probe key through projections, aggregations, and joins
        assertQuery("" +
                "SELECT *\n" +
                "FROM (\n" +
                "  SELECT orderkey % 64 AS a, suppkey % 2 AS b, orderkey AS c, linenumber % 2 AS d\n" +
                "  FROM lineitem\n" +
                "  WHERE partkey % 7 = 0) l\n" +
                "JOIN (\n" +
                "  SELECT t1.a AS a, t1.b AS b, t2.orderkey AS c, SUM(LENGTH(t2.comment)) % 2 AS d\n" +
                "  FROM (\n" +
                "    SELECT orderkey AS a, custkey % 3 AS b\n" +
                "    FROM orders\n" +
                "  ) t1\n" +
                "  JOIN orders t2 ON t1.a = (t2.orderkey % 1000)\n" +
                "  WHERE t1.a % 1000 = 0\n" +
                "  GROUP BY t1.a, t1.b, t2.orderkey) o\n" +
                "  ON l.a = o.a AND l.b = o.b AND l.c = o.c AND l.d = o.d");
    }

    @Test
    public void testIndexJoinConstantPropagation()
    {
        assertQuery("" +
                "SELECT x, y, COUNT(*)\n" +
                "FROM (SELECT orderkey, 0 AS x FROM orders) a \n" +
                "JOIN (SELECT orderkey, 1 AS y FROM orders) b \n" +
                "ON a.orderkey = b.orderkey\n" +
                "GROUP BY 1, 2");
    }

    @Test
    public void testIndexJoinThroughWindow()
    {
        assertQuery("" +
                        "SELECT *\n" +
                        "FROM (\n" +
                        "  SELECT *\n" +
                        "  FROM lineitem\n" +
                        "  WHERE partkey % 16 = 0) l\n" +
                        "JOIN (\n" +
                        "  SELECT *, COUNT(*) OVER (PARTITION BY orderkey)\n" +
                        "  FROM orders) o\n" +
                        "  ON l.orderkey = o.orderkey",
                "" +
                        "SELECT *\n" +
                        "FROM (\n" +
                        "  SELECT *\n" +
                        "  FROM lineitem\n" +
                        "  WHERE partkey % 16 = 0) l\n" +
                        "JOIN (\n" +
                        "  SELECT *, 1\n" +
                        "  FROM orders) o\n" +
                        "  ON l.orderkey = o.orderkey");
    }

    @Test
    public void testIndexJoinThroughWindowDoubleAggregation()
    {
        assertQuery("" +
                        "SELECT *\n" +
                        "FROM (\n" +
                        "  SELECT *\n" +
                        "  FROM lineitem\n" +
                        "  WHERE partkey % 16 = 0) l\n" +
                        "JOIN (\n" +
                        "  SELECT *, COUNT(*) OVER (PARTITION BY orderkey), SUM(orderkey) OVER (PARTITION BY orderkey)\n" +
                        "  FROM orders) o\n" +
                        "  ON l.orderkey = o.orderkey",
                "" +
                        "SELECT *\n" +
                        "FROM (\n" +
                        "  SELECT *\n" +
                        "  FROM lineitem\n" +
                        "  WHERE partkey % 16 = 0) l\n" +
                        "JOIN (\n" +
                        "  SELECT *, 1, orderkey as o\n" +
                        "  FROM orders) o\n" +
                        "  ON l.orderkey = o.orderkey");
    }

    @Test
    public void testIndexJoinThroughWindowPartialPartition()
    {
        assertQuery("" +
                        "SELECT *\n" +
                        "FROM (\n" +
                        "  SELECT *\n" +
                        "  FROM lineitem\n" +
                        "  WHERE partkey % 16 = 0) l\n" +
                        "JOIN (\n" +
                        "  SELECT *, COUNT(*) OVER (PARTITION BY orderkey, custkey)\n" +
                        "  FROM orders) o\n" +
                        "  ON l.orderkey = o.orderkey",
                "" +
                        "SELECT *\n" +
                        "FROM (\n" +
                        "  SELECT *\n" +
                        "  FROM lineitem\n" +
                        "  WHERE partkey % 16 = 0) l\n" +
                        "JOIN (\n" +
                        "  SELECT *, 1\n" +
                        "  FROM orders) o\n" +
                        "  ON l.orderkey = o.orderkey");
    }

    @Test
    public void testNoIndexJoinThroughWindowWithRowNumberFunction()
    {
        assertQuery("" +
                        "SELECT *\n" +
                        "FROM (\n" +
                        "  SELECT *\n" +
                        "  FROM lineitem\n" +
                        "  WHERE partkey % 16 = 0) l\n" +
                        "JOIN (\n" +
                        "  SELECT *, row_number() OVER (PARTITION BY orderkey)\n" +
                        "  FROM orders) o\n" +
                        "  ON l.orderkey = o.orderkey",
                "" +
                        "SELECT *\n" +
                        "FROM (\n" +
                        "  SELECT *\n" +
                        "  FROM lineitem\n" +
                        "  WHERE partkey % 16 = 0) l\n" +
                        "JOIN (\n" +
                        "  SELECT *, 1\n" +
                        "  FROM orders) o\n" +
                        "  ON l.orderkey = o.orderkey");
    }

    @Test
    public void testNoIndexJoinThroughWindowWithOrderBy()
    {
        assertQuery("" +
                        "SELECT *\n" +
                        "FROM (\n" +
                        "  SELECT *\n" +
                        "  FROM lineitem\n" +
                        "  WHERE partkey % 16 = 0) l\n" +
                        "JOIN (\n" +
                        "  SELECT *, COUNT(*) OVER (PARTITION BY orderkey ORDER BY custkey)\n" +
                        "  FROM orders) o\n" +
                        "  ON l.orderkey = o.orderkey",
                "" +
                        "SELECT *\n" +
                        "FROM (\n" +
                        "  SELECT *\n" +
                        "  FROM lineitem\n" +
                        "  WHERE partkey % 16 = 0) l\n" +
                        "JOIN (\n" +
                        "  SELECT *, 1\n" +
                        "  FROM orders) o\n" +
                        "  ON l.orderkey = o.orderkey");
    }

    @Test
    public void testNoIndexJoinThroughWindowWithRowFrame()
    {
        assertQuery("" +
                        "SELECT l.orderkey, o.c\n" +
                        "FROM (\n" +
                        "  SELECT *\n" +
                        "  FROM lineitem\n" +
                        "  WHERE partkey % 16 = 0) l\n" +
                        "JOIN (\n" +
                        "  SELECT *, COUNT(*) OVER (PARTITION BY orderkey ROWS 1 PRECEDING) as c\n" +
                        "  FROM orders) o\n" +
                        "  ON l.orderkey = o.orderkey",
                "" +
                        "SELECT l.orderkey, o.c\n" +
                        "FROM (\n" +
                        "  SELECT *\n" +
                        "  FROM lineitem\n" +
                        "  WHERE partkey % 16 = 0) l\n" +
                        "JOIN (\n" +
                        "  SELECT *, 1 as c\n" +
                        "  FROM orders) o\n" +
                        "  ON l.orderkey = o.orderkey");
    }

    @Test
    public void testOuterNonEquiJoins()
    {
        assertQuery("SELECT COUNT(*) FROM lineitem LEFT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.quantity > 5 WHERE orders.orderkey IS NULL");
        assertQuery("SELECT COUNT(*) FROM orders RIGHT OUTER JOIN lineitem ON lineitem.orderkey = orders.orderkey AND lineitem.quantity > 5 WHERE orders.orderkey IS NULL");
    }

    @Test
    public void testNonEquiJoin()
    {
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.quantity + length(orders.comment) > 7");
    }
}
