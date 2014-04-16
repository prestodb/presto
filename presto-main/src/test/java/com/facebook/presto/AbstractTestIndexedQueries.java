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
package com.facebook.presto;

import com.facebook.presto.tpch.TpchIndexSpec;
import com.facebook.presto.tpch.TpchMetadata;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

public abstract class AbstractTestIndexedQueries
        extends AbstractTestQueryFramework
{
    private final TpchIndexSpec indexSpec;

    protected AbstractTestIndexedQueries()
    {
        // Generate the indexed data sets
        this.indexSpec = new TpchIndexSpec.Builder()
                .addIndex("orders", TpchMetadata.TINY_SCALE_FACTOR, ImmutableSet.of("orderkey"))
                .addIndex("orders", TpchMetadata.TINY_SCALE_FACTOR, ImmutableSet.of("orderkey", "orderstatus"))
                .addIndex("orders", TpchMetadata.TINY_SCALE_FACTOR, ImmutableSet.of("orderkey", "custkey"))
                .build();
    }

    protected TpchIndexSpec getTpchIndexSpec()
    {
        return indexSpec;
    }

    @Test
    public void testBasicIndexJoin()
            throws Exception
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
            throws Exception
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
            throws Exception
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
            throws Exception
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
            throws Exception
    {
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
    public void testPredicateDerivedKey()
            throws Exception
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
            throws Exception
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
            throws Exception
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
            throws Exception
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
            throws Exception
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
            throws Exception
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
            throws Exception
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
            throws Exception
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
            throws Exception
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
            throws Exception
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
            throws Exception
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
            throws Exception
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
            throws Exception
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
    public void testOverlappingIndexJoinProbeSymbol()
            throws Exception
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
    public void testRepeatedIndexJoinClause()
            throws Exception
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
}
