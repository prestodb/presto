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
package com.facebook.presto.hive;

import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.tests.AbstractTestDistributedQueries;
import org.testng.annotations.Test;

import static com.facebook.presto.hive.HiveQueryRunner.createMaterializingQueryRunner;
import static com.facebook.presto.hive.TestHiveIntegrationSmokeTest.assertRemoteMaterializedExchangesCount;
import static com.facebook.presto.sql.tree.ExplainType.Type.LOGICAL;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.tpch.TpchTable.getTables;
import static org.testng.Assert.assertEquals;

public class TestHiveDistributedQueriesWithExchangeMaterialization
        extends AbstractTestDistributedQueries
{
    public TestHiveDistributedQueriesWithExchangeMaterialization()
    {
        super(() -> createMaterializingQueryRunner(getTables()));
    }

    @Test
    public void testMaterializedExchangesEnabled()
    {
        assertQuery(getSession(), "SELECT orderkey, COUNT(*) lines FROM lineitem GROUP BY orderkey", assertRemoteMaterializedExchangesCount(1));
    }

    @Override
    public void testDelete()
    {
        // Hive connector currently does not support row-by-row delete
    }

    @Override
    public void testExcept()
    {
        // decimal type is not supported by the Hive hash code function
    }

    @Override
    public void testIntersect()
    {
        // decimal type is not supported by the Hive hash code function
    }

    @Override
    public void testNullOnLhsOfInPredicateAllowed()
    {
        // unknown type is not supported by the Hive hash code function
    }

    @Override
    public void testQuantifiedComparison()
    {
        // decimal type is not supported by the Hive hash code function
    }

    @Override
    public void testSemiJoin()
    {
        // decimal type is not supported by the Hive hash code function
    }

    @Override
    public void testUnion()
    {
        // unknown type is not supported by the Hive hash code function
    }

    @Override
    public void testUnionRequiringCoercion()
    {
        // decimal type is not supported by the Hive hash code function
    }

    @Override
    public void testValues()
    {
        // decimal type is not supported by the Hive hash code function
    }

    @Override
    public void testAntiJoinNullHandling()
    {
        // Unsupported Hive type: unknown
    }

    @Test
    public void testExchangeMaterializationWithConstantFolding()
    {
        try {
            assertUpdate(
                    // bucket count has to be different from materialized bucket number
                    "CREATE TABLE test_constant_folding_lineitem_bucketed\n" +
                            "WITH (bucket_count = 17, bucketed_by = ARRAY['partkey_mod_9', 'partkey', 'suppkey', 'suppkey_varchar']) AS\n" +
                            "SELECT partkey % 9 partkey_mod_9, partkey, suppkey, CAST(suppkey AS VARCHAR) suppkey_varchar, comment FROM lineitem",
                    "SELECT count(*) from lineitem");
            assertUpdate(
                    "CREATE TABLE test_constant_folding_partsupp_unbucketed AS\n" +
                            "SELECT partkey % 9 partkey_mod_9, partkey, suppkey, CAST(suppkey AS VARCHAR) suppkey_varchar, comment FROM partsupp",
                    "SELECT count(*) from partsupp");

            // one constant, third position (suppkey BIGINT)
            assertQuery(
                    getSession(),
                    "SELECT lineitem.partkey, lineitem.suppkey, lineitem.comment lineitem_comment, partsupp.comment partsupp_comment\n" +
                            "FROM test_constant_folding_lineitem_bucketed lineitem JOIN test_constant_folding_partsupp_unbucketed partsupp\n" +
                            "ON\n" +
                            "  lineitem.partkey = partsupp.partkey AND\n" +
                            "  lineitem.partkey_mod_9 = partsupp.partkey_mod_9 AND\n" +
                            "  lineitem.suppkey = partsupp.suppkey AND\n" +
                            "  lineitem.suppkey_varchar = partsupp.suppkey_varchar\n" +
                            "WHERE lineitem.suppkey = 42",
                    "SELECT lineitem.partkey, lineitem.suppkey, lineitem.comment lineitem_comment, partsupp.comment partsupp_comment\n" +
                            "FROM lineitem JOIN partsupp\n" +
                            "ON lineitem.partkey = partsupp.partkey AND\n" +
                            "lineitem.suppkey = partsupp.suppkey\n" +
                            "WHERE lineitem.suppkey = 42",
                    assertRemoteMaterializedExchangesCount(1));

            // one constant, fourth position (suppkey_varchar VARCHAR)
            assertQuery(
                    getSession(),
                    "SELECT lineitem.partkey, lineitem.suppkey, lineitem.comment lineitem_comment, partsupp.comment partsupp_comment\n" +
                            "FROM test_constant_folding_lineitem_bucketed lineitem JOIN test_constant_folding_partsupp_unbucketed partsupp\n" +
                            "ON\n" +
                            "  lineitem.partkey = partsupp.partkey AND\n" +
                            "  lineitem.partkey_mod_9 = partsupp.partkey_mod_9 AND\n" +
                            "  lineitem.suppkey = partsupp.suppkey AND\n" +
                            "  lineitem.suppkey_varchar = partsupp.suppkey_varchar\n" +
                            "WHERE lineitem.suppkey_varchar = '42'",
                    "SELECT lineitem.partkey, lineitem.suppkey, lineitem.comment lineitem_comment, partsupp.comment partsupp_comment\n" +
                            "FROM lineitem JOIN partsupp\n" +
                            "ON lineitem.partkey = partsupp.partkey AND\n" +
                            "lineitem.suppkey = partsupp.suppkey\n" +
                            "WHERE lineitem.suppkey = 42",
                    assertRemoteMaterializedExchangesCount(1));

            // two constants, first and third position (partkey_mod_9 BIGINT, suppkey BIGINT)
            assertQuery(
                    getSession(),
                    "SELECT lineitem.partkey, lineitem.suppkey, lineitem.comment lineitem_comment, partsupp.comment partsupp_comment\n" +
                    "FROM test_constant_folding_lineitem_bucketed lineitem JOIN test_constant_folding_partsupp_unbucketed partsupp\n" +
                    "ON\n" +
                    "  lineitem.partkey = partsupp.partkey AND\n" +
                    "  lineitem.partkey_mod_9 = partsupp.partkey_mod_9 AND\n" +
                    "  lineitem.suppkey = partsupp.suppkey AND\n" +
                    "  lineitem.suppkey_varchar = partsupp.suppkey_varchar\n" +
                    "WHERE lineitem.partkey_mod_9 = 7 AND lineitem.suppkey = 42",
                    "SELECT lineitem.partkey, lineitem.suppkey, lineitem.comment lineitem_comment, partsupp.comment partsupp_comment\n" +
                            "FROM lineitem JOIN partsupp\n" +
                            "ON lineitem.partkey = partsupp.partkey AND\n" +
                            "lineitem.suppkey = partsupp.suppkey\n" +
                            "WHERE lineitem.partkey % 9 = 7 AND lineitem.suppkey = 42",
                    assertRemoteMaterializedExchangesCount(1));

            // two constants, first and forth position (partkey_mod_9 BIGINT, suppkey_varchar VARCHAR)
            assertQuery(
                    getSession(),
                    "SELECT lineitem.partkey, lineitem.suppkey, lineitem.comment lineitem_comment, partsupp.comment partsupp_comment\n" +
                    "FROM test_constant_folding_lineitem_bucketed lineitem JOIN test_constant_folding_partsupp_unbucketed partsupp\n" +
                    "ON\n" +
                    "  lineitem.partkey = partsupp.partkey AND\n" +
                    "  lineitem.partkey_mod_9 = partsupp.partkey_mod_9 AND\n" +
                    "  lineitem.suppkey = partsupp.suppkey AND\n" +
                    "  lineitem.suppkey_varchar = partsupp.suppkey_varchar\n" +
                    "WHERE lineitem.partkey_mod_9 = 7 AND lineitem.suppkey_varchar = '42'",
                    "SELECT lineitem.partkey, lineitem.suppkey, lineitem.comment lineitem_comment, partsupp.comment partsupp_comment\n" +
                            "FROM lineitem JOIN partsupp\n" +
                            "ON lineitem.partkey = partsupp.partkey AND\n" +
                            "lineitem.suppkey = partsupp.suppkey\n" +
                            "WHERE lineitem.partkey % 9 = 7 AND lineitem.suppkey = 42",
                    assertRemoteMaterializedExchangesCount(1));
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS test_constant_folding_lineitem_bucketed");
            assertUpdate("DROP TABLE IF EXISTS test_constant_folding_partsupp_unbucketed");
        }
    }

    @Test
    public void testExplainOfCreateTableAs()
    {
        String query = "CREATE TABLE copy_orders AS SELECT * FROM orders";
        MaterializedResult result = computeActual("EXPLAIN " + query);
        assertEquals(getOnlyElement(result.getOnlyColumnAsSet()), getExplainPlan(query, LOGICAL));
    }

    @Override
    protected boolean supportsNotNullColumns()
    {
        return false;
    }

    // Hive specific tests should normally go in TestHiveIntegrationSmokeTest
}
