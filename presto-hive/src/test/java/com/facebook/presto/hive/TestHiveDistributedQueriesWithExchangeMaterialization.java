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

    @Override
    public void testApproxSetBigintGroupBy()
    {
        // Unsupported Hive type: HyperLogLog
    }

    @Override
    public void testApproxSetVarcharGroupBy()
    {
        // Unsupported Hive type: HyperLogLog
    }

    @Override
    public void testApproxSetDoubleGroupBy()
    {
        // Unsupported Hive type: HyperLogLog
    }

    @Override
    public void testApproxSetGroupByWithOnlyNullsInOneGroup()
    {
        // Unsupported Hive type: HyperLogLog
    }

    @Override
    public void testApproxSetGroupByWithNulls()
    {
        // Unsupported Hive type: HyperLogLog
    }

    @Override
    public void testMergeHyperLogLogGroupBy()
    {
        // Unsupported Hive type: HyperLogLog
    }

    @Override
    public void testMergeHyperLogLogGroupByWithNulls()
    {
        // Unsupported Hive type: HyperLogLog
    }

    @Override
    public void testP4ApproxSetBigintGroupBy()
    {
        // Unsupported Hive type: HyperLogLog
    }

    @Override
    public void testP4ApproxSetVarcharGroupBy()
    {
        // Unsupported Hive type: HyperLogLog
    }

    @Override
    public void testP4ApproxSetDoubleGroupBy()
    {
        // Unsupported Hive type: HyperLogLog
    }

    @Override
    public void testP4ApproxSetGroupByWithOnlyNullsInOneGroup()
    {
        // Unsupported Hive type: HyperLogLog
    }

    @Override
    public void testP4ApproxSetGroupByWithNulls()
    {
        // Unsupported Hive type: HyperLogLog
    }

    @Override
    public void testHaving3()
    {
        // Anonymous row type is not supported in Hive
    }

    @Override
    public void testJoinCoercionOnEqualityComparison()
    {
        // Anonymous row type is not supported in Hive
    }

    @Override
    public void testCorrelatedScalarSubqueriesWithScalarAggregation()
    {
        // Anonymous row type is not supported in Hive
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
