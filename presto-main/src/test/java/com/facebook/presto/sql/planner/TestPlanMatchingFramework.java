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
package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.any;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.columnReference;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.functionCall;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.output;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;

public class TestPlanMatchingFramework
        extends BasePlanTest
{
    @Test
    public void testOutput()
    {
        assertMinimallyOptimizedPlan("SELECT orderkey FROM lineitem",
                node(OutputNode.class,
                        node(TableScanNode.class).withAlias("ORDERKEY", columnReference("lineitem", "orderkey")))
                .withOutputs(ImmutableList.of("ORDERKEY")));
    }

    @Test
    public void testOutputSameColumnMultipleTimes()
    {
        assertMinimallyOptimizedPlan("SELECT orderkey, orderkey FROM lineitem",
                output(ImmutableList.of("ORDERKEY", "ORDERKEY"),
                        tableScan("lineitem", ImmutableMap.of("ORDERKEY", "orderkey"))));
    }

    @Test
    public void testOutputSameColumnMultipleTimesWithOtherOutputs()
    {
        assertMinimallyOptimizedPlan("SELECT extendedprice, orderkey, discount, orderkey, linenumber FROM lineitem",
                output(ImmutableList.of("ORDERKEY", "ORDERKEY"),
                        /*
                         * This is a project node, but this gives us a convenient way to verify that
                         * visitProject is correctly handled through an anyTree.
                         */
                        anyTree(
                                tableScan("lineitem", ImmutableMap.of("ORDERKEY", "orderkey")))));
    }

    @Test
    public void testUnreferencedSymbolsDontNeedBinding()
    {
        assertMinimallyOptimizedPlan("SELECT orderkey, 2 FROM lineitem",
                output(ImmutableList.of("ORDERKEY"),
                        anyTree(
                                tableScan("lineitem", ImmutableMap.of("ORDERKEY", "orderkey")))));
    }

    @Test
    public void testAliasConstantFromProject()
    {
        assertMinimallyOptimizedPlan("SELECT orderkey, 2 FROM lineitem",
                output(ImmutableList.of("ORDERKEY", "TWO"),
                        project(ImmutableMap.of("TWO", expression("2")),
                                tableScan("lineitem", ImmutableMap.of("ORDERKEY", "orderkey")))));
    }

    @Test
    public void testAliasExpressionFromProject()
    {
        assertMinimallyOptimizedPlan("SELECT orderkey, 1 + orderkey FROM lineitem",
                output(ImmutableList.of("ORDERKEY", "EXPRESSION"),
                        project(ImmutableMap.of("EXPRESSION", expression("1 + ORDERKEY")),
                                tableScan("lineitem", ImmutableMap.of("ORDERKEY", "orderkey")))));
    }

    @Test
    public void testTableScan()
    {
        assertMinimallyOptimizedPlan("SELECT orderkey FROM lineitem",
                output(ImmutableList.of("ORDERKEY"),
                        tableScan("lineitem", ImmutableMap.of("ORDERKEY", "orderkey"))));
    }

    @Test
    public void testJoinMatcher()
    {
        assertPlan("SELECT o.orderkey FROM orders o, lineitem l WHERE l.orderkey = o.orderkey",
                anyTree(
                        join(INNER, ImmutableList.of(equiJoinClause("ORDERS_OK", "LINEITEM_OK")),
                                any(
                                        tableScan("orders").withAlias("ORDERS_OK", columnReference("orders", "orderkey"))),
                                anyTree(
                                        tableScan("lineitem").withAlias("LINEITEM_OK", columnReference("lineitem", "orderkey"))))));
    }

    @Test
    public void testSelfJoin()
    {
        assertPlan("SELECT l.orderkey FROM orders l, orders r WHERE l.orderkey = r.orderkey",
                anyTree(
                        join(INNER, ImmutableList.of(equiJoinClause("L_ORDERS_OK", "R_ORDERS_OK")),
                                any(
                                        tableScan("orders").withAlias("L_ORDERS_OK", columnReference("orders", "orderkey"))),
                                anyTree(
                                        tableScan("orders").withAlias("R_ORDERS_OK", columnReference("orders", "orderkey"))))));
    }

    @Test
    public void testAggregation()
    {
        assertMinimallyOptimizedPlan("SELECT COUNT(nationkey) FROM nation",
                output(ImmutableList.of("COUNT"),
                        aggregation(ImmutableMap.of("COUNT", functionCall("count", ImmutableList.of("NATIONKEY"))),
                                tableScan("nation", ImmutableMap.of("NATIONKEY", "nationkey")))));
    }

    @Test
    public void testValues()
    {
        assertMinimallyOptimizedPlan("SELECT * from (VALUES 1, 2)",
                output(ImmutableList.of("VALUE"),
                        values(ImmutableMap.of("VALUE", 0))));
    }

    @Test(expectedExceptions = { IllegalStateException.class }, expectedExceptionsMessageRegExp = ".* doesn't have column .*")
    public void testAliasNonexistentColumn()
    {
        assertMinimallyOptimizedPlan("SELECT orderkey FROM lineitem",
                node(OutputNode.class,
                        node(TableScanNode.class).withAlias("ORDERKEY", columnReference("lineitem", "NXCOLUMN"))));
    }

    @Test(expectedExceptions = { IllegalStateException.class }, expectedExceptionsMessageRegExp = "missing expression for alias .*")
    public void testReferenceNonexistentAlias()
    {
        assertMinimallyOptimizedPlan("SELECT orderkey FROM lineitem",
                output(ImmutableList.of("NXALIAS"),
                        tableScan("lineitem", ImmutableMap.of("ORDERKEY", "orderkey"))));
    }

    @Test(expectedExceptions = { IllegalStateException.class }, expectedExceptionsMessageRegExp = ".*already bound to expression.*")
    public void testDuplicateAliases()
    {
        assertPlan("SELECT o.orderkey FROM orders o, lineitem l WHERE l.orderkey = o.orderkey",
                anyTree(
                        join(INNER, ImmutableList.of(equiJoinClause("ORDERS_OK", "LINEITEM_OK")),
                                any(
                                        tableScan("orders").withAlias("ORDERS_OK", columnReference("orders", "orderkey"))),
                                anyTree(
                                        tableScan("lineitem").withAlias("ORDERS_OK", columnReference("lineitem", "orderkey"))))));
    }

    @Test(expectedExceptions = { IllegalStateException.class }, expectedExceptionsMessageRegExp = ".*already bound in.*")
    public void testBindMultipleAliasesSameExpression()
    {
        assertMinimallyOptimizedPlan("SELECT orderkey FROM lineitem",
                output(ImmutableList.of("ORDERKEY", "TWO"),
                        tableScan("lineitem", ImmutableMap.of("FIRST", "orderkey", "SECOND", "orderkey"))));
    }

    @Test(expectedExceptions = { IllegalStateException.class }, expectedExceptionsMessageRegExp = "missing expression for alias .*")
    public void testProjectLimitsScope()
    {
        assertMinimallyOptimizedPlan("SELECT 1 + orderkey FROM lineitem",
                output(ImmutableList.of("ORDERKEY"),
                        project(ImmutableMap.of("EXPRESSION", expression("1 + ORDERKEY")),
                                tableScan("lineitem", ImmutableMap.of("ORDERKEY", "orderkey")))));
    }
}
