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

import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.facebook.presto.sql.planner.plan.IndexJoinNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

import static com.facebook.presto.spi.predicate.Domain.singleValue;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.any;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.apply;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.constrainedTableScan;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.functionCall;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.semiJoin;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.LEFT;
import static io.airlift.slice.Slices.utf8Slice;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;

public class TestLogicalPlanner
        extends BasePlanTest
{
    @Test
    public void testJoin()
    {
        assertPlan("SELECT o.orderkey FROM orders o, lineitem l WHERE l.orderkey = o.orderkey",
                anyTree(
                        join(INNER, ImmutableList.of(equiJoinClause("ORDERS_OK", "LINEITEM_OK")),
                                any(
                                        tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey"))),
                                anyTree(
                                        tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey"))))));
    }

    @Test
    public void testJoinWithOrderBySameKey()
    {
        assertPlan("SELECT o.orderkey FROM orders o, lineitem l WHERE l.orderkey = o.orderkey ORDER BY l.orderkey ASC, o.orderkey ASC",
                anyTree(
                        join(INNER, ImmutableList.of(equiJoinClause("ORDERS_OK", "LINEITEM_OK")),
                                any(
                                        tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey"))),
                                anyTree(
                                        tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey"))))));
    }

    @Test
    public void testUncorrelatedSubqueries()
    {
        assertPlan("SELECT * FROM orders WHERE orderkey = (SELECT orderkey FROM lineitem ORDER BY orderkey LIMIT 1)",
                anyTree(
                        join(INNER, ImmutableList.of(equiJoinClause("X", "Y")),
                                project(
                                        tableScan("orders", ImmutableMap.of("X", "orderkey"))),
                                project(
                                        node(EnforceSingleRowNode.class,
                                                anyTree(
                                                        tableScan("lineitem", ImmutableMap.of("Y", "orderkey"))))))));

        assertPlan("SELECT * FROM orders WHERE orderkey IN (SELECT orderkey FROM lineitem WHERE linenumber % 4 = 0)",
                anyTree(
                        filter("S",
                                project(
                                        semiJoin("X", "Y", "S",
                                                anyTree(
                                                        tableScan("orders", ImmutableMap.of("X", "orderkey"))),
                                                anyTree(
                                                        tableScan("lineitem", ImmutableMap.of("Y", "orderkey"))))))));

        assertPlan("SELECT * FROM orders WHERE orderkey NOT IN (SELECT orderkey FROM lineitem WHERE linenumber < 0)",
                anyTree(
                        filter("NOT S",
                                project(
                                        semiJoin("X", "Y", "S",
                                                anyTree(
                                                        tableScan("orders", ImmutableMap.of("X", "orderkey"))),
                                                anyTree(
                                                        tableScan("lineitem", ImmutableMap.of("Y", "orderkey"))))))));
    }

    @Test
    public void testPushDownJoinConditionConjunctsToInnerSideBasedOnInheritedPredicate()
    {
        Map<String, Domain> tableScanConstraint = ImmutableMap.<String, Domain>builder()
                .put("name", singleValue(createVarcharType(25), utf8Slice("blah")))
                .build();

        assertPlan(
                "SELECT nationkey FROM nation LEFT OUTER JOIN region " +
                        "ON nation.regionkey = region.regionkey and nation.name = region.name WHERE nation.name = 'blah'",
                anyTree(
                        join(LEFT, ImmutableList.of(equiJoinClause("NATION_NAME", "REGION_NAME"), equiJoinClause("NATION_REGIONKEY", "REGION_REGIONKEY")),
                                anyTree(
                                        constrainedTableScan("nation", tableScanConstraint, ImmutableMap.of(
                                                "NATION_NAME", "name",
                                                "NATION_REGIONKEY", "regionkey"))),
                                anyTree(
                                        constrainedTableScan("region", tableScanConstraint, ImmutableMap.of(
                                                "REGION_NAME", "name",
                                                "REGION_REGIONKEY", "regionkey"))))));
    }

    @Test
    public void testSameScalarSubqueryIsAppliedOnlyOnce()
    {
        // three subqueries with two duplicates (coerced to two different types), only two scalar joins should be in plan
        assertEquals(
                countOfMatchingNodes(
                        plan("SELECT * FROM orders WHERE CAST(orderkey AS INTEGER) = (SELECT 1) AND custkey = (SELECT 2) AND CAST(custkey as REAL) != (SELECT 1)"),
                        EnforceSingleRowNode.class::isInstance),
                2);
        // same query used for left, right and complex join condition
        assertEquals(
                countOfMatchingNodes(
                        plan("SELECT * FROM orders o1 JOIN orders o2 ON o1.orderkey = (SELECT 1) AND o2.orderkey = (SELECT 1) AND o1.orderkey + o2.orderkey = (SELECT 1)"),
                        EnforceSingleRowNode.class::isInstance),
                1);
    }

    @Test
    public void testSameInSubqueryIsAppliedOnlyOnce()
    {
        // same IN query used for left, right and complex condition
        assertEquals(
                countOfMatchingNodes(
                        plan("SELECT * FROM orders o1 JOIN orders o2 ON o1.orderkey IN (SELECT 1) AND (o1.orderkey IN (SELECT 1) OR o1.orderkey IN (SELECT 1))"),
                        SemiJoinNode.class::isInstance),
                1);

        // one subquery used for "1 IN (SELECT 1)", one subquery used for "2 IN (SELECT 1)"
        assertEquals(
                countOfMatchingNodes(
                        plan("SELECT 1 IN (SELECT 1), 2 IN (SELECT 1) WHERE 1 IN (SELECT 1)"),
                        SemiJoinNode.class::isInstance),
                2);
    }

    @Test
    public void testSameQualifiedSubqueryIsAppliedOnlyOnce()
    {
        // same ALL query used for left, right and complex condition
        assertEquals(
                countOfMatchingNodes(
                        plan("SELECT * FROM orders o1 JOIN orders o2 ON o1.orderkey <= ALL(SELECT 1) AND (o1.orderkey <= ALL(SELECT 1) OR o1.orderkey <= ALL(SELECT 1))"),
                        AggregationNode.class::isInstance),
                1);

        // one subquery used for "1 <= ALL(SELECT 1)", one subquery used for "2 <= ALL(SELECT 1)"
        assertEquals(
                countOfMatchingNodes(
                        plan("SELECT 1 <= ALL(SELECT 1), 2 <= ALL(SELECT 1) WHERE 1 <= ALL(SELECT 1)"),
                        AggregationNode.class::isInstance),
                2);
    }

    private static int countOfMatchingNodes(Plan plan, Predicate<PlanNode> predicate)
    {
        PlanNodeExtractor planNodeExtractor = new PlanNodeExtractor(predicate);
        plan.getRoot().accept(planNodeExtractor, null);
        return planNodeExtractor.getNodes().size();
    }

    @Test
    public void testRemoveUnreferencedScalarInputApplyNodes()
    {
        assertPlanContainsNoApplyOrJoin("SELECT (SELECT 1)");
    }

    @Test
    public void testSubqueryPruning()
    {
        List<String> subqueries = ImmutableList.of(
                "orderkey IN (SELECT orderkey FROM lineitem WHERE orderkey % 2 = 0)",
                "EXISTS(SELECT orderkey FROM lineitem WHERE orderkey % 2 = 0)",
                "0 = (SELECT orderkey FROM lineitem WHERE orderkey % 2 = 0)");

        for (String subquery : subqueries) {
            // Apply can be rewritten to *join so we expect no join here as well
            assertPlanContainsNoApplyOrJoin("SELECT COUNT(*) FROM (SELECT " + subquery + " FROM orders)");
            // TODO enable when pruning apply nodes works for this kind of query
            // assertPlanContainsNoApplyOrJoin("SELECT * FROM orders WHERE true OR " + subquery);
        }
    }

    private void assertPlanContainsNoApplyOrJoin(String sql)
    {
        PlanNodeExtractor planNodeExtractor = new PlanNodeExtractor(
                planNode -> planNode instanceof ApplyNode
                        || planNode instanceof JoinNode
                        || planNode instanceof IndexJoinNode
                        || planNode instanceof SemiJoinNode);
        plan(sql, LogicalPlanner.Stage.OPTIMIZED).getRoot().accept(planNodeExtractor, null);
        assertEquals(planNodeExtractor.getNodes().size(), 0, "Unexpected node for query: " + sql);
    }

    @Test
    public void testCorrelatedSubqueries()
    {
        assertPlan(
                "SELECT orderkey FROM orders WHERE 3 = (SELECT orderkey)",
                LogicalPlanner.Stage.OPTIMIZED,
                anyTree(
                        filter("3 = X",
                                apply(ImmutableList.of("X"),
                                        ImmutableMap.of(),
                                        tableScan("orders", ImmutableMap.of("X", "orderkey")),
                                        node(EnforceSingleRowNode.class,
                                                project(
                                                        node(ValuesNode.class)
                                                ))))));
    }

    @Test
    public void testDoubleNestedCorrelatedSubqueries()
    {
        assertPlan(
                "SELECT orderkey FROM orders o " +
                        "WHERE 3 IN (SELECT o.custkey FROM lineitem l WHERE (SELECT l.orderkey = o.orderkey))",
                LogicalPlanner.Stage.OPTIMIZED,
                anyTree(
                        filter("OUTER_FILTER",
                                apply(ImmutableList.of("C", "O"),
                                        ImmutableMap.of("OUTER_FILTER", expression("THREE IN (C)")),
                                        project(ImmutableMap.of("THREE", expression("3")),
                                                tableScan("orders", ImmutableMap.of(
                                                        "O", "orderkey",
                                                        "C", "custkey"))),
                                        anyTree(
                                                apply(ImmutableList.of("L"),
                                                        ImmutableMap.of(),
                                                        tableScan("lineitem", ImmutableMap.of("L", "orderkey")),
                                                        node(EnforceSingleRowNode.class,
                                                                project(
                                                                        node(ValuesNode.class)
                                                                ))))))));
    }

    @Test
    public void testCorrelatedScalarAggregationRewriteToLeftOuterJoin()
    {
        assertPlan(
                "SELECT orderkey FROM orders WHERE EXISTS(SELECT 1 WHERE orderkey = 3)", // EXISTS maps to count(*) = 1
                anyTree(
                        filter("FINAL_COUNT > 0",
                                any(
                                        aggregation(ImmutableMap.of("FINAL_COUNT", functionCall("count", ImmutableList.of("PARTIAL_COUNT"))),
                                                any(
                                                        aggregation(ImmutableMap.of("PARTIAL_COUNT", functionCall("count", ImmutableList.of("NON_NULL"))),
                                                                any(
                                                                        join(LEFT, ImmutableList.of(), Optional.of("ORDERKEY = 3"),
                                                                                any(
                                                                                        tableScan("orders", ImmutableMap.of("ORDERKEY", "orderkey"))),
                                                                                project(ImmutableMap.of("NON_NULL", expression("true")),
                                                                                        node(ValuesNode.class)))))))))));
    }

    private static final class PlanNodeExtractor
            extends SimplePlanVisitor<Void>
    {
        private final Predicate<PlanNode> predicate;
        private ImmutableList.Builder<PlanNode> nodes = ImmutableList.builder();

        public PlanNodeExtractor(Predicate<PlanNode> predicate)
        {
            this.predicate = requireNonNull(predicate, "predicate is null");
        }

        @Override
        protected Void visitPlan(PlanNode node, Void context)
        {
            if (predicate.test(node)) {
                nodes.add(node);
            }
            return super.visitPlan(node, null);
        }

        public List<PlanNode> getNodes()
        {
            return nodes.build();
        }
    }
}
