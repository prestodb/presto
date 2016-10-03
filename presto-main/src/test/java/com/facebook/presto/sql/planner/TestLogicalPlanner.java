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
import com.facebook.presto.sql.planner.assertions.PlanAssert;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.facebook.presto.sql.planner.plan.IndexJoinNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static com.facebook.presto.spi.predicate.Domain.singleValue;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aliasPair;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.any;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.apply;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.semiJoin;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.LEFT;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static io.airlift.slice.Slices.utf8Slice;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestLogicalPlanner
{
    private final LocalQueryRunner queryRunner;

    public TestLogicalPlanner()
    {
        this.queryRunner = new LocalQueryRunner(testSessionBuilder()
                .setCatalog("local")
                .setSchema("tiny")
                .build());

        queryRunner.createCatalog(queryRunner.getDefaultSession().getCatalog().get(),
                new TpchConnectorFactory(1),
                ImmutableMap.<String, String>of());
    }

    @Test
    public void testJoin()
    {
        assertPlan("SELECT o.orderkey FROM orders o, lineitem l WHERE l.orderkey = o.orderkey",
                anyTree(
                        join(INNER, ImmutableList.of(aliasPair("O", "L")),
                                any(
                                        tableScan("orders").withSymbol("orderkey", "O")),
                                anyTree(
                                        tableScan("lineitem").withSymbol("orderkey", "L")))));
    }

    @Test
    public void testUncorrelatedSubqueries()
    {
        assertPlan("SELECT * FROM orders WHERE orderkey = (SELECT orderkey FROM lineitem ORDER BY orderkey LIMIT 1)",
                anyTree(
                        join(INNER, ImmutableList.of(aliasPair("X", "Y")),
                                project(
                                        tableScan("orders").withSymbol("orderkey", "X")),
                                project(
                                        node(EnforceSingleRowNode.class,
                                                anyTree(
                                                        tableScan("lineitem").withSymbol("orderkey", "Y")))))));

        assertPlan("SELECT * FROM orders WHERE orderkey IN (SELECT orderkey FROM lineitem WHERE linenumber % 4 = 0)",
                anyTree(
                        filter("S",
                                project(
                                        semiJoin("X", "Y", "S",
                                                anyTree(
                                                        tableScan("orders").withSymbol("orderkey", "X")),
                                                anyTree(
                                                        tableScan("lineitem").withSymbol("orderkey", "Y")))))));

        assertPlan("SELECT * FROM orders WHERE orderkey NOT IN (SELECT orderkey FROM lineitem WHERE linenumber < 0)",
                anyTree(
                        filter("NOT S",
                                project(
                                        semiJoin("X", "Y", "S",
                                                anyTree(
                                                        tableScan("orders").withSymbol("orderkey", "X")),
                                                anyTree(
                                                        tableScan("lineitem").withSymbol("orderkey", "Y")))))));
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
                        join(LEFT, ImmutableList.of(aliasPair("name", "name_1"), aliasPair("regionkey", "regionkey_0")),
                                anyTree(
                                        tableScan("nation", tableScanConstraint)),
                                anyTree(
                                        tableScan("region", tableScanConstraint)))));
    }

    @Test
    public void testSameScalarSubqueryIsAppliedOnlyOnce()
    {
        // three subqueries with two duplicates, only two scalar joins should be in plan
        assertEquals(
                countOfMatchingNodes(
                        plan("SELECT * FROM orders WHERE orderkey = (SELECT 1) AND custkey = (SELECT 2) AND custkey != (SELECT 1)"),
                        EnforceSingleRowNode.class::isInstance),
                2);
        // same query used for left, right and complex join condition
        assertEquals(
                countOfMatchingNodes(
                        plan("SELECT * FROM orders o1 JOIN orders o2 ON o1.orderkey = (SELECT 1) AND o2.orderkey = (SELECT 1) AND o1.orderkey + o2.orderkey = (SELECT 1)"),
                        EnforceSingleRowNode.class::isInstance),
                2);
    }

    private int countOfMatchingNodes(Plan plan, Predicate<PlanNode> predicate)
    {
        PlanNodeExtractor planNodeExtractor = new PlanNodeExtractor(predicate);
        plan.getRoot().accept(planNodeExtractor, null);
        return planNodeExtractor.getNodes().size();
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
                                        tableScan("orders").withSymbol("orderkey", "X"),
                                        node(EnforceSingleRowNode.class,
                                                project(
                                                        node(ValuesNode.class)
                                                ))))));

        // double nesting
        assertPlan(
                "SELECT orderkey FROM orders o " +
                        "WHERE 3 IN (SELECT o.custkey FROM lineitem l WHERE (SELECT l.orderkey = o.orderkey))",
                LogicalPlanner.Stage.OPTIMIZED,
                anyTree(
                        filter("3 IN (C)",
                                apply(ImmutableList.of("C", "O"),
                                        project(
                                                tableScan("orders").withSymbol("orderkey", "O").withSymbol("custkey", "C")),
                                        anyTree(
                                                apply(ImmutableList.of("L"),
                                                        tableScan("lineitem").withSymbol("orderkey", "L"),
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
                        filter("count > 0",
                                anyTree(
                                        node(AggregationNode.class,
                                                anyTree(
                                                        join(LEFT, ImmutableList.of(),
                                                                anyTree(
                                                                        tableScan("orders")),
                                                                anyTree(
                                                                        node(ValuesNode.class)
                                                                ))))))));
    }

    private void assertPlan(String sql, PlanMatchPattern pattern)
    {
        assertPlan(sql, LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED, pattern);
    }

    private void assertPlan(String sql, LogicalPlanner.Stage stage, PlanMatchPattern pattern)
    {
        Plan actualPlan = plan(sql, stage);
        queryRunner.inTransaction(transactionSession -> {
            PlanAssert.assertPlan(transactionSession, queryRunner.getMetadata(), actualPlan, pattern);
            return null;
        });
    }

    private Plan plan(String sql)
    {
        return plan(sql, LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED);
    }

    private Plan plan(String sql, LogicalPlanner.Stage stage)
    {
        try {
            return queryRunner.inTransaction(transactionSession -> queryRunner.createPlan(transactionSession, sql, stage));
        }
        catch (RuntimeException ex) {
            fail("Invalid SQL: " + sql, ex);
            return null; // make compiler happy
        }
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
