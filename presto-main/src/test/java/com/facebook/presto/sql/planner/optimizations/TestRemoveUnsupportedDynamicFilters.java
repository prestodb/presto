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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.cost.StatsAndCosts;
import com.facebook.presto.expressions.LogicalRowExpressions;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.PlanVariableAllocator;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.sql.planner.assertions.PlanAssert;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.iterative.rule.RemoveUnsupportedDynamicFilters;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.sanity.DynamicFiltersChecker;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.facebook.presto.tpch.TpchColumnHandle;
import com.facebook.presto.tpch.TpchTableHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.sql.ExpressionUtils.combineDisjuncts;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.output;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.sql.planner.optimizations.PredicatePushDown.createDynamicFilterExpression;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestRemoveUnsupportedDynamicFilters
        extends BasePlanTest
{
    private Metadata metadata;
    private LogicalRowExpressions logicalRowExpressions;
    private PlanBuilder builder;
    private VariableReferenceExpression lineitemOrderKeyVariable;
    private TableScanNode lineitemTableScanNode;
    private VariableReferenceExpression ordersOrderKeyVariable;
    private TableScanNode ordersTableScanNode;

    @BeforeClass
    public void setup()
    {
        metadata = getQueryRunner().getMetadata();
        logicalRowExpressions = new LogicalRowExpressions(
                new RowExpressionDeterminismEvaluator(metadata.getFunctionAndTypeManager()),
                new FunctionResolution(metadata.getFunctionAndTypeManager()),
                metadata.getFunctionAndTypeManager());
        builder = new PlanBuilder(getQueryRunner().getDefaultSession(), new PlanNodeIdAllocator(), metadata);
        ConnectorId connectorId = getCurrentConnectorId();
        TableHandle lineitemTableHandle = new TableHandle(
                connectorId,
                new TpchTableHandle("lineitem", 1.0),
                TestingTransactionHandle.create(),
                Optional.empty());
        lineitemOrderKeyVariable = builder.variable("LINEITEM_OK", BIGINT);
        lineitemTableScanNode = builder.tableScan(lineitemTableHandle, ImmutableList.of(lineitemOrderKeyVariable), ImmutableMap.of(lineitemOrderKeyVariable, new TpchColumnHandle("orderkey", BIGINT)));

        TableHandle ordersTableHandle = new TableHandle(
                connectorId,
                new TpchTableHandle("orders", 1.0),
                TestingTransactionHandle.create(),
                Optional.empty());
        ordersOrderKeyVariable = builder.variable("ORDERS_OK", BIGINT);
        ordersTableScanNode = builder.tableScan(ordersTableHandle, ImmutableList.of(ordersOrderKeyVariable), ImmutableMap.of(ordersOrderKeyVariable, new TpchColumnHandle("orderkey", BIGINT)));
    }

    @Test
    public void testUnconsumedDynamicFilterInJoin()
    {
        PlanNode root = builder.join(
                INNER,
                builder.filter(builder.rowExpression("ORDERS_OK > 0"), ordersTableScanNode),
                lineitemTableScanNode,
                ImmutableList.of(new JoinNode.EquiJoinClause(ordersOrderKeyVariable, lineitemOrderKeyVariable)),
                ImmutableList.of(ordersOrderKeyVariable),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of("DF", lineitemOrderKeyVariable));
        assertPlan(
                removeUnsupportedDynamicFilters(root),
                join(INNER,
                        ImmutableList.of(equiJoinClause("ORDERS_OK", "LINEITEM_OK")),
                        filter("ORDERS_OK > 0",
                                tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey"))),
                        tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey"))));
    }

    @Test
    public void testDynamicFilterConsumedOnBuildSide()
    {
        PlanNode root = builder.join(
                INNER,
                builder.filter(
                        createDynamicFilterExpression("DF", ordersOrderKeyVariable, metadata.getFunctionAndTypeManager()),
                        ordersTableScanNode),
                builder.filter(
                        createDynamicFilterExpression("DF", ordersOrderKeyVariable, metadata.getFunctionAndTypeManager()),
                        lineitemTableScanNode),
                ImmutableList.of(new JoinNode.EquiJoinClause(ordersOrderKeyVariable, lineitemOrderKeyVariable)),
                ImmutableList.of(ordersOrderKeyVariable),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of("DF", lineitemOrderKeyVariable));

        PlanNode planNode = removeUnsupportedDynamicFilters(root);
        assertTrue(planNode instanceof JoinNode);
        JoinNode joinNode = (JoinNode) planNode;
        assertEquals(joinNode.getDynamicFilters(), ImmutableMap.of("DF", lineitemOrderKeyVariable));
    }

    @Test
    public void testUnmatchedDynamicFilter()
    {
        PlanNode root = builder.output(
                ImmutableList.of(),
                ImmutableList.of(),
                builder.join(
                        INNER,
                        ordersTableScanNode,
                        builder.filter(
                                logicalRowExpressions.combineConjuncts(
                                        builder.rowExpression("LINEITEM_OK > 0"),
                                        createDynamicFilterExpression("DF", lineitemOrderKeyVariable, metadata.getFunctionAndTypeManager())),
                                lineitemTableScanNode),
                        ImmutableList.of(new JoinNode.EquiJoinClause(ordersOrderKeyVariable, lineitemOrderKeyVariable)),
                        ImmutableList.of(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableMap.of()));
        assertPlan(
                removeUnsupportedDynamicFilters(root),
                output(
                        join(INNER,
                                ImmutableList.of(equiJoinClause("ORDERS_OK", "LINEITEM_OK")),
                                tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey")),
                                filter("LINEITEM_OK > 0",
                                        tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey"))))));
    }

    @Test
    public void testNestedDynamicFilterDisjunctionRewrite()
    {
        PlanNode root = builder.output(
                ImmutableList.of(),
                ImmutableList.of(),
                builder.join(
                        INNER,
                        ordersTableScanNode,
                        builder.filter(
                                logicalRowExpressions.combineConjuncts(
                                        logicalRowExpressions.combineDisjuncts(
                                                builder.rowExpression("LINEITEM_OK IS NULL"),
                                                createDynamicFilterExpression("DF", lineitemOrderKeyVariable, metadata.getFunctionAndTypeManager())),
                                        logicalRowExpressions.combineDisjuncts(
                                                builder.rowExpression("LINEITEM_OK IS NOT NULL"),
                                                createDynamicFilterExpression("DF", lineitemOrderKeyVariable, metadata.getFunctionAndTypeManager()))),
                                lineitemTableScanNode),
                        ImmutableList.of(new JoinNode.EquiJoinClause(ordersOrderKeyVariable, lineitemOrderKeyVariable)),
                        ImmutableList.of(ordersOrderKeyVariable),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableMap.of()));
        assertPlan(
                removeUnsupportedDynamicFilters(root),
                output(
                        join(INNER,
                                ImmutableList.of(equiJoinClause("ORDERS_OK", "LINEITEM_OK")),
                                tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey")),
                                tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey")))));
    }

    @Test
    public void testNestedDynamicFilterConjunctionRewrite()
    {
        PlanNode root = builder.output(ImmutableList.of(), ImmutableList.of(),
                builder.join(
                        INNER,
                        ordersTableScanNode,
                        builder.filter(
                                logicalRowExpressions.combineDisjuncts(
                                        logicalRowExpressions.combineConjuncts(
                                                builder.rowExpression("LINEITEM_OK IS NULL"),
                                                createDynamicFilterExpression("DF", lineitemOrderKeyVariable, metadata.getFunctionAndTypeManager())),
                                        logicalRowExpressions.combineConjuncts(
                                                builder.rowExpression("LINEITEM_OK IS NOT NULL"),
                                                createDynamicFilterExpression("DF", lineitemOrderKeyVariable, metadata.getFunctionAndTypeManager()))),
                                lineitemTableScanNode),
                        ImmutableList.of(new JoinNode.EquiJoinClause(ordersOrderKeyVariable, lineitemOrderKeyVariable)),
                        ImmutableList.of(ordersOrderKeyVariable),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableMap.of()));
        assertPlan(
                removeUnsupportedDynamicFilters(root),
                output(
                        join(INNER,
                                ImmutableList.of(equiJoinClause("ORDERS_OK", "LINEITEM_OK")),
                                tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey")),
                                filter(
                                        combineDisjuncts(ImmutableList.of(
                                                PlanBuilder.expression("LINEITEM_OK IS NULL"),
                                                PlanBuilder.expression("LINEITEM_OK IS NOT NULL"))),
                                        tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey"))))));
    }

    PlanNode removeUnsupportedDynamicFilters(PlanNode root)
    {
        return getQueryRunner().inTransaction(session -> {
            // metadata.getCatalogHandle() registers the catalog for the transaction
            session.getCatalog().ifPresent(catalog -> metadata.getCatalogHandle(session, catalog));
            PlanNode rewrittenPlan = new RemoveUnsupportedDynamicFilters(metadata.getFunctionAndTypeManager()).optimize(root, session, TypeProvider.empty(), new PlanVariableAllocator(), new PlanNodeIdAllocator(), WarningCollector.NOOP);
            new DynamicFiltersChecker().validate(rewrittenPlan, session, metadata, new SqlParser(), TypeProvider.empty(), WarningCollector.NOOP);
            return rewrittenPlan;
        });
    }

    protected void assertPlan(PlanNode actual, PlanMatchPattern pattern)
    {
        getQueryRunner().inTransaction(session -> {
            // metadata.getCatalogHandle() registers the catalog for the transaction
            session.getCatalog().ifPresent(catalog -> metadata.getCatalogHandle(session, catalog));
            PlanAssert.assertPlan(session, metadata, getQueryRunner().getStatsCalculator(), new Plan(actual, builder.getTypes(), StatsAndCosts.empty()), pattern);
            return null;
        });
    }
}
