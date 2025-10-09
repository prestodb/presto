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

import com.facebook.presto.Session;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.cost.StatsAndCosts;
import com.facebook.presto.cost.StatsProvider;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorPlanOptimizer;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.OutputNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.PlanVisitor;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.UnionNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.assertions.MatchResult;
import com.facebook.presto.sql.planner.assertions.Matcher;
import com.facebook.presto.sql.planner.assertions.PlanAssert;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.assertions.SymbolAliases;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.expressions.LogicalRowExpressions.and;
import static com.facebook.presto.expressions.LogicalRowExpressions.or;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.lang.reflect.Modifier.isFinal;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static org.testng.Assert.assertEquals;

public class TestConnectorOptimization
{
    private static final Metadata METADATA = MetadataManager.createTestMetadataManager();
    private static final PlanBuilder PLAN_BUILDER = new PlanBuilder(TEST_SESSION, new PlanNodeIdAllocator(), METADATA);

    @Test
    public void testSupportedPlanNodes()
    {
        @SuppressWarnings("unchecked")
        Set<Class<? extends PlanNode>> expected = Arrays.stream(PlanVisitor.class.getDeclaredMethods())
                .map(Method::getParameterTypes)
                .filter(parameterTypes -> parameterTypes.length > 0)
                .filter(parameterTypes -> PlanNode.class.isAssignableFrom(parameterTypes[0]))  // is accessible in SPI
                .filter(parameterTypes -> isFinal(parameterTypes[0].getModifiers()))  // is a final class
                .map(parameterTypes -> (Class<? extends PlanNode>) parameterTypes[0])
                .collect(toImmutableSet());

        assertEquals(ApplyConnectorOptimization.CONNECTOR_ACCESSIBLE_PLAN_NODES, expected);
    }

    @Test
    public void testEmptyOptimizers()
    {
        PlanNode plan = output(filter(tableScan("cat1", "a", "b"), TRUE_CONSTANT), "a");
        PlanNode actual = optimize(plan, ImmutableMap.of());
        assertEquals(actual, plan);

        actual = optimize(plan, ImmutableMap.of(new ConnectorId("cat2"), ImmutableSet.of(noop())));
        assertEquals(actual, plan);
    }

    @Test
    public void testMultipleConnectors()
    {
        PlanNode plan = output(
                union(
                        tableScan("cat1", "a", "b"),
                        tableScan("cat2", "a", "b"),
                        tableScan("cat3", "a", "b"),
                        tableScan("cat4", "a", "b"),
                        tableScan("cat2", "a", "b"),
                        tableScan("cat1", "a", "b"),
                        values("a", "b")),
                "a");

        PlanNode actual = optimize(plan, ImmutableMap.of());
        assertEquals(actual, plan);

        actual = optimize(plan, ImmutableMap.of(new ConnectorId("cat2"), ImmutableSet.of(noop())));
        assertEquals(actual, plan);
    }

    @Test
    public void testPlanUpdateWithComplexStructures()
    {
        PlanNode plan = output(
                union(
                        filter(tableScan("cat1", "a", "b"), TRUE_CONSTANT),
                        filter(tableScan("cat2", "a", "b"), TRUE_CONSTANT),
                        union(
                                filter(tableScan("cat3", "a", "b"), TRUE_CONSTANT),
                                union(
                                        filter(tableScan("cat4", "a", "b"), TRUE_CONSTANT),
                                        filter(tableScan("cat1", "a", "b"), TRUE_CONSTANT))),
                        filter(tableScan("cat2", "a", "b"), TRUE_CONSTANT),
                        union(filter(tableScan("cat1", "a", "b"), TRUE_CONSTANT))),
                "a");

        PlanNode actual = optimize(plan, ImmutableMap.of());
        assertEquals(actual, plan);

        // force updating every leaf node
        actual = optimize(
                plan,
                ImmutableMap.of(
                        new ConnectorId("cat1"), ImmutableSet.of(filterPushdown()),
                        new ConnectorId("cat2"), ImmutableSet.of(filterPushdown()),
                        new ConnectorId("cat3"), ImmutableSet.of(filterPushdown()),
                        new ConnectorId("cat4"), ImmutableSet.of(filterPushdown())));

        // assert all filters removed
        assertPlanMatch(
                actual,
                PlanMatchPattern.output(
                        PlanMatchPattern.union(
                                SimpleTableScanMatcher.tableScan("cat1", TRUE_CONSTANT),
                                SimpleTableScanMatcher.tableScan("cat2", TRUE_CONSTANT),
                                PlanMatchPattern.union(
                                        SimpleTableScanMatcher.tableScan("cat3", TRUE_CONSTANT),
                                        PlanMatchPattern.union(
                                                SimpleTableScanMatcher.tableScan("cat4", TRUE_CONSTANT),
                                                SimpleTableScanMatcher.tableScan("cat1", TRUE_CONSTANT))),
                                SimpleTableScanMatcher.tableScan("cat2", TRUE_CONSTANT),
                                PlanMatchPattern.union(
                                        SimpleTableScanMatcher.tableScan("cat1", TRUE_CONSTANT)))));
    }

    @Test
    public void testPushFilterToTableScan()
    {
        RowExpression expectedPredicate = and(newBigintVariable("a"), newBigintVariable("b"));
        PlanNode plan = output(
                filter(
                        tableScan("cat1", "a", "b"),
                        expectedPredicate),
                "a");
        PlanNode actual = optimize(plan, ImmutableMap.of(new ConnectorId("cat1"), ImmutableSet.of(filterPushdown())));

        // assert structure; FilterNode is removed
        assertPlanMatch(actual, PlanMatchPattern.output(SimpleTableScanMatcher.tableScan("cat1", expectedPredicate)));
    }

    @Test
    public void testAddFilterToTableScan()
    {
        RowExpression expectedPredicate = and(newBigintVariable("a"), newBigintVariable("b"));

        // (1) without filter node case
        PlanNode plan = output(tableScan("cat1", "a", "b"), "a");
        PlanNode actual = optimize(plan, ImmutableMap.of(new ConnectorId("cat1"), ImmutableSet.of(addFilterToTableScan(expectedPredicate))));

        // assert FilterNode is added
        assertPlanMatch(
                actual,
                PlanMatchPattern.output(
                        PlanMatchPattern.filter(
                                "a AND b",
                                SimpleTableScanMatcher.tableScan("cat1", "a", "b"))),
                TypeProvider.viewOf(ImmutableMap.of("a", BIGINT, "b", BIGINT)));

        // (2) with filter node case
        RowExpression existingPredicate = or(newBigintVariable("a"), newBigintVariable("b"));
        plan = output(
                filter(
                        tableScan("cat1", "a", "b"),
                        existingPredicate),
                "a");
        actual = optimize(plan, ImmutableMap.of(new ConnectorId("cat1"), ImmutableSet.of(addFilterToTableScan(expectedPredicate))));

        // assert filter gets added as a part of conjuncts
        assertPlanMatch(
                actual,
                PlanMatchPattern.output(
                        PlanMatchPattern.filter(
                                "(a OR b) AND (a AND b)",
                                SimpleTableScanMatcher.tableScan("cat1", "a", "b"))),
                TypeProvider.viewOf(ImmutableMap.of("a", BIGINT, "b", BIGINT)));
    }

    @Test
    public void testMultipleConnectorOptimization()
    {
        PlanNode plan = output(
                union(
                        tableScan("cat1", "a", "b"),
                        tableScan("cat2", "a", "b")),
                "a");

        ConnectorPlanOptimizer multiConnectorOptimizer = createMultiConnectorOptimizer(
                ImmutableList.of(new ConnectorId("cat1"), new ConnectorId("cat2")));

        PlanNode actual = optimize(plan, ImmutableMap.of(
                new ConnectorId("cat1"), ImmutableSet.of(multiConnectorOptimizer)));

        assertPlanMatch(
                actual,
                PlanMatchPattern.output(
                        PlanMatchPattern.union(
                                PlanMatchPattern.filter(
                                        "true",
                                        SimpleTableScanMatcher.tableScan("cat1", "a", "b")),
                                PlanMatchPattern.filter(
                                        "true",
                                        SimpleTableScanMatcher.tableScan("cat2", "a", "b")))));

        ConnectorPlanOptimizer crossConnectorUnionOptimizer = createCrossConnectorUnionOptimizer(
                ImmutableList.of(new ConnectorId("cat1"), new ConnectorId("cat2")));

        actual = optimize(plan, ImmutableMap.of(
                new ConnectorId("cat1"), ImmutableSet.of(crossConnectorUnionOptimizer)));

        assertPlanMatch(
                actual,
                PlanMatchPattern.output(
                        PlanMatchPattern.union(
                                PlanMatchPattern.filter(
                                        "true",
                                        SimpleTableScanMatcher.tableScan("cat1", "a", "b")),
                                PlanMatchPattern.filter(
                                        "true",
                                        SimpleTableScanMatcher.tableScan("cat2", "a", "b")))));

        plan = output(
                union(
                        filter(tableScan("cat1", "a", "b"), TRUE_CONSTANT),
                        filter(tableScan("cat2", "a", "b"), TRUE_CONSTANT),
                        filter(tableScan("cat3", "a", "b"), TRUE_CONSTANT)),
                "a");

        ConnectorPlanOptimizer multiConnectorOptimizer12 = createCrossConnectorUnionOptimizer(
                ImmutableList.of(new ConnectorId("cat1"), new ConnectorId("cat2")));

        actual = optimize(plan, ImmutableMap.of(
                new ConnectorId("cat1"), ImmutableSet.of(multiConnectorOptimizer12),
                new ConnectorId("cat3"), ImmutableSet.of(filterPushdown())));

        assertPlanMatch(
                actual,
                PlanMatchPattern.output(
                        PlanMatchPattern.union(
                                PlanMatchPattern.filter(
                                        "true",
                                        SimpleTableScanMatcher.tableScan("cat1", "a", "b")),
                                PlanMatchPattern.filter(
                                        "true",
                                        SimpleTableScanMatcher.tableScan("cat2", "a", "b")),
                                SimpleTableScanMatcher.tableScan("cat3", TRUE_CONSTANT))));

        plan = output(
                union(
                        union(
                                tableScan("cat1", "a", "b"),
                                tableScan("cat2", "a", "b")), // This union only contains supported connectors
                        tableScan("cat4", "a", "b")), // cat4 in separate part of plan
                "a");

        actual = optimize(plan, ImmutableMap.of(
                new ConnectorId("cat1"), ImmutableSet.of(crossConnectorUnionOptimizer)));

        assertPlanMatch(
                actual,
                PlanMatchPattern.output(
                        PlanMatchPattern.union(
                                PlanMatchPattern.union(
                                        PlanMatchPattern.filter(
                                                "true",
                                                SimpleTableScanMatcher.tableScan("cat1", "a", "b")),
                                        PlanMatchPattern.filter(
                                                "true",
                                                SimpleTableScanMatcher.tableScan("cat2", "a", "b"))),
                                SimpleTableScanMatcher.tableScan("cat4", "a", "b"))));

        plan = output(
                union(
                        tableScan("cat1", "a", "b"),
                        tableScan("cat2", "a", "b"),
                        tableScan("cat3", "a", "b")),
                "a");

        ConnectorPlanOptimizer singleConnectorOptimizer1 = addFilterToTableScan(TRUE_CONSTANT);
        ConnectorPlanOptimizer singleConnectorOptimizer2 = noop();
        ConnectorPlanOptimizer multiConnectorOptimizer13 = createMultiConnectorOptimizer(
                ImmutableList.of(new ConnectorId("cat1"), new ConnectorId("cat3")));

        actual = optimize(plan, ImmutableMap.of(
                new ConnectorId("cat1"), ImmutableSet.of(singleConnectorOptimizer1, multiConnectorOptimizer13),
                new ConnectorId("cat2"), ImmutableSet.of(singleConnectorOptimizer2),
                new ConnectorId("cat3"), ImmutableSet.of(singleConnectorOptimizer1)));

        assertPlanMatch(
                actual,
                PlanMatchPattern.output(
                        PlanMatchPattern.union(
                                PlanMatchPattern.filter(
                                        "true",
                                        SimpleTableScanMatcher.tableScan("cat1", "a", "b")),
                                SimpleTableScanMatcher.tableScan("cat2", "a", "b"),
                                PlanMatchPattern.filter(
                                        "true",
                                        SimpleTableScanMatcher.tableScan("cat3", "a", "b")))));

        plan = output(
                union(
                        union(
                                tableScan("cat1", "a", "b"),
                                tableScan("cat3", "a", "b")), // This inner union has exactly cat1, cat3
                        union(
                                tableScan("cat2", "a", "b"),
                                tableScan("cat4", "a", "b"))), // This inner union has cat2, cat4
                "a");

        ConnectorPlanOptimizer exactMatchOptimizer = createCrossConnectorUnionOptimizer(
                ImmutableList.of(new ConnectorId("cat1"), new ConnectorId("cat3"))); // Only supports cat1 and cat3

        actual = optimize(plan, ImmutableMap.of(
                new ConnectorId("cat1"), ImmutableSet.of(exactMatchOptimizer),
                new ConnectorId("cat2"), ImmutableSet.of(filterPushdown()),
                new ConnectorId("cat4"), ImmutableSet.of(filterPushdown())));

        assertPlanMatch(
                actual,
                PlanMatchPattern.output(
                        PlanMatchPattern.union(
                                PlanMatchPattern.union(
                                        PlanMatchPattern.filter(
                                                "true",
                                                SimpleTableScanMatcher.tableScan("cat1", "a", "b")),
                                        PlanMatchPattern.filter(
                                                "true",
                                                SimpleTableScanMatcher.tableScan("cat3", "a", "b"))),
                                PlanMatchPattern.union(
                                        SimpleTableScanMatcher.tableScan("cat2", "a", "b"),
                                        SimpleTableScanMatcher.tableScan("cat4", "a", "b")))));

        plan = output(
                union(
                        tableScan("cat1", "a", "b"),
                        tableScan("cat2", "a", "b"),
                        tableScan("cat3", "a", "b")),
                "a");

        ConnectorPlanOptimizer partialCoverageOptimizer = createMultiConnectorOptimizer(
                ImmutableList.of(new ConnectorId("cat1"), new ConnectorId("cat2"))); // Only supports cat1, cat2

        actual = optimize(plan, ImmutableMap.of(
                new ConnectorId("cat1"), ImmutableSet.of(partialCoverageOptimizer),
                new ConnectorId("cat3"), ImmutableSet.of(filterPushdown())));

        assertPlanMatch(
                actual,
                PlanMatchPattern.output(
                        PlanMatchPattern.union(
                                SimpleTableScanMatcher.tableScan("cat1", "a", "b"),
                                SimpleTableScanMatcher.tableScan("cat2", "a", "b"),
                                SimpleTableScanMatcher.tableScan("cat3", "a", "b"))));

        plan = output(
                union(
                        union(
                                tableScan("cat1", "a", "b"),
                                tableScan("cat2", "a", "b")), // This inner union has exactly cat1, cat2
                        union(
                                tableScan("cat2", "a", "b"),
                                tableScan("cat3", "a", "b"))), // This inner union has exactly cat2, cat3
                "a");

        ConnectorPlanOptimizer multiConnectorOptimizer12v2 = createMultiConnectorOptimizer(
                ImmutableList.of(new ConnectorId("cat1"), new ConnectorId("cat2")));
        ConnectorPlanOptimizer multiConnectorOptimizer23 = createCrossConnectorUnionOptimizer(
                ImmutableList.of(new ConnectorId("cat2"), new ConnectorId("cat3")));

        actual = optimize(plan, ImmutableMap.of(
                new ConnectorId("cat1"), ImmutableSet.of(multiConnectorOptimizer12v2),
                new ConnectorId("cat2"), ImmutableSet.of(multiConnectorOptimizer23),
                new ConnectorId("cat3"), ImmutableSet.of(noop())));

        assertPlanMatch(
                actual,
                PlanMatchPattern.output(
                        PlanMatchPattern.union(
                                PlanMatchPattern.union(
                                        PlanMatchPattern.filter(
                                                "true",
                                                SimpleTableScanMatcher.tableScan("cat1", "a", "b")),
                                        PlanMatchPattern.filter(
                                                "true",
                                                SimpleTableScanMatcher.tableScan("cat2", "a", "b"))),
                                PlanMatchPattern.union(
                                        PlanMatchPattern.filter(
                                                "true",
                                                SimpleTableScanMatcher.tableScan("cat2", "a", "b")),
                                        PlanMatchPattern.filter(
                                                "true",
                                                SimpleTableScanMatcher.tableScan("cat3", "a", "b"))))));
    }

    private TableScanNode tableScan(String connectorName, String... columnNames)
    {
        return PLAN_BUILDER.tableScan(
                connectorName,
                Arrays.stream(columnNames).map(TestConnectorOptimization::newBigintVariable).collect(toImmutableList()),
                Arrays.stream(columnNames).map(TestConnectorOptimization::newBigintVariable).collect(toMap(identity(), variable -> new ColumnHandle() {})));
    }

    private FilterNode filter(PlanNode source, RowExpression predicate)
    {
        return PLAN_BUILDER.filter(predicate, source);
    }

    private OutputNode output(PlanNode source, String... columnNames)
    {
        return PLAN_BUILDER.output(
                Arrays.stream(columnNames).collect(toImmutableList()),
                Arrays.stream(columnNames).map(TestConnectorOptimization::newBigintVariable).collect(toImmutableList()),
                source);
    }

    private UnionNode union(PlanNode... sources)
    {
        ImmutableListMultimap.Builder<VariableReferenceExpression, VariableReferenceExpression> outputsToInputs = ImmutableListMultimap.builder();
        for (PlanNode source : sources) {
            outputsToInputs.putAll(source.getOutputVariables().stream().collect(toMap(identity(), identity())).entrySet());
        }
        return PLAN_BUILDER.union(outputsToInputs.build(), Arrays.asList(sources));
    }

    private ValuesNode values(String... columnNames)
    {
        VariableReferenceExpression[] columns = new VariableReferenceExpression[columnNames.length];
        for (int i = 0; i < columnNames.length; i++) {
            columns[i] = newBigintVariable(columnNames[i]);
        }
        return PLAN_BUILDER.values(5, columns);
    }

    private static VariableReferenceExpression newBigintVariable(String name)
    {
        return new VariableReferenceExpression(Optional.empty(), name, BIGINT);
    }

    private static void assertPlanMatch(PlanNode actual, PlanMatchPattern expected)
    {
        assertPlanMatch(actual, expected, TypeProvider.empty());
    }

    private static void assertPlanMatch(PlanNode actual, PlanMatchPattern expected, TypeProvider typeProvider)
    {
        PlanAssert.assertPlan(
                TEST_SESSION,
                METADATA,
                (node, sourceStats, lookup, session, types) -> PlanNodeStatsEstimate.unknown(),
                new Plan(actual, typeProvider, StatsAndCosts.empty()),
                expected);
    }

    private static PlanNode optimize(PlanNode plan, Map<ConnectorId, Set<ConnectorPlanOptimizer>> optimizers)
    {
        ApplyConnectorOptimization optimizer = new ApplyConnectorOptimization(() -> optimizers);
        return optimizer.optimize(plan, TEST_SESSION, TypeProvider.empty(), new VariableAllocator(), new PlanNodeIdAllocator(), WarningCollector.NOOP).getPlanNode();
    }

    private static ConnectorPlanOptimizer filterPushdown()
    {
        return (maxSubplan, session, variableAllocator, idAllocator) -> maxSubplan.accept(new TestFilterPushdownVisitor(), null);
    }

    private static ConnectorPlanOptimizer addFilterToTableScan(RowExpression filter)
    {
        return (maxSubplan, session, variableAllocator, idAllocator) -> maxSubplan.accept(new TestAddFilterVisitor(filter, idAllocator), null);
    }

    private static ConnectorPlanOptimizer noop()
    {
        return (maxSubplan, session, variableAllocator, idAllocator) -> maxSubplan;
    }

    private static ConnectorPlanOptimizer createMultiConnectorOptimizer(java.util.List<ConnectorId> supportedConnectors)
    {
        return new ConnectorPlanOptimizer()
        {
            @Override
            public PlanNode optimize(PlanNode maxSubplan, com.facebook.presto.spi.ConnectorSession session, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator)
            {
                return maxSubplan.accept(new TestMultiConnectorOptimizationVisitor(supportedConnectors, idAllocator), null);
            }

            @Override
            public java.util.List<ConnectorId> getSupportedConnectorIds()
            {
                return supportedConnectors;
            }
        };
    }

    private static ConnectorPlanOptimizer createCrossConnectorUnionOptimizer(java.util.List<ConnectorId> supportedConnectors)
    {
        return new ConnectorPlanOptimizer()
        {
            @Override
            public PlanNode optimize(PlanNode maxSubplan, com.facebook.presto.spi.ConnectorSession session, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator)
            {
                return maxSubplan.accept(new TestCrossConnectorUnionVisitor(supportedConnectors, idAllocator), null);
            }

            @Override
            public java.util.List<ConnectorId> getSupportedConnectorIds()
            {
                return supportedConnectors;
            }
        };
    }

    private static class TestPlanOptimizationVisitor
            extends PlanVisitor<PlanNode, Void>
    {
        @Override
        public PlanNode visitPlan(PlanNode node, Void context)
        {
            ImmutableList.Builder<PlanNode> children = ImmutableList.builder();
            for (PlanNode child : node.getSources()) {
                children.add(child.accept(this, null));
            }
            return node.replaceChildren(children.build());
        }
    }

    private static class TestFilterPushdownVisitor
            extends TestPlanOptimizationVisitor
    {
        @Override
        public PlanNode visitFilter(FilterNode node, Void context)
        {
            if (node.getSource() instanceof TableScanNode) {
                TableScanNode tableScanNode = (TableScanNode) node.getSource();
                TableHandle handle = tableScanNode.getTable();
                return new TableScanNode(
                        Optional.empty(),
                        tableScanNode.getId(),
                        new TableHandle(
                                handle.getConnectorId(),
                                handle.getConnectorHandle(),
                                handle.getTransaction(),
                                Optional.of(new TestConnectorTableLayoutHandle(node.getPredicate()))),
                        tableScanNode.getOutputVariables(),
                        tableScanNode.getAssignments(),
                        tableScanNode.getTableConstraints(),
                        TupleDomain.all(),
                        TupleDomain.all(), Optional.empty());
            }
            return node;
        }

        static class TestConnectorTableLayoutHandle
                implements ConnectorTableLayoutHandle
        {
            private final RowExpression predicate;

            TestConnectorTableLayoutHandle(RowExpression predicate)
            {
                this.predicate = predicate;
            }

            @Override
            public boolean equals(Object obj)
            {
                if (this == obj) {
                    return true;
                }

                if (!(obj instanceof TestConnectorTableLayoutHandle)) {
                    return false;
                }

                TestConnectorTableLayoutHandle other = (TestConnectorTableLayoutHandle) obj;
                return Objects.equals(predicate, other.predicate);
            }

            @Override
            public int hashCode()
            {
                return Objects.hashCode(predicate);
            }
        }
    }

    private static class TestAddFilterVisitor
            extends TestPlanOptimizationVisitor
    {
        private final RowExpression filter;
        private final PlanNodeIdAllocator idAllocator;

        TestAddFilterVisitor(RowExpression filter, PlanNodeIdAllocator idAllocator)
        {
            this.filter = filter;
            this.idAllocator = idAllocator;
        }

        @Override
        public PlanNode visitFilter(FilterNode node, Void context)
        {
            if (node.getSource() instanceof TableScanNode) {
                return new FilterNode(Optional.empty(), node.getId(), node.getSource(), and(node.getPredicate(), filter));
            }
            return node;
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, Void context)
        {
            return new FilterNode(Optional.empty(), idAllocator.getNextId(), node, filter);
        }
    }

    /**
     * Multi-connector visitor that adds filters to table scans from supported connectors
     */
    private static class TestMultiConnectorOptimizationVisitor
            extends TestPlanOptimizationVisitor
    {
        private final java.util.List<ConnectorId> supportedConnectors;
        private final PlanNodeIdAllocator idAllocator;

        TestMultiConnectorOptimizationVisitor(java.util.List<ConnectorId> supportedConnectors, PlanNodeIdAllocator idAllocator)
        {
            this.supportedConnectors = supportedConnectors;
            this.idAllocator = idAllocator;
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, Void context)
        {
            if (supportedConnectors.contains(node.getTable().getConnectorId())) {
                return new FilterNode(Optional.empty(), idAllocator.getNextId(), node, TRUE_CONSTANT);
            }
            return node;
        }
    }

    /**
     * Multi-connector visitor that optimizes unions across different connectors
     */
    private static class TestCrossConnectorUnionVisitor
            extends TestPlanOptimizationVisitor
    {
        private final java.util.List<ConnectorId> supportedConnectors;
        private final PlanNodeIdAllocator idAllocator;

        TestCrossConnectorUnionVisitor(java.util.List<ConnectorId> supportedConnectors, PlanNodeIdAllocator idAllocator)
        {
            this.supportedConnectors = supportedConnectors;
            this.idAllocator = idAllocator;
        }

        @Override
        public PlanNode visitUnion(UnionNode node, Void context)
        {
            Set<ConnectorId> foundConnectors = new java.util.HashSet<>();
            boolean hasMultipleConnectors = false;

            for (PlanNode source : node.getSources()) {
                if (source instanceof TableScanNode) {
                    ConnectorId connectorId = ((TableScanNode) source).getTable().getConnectorId();
                    if (supportedConnectors.contains(connectorId)) {
                        foundConnectors.add(connectorId);
                        if (foundConnectors.size() > 1) {
                            hasMultipleConnectors = true;
                            break;
                        }
                    }
                }
            }

            if (hasMultipleConnectors) {
                ImmutableList.Builder<PlanNode> newSources = ImmutableList.builder();
                for (PlanNode source : node.getSources()) {
                    if (source instanceof TableScanNode) {
                        TableScanNode tableScan = (TableScanNode) source;
                        if (supportedConnectors.contains(tableScan.getTable().getConnectorId())) {
                            newSources.add(new FilterNode(Optional.empty(), idAllocator.getNextId(), tableScan, TRUE_CONSTANT));
                        }
                        else {
                            newSources.add(source);
                        }
                    }
                    else {
                        newSources.add(source.accept(this, context));
                    }
                }
                return node.replaceChildren(newSources.build());
            }

            return super.visitUnion(node, context);
        }
    }

    /**
     * A simplified table scan matcher for multiple-connector support.
     * The goal is to test plan structural matching rather than table scan details
     */
    private static final class SimpleTableScanMatcher
            implements Matcher
    {
        private final ConnectorId connectorId;
        private final Optional<ConnectorTableLayoutHandle> connectorTableLayoutHandle;
        private final String[] columns;

        public static PlanMatchPattern tableScan(String connectorName, RowExpression predicate, String... columnNames)
        {
            return node(TableScanNode.class)
                    .with(new SimpleTableScanMatcher(
                            new ConnectorId(connectorName),
                            Optional.ofNullable(predicate).map(TestFilterPushdownVisitor.TestConnectorTableLayoutHandle::new),
                            columnNames));
        }

        public static PlanMatchPattern tableScan(String connectorName, String... columnNames)
        {
            return tableScan(connectorName, null, columnNames);
        }

        private SimpleTableScanMatcher(
                ConnectorId connectorId,
                Optional<ConnectorTableLayoutHandle> connectorTableLayoutHandle,
                String... columns)
        {
            this.connectorId = connectorId;
            this.connectorTableLayoutHandle = connectorTableLayoutHandle;
            this.columns = columns;
        }

        @Override
        public boolean shapeMatches(PlanNode node)
        {
            return node instanceof TableScanNode;
        }

        @Override
        public MatchResult detailMatches(PlanNode node, StatsProvider stats, Session session, Metadata metadata, SymbolAliases symbolAliases)
        {
            checkState(shapeMatches(node), "Plan testing framework error: shapeMatches returned false in detailMatches in %s", this.getClass().getName());

            TableScanNode tableScanNode = (TableScanNode) node;
            if (connectorId.equals(tableScanNode.getTable().getConnectorId()) &&
                    connectorTableLayoutHandle.equals(tableScanNode.getTable().getLayout())) {
                return MatchResult.match(SymbolAliases.builder().putAll(Arrays.stream(columns).collect(toMap(identity(), SymbolReference::new))).build());
            }

            return MatchResult.NO_MATCH;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .omitNullValues()
                    .add("connectorId", connectorId)
                    .add("connectorTableLayoutHandle", connectorTableLayoutHandle.orElse(null))
                    .toString();
        }
    }
}
