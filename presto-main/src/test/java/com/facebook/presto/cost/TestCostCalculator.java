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
package com.facebook.presto.cost;

import com.facebook.presto.Session;
import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.SymbolReference;
import com.facebook.presto.tpch.TpchColumnHandle;
import com.facebook.presto.tpch.TpchTableHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.facebook.presto.cost.PlanNodeCostEstimate.UNKNOWN_COST;
import static com.facebook.presto.cost.PlanNodeCostEstimate.cpuCost;
import static com.facebook.presto.cost.PlanNodeStatsEstimate.UNKNOWN_STATS;
import static com.facebook.presto.metadata.FunctionKind.AGGREGATE;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.Double.isNaN;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestCostCalculator
{
    private static final int NUMBER_OF_NODES = 10;
    private final CostCalculator costCalculatorUsingExchanges = new CostCalculatorUsingExchanges(() -> NUMBER_OF_NODES);
    private final CostCalculator costCalculatorWithEstimatedExchanges = new CostCalculatorWithEstimatedExchanges(costCalculatorUsingExchanges, () -> NUMBER_OF_NODES);
    private Session session = testSessionBuilder().build();

    @Test
    public void testTableScan()
    {
        TableScanNode tableScan = tableScan("ts", "orderkey");

        assertCost(
                tableScan,
                ImmutableMap.of(),
                ImmutableMap.of("ts", statsEstimate(1000)))
                .cpu(1000)
                .memory(0)
                .network(0);
        assertCostEstimatedExchanges(
                tableScan,
                ImmutableMap.of(),
                ImmutableMap.of("ts", statsEstimate(1000)))
                .cpu(1000)
                .memory(0)
                .network(0);

        assertCostHasUnknownComponentsForUnknownStats(tableScan);
    }

    @Test
    public void testProject()
    {
        PlanNode project = project("project", tableScan("ts", "orderkey"), "string", new Cast(new SymbolReference("orderkey"), "STRING"));
        Map<String, PlanNodeCostEstimate> costs = ImmutableMap.of("ts", cpuCost(1000));
        Map<String, PlanNodeStatsEstimate> stats = ImmutableMap.of("project", statsEstimate(4000), "ts", statsEstimate(1000));

        assertCost(
                project,
                costs,
                stats)
                .cpu(1000 + 4000)
                .memory(0)
                .network(0);

        assertCostEstimatedExchanges(
                project,
                costs,
                stats)
                .cpu(1000 + 4000)
                .memory(0)
                .network(0);

        assertCostHasUnknownComponentsForUnknownStats(project);
    }

    @Test
    public void testRepartitionedJoin()
    {
        JoinNode join = join("join",
                tableScan("ts1", "orderkey"),
                tableScan("ts2", "orderkey_0"),
                JoinNode.DistributionType.PARTITIONED,
                "orderkey",
                "orderkey_0");

        Map<String, PlanNodeCostEstimate> costs = ImmutableMap.of(
                "ts1", cpuCost(6000),
                "ts2", cpuCost(1000));

        Map<String, PlanNodeStatsEstimate> stats = ImmutableMap.of(
                "join", statsEstimate(12000),
                "ts1", statsEstimate(6000),
                "ts2", statsEstimate(1000));

        assertCost(
                join,
                costs,
                stats)
                .cpu(12000 + 6000 + 1000 + 6000 + 1000);

        assertCostEstimatedExchanges(
                join,
                costs,
                stats)
                .cpu(12000 + 6000 + 1000 + 6000 + 1000 + 6000 + 1000 + 1000);

        assertCostHasUnknownComponentsForUnknownStats(join);
    }

    @Test
    public void testReplicatedJoin()
    {
        JoinNode join = join("join",
                tableScan("ts1", "orderkey"),
                tableScan("ts2", "orderkey_0"),
                JoinNode.DistributionType.REPLICATED,
                "orderkey",
                "orderkey_0");

        Map<String, PlanNodeCostEstimate> costs = ImmutableMap.of(
                "ts1", cpuCost(6000),
                "ts2", cpuCost(1000));

        Map<String, PlanNodeStatsEstimate> stats = ImmutableMap.of(
                "join", statsEstimate(12000),
                "ts1", statsEstimate(6000),
                "ts2", statsEstimate(1000));

        assertCost(
                join,
                costs,
                stats)
                .cpu(12000 + 6000 + 10000 + 6000 + 1000 + 1000 * (NUMBER_OF_NODES - 1));
        assertCostEstimatedExchanges(
                join,
                costs,
                stats)
                .cpu(12000 + 6000 + 10000 + 6000 + 1000 + 1000 * NUMBER_OF_NODES);

        assertCostHasUnknownComponentsForUnknownStats(join);
    }

    @Test
    public void testAggregation()
    {
        AggregationNode aggregationNode = aggregation("agg",
                tableScan("ts", "orderkey"));

        Map<String, PlanNodeCostEstimate> costs = ImmutableMap.of("ts", cpuCost(6000));
        Map<String, PlanNodeStatsEstimate> stats = ImmutableMap.of(
                "ts", statsEstimate(6000),
                "agg", statsEstimate(8));

        assertCost(aggregationNode, costs, stats)
                .cpu(6000 + 6000);
        assertCostEstimatedExchanges(aggregationNode, costs, stats)
                .cpu(6000 + 6000 + 6000 + 6000);

        assertCostHasUnknownComponentsForUnknownStats(aggregationNode);
    }

    private CostAssertionBuilder assertCost(
            PlanNode node,
            Map<String, PlanNodeCostEstimate> costs,
            Map<String, PlanNodeStatsEstimate> stats)
    {
        return new CostAssertionBuilder(costCalculatorUsingExchanges.calculateCumulativeCost(
                node,
                new FixedLookup(costs, stats),
                session,
                ImmutableMap.of()));
    }

    private CostAssertionBuilder assertCostEstimatedExchanges(
            PlanNode node,
            Map<String, PlanNodeCostEstimate> costs,
            Map<String, PlanNodeStatsEstimate> stats)
    {
        return new CostAssertionBuilder(costCalculatorWithEstimatedExchanges.calculateCumulativeCost(
                node,
                new FixedLookup(costs, stats),
                session,
                ImmutableMap.of()));
    }

    private void assertCostHasUnknownComponentsForUnknownStats(PlanNode planNode)
    {
        new CostAssertionBuilder(costCalculatorUsingExchanges.calculateCumulativeCost(
                planNode,
                new FixedLookup(id -> UNKNOWN_COST, id -> UNKNOWN_STATS),
                session,
                ImmutableMap.of()))
                .hasUnknownComponents();
        new CostAssertionBuilder(costCalculatorWithEstimatedExchanges.calculateCumulativeCost(
                planNode,
                new FixedLookup(id -> UNKNOWN_COST, id -> UNKNOWN_STATS),
                session,
                ImmutableMap.of()))
                .hasUnknownComponents();
    }

    private static class CostAssertionBuilder
    {
        private final PlanNodeCostEstimate actual;

        public CostAssertionBuilder(PlanNodeCostEstimate actual)
        {
            this.actual = requireNonNull(actual, "actual is null");
        }

        public CostAssertionBuilder network(double value)
        {
            assertEquals(actual.getNetworkCost(), value, 0.1);
            return this;
        }

        public CostAssertionBuilder networkUnknown()
        {
            assertIsNaN(actual.getNetworkCost());
            return this;
        }

        public CostAssertionBuilder cpu(double value)
        {
            assertEquals(actual.getCpuCost(), value, 0.1);
            return this;
        }

        public CostAssertionBuilder cpuUnknown()
        {
            assertIsNaN(actual.getCpuCost());
            return this;
        }

        public CostAssertionBuilder memory(double value)
        {
            assertEquals(actual.getMemoryCost(), value, 0.1);
            return this;
        }

        public CostAssertionBuilder memoryUnknown()
        {
            assertIsNaN(actual.getMemoryCost());
            return this;
        }

        public CostAssertionBuilder hasUnknownComponents()
        {
            assertTrue(actual.hasUnknownComponents());
            return this;
        }

        private void assertIsNaN(double value)
        {
            assertTrue(isNaN(value), "Expected NaN got " + value);
        }
    }

    private static PlanNodeStatsEstimate statsEstimate(int outputSizeInBytes)
    {
        double rowCount = Math.max(outputSizeInBytes / 8, 1);

        return PlanNodeStatsEstimate.builder()
                .setOutputRowCount(rowCount)
                .addSymbolStatistics(
                        new Symbol("s"),
                        SymbolStatsEstimate.builder()
                                .setAverageRowSize(outputSizeInBytes / rowCount)
                                .build())
                .build();
    }

    private TableScanNode tableScan(String id, String... symbols)
    {
        List<Symbol> symbolsList = Arrays.stream(symbols).map(Symbol::new).collect(toImmutableList());
        ImmutableMap.Builder<Symbol, ColumnHandle> assignments = ImmutableMap.builder();

        for (Symbol symbol : symbolsList) {
            assignments.put(symbol, new TpchColumnHandle("orderkey", BIGINT));
        }

        return new TableScanNode(
                new PlanNodeId(id),
                new TableHandle(new ConnectorId("tpch"), new TpchTableHandle("local", "orders", 1.0)),
                symbolsList,
                assignments.build(),
                Optional.empty(),
                TupleDomain.none(),
                null);
    }

    private PlanNode project(String id, PlanNode source, String symbol, Expression expression)
    {
        return new ProjectNode(
                new PlanNodeId(id),
                source,
                Assignments.of(new Symbol(symbol), expression));
    }

    private String symbol(String name)
    {
        return name;
    }

    private AggregationNode aggregation(String id, PlanNode source)
    {
        AggregationNode.Aggregation aggregation = new AggregationNode.Aggregation(
                new FunctionCall(QualifiedName.of("count"), ImmutableList.of()),
                new Signature("count", AGGREGATE, parseTypeSignature(StandardTypes.BIGINT)),
                Optional.empty());

        return new AggregationNode(
                new PlanNodeId(id),
                source,
                ImmutableMap.of(new Symbol("count"), aggregation),
                ImmutableList.of(source.getOutputSymbols()),
                AggregationNode.Step.FINAL,
                Optional.empty(),
                Optional.empty());
    }

    private JoinNode join(String planNodeId, PlanNode left, PlanNode right, String... symbols)
    {
        return join(planNodeId, left, right, JoinNode.DistributionType.PARTITIONED, symbols);
    }

    /**
     * EquiJoinClause is created from symbols in form of:
     * symbol[0] = symbol[1] AND symbol[2] = symbol[3] AND ...
     */
    private JoinNode join(String planNodeId, PlanNode left, PlanNode right, JoinNode.DistributionType distributionType, String... symbols)
    {
        checkArgument(symbols.length % 2 == 0);
        ImmutableList.Builder<JoinNode.EquiJoinClause> criteria = ImmutableList.builder();

        for (int i = 0; i < symbols.length; i += 2) {
            criteria.add(new JoinNode.EquiJoinClause(new Symbol(symbols[i]), new Symbol(symbols[i + 1])));
        }

        return new JoinNode(
                new PlanNodeId(planNodeId),
                JoinNode.Type.INNER,
                left,
                right,
                criteria.build(),
                ImmutableList.<Symbol>builder()
                        .addAll(left.getOutputSymbols())
                        .addAll(right.getOutputSymbols())
                        .build(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(distributionType));
    }

    private ValuesNode values(int planNodeId, String... symbols)
    {
        return new ValuesNode(
                new PlanNodeId(Integer.toString(planNodeId)),
                Arrays.stream(symbols)
                        .map(Symbol::new)
                        .collect(toImmutableList()),
                ImmutableList.of());
    }

    private class FixedLookup
            implements Lookup
    {
        private Function<String, PlanNodeCostEstimate> costs;
        private Function<String, PlanNodeStatsEstimate> stats;

        public FixedLookup(Function<String, PlanNodeCostEstimate> costs, Function<String, PlanNodeStatsEstimate> stats)
        {
            this.costs = costs;
            this.stats = stats;
        }

        public FixedLookup(Map<String, PlanNodeCostEstimate> costs, Map<String, PlanNodeStatsEstimate> stats)
        {
            this(costs::get, stats::get);
        }

        @Override
        public PlanNode resolve(PlanNode node)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public PlanNodeStatsEstimate getStats(PlanNode node, Session session, Map<Symbol, Type> types)
        {
            return stats.apply(node.getId().toString());
        }

        @Override
        public PlanNodeCostEstimate getCumulativeCost(PlanNode node, Session session, Map<Symbol, Type> types)
        {
            return costs.apply(node.getId().toString());
        }
    }
}
