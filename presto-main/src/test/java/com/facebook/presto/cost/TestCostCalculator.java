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
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.SymbolReference;
import com.facebook.presto.tpch.TpchColumnHandle;
import com.facebook.presto.tpch.TpchTableHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.facebook.presto.cost.PlanNodeCostEstimate.UNKNOWN_COST;
import static com.facebook.presto.cost.PlanNodeCostEstimate.ZERO_COST;
import static com.facebook.presto.cost.PlanNodeCostEstimate.cpuCost;
import static com.facebook.presto.cost.PlanNodeStatsEstimate.UNKNOWN_STATS;
import static com.facebook.presto.metadata.FunctionKind.AGGREGATE;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.planner.iterative.Lookup.noLookup;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestCostCalculator
{
    private static final int NUMBER_OF_NODES = 10;
    private static final double AVERAGE_ROW_SIZE = 8.;
    private static final double IS_NULL_OVERHEAD = 9. / AVERAGE_ROW_SIZE;
    private static final double OFFSET_AND_IS_NULL_OVERHEAD = 13. / AVERAGE_ROW_SIZE;
    private final CostCalculator costCalculatorUsingExchanges = new CostCalculatorUsingExchanges(() -> NUMBER_OF_NODES);
    private final CostCalculator costCalculatorWithEstimatedExchanges = new CostCalculatorWithEstimatedExchanges(costCalculatorUsingExchanges, () -> NUMBER_OF_NODES);
    private Session session = testSessionBuilder().build();

    @Test
    public void testTableScan()
    {
        TableScanNode tableScan = tableScan("ts", "orderkey");
        Map<String, Type> types = ImmutableMap.of("orderkey", BIGINT);

        assertCost(tableScan, ImmutableMap.of(), ImmutableMap.of("ts", statsEstimate(tableScan, 1000)), types)
                .cpu(1000 * IS_NULL_OVERHEAD)
                .memory(0)
                .network(0);

        assertCostEstimatedExchanges(tableScan, ImmutableMap.of(), ImmutableMap.of("ts", statsEstimate(tableScan, 1000)), types)
                .cpu(1000 * IS_NULL_OVERHEAD)
                .memory(0)
                .network(0);

        assertCostHasUnknownComponentsForUnknownStats(tableScan, types);
    }

    @Test
    public void testProject()
    {
        TableScanNode tableScan = tableScan("ts", "orderkey");
        PlanNode project = project("project", tableScan, "string", new Cast(new SymbolReference("orderkey"), "STRING"));
        Map<String, PlanNodeCostEstimate> costs = ImmutableMap.of("ts", cpuCost(1000));
        Map<String, PlanNodeStatsEstimate> stats = ImmutableMap.of(
                "project", statsEstimate(project, 4000),
                "ts", statsEstimate(tableScan, 1000));
        Map<String, Type> types = ImmutableMap.of(
                "orderkey", BIGINT,
                "string", VARCHAR);

        assertCost(project, costs, stats, types)
                .cpu(1000 + 4000 * OFFSET_AND_IS_NULL_OVERHEAD)
                .memory(0)
                .network(0);

        assertCostEstimatedExchanges(project, costs, stats, types)
                .cpu(1000 + 4000 * OFFSET_AND_IS_NULL_OVERHEAD)
                .memory(0)
                .network(0);

        assertCostHasUnknownComponentsForUnknownStats(project, types);
    }

    @Test
    public void testRepartitionedJoin()
    {
        TableScanNode ts1 = tableScan("ts1", "orderkey");
        TableScanNode ts2 = tableScan("ts2", "orderkey_0");
        JoinNode join = join("join",
                ts1,
                ts2,
                JoinNode.DistributionType.PARTITIONED,
                "orderkey",
                "orderkey_0");

        Map<String, PlanNodeCostEstimate> costs = ImmutableMap.of(
                "ts1", cpuCost(6000),
                "ts2", cpuCost(1000));

        Map<String, PlanNodeStatsEstimate> stats = ImmutableMap.of(
                "join", statsEstimate(join, 12000),
                "ts1", statsEstimate(ts1, 6000),
                "ts2", statsEstimate(ts2, 1000));
        Map<String, Type> types = ImmutableMap.of(
                "orderkey", BIGINT,
                "orderkey_0", BIGINT);

        assertCost(join, costs, stats, types)
                .cpu(6000 + 1000 + (12000 + 6000 + 1000) * IS_NULL_OVERHEAD)
                .memory(1000 * IS_NULL_OVERHEAD)
                .network(0);

        assertCostEstimatedExchanges(join, costs, stats, types)
                .cpu(6000 + 1000 + (12000 + 6000 + 1000 + 6000 + 1000 + 1000) * IS_NULL_OVERHEAD)
                .memory(1000 * IS_NULL_OVERHEAD)
                .network((6000 + 1000) * IS_NULL_OVERHEAD);

        assertCostHasUnknownComponentsForUnknownStats(join, types);
    }

    @Test
    public void testReplicatedJoin()
    {
        TableScanNode ts1 = tableScan("ts1", "orderkey");
        TableScanNode ts2 = tableScan("ts2", "orderkey_0");
        JoinNode join = join("join",
                ts1,
                ts2,
                JoinNode.DistributionType.REPLICATED,
                "orderkey",
                "orderkey_0");

        Map<String, PlanNodeCostEstimate> costs = ImmutableMap.of(
                "ts1", cpuCost(6000),
                "ts2", cpuCost(1000));

        Map<String, PlanNodeStatsEstimate> stats = ImmutableMap.of(
                "join", statsEstimate(join, 12000),
                "ts1", statsEstimate(ts1, 6000),
                "ts2", statsEstimate(ts2, 1000));

        Map<String, Type> types = ImmutableMap.of(
                "orderkey", BIGINT,
                "orderkey_0", BIGINT);

        assertCost(join, costs, stats, types)
                .cpu(1000 + 6000 + (12000 + 6000 + 10000 + 1000 * (NUMBER_OF_NODES - 1)) * IS_NULL_OVERHEAD)
                .memory(1000 * NUMBER_OF_NODES * IS_NULL_OVERHEAD)
                .network(0);

        assertCostEstimatedExchanges(join, costs, stats, types)
                .cpu(1000 + 6000 + (12000 + 6000 + 10000 + 1000 * NUMBER_OF_NODES) * IS_NULL_OVERHEAD)
                .memory(1000 * NUMBER_OF_NODES * IS_NULL_OVERHEAD)
                .network(1000 * NUMBER_OF_NODES * IS_NULL_OVERHEAD);

        assertCostHasUnknownComponentsForUnknownStats(join, types);
    }

    @Test
    public void testAggregation()
    {
        TableScanNode tableScan = tableScan("ts", "orderkey");
        AggregationNode aggregation = aggregation("agg", tableScan);

        Map<String, PlanNodeCostEstimate> costs = ImmutableMap.of("ts", cpuCost(6000));
        Map<String, PlanNodeStatsEstimate> stats = ImmutableMap.of(
                "ts", statsEstimate(tableScan, 6000),
                "agg", statsEstimate(aggregation, 13));
        Map<String, Type> types = ImmutableMap.of(
                "orderkey", BIGINT,
                "count", BIGINT);

        assertCost(aggregation, costs, stats, types)
                .cpu(6000 * IS_NULL_OVERHEAD + 6000)
                .memory(13 * IS_NULL_OVERHEAD)
                .network(0);

        assertCostEstimatedExchanges(aggregation, costs, stats, types)
                .cpu((6000 + 6000 + 6000) * IS_NULL_OVERHEAD + 6000)
                .memory(13 * IS_NULL_OVERHEAD)
                .network(6000 * IS_NULL_OVERHEAD);

        assertCostHasUnknownComponentsForUnknownStats(aggregation, types);
    }

    private CostAssertionBuilder assertCost(
            PlanNode node,
            Map<String, PlanNodeCostEstimate> costs,
            Map<String, PlanNodeStatsEstimate> stats,
            Map<String, Type> types)
    {
        return assertCost(costCalculatorUsingExchanges, node, costs, stats, types);
    }

    private CostAssertionBuilder assertCostEstimatedExchanges(
            PlanNode node,
            Map<String, PlanNodeCostEstimate> costs,
            Map<String, PlanNodeStatsEstimate> stats,
            Map<String, Type> types)
    {
        return assertCost(costCalculatorWithEstimatedExchanges, node, costs, stats, types);
    }

    private CostAssertionBuilder assertCost(
            CostCalculator costCalculator,
            PlanNode node,
            Map<String, PlanNodeCostEstimate> costs,
            Map<String, PlanNodeStatsEstimate> stats,
            Map<String, Type> types)
    {
        PlanNodeCostEstimate cumulativeCost = calculateCumulativeCost(
                costCalculator,
                node,
                planNode -> costs.get(planNode.getId().toString()),
                planNode -> stats.get(planNode.getId().toString()),
                types);
        return new CostAssertionBuilder(cumulativeCost);
    }

    private void assertCostHasUnknownComponentsForUnknownStats(PlanNode node, Map<String, Type> types)
    {
        new CostAssertionBuilder(calculateCumulativeCost(
                costCalculatorUsingExchanges,
                node,
                planNode -> UNKNOWN_COST,
                planNode -> UNKNOWN_STATS,
                types))
                .hasUnknownComponents();
        new CostAssertionBuilder(calculateCumulativeCost(
                costCalculatorWithEstimatedExchanges,
                node,
                planNode -> UNKNOWN_COST,
                planNode -> UNKNOWN_STATS,
                types))
                .hasUnknownComponents();
    }

    private PlanNodeCostEstimate calculateCumulativeCost(
            CostCalculator costCalculator,
            PlanNode node,
            Function<PlanNode, PlanNodeCostEstimate> costs,
            Function<PlanNode, PlanNodeStatsEstimate> stats,
            Map<String, Type> types)
    {
        PlanNodeCostEstimate localCost = costCalculator.calculateCost(
                node,
                planNode -> requireNonNull(stats.apply(planNode), "no stats for node"),
                noLookup(),
                session,
                TypeProvider.copyOf(types.entrySet().stream()
                        .collect(ImmutableMap.toImmutableMap(entry -> new Symbol(entry.getKey()), Map.Entry::getValue))));

        PlanNodeCostEstimate sourcesCost = node.getSources().stream()
                .map(source -> requireNonNull(costs.apply(source), "no cost for source"))
                .reduce(ZERO_COST, PlanNodeCostEstimate::add);
        return sourcesCost.add(localCost);
    }

    private static class CostAssertionBuilder
    {
        private final PlanNodeCostEstimate actual;

        CostAssertionBuilder(PlanNodeCostEstimate actual)
        {
            this.actual = requireNonNull(actual, "actual is null");
        }

        CostAssertionBuilder cpu(double value)
        {
            assertEquals(actual.getCpuCost(), value, 0.000001);
            return this;
        }

        CostAssertionBuilder memory(double value)
        {
            assertEquals(actual.getMemoryCost(), value, 0.000001);
            return this;
        }

        CostAssertionBuilder network(double value)
        {
            assertEquals(actual.getNetworkCost(), value, 0.000001);
            return this;
        }

        CostAssertionBuilder hasUnknownComponents()
        {
            assertTrue(actual.hasUnknownComponents());
            return this;
        }
    }

    private static PlanNodeStatsEstimate statsEstimate(PlanNode node, double outputSizeInBytes)
    {
        return statsEstimate(node.getOutputSymbols(), outputSizeInBytes);
    }

    private static PlanNodeStatsEstimate statsEstimate(Collection<Symbol> symbols, double outputSizeInBytes)
    {
        checkArgument(symbols.size() > 0, "No symbols");
        checkArgument(ImmutableSet.copyOf(symbols).size() == symbols.size(), "Duplicate symbols");

        double rowCount = outputSizeInBytes / symbols.size() / AVERAGE_ROW_SIZE;

        PlanNodeStatsEstimate.Builder builder = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(rowCount);
        for (Symbol symbol : symbols) {
            builder.addSymbolStatistics(
                    symbol,
                    SymbolStatsEstimate.builder()
                            .setNullsFraction(0)
                            .setAverageRowSize(AVERAGE_ROW_SIZE)
                            .build());
        }
        return builder.build();
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
                TupleDomain.all(),
                null);
    }

    private PlanNode project(String id, PlanNode source, String symbol, Expression expression)
    {
        return new ProjectNode(
                new PlanNodeId(id),
                source,
                Assignments.of(new Symbol(symbol), expression));
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
                ImmutableList.of(),
                AggregationNode.Step.FINAL,
                Optional.empty(),
                Optional.empty());
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
}
