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
import com.facebook.presto.execution.NodeTaskMap;
import com.facebook.presto.execution.QueryManagerConfig;
import com.facebook.presto.execution.scheduler.LegacyNetworkTopology;
import com.facebook.presto.execution.scheduler.NodeScheduler;
import com.facebook.presto.execution.scheduler.NodeSchedulerConfig;
import com.facebook.presto.metadata.CatalogManager;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.metadata.TableLayoutHandle;
import com.facebook.presto.security.AllowAllAccessControl;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.planner.NodePartitioningManager;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.PlanFragmenter;
import com.facebook.presto.sql.planner.SubPlan;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
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
import com.facebook.presto.tpch.TpchTableLayoutHandle;
import com.facebook.presto.transaction.TransactionManager;
import com.facebook.presto.util.FinalizerService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.facebook.presto.cost.PlanNodeCostEstimate.cpuCost;
import static com.facebook.presto.metadata.FunctionKind.AGGREGATE;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.planner.plan.AggregationNode.singleGroupingSet;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.partitionedExchange;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.replicatedExchange;
import static com.facebook.presto.testing.TestingSession.createBogusTestingCatalog;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tpch.TpchTransactionHandle.INSTANCE;
import static com.facebook.presto.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static com.facebook.presto.transaction.TransactionBuilder.transaction;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestCostCalculator
{
    private static final int NUMBER_OF_NODES = 10;
    private static final double AVERAGE_ROW_SIZE = 8.;
    private static final double IS_NULL_OVERHEAD = 9. / AVERAGE_ROW_SIZE;
    private static final double OFFSET_AND_IS_NULL_OVERHEAD = 13. / AVERAGE_ROW_SIZE;
    private CostCalculator costCalculatorUsingExchanges;
    private CostCalculator costCalculatorWithEstimatedExchanges;
    private PlanFragmenter planFragmenter;
    private Session session;
    private MetadataManager metadata;
    private TransactionManager transactionManager;
    private FinalizerService finalizerService;
    private NodeScheduler nodeScheduler;
    private NodePartitioningManager nodePartitioningManager;

    @BeforeClass
    public void setUp()
    {
        costCalculatorUsingExchanges = new CostCalculatorUsingExchanges(() -> NUMBER_OF_NODES);
        costCalculatorWithEstimatedExchanges = new CostCalculatorWithEstimatedExchanges(costCalculatorUsingExchanges, () -> NUMBER_OF_NODES);
        planFragmenter = new PlanFragmenter(new QueryManagerConfig());

        session = testSessionBuilder().setCatalog("tpch").build();

        CatalogManager catalogManager = new CatalogManager();
        catalogManager.registerCatalog(createBogusTestingCatalog("tpch"));
        transactionManager = createTestTransactionManager(catalogManager);
        metadata = createTestMetadataManager(transactionManager, new FeaturesConfig());

        finalizerService = new FinalizerService();
        finalizerService.start();
        nodeScheduler = new NodeScheduler(
                new LegacyNetworkTopology(),
                new InMemoryNodeManager(),
                new NodeSchedulerConfig().setIncludeCoordinator(true),
                new NodeTaskMap(finalizerService));
        nodePartitioningManager = new NodePartitioningManager(nodeScheduler);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        costCalculatorUsingExchanges = null;
        costCalculatorWithEstimatedExchanges = null;
        planFragmenter = null;
        session = null;
        transactionManager = null;
        metadata = null;
        finalizerService.destroy();
        finalizerService = null;
        nodeScheduler.stop();
        nodeScheduler = null;
        nodePartitioningManager = null;
    }

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

        assertCostFragmentedPlan(tableScan, ImmutableMap.of(), ImmutableMap.of("ts", statsEstimate(tableScan, 1000)), types)
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

        assertCostFragmentedPlan(project, costs, stats, types)
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

        assertCostFragmentedPlan(join, costs, stats, types)
                .cpu(6000 + 1000 + (12000 + 6000 + 1000) * IS_NULL_OVERHEAD)
                .memory(1000 * IS_NULL_OVERHEAD)
                .network(0);

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

        assertCostFragmentedPlan(join, costs, stats, types)
                .cpu(1000 + 6000 + (12000 + 6000 + 10000 + 1000 * (NUMBER_OF_NODES - 1)) * IS_NULL_OVERHEAD)
                .memory(1000 * NUMBER_OF_NODES * IS_NULL_OVERHEAD)
                .network(0);

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

        assertCostFragmentedPlan(aggregation, costs, stats, types)
                .cpu(6000 + 6000 * IS_NULL_OVERHEAD)
                .memory(13 * IS_NULL_OVERHEAD)
                .network(0 * IS_NULL_OVERHEAD);

        assertCostHasUnknownComponentsForUnknownStats(aggregation, types);
    }

    @Test
    public void testRepartitionedJoinWithExchange()
    {
        TableScanNode ts1 = tableScan("ts1", "orderkey");
        TableScanNode ts2 = tableScan("ts2", "orderkey_0");
        ExchangeNode remoteExchange1 = partitionedExchange(new PlanNodeId("re1"), REMOTE, ts1, ImmutableList.of(new Symbol("orderkey")), Optional.empty());
        ExchangeNode remoteExchange2 = partitionedExchange(new PlanNodeId("re2"), REMOTE, ts2, ImmutableList.of(new Symbol("orderkey_0")), Optional.empty());
        ExchangeNode localExchange = partitionedExchange(new PlanNodeId("le"), LOCAL, remoteExchange2, ImmutableList.of(new Symbol("orderkey_0")), Optional.empty());

        JoinNode join = join("join",
                remoteExchange1,
                localExchange,
                JoinNode.DistributionType.PARTITIONED,
                "orderkey",
                "orderkey_0");

        Map<String, PlanNodeStatsEstimate> stats = ImmutableMap.<String, PlanNodeStatsEstimate>builder()
                .put("join", statsEstimate(join, 12000))
                .put("re1", statsEstimate(remoteExchange1, 10000))
                .put("re2", statsEstimate(remoteExchange2, 10000))
                .put("le", statsEstimate(localExchange, 6000))
                .put("ts1", statsEstimate(ts1, 6000))
                .put("ts2", statsEstimate(ts2, 1000))
                .build();
        Map<String, Type> types = ImmutableMap.of(
                "orderkey", BIGINT,
                "orderkey_0", BIGINT);

        assertFragmentedEqualsUnfragmented(join, stats, types);
    }

    @Test
    public void testReplicatedJoinWithExchange()
    {
        TableScanNode ts1 = tableScan("ts1", "orderkey");
        TableScanNode ts2 = tableScan("ts2", "orderkey_0");
        ExchangeNode remoteExchange2 = replicatedExchange(new PlanNodeId("re2"), REMOTE, ts2);
        ExchangeNode localExchange = partitionedExchange(new PlanNodeId("le"), LOCAL, remoteExchange2, ImmutableList.of(new Symbol("orderkey_0")), Optional.empty());

        JoinNode join = join("join",
                ts1,
                localExchange,
                JoinNode.DistributionType.REPLICATED,
                "orderkey",
                "orderkey_0");

        Map<String, PlanNodeStatsEstimate> stats = ImmutableMap.<String, PlanNodeStatsEstimate>builder()
                .put("join", statsEstimate(join, 12000))
                .put("re2", statsEstimate(remoteExchange2, 10000))
                .put("le", statsEstimate(localExchange, 6000))
                .put("ts1", statsEstimate(ts1, 6000))
                .put("ts2", statsEstimate(ts2, 1000))
                .build();
        Map<String, Type> types = ImmutableMap.of(
                "orderkey", BIGINT,
                "orderkey_0", BIGINT);

        assertFragmentedEqualsUnfragmented(join, stats, types);
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

    private CostAssertionBuilder assertCostFragmentedPlan(
            PlanNode node,
            Map<String, PlanNodeCostEstimate> costs,
            Map<String, PlanNodeStatsEstimate> stats,
            Map<String, Type> types)
    {
        TypeProvider typeProvider = TypeProvider.copyOf(types.entrySet().stream()
                .collect(ImmutableMap.toImmutableMap(entry -> new Symbol(entry.getKey()), Map.Entry::getValue)));
        StatsProvider statsProvider = new CachingStatsProvider(statsCalculator(stats), session, typeProvider);
        CostProvider costProvider = new TestingCostProvider(costs, costCalculatorUsingExchanges, statsProvider, session, typeProvider);
        SubPlan subPlan = fragment(new Plan(node, typeProvider, StatsAndCosts.create(node, statsProvider, costProvider)));
        return new CostAssertionBuilder(subPlan.getFragment().getStatsAndCosts().getCosts().getOrDefault(node.getId(), PlanNodeCostEstimate.unknown()));
    }

    private static class TestingCostProvider
            implements CostProvider
    {
        private final Map<String, PlanNodeCostEstimate> costs;
        private final CostCalculator costCalculator;
        private final StatsProvider statsProvider;
        private final Session session;
        private final TypeProvider types;

        private TestingCostProvider(Map<String, PlanNodeCostEstimate> costs, CostCalculator costCalculator, StatsProvider statsProvider, Session session, TypeProvider types)
        {
            this.costs = ImmutableMap.copyOf(requireNonNull(costs, "costs is null"));
            this.costCalculator = requireNonNull(costCalculator, "costCalculator is null");
            this.statsProvider = requireNonNull(statsProvider, "statsProvider is null");
            this.session = requireNonNull(session, "session is null");
            this.types = requireNonNull(types, "types is null");
        }

        @Override
        public PlanNodeCostEstimate getCumulativeCost(PlanNode node)
        {
            if (costs.containsKey(node.getId().toString())) {
                return costs.get(node.getId().toString());
            }
            return calculateCumulativeCost(node);
        }

        private PlanNodeCostEstimate calculateCumulativeCost(PlanNode node)
        {
            PlanNodeCostEstimate sourcesCost = node.getSources().stream()
                    .map(this::getCumulativeCost)
                    .reduce(PlanNodeCostEstimate.zero(), PlanNodeCostEstimate::add);

            return costCalculator.calculateCost(node, statsProvider, session, types).add(sourcesCost);
        }
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
                planNode -> PlanNodeCostEstimate.unknown(),
                planNode -> PlanNodeStatsEstimate.unknown(),
                types))
                .hasUnknownComponents();
        new CostAssertionBuilder(calculateCumulativeCost(
                costCalculatorWithEstimatedExchanges,
                node,
                planNode -> PlanNodeCostEstimate.unknown(),
                planNode -> PlanNodeStatsEstimate.unknown(),
                types))
                .hasUnknownComponents();
    }

    private void assertFragmentedEqualsUnfragmented(PlanNode node, Map<String, PlanNodeStatsEstimate> stats, Map<String, Type> types)
    {
        StatsCalculator statsCalculator = statsCalculator(stats);
        PlanNodeCostEstimate costWithExchanges = calculateCumulativeCost(node, costCalculatorUsingExchanges, statsCalculator, types);
        PlanNodeCostEstimate costWithFragments = calculateCumulativeCostFragmentedPlan(node, statsCalculator, types);
        assertEquals(costWithExchanges, costWithFragments);
    }

    private StatsCalculator statsCalculator(Map<String, PlanNodeStatsEstimate> stats)
    {
        return (node, sourceStats, lookup, session, types) ->
                requireNonNull(stats.get(node.getId().toString()), "no stats for node");
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
                session,
                TypeProvider.copyOf(types.entrySet().stream()
                        .collect(ImmutableMap.toImmutableMap(entry -> new Symbol(entry.getKey()), Map.Entry::getValue))));

        PlanNodeCostEstimate sourcesCost = node.getSources().stream()
                .map(source -> requireNonNull(costs.apply(source), format("no cost for source: %s", source.getId())))
                .reduce(PlanNodeCostEstimate.zero(), PlanNodeCostEstimate::add);
        return sourcesCost.add(localCost);
    }

    private PlanNodeCostEstimate calculateCumulativeCost(PlanNode node, CostCalculator costCalculator, StatsCalculator statsCalculator, Map<String, Type> types)
    {
        TypeProvider typeProvider = TypeProvider.copyOf(types.entrySet().stream()
                .collect(ImmutableMap.toImmutableMap(entry -> new Symbol(entry.getKey()), Map.Entry::getValue)));
        StatsProvider statsProvider = new CachingStatsProvider(statsCalculator, session, typeProvider);
        CostProvider costProvider = new CachingCostProvider(costCalculator, statsProvider, Optional.empty(), session, typeProvider);
        return costProvider.getCumulativeCost(node);
    }

    private PlanNodeCostEstimate calculateCumulativeCostFragmentedPlan(PlanNode node, StatsCalculator statsCalculator, Map<String, Type> types)
    {
        TypeProvider typeProvider = TypeProvider.copyOf(types.entrySet().stream()
                .collect(ImmutableMap.toImmutableMap(entry -> new Symbol(entry.getKey()), Map.Entry::getValue)));
        StatsProvider statsProvider = new CachingStatsProvider(statsCalculator, session, typeProvider);
        CostProvider costProvider = new CachingCostProvider(costCalculatorUsingExchanges, statsProvider, Optional.empty(), session, typeProvider);
        SubPlan subPlan = fragment(new Plan(node, typeProvider, StatsAndCosts.create(node, statsProvider, costProvider)));
        return subPlan.getFragment().getStatsAndCosts().getCosts().getOrDefault(node.getId(), PlanNodeCostEstimate.unknown());
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

        TpchTableHandle tableHandle = new TpchTableHandle("local", "orders", 1.0);
        return new TableScanNode(
                new PlanNodeId(id),
                new TableHandle(new ConnectorId("tpch"), new TpchTableHandle("local", "orders", 1.0)),
                symbolsList,
                assignments.build(),
                Optional.of(new TableLayoutHandle(new ConnectorId("tpch"), INSTANCE, new TpchTableLayoutHandle(tableHandle, TupleDomain.all()))),
                TupleDomain.all(),
                TupleDomain.all());
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
                singleGroupingSet(source.getOutputSymbols()),
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

    private SubPlan fragment(Plan plan)
    {
        return inTransaction(session -> planFragmenter.createSubPlans(session, metadata, nodePartitioningManager, plan, false));
    }

    private <T> T inTransaction(Function<Session, T> transactionSessionConsumer)
    {
        return transaction(transactionManager, new AllowAllAccessControl())
                .singleStatement()
                .execute(session, session -> {
                    // metadata.getCatalogHandle() registers the catalog for the transaction
                    session.getCatalog().ifPresent(catalog -> metadata.getCatalogHandle(session, catalog));
                    return transactionSessionConsumer.apply(session);
                });
    }
}
