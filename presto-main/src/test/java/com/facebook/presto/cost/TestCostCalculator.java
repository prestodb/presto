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
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.execution.NodeTaskMap;
import com.facebook.presto.execution.QueryManagerConfig;
import com.facebook.presto.execution.scheduler.LegacyNetworkTopology;
import com.facebook.presto.execution.scheduler.NodeScheduler;
import com.facebook.presto.execution.scheduler.NodeSchedulerConfig;
import com.facebook.presto.execution.scheduler.nodeSelection.NodeSelectionStats;
import com.facebook.presto.metadata.CatalogManager;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.security.AllowAllAccessControl;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.UnionNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.NodePartitioningManager;
import com.facebook.presto.sql.planner.PartitioningProviderManager;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.PlanFragmenter;
import com.facebook.presto.sql.planner.PlanVariableAllocator;
import com.facebook.presto.sql.planner.RuleStatsRecorder;
import com.facebook.presto.sql.planner.SubPlan;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.iterative.IterativeOptimizer;
import com.facebook.presto.sql.planner.iterative.rule.TranslateExpressions;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.SymbolReference;
import com.facebook.presto.tpch.TpchColumnHandle;
import com.facebook.presto.tpch.TpchTableHandle;
import com.facebook.presto.tpch.TpchTableLayoutHandle;
import com.facebook.presto.tpch.TpchTransactionHandle;
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

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.spi.plan.AggregationNode.singleGroupingSet;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.assignment;
import static com.facebook.presto.sql.planner.optimizations.AggregationNodeUtils.count;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.REMOTE_STREAMING;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.replicatedExchange;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.systemPartitionedExchange;
import static com.facebook.presto.sql.relational.Expressions.variable;
import static com.facebook.presto.testing.TestingSession.createBogusTestingCatalog;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
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
        TaskCountEstimator taskCountEstimator = new TaskCountEstimator(() -> NUMBER_OF_NODES);
        costCalculatorUsingExchanges = new CostCalculatorUsingExchanges(taskCountEstimator);
        costCalculatorWithEstimatedExchanges = new CostCalculatorWithEstimatedExchanges(costCalculatorUsingExchanges, taskCountEstimator);

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
                new NodeSelectionStats(),
                new NodeSchedulerConfig().setIncludeCoordinator(true),
                new NodeTaskMap(finalizerService));
        PartitioningProviderManager partitioningProviderManager = new PartitioningProviderManager();
        nodePartitioningManager = new NodePartitioningManager(nodeScheduler, partitioningProviderManager);
        planFragmenter = new PlanFragmenter(metadata, nodePartitioningManager, new QueryManagerConfig(), new SqlParser(), new FeaturesConfig());
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

        assertCost(tableScan, ImmutableMap.of(), ImmutableMap.of("ts", statsEstimate(tableScan, 1000)))
                .cpu(1000 * IS_NULL_OVERHEAD)
                .memory(0)
                .network(0);

        assertCostEstimatedExchanges(tableScan, ImmutableMap.of(), ImmutableMap.of("ts", statsEstimate(tableScan, 1000)))
                .cpu(1000 * IS_NULL_OVERHEAD)
                .memory(0)
                .network(0);

        assertCostSingleStageFragmentedPlan(tableScan, ImmutableMap.of(), ImmutableMap.of("ts", statsEstimate(tableScan, 1000)), types)
                .cpu(1000 * IS_NULL_OVERHEAD)
                .memory(0)
                .network(0);

        assertCostHasUnknownComponentsForUnknownStats(tableScan);
    }

    @Test
    public void testProject()
    {
        TableScanNode tableScan = tableScan("ts", "orderkey");
        PlanNode project = project("project", tableScan, new VariableReferenceExpression("string", VARCHAR), new Cast(new SymbolReference("orderkey"), "VARCHAR"));
        Map<String, PlanCostEstimate> costs = ImmutableMap.of("ts", cpuCost(1000));
        Map<String, PlanNodeStatsEstimate> stats = ImmutableMap.of(
                "project", statsEstimate(project, 4000),
                "ts", statsEstimate(tableScan, 1000));
        Map<String, Type> types = ImmutableMap.of(
                "orderkey", BIGINT,
                "string", VARCHAR);

        assertCost(project, costs, stats)
                .cpu(1000 + 4000 * OFFSET_AND_IS_NULL_OVERHEAD)
                .memory(0)
                .network(0);

        assertCostEstimatedExchanges(project, costs, stats)
                .cpu(1000 + 4000 * OFFSET_AND_IS_NULL_OVERHEAD)
                .memory(0)
                .network(0);

        assertCostSingleStageFragmentedPlan(project, costs, stats, types)
                .cpu(1000 + 4000 * OFFSET_AND_IS_NULL_OVERHEAD)
                .memory(0)
                .network(0);

        assertCostHasUnknownComponentsForUnknownStats(project);
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

        Map<String, PlanCostEstimate> costs = ImmutableMap.of(
                "ts1", cpuCost(6000),
                "ts2", cpuCost(1000));

        Map<String, PlanNodeStatsEstimate> stats = ImmutableMap.of(
                "join", statsEstimate(join, 12000),
                "ts1", statsEstimate(ts1, 6000),
                "ts2", statsEstimate(ts2, 1000));
        Map<String, Type> types = ImmutableMap.of(
                "orderkey", BIGINT,
                "orderkey_0", BIGINT);

        assertCost(join, costs, stats)
                .cpu(6000 + 1000 + (12000 + 6000 + 1000) * IS_NULL_OVERHEAD)
                .memory(1000 * IS_NULL_OVERHEAD)
                .network(0);

        assertCostEstimatedExchanges(join, costs, stats)
                .cpu(6000 + 1000 + (12000 + 6000 + 1000 + 6000 + 1000 + 1000) * IS_NULL_OVERHEAD)
                .memory(1000 * IS_NULL_OVERHEAD)
                .network((6000 + 1000) * IS_NULL_OVERHEAD);

        assertCostSingleStageFragmentedPlan(join, costs, stats, types)
                .cpu(6000 + 1000 + (12000 + 6000 + 1000) * IS_NULL_OVERHEAD)
                .memory(1000 * IS_NULL_OVERHEAD)
                .network(0);

        assertCostHasUnknownComponentsForUnknownStats(join);
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

        Map<String, PlanCostEstimate> costs = ImmutableMap.of(
                "ts1", cpuCost(6000),
                "ts2", cpuCost(1000));

        Map<String, PlanNodeStatsEstimate> stats = ImmutableMap.of(
                "join", statsEstimate(join, 12000),
                "ts1", statsEstimate(ts1, 6000),
                "ts2", statsEstimate(ts2, 1000));

        Map<String, Type> types = ImmutableMap.of(
                "orderkey", BIGINT,
                "orderkey_0", BIGINT);

        assertCost(join, costs, stats)
                .cpu(1000 + 6000 + (12000 + 6000 + 10000 + 1000 * (NUMBER_OF_NODES - 1)) * IS_NULL_OVERHEAD)
                .memory(1000 * NUMBER_OF_NODES * IS_NULL_OVERHEAD)
                .network(0);

        assertCostEstimatedExchanges(join, costs, stats)
                .cpu(1000 + 6000 + (12000 + 6000 + 10000 + 1000 * NUMBER_OF_NODES) * IS_NULL_OVERHEAD)
                .memory(1000 * NUMBER_OF_NODES * IS_NULL_OVERHEAD)
                .network(1000 * NUMBER_OF_NODES * IS_NULL_OVERHEAD);

        assertCostSingleStageFragmentedPlan(join, costs, stats, types)
                .cpu(1000 + 6000 + (12000 + 6000 + 10000 + 1000 * (NUMBER_OF_NODES - 1)) * IS_NULL_OVERHEAD)
                .memory(1000 * NUMBER_OF_NODES * IS_NULL_OVERHEAD)
                .network(0);

        assertCostHasUnknownComponentsForUnknownStats(join);
    }

    @Test
    public void testMemoryCostJoinAboveJoin()
    {
        //      join
        //     /   \
        //   ts1    join23
        //          /    \
        //        ts2     ts3

        TableScanNode ts1 = tableScan("ts1", "key1");
        TableScanNode ts2 = tableScan("ts2", "key2");
        TableScanNode ts3 = tableScan("ts3", "key3");
        JoinNode join23 = join(
                "join23",
                ts2,
                ts3,
                JoinNode.DistributionType.PARTITIONED,
                "key2",
                "key3");
        JoinNode join = join(
                "join",
                ts1,
                join23,
                JoinNode.DistributionType.PARTITIONED,
                "key1",
                "key2");

        Map<String, PlanCostEstimate> costs = ImmutableMap.of(
                "ts1", new PlanCostEstimate(0, 128, 128, 0),
                "ts2", new PlanCostEstimate(0, 64, 64, 0),
                "ts3", new PlanCostEstimate(0, 32, 32, 0));

        Map<String, PlanNodeStatsEstimate> stats = ImmutableMap.of(
                "join", statsEstimate(join, 10_000),
                "join23", statsEstimate(join23, 2_000),
                "ts1", statsEstimate(ts1, 10_000),
                "ts2", statsEstimate(ts2, 1_000),
                "ts3", statsEstimate(ts3, 100));

        Map<String, Type> types = ImmutableMap.of("key1", BIGINT, "key2", BIGINT, "key3", BIGINT);

        assertCost(join23, costs, stats)
                .memory(
                        100 * IS_NULL_OVERHEAD // join23 memory footprint
                                + 64 + 32) // ts2, ts3 memory footprint
                .memoryWhenOutputting(
                        100 * IS_NULL_OVERHEAD // join23 memory footprint
                                + 64); // ts2 memory footprint

        assertCost(join, costs, stats)
                .memory(
                        2000 * IS_NULL_OVERHEAD // join memory footprint
                                + 100 * IS_NULL_OVERHEAD + 64 // join23 total memory when outputting
                                + 128) // ts1 memory footprint
                .memoryWhenOutputting(
                        2000 * IS_NULL_OVERHEAD // join memory footprint
                                + 128); // ts1 memory footprint

        assertCostEstimatedExchanges(join23, costs, stats)
                .memory(
                        100 * IS_NULL_OVERHEAD // join23 memory footprint
                                + 64 + 32) // ts2, ts3 memory footprint
                .memoryWhenOutputting(
                        100 * IS_NULL_OVERHEAD // join23 memory footprint
                                + 64); // ts2 memory footprint

        assertCostEstimatedExchanges(join, costs, stats)
                .memory(
                        2000 * IS_NULL_OVERHEAD // join memory footprint
                                + 100 * IS_NULL_OVERHEAD + 64 // join23 total memory when outputting
                                + 128) // ts1 memory footprint
                .memoryWhenOutputting(
                        2000 * IS_NULL_OVERHEAD // join memory footprint
                                + 128); // ts1 memory footprint

        assertCostSingleStageFragmentedPlan(join23, costs, stats, types)
                .memory(
                        100 * IS_NULL_OVERHEAD // join23 memory footprint
                                + 64 + 32) // ts2, ts3 memory footprint
                .memoryWhenOutputting(
                        100 * IS_NULL_OVERHEAD // join23 memory footprint
                                + 64); // ts2 memory footprint

        assertCostSingleStageFragmentedPlan(join, costs, stats, types)
                .memory(
                        2000 * IS_NULL_OVERHEAD // join memory footprint
                                + 100 * IS_NULL_OVERHEAD + 64 // join23 total memory when outputting
                                + 128) // ts1 memory footprint
                .memoryWhenOutputting(
                        2000 * IS_NULL_OVERHEAD // join memory footprint
                                + 128); // ts1 memory footprint
    }

    @Test
    public void testAggregation()
    {
        TableScanNode tableScan = tableScan("ts", "orderkey");
        AggregationNode aggregation = aggregation("agg", tableScan);

        Map<String, PlanCostEstimate> costs = ImmutableMap.of("ts", cpuCost(6000));
        Map<String, PlanNodeStatsEstimate> stats = ImmutableMap.of(
                "ts", statsEstimate(tableScan, 6000),
                "agg", statsEstimate(aggregation, 13));
        Map<String, Type> types = ImmutableMap.of(
                "orderkey", BIGINT,
                "count", BIGINT);

        assertCost(aggregation, costs, stats)
                .cpu(6000 * IS_NULL_OVERHEAD + 6000)
                .memory(13 * IS_NULL_OVERHEAD)
                .network(0);

        assertCostEstimatedExchanges(aggregation, costs, stats)
                .cpu((6000 + 6000 + 6000) * IS_NULL_OVERHEAD + 6000)
                .memory(13 * IS_NULL_OVERHEAD)
                .network(6000 * IS_NULL_OVERHEAD);

        assertCostSingleStageFragmentedPlan(aggregation, costs, stats, types)
                .cpu(6000 + 6000 * IS_NULL_OVERHEAD)
                .memory(13 * IS_NULL_OVERHEAD)
                .network(0 * IS_NULL_OVERHEAD);

        assertCostHasUnknownComponentsForUnknownStats(aggregation);
    }

    @Test
    public void testRepartitionedJoinWithExchange()
    {
        TableScanNode ts1 = tableScan("ts1", "orderkey");
        TableScanNode ts2 = tableScan("ts2", "orderkey_0");
        PlanNode p1 = project("p1", ts1, variable("orderkey_1", BIGINT), new SymbolReference("orderkey"));
        ExchangeNode remoteExchange1 = systemPartitionedExchange(
                new PlanNodeId("re1"),
                REMOTE_STREAMING,
                p1,
                ImmutableList.of(new VariableReferenceExpression("orderkey_1", BIGINT)),
                Optional.empty());
        ExchangeNode remoteExchange2 = systemPartitionedExchange(
                new PlanNodeId("re2"),
                REMOTE_STREAMING,
                ts2,
                ImmutableList.of(new VariableReferenceExpression("orderkey_0", BIGINT)),
                Optional.empty());
        ExchangeNode localExchange = systemPartitionedExchange(
                new PlanNodeId("le"),
                LOCAL,
                remoteExchange2,
                ImmutableList.of(new VariableReferenceExpression("orderkey_0", BIGINT)),
                Optional.empty());

        JoinNode join = join("join",
                remoteExchange1,
                localExchange,
                JoinNode.DistributionType.PARTITIONED,
                "orderkey_1",
                "orderkey_0");

        Map<String, PlanNodeStatsEstimate> stats = ImmutableMap.<String, PlanNodeStatsEstimate>builder()
                .put("join", statsEstimate(join, 12000))
                .put("re1", statsEstimate(remoteExchange1, 10000))
                .put("re2", statsEstimate(remoteExchange2, 10000))
                .put("le", statsEstimate(localExchange, 6000))
                .put("p1", statsEstimate(p1, 6000))
                .put("ts1", statsEstimate(ts1, 6000))
                .put("ts2", statsEstimate(ts2, 1000))
                .build();
        Map<String, Type> types = ImmutableMap.of(
                "orderkey", BIGINT,
                "orderkey_1", BIGINT,
                "orderkey_0", BIGINT);

        assertFragmentedEqualsUnfragmented(join, stats, types);
    }

    @Test
    public void testReplicatedJoinWithExchange()
    {
        TableScanNode ts1 = tableScan("ts1", ImmutableList.of(new VariableReferenceExpression("orderkey", BIGINT)));
        TableScanNode ts2 = tableScan("ts2", ImmutableList.of(new VariableReferenceExpression("orderkey_0", BIGINT)));
        ExchangeNode remoteExchange2 = replicatedExchange(new PlanNodeId("re2"), REMOTE_STREAMING, ts2);
        ExchangeNode localExchange = systemPartitionedExchange(
                new PlanNodeId("le"),
                LOCAL,
                remoteExchange2,
                ImmutableList.of(new VariableReferenceExpression("orderkey_0", BIGINT)),
                Optional.empty());

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

    @Test
    public void testUnion()
    {
        TableScanNode ts1 = tableScan("ts1", "orderkey");
        TableScanNode ts2 = tableScan("ts2", "orderkey_0");
        UnionNode union = new UnionNode(
                new PlanNodeId("union"),
                ImmutableList.of(ts1, ts2),
                ImmutableList.of(new VariableReferenceExpression("orderkey_1", BIGINT)),
                ImmutableMap.of(
                        new VariableReferenceExpression("orderkey_1", BIGINT),
                        ImmutableList.of(new VariableReferenceExpression("orderkey", BIGINT), new VariableReferenceExpression("orderkey_0", BIGINT))));
        Map<String, PlanNodeStatsEstimate> stats = ImmutableMap.of(
                "ts1", statsEstimate(ts1, 4000),
                "ts2", statsEstimate(ts2, 1000),
                "union", statsEstimate(ts1, 5000));
        Map<String, PlanCostEstimate> costs = ImmutableMap.of(
                "ts1", cpuCost(1000),
                "ts2", cpuCost(1000));
        Map<String, Type> types = ImmutableMap.of(
                "orderkey", BIGINT,
                "orderkey_0", BIGINT,
                "orderkey_1", BIGINT);
        assertCost(union, costs, stats)
                .cpu(2000)
                .memory(0)
                .network(0);
        assertCostEstimatedExchanges(union, costs, stats)
                .cpu(2000)
                .memory(0)
                .network(5000 * IS_NULL_OVERHEAD);
    }

    private CostAssertionBuilder assertCost(
            PlanNode node,
            Map<String, PlanCostEstimate> costs,
            Map<String, PlanNodeStatsEstimate> stats)
    {
        return assertCost(costCalculatorUsingExchanges, node, costs, stats);
    }

    private CostAssertionBuilder assertCostEstimatedExchanges(
            PlanNode node,
            Map<String, PlanCostEstimate> costs,
            Map<String, PlanNodeStatsEstimate> stats)
    {
        return assertCost(costCalculatorWithEstimatedExchanges, node, costs, stats);
    }

    private CostAssertionBuilder assertCostSingleStageFragmentedPlan(
            PlanNode node,
            Map<String, PlanCostEstimate> costs,
            Map<String, PlanNodeStatsEstimate> stats,
            Map<String, Type> types)
    {
        TypeProvider typeProvider = TypeProvider.copyOf(types);
        StatsProvider statsProvider = new CachingStatsProvider(statsCalculator(stats), session, typeProvider);
        CostProvider costProvider = new TestingCostProvider(costs, costCalculatorUsingExchanges, statsProvider, session);
        // Explicitly generate the statsAndCosts, bypass fragment generation and sanity checks for mock plans.
        StatsAndCosts statsAndCosts = StatsAndCosts.create(node, statsProvider, costProvider).getForSubplan(node);
        return new CostAssertionBuilder(statsAndCosts.getCosts().getOrDefault(node.getId(), PlanCostEstimate.unknown()));
    }

    private PlanNode translateExpression(PlanNode node, StatsCalculator statsCalculator, TypeProvider typeProvider)
    {
        IterativeOptimizer optimizer = new IterativeOptimizer(new RuleStatsRecorder(), statsCalculator, costCalculatorUsingExchanges, new TranslateExpressions(metadata, new SqlParser()).rules());
        return optimizer.optimize(node, session, typeProvider, new PlanVariableAllocator(typeProvider.allVariables()), new PlanNodeIdAllocator(), WarningCollector.NOOP);
    }

    private static class TestingCostProvider
            implements CostProvider
    {
        private final Map<String, PlanCostEstimate> costs;
        private final CostCalculator costCalculator;
        private final StatsProvider statsProvider;
        private final Session session;

        private TestingCostProvider(Map<String, PlanCostEstimate> costs, CostCalculator costCalculator, StatsProvider statsProvider, Session session)
        {
            this.costs = ImmutableMap.copyOf(requireNonNull(costs, "costs is null"));
            this.costCalculator = requireNonNull(costCalculator, "costCalculator is null");
            this.statsProvider = requireNonNull(statsProvider, "statsProvider is null");
            this.session = requireNonNull(session, "session is null");
        }

        @Override
        public PlanCostEstimate getCost(PlanNode node)
        {
            if (costs.containsKey(node.getId().toString())) {
                return costs.get(node.getId().toString());
            }
            return costCalculator.calculateCost(node, statsProvider, this, session);
        }
    }

    private CostAssertionBuilder assertCost(
            CostCalculator costCalculator,
            PlanNode node,
            Map<String, PlanCostEstimate> costs,
            Map<String, PlanNodeStatsEstimate> stats)
    {
        Function<PlanNode, PlanNodeStatsEstimate> statsProvider = planNode -> stats.get(planNode.getId().toString());
        PlanCostEstimate cost = calculateCost(
                costCalculator,
                node,
                sourceCostProvider(costCalculator, costs, statsProvider),
                statsProvider);
        return new CostAssertionBuilder(cost);
    }

    private Function<PlanNode, PlanCostEstimate> sourceCostProvider(
            CostCalculator costCalculator,
            Map<String, PlanCostEstimate> costs,
            Function<PlanNode, PlanNodeStatsEstimate> statsProvider)
    {
        return node -> {
            PlanCostEstimate providedCost = costs.get(node.getId().toString());
            if (providedCost != null) {
                return providedCost;
            }
            return calculateCost(
                    costCalculator,
                    node,
                    sourceCostProvider(costCalculator, costs, statsProvider),
                    statsProvider);
        };
    }

    private void assertCostHasUnknownComponentsForUnknownStats(PlanNode node)
    {
        new CostAssertionBuilder(calculateCost(
                costCalculatorUsingExchanges,
                node,
                planNode -> PlanCostEstimate.unknown(),
                planNode -> PlanNodeStatsEstimate.unknown()))
                .hasUnknownComponents();
        new CostAssertionBuilder(calculateCost(
                costCalculatorWithEstimatedExchanges,
                node,
                planNode -> PlanCostEstimate.unknown(),
                planNode -> PlanNodeStatsEstimate.unknown()))
                .hasUnknownComponents();
    }

    private void assertFragmentedEqualsUnfragmented(PlanNode node, Map<String, PlanNodeStatsEstimate> stats, Map<String, Type> types)
    {
        StatsCalculator statsCalculator = statsCalculator(stats);
        PlanCostEstimate costWithExchanges = calculateCost(node, costCalculatorUsingExchanges, statsCalculator, types);
        PlanCostEstimate costWithFragments = calculateCostFragmentedPlan(node, statsCalculator, types);
        assertEquals(costWithExchanges, costWithFragments);
    }

    private StatsCalculator statsCalculator(Map<String, PlanNodeStatsEstimate> stats)
    {
        return (node, sourceStats, lookup, session, types) ->
                requireNonNull(stats.get(node.getId().toString()), "no stats for node");
    }

    private PlanCostEstimate calculateCost(
            CostCalculator costCalculator,
            PlanNode node,
            Function<PlanNode, PlanCostEstimate> costs,
            Function<PlanNode, PlanNodeStatsEstimate> stats)
    {
        return costCalculator.calculateCost(
                node,
                planNode -> requireNonNull(stats.apply(planNode), "no stats for node"),
                source -> requireNonNull(costs.apply(source), format("no cost for source: %s", source.getId())),
                session);
    }

    private PlanCostEstimate calculateCost(PlanNode node, CostCalculator costCalculator, StatsCalculator statsCalculator, Map<String, Type> types)
    {
        TypeProvider typeProvider = TypeProvider.copyOf(types);
        StatsProvider statsProvider = new CachingStatsProvider(statsCalculator, session, typeProvider);
        CostProvider costProvider = new CachingCostProvider(costCalculator, statsProvider, Optional.empty(), session);
        return costProvider.getCost(node);
    }

    private PlanCostEstimate calculateCostFragmentedPlan(PlanNode node, StatsCalculator statsCalculator, Map<String, Type> types)
    {
        TypeProvider typeProvider = TypeProvider.copyOf(types);
        StatsProvider statsProvider = new CachingStatsProvider(statsCalculator, session, typeProvider);
        CostProvider costProvider = new CachingCostProvider(costCalculatorUsingExchanges, statsProvider, Optional.empty(), session);
        node = translateExpression(node, statsCalculator, typeProvider);
        SubPlan subPlan = fragment(new Plan(node, typeProvider, StatsAndCosts.create(node, statsProvider, costProvider)));
        return subPlan.getFragment().getStatsAndCosts().getCosts().getOrDefault(node.getId(), PlanCostEstimate.unknown());
    }

    private static class CostAssertionBuilder
    {
        private final PlanCostEstimate actual;

        CostAssertionBuilder(PlanCostEstimate actual)
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
            assertEquals(actual.getMaxMemory(), value, 0.000001);
            return this;
        }

        CostAssertionBuilder memoryWhenOutputting(double value)
        {
            assertEquals(actual.getMaxMemoryWhenOutputting(), value, 0.000001);
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
        return statsEstimate(node.getOutputVariables(), outputSizeInBytes);
    }

    private static PlanNodeStatsEstimate statsEstimate(Collection<VariableReferenceExpression> variables, double outputSizeInBytes)
    {
        checkArgument(variables.size() > 0, "No variables");
        checkArgument(ImmutableSet.copyOf(variables).size() == variables.size(), "Duplicate variables");

        double rowCount = outputSizeInBytes / variables.size() / AVERAGE_ROW_SIZE;

        PlanNodeStatsEstimate.Builder builder = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(rowCount);
        for (VariableReferenceExpression variable : variables) {
            builder.addVariableStatistics(
                    variable,
                    VariableStatsEstimate.builder()
                            .setNullsFraction(0)
                            .setAverageRowSize(AVERAGE_ROW_SIZE)
                            .build());
        }
        return builder.build();
    }

    private TableScanNode tableScan(String id, String... symbols)
    {
        List<VariableReferenceExpression> variables = Arrays.stream(symbols)
                .map(symbol -> new VariableReferenceExpression(symbol, BIGINT))
                .collect(toImmutableList());
        return tableScan(id, variables);
    }

    private TableScanNode tableScan(String id, List<VariableReferenceExpression> variables)
    {
        ImmutableMap.Builder<VariableReferenceExpression, ColumnHandle> assignments = ImmutableMap.builder();

        for (VariableReferenceExpression variable : variables) {
            assignments.put(variable, new TpchColumnHandle("orderkey", BIGINT));
        }

        TpchTableHandle tableHandle = new TpchTableHandle("orders", 1.0);
        return new TableScanNode(
                new PlanNodeId(id),
                new TableHandle(
                        new ConnectorId("tpch"),
                        tableHandle,
                        TpchTransactionHandle.INSTANCE,
                        Optional.of(new TpchTableLayoutHandle(tableHandle, TupleDomain.all()))),
                variables,
                assignments.build(),
                TupleDomain.all(),
                TupleDomain.all());
    }

    private PlanNode project(String id, PlanNode source, VariableReferenceExpression variable, Expression expression)
    {
        return new ProjectNode(
                new PlanNodeId(id),
                source,
                assignment(variable, expression));
    }

    private AggregationNode aggregation(String id, PlanNode source)
    {
        AggregationNode.Aggregation aggregation = count(metadata.getFunctionAndTypeManager());

        return new AggregationNode(
                new PlanNodeId(id),
                source,
                ImmutableMap.of(new VariableReferenceExpression("count", BIGINT), aggregation),
                singleGroupingSet(source.getOutputVariables()),
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
            criteria.add(new JoinNode.EquiJoinClause(new VariableReferenceExpression(symbols[i], BIGINT), new VariableReferenceExpression(symbols[i + 1], BIGINT)));
        }

        return new JoinNode(
                new PlanNodeId(planNodeId),
                JoinNode.Type.INNER,
                left,
                right,
                criteria.build(),
                ImmutableList.<VariableReferenceExpression>builder()
                        .addAll(left.getOutputVariables())
                        .addAll(right.getOutputVariables())
                        .build(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(distributionType),
                ImmutableMap.of());
    }

    private SubPlan fragment(Plan plan)
    {
        return inTransaction(session -> planFragmenter.createSubPlans(session, plan, false, new PlanNodeIdAllocator(), WarningCollector.NOOP));
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

    private static PlanCostEstimate cpuCost(double cpuCost)
    {
        return new PlanCostEstimate(cpuCost, 0, 0, 0);
    }
}
