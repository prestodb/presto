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
package com.facebook.presto.spark.planner;

import com.facebook.presto.Session;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.cost.StatsAndCosts;
import com.facebook.presto.dispatcher.NoOpQueryManager;
import com.facebook.presto.execution.NodeTaskMap;
import com.facebook.presto.execution.QueryManagerConfig;
import com.facebook.presto.execution.scheduler.LegacyNetworkTopology;
import com.facebook.presto.execution.scheduler.NodeScheduler;
import com.facebook.presto.execution.scheduler.NodeSchedulerConfig;
import com.facebook.presto.execution.scheduler.nodeSelection.NodeSelectionStats;
import com.facebook.presto.execution.scheduler.nodeSelection.SimpleTtlNodeSelectorConfig;
import com.facebook.presto.metadata.CatalogManager;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spark.planner.IterativePlanFragmenter.PlanAndFragments;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.JoinDistributionType;
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.plan.PartitioningHandle;
import com.facebook.presto.spi.plan.PartitioningScheme;
import com.facebook.presto.spi.plan.PlanFragmentId;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.SimplePlanFragment;
import com.facebook.presto.spi.plan.StageExecutionDescriptor;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.security.AllowAllAccessControl;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.planner.NodePartitioningManager;
import com.facebook.presto.sql.planner.PartitioningProviderManager;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.PlanFragmenter;
import com.facebook.presto.sql.planner.SubPlan;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.JsonCodecSimplePlanFragmentSerde;
import com.facebook.presto.sql.planner.sanity.PlanChecker;
import com.facebook.presto.sql.planner.sanity.PlanCheckerProviderManager;
import com.facebook.presto.tpch.TpchColumnHandle;
import com.facebook.presto.tpch.TpchTableHandle;
import com.facebook.presto.tpch.TpchTableLayoutHandle;
import com.facebook.presto.tpch.TpchTransactionHandle;
import com.facebook.presto.transaction.TransactionManager;
import com.facebook.presto.ttl.nodettlfetchermanagers.ThrowingNodeTtlFetcherManager;
import com.facebook.presto.util.FinalizerService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.presto.SystemSessionProperties.FORCE_SINGLE_NODE_OUTPUT;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.spark.planner.TestIterativePlanFragmenter.CanonicalTestFragment.toCanonicalTestFragment;
import static com.facebook.presto.spi.plan.AggregationNode.singleGroupingSet;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.assignment;
import static com.facebook.presto.sql.planner.optimizations.AggregationNodeUtils.count;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.REMOTE_STREAMING;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.systemPartitionedExchange;
import static com.facebook.presto.sql.relational.Expressions.variable;
import static com.facebook.presto.testing.TestingSession.createBogusTestingCatalog;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static com.facebook.presto.transaction.TransactionBuilder.transaction;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestIterativePlanFragmenter
{
    private PlanFragmenter planFragmenter;
    private Session session;
    private MetadataManager metadata;
    private TransactionManager transactionManager;
    private FinalizerService finalizerService;
    private NodeScheduler nodeScheduler;
    private NodePartitioningManager nodePartitioningManager;
    private PlanCheckerProviderManager planCheckerProviderManager;

    @BeforeClass
    public void setUp()
    {
        session = testSessionBuilder()
                .setCatalog("tpch")
                .setSystemProperty(FORCE_SINGLE_NODE_OUTPUT, "false")
                .build();

        CatalogManager catalogManager = new CatalogManager();
        catalogManager.registerCatalog(createBogusTestingCatalog("tpch"));
        transactionManager = createTestTransactionManager(catalogManager);
        metadata = createTestMetadataManager(transactionManager);

        finalizerService = new FinalizerService();
        finalizerService.start();
        nodeScheduler = new NodeScheduler(
                new LegacyNetworkTopology(),
                new InMemoryNodeManager(),
                new NodeSelectionStats(),
                new NodeSchedulerConfig().setIncludeCoordinator(true),
                new NodeTaskMap(finalizerService),
                new ThrowingNodeTtlFetcherManager(),
                new NoOpQueryManager(),
                new SimpleTtlNodeSelectorConfig());
        PartitioningProviderManager partitioningProviderManager = new PartitioningProviderManager();
        nodePartitioningManager = new NodePartitioningManager(nodeScheduler, partitioningProviderManager, new NodeSelectionStats());
        planCheckerProviderManager = new PlanCheckerProviderManager(new JsonCodecSimplePlanFragmentSerde(jsonCodec(SimplePlanFragment.class)));
        planFragmenter = new PlanFragmenter(metadata, nodePartitioningManager, new QueryManagerConfig(), new FeaturesConfig(), planCheckerProviderManager);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
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
    public void testIterativePlanFragmenter()
    {
        TableScanNode ts1 = tableScan("ts1", "orderkey");
        TableScanNode ts2 = tableScan("ts2", "orderkey_0");
        PlanNode p1 = project("p1", ts1, variable("orderkey_1", BIGINT), variable("orderkey", BIGINT));
        ExchangeNode remoteExchange1 = systemPartitionedExchange(
                new PlanNodeId("re1"),
                REMOTE_STREAMING,
                p1,
                ImmutableList.of(new VariableReferenceExpression(Optional.empty(), "orderkey_1", BIGINT)),
                Optional.empty());
        ExchangeNode remoteExchange2 = systemPartitionedExchange(
                new PlanNodeId("re2"),
                REMOTE_STREAMING,
                ts2,
                ImmutableList.of(new VariableReferenceExpression(Optional.empty(), "orderkey_0", BIGINT)),
                Optional.empty());
        ExchangeNode localExchange = systemPartitionedExchange(
                new PlanNodeId("le"),
                LOCAL,
                remoteExchange2,
                ImmutableList.of(new VariableReferenceExpression(Optional.empty(), "orderkey_0", BIGINT)),
                Optional.empty());

        JoinNode join = join("join",
                remoteExchange1,
                localExchange,
                JoinDistributionType.PARTITIONED,
                "orderkey_1",
                "orderkey_0");
        Map<String, Type> types = ImmutableMap.of(
                "orderkey", BIGINT,
                "orderkey_1", BIGINT,
                "orderkey_0", BIGINT);
        TypeProvider typeProvider = TypeProvider.copyOf(types);
        Plan plan = new Plan(join, typeProvider, StatsAndCosts.empty());

        SubPlan fullFragmentedPlan = getFullFragmentedPlan(plan);

        inTransaction(session -> runTestIterativePlanFragmenter(join, plan, fullFragmentedPlan, session));
    }

    private Void runTestIterativePlanFragmenter(PlanNode node, Plan plan, SubPlan fullFragmentedPlan, Session session)
    {
        TestingFragmentTracker testingFragmentTracker = new TestingFragmentTracker();
        IterativePlanFragmenter iterativePlanFragmenter = new IterativePlanFragmenter(
                plan,
                testingFragmentTracker::isFragmentFinished,
                metadata,
                new PlanChecker(new FeaturesConfig(), planCheckerProviderManager),
                new PlanNodeIdAllocator(),
                nodePartitioningManager,
                new QueryManagerConfig(),
                session,
                WarningCollector.NOOP,
                false);

        PlanAndFragments nextPlanAndFragments = getNextPlanAndFragments(iterativePlanFragmenter, node);
        assertTrue(nextPlanAndFragments.getRemainingPlan().isPresent());
        assertEquals(nextPlanAndFragments.getReadyFragments().size(), 2);

        // nothing new is ready for execution, you are returned the same plan you sent in
        // and no fragments.
        PlanAndFragments previousPlanAndFragments = nextPlanAndFragments;
        nextPlanAndFragments = getNextPlanAndFragments(iterativePlanFragmenter, previousPlanAndFragments.getRemainingPlan().get());
        assertTrue(nextPlanAndFragments.getReadyFragments().isEmpty());
        assertTrue(nextPlanAndFragments.getRemainingPlan().isPresent());
        assertEquals(previousPlanAndFragments.getRemainingPlan().get(), nextPlanAndFragments.getRemainingPlan().get());

        // finish one fragment
        // still nothing is ready for execution as the join stage has two dependencies
        previousPlanAndFragments = nextPlanAndFragments;
        testingFragmentTracker.addFinishedFragment(new PlanFragmentId(1));

        nextPlanAndFragments = getNextPlanAndFragments(iterativePlanFragmenter, previousPlanAndFragments.getRemainingPlan().get());
        assertEquals(previousPlanAndFragments, nextPlanAndFragments);

        testingFragmentTracker.addFinishedFragment(new PlanFragmentId(2));
        previousPlanAndFragments = nextPlanAndFragments;
        nextPlanAndFragments = getNextPlanAndFragments(iterativePlanFragmenter, previousPlanAndFragments.getRemainingPlan().get());

        // when the root fragment is ready to execute, there should be no remaining plan left
        assertFalse(nextPlanAndFragments.getRemainingPlan().isPresent());
        assertEquals(nextPlanAndFragments.getReadyFragments().size(), 1);

        assertSubPlansEquivalent(nextPlanAndFragments.getReadyFragments().get(0), fullFragmentedPlan);
        return null;
    }

    private void assertSubPlansEquivalent(SubPlan subPlan1, SubPlan subPlan2)
    {
        assertEquals(toCanonicalTestFragment(subPlan1.getFragment()), toCanonicalTestFragment(subPlan2.getFragment()));
        Set<CanonicalTestFragment> subPlan1Children = subPlan1.getChildren().stream()
                .map(child -> toCanonicalTestFragment(child.getFragment()))
                .collect(toImmutableSet());
        Set<CanonicalTestFragment> subPlan2Children = subPlan2.getChildren().stream()
                .map(child -> toCanonicalTestFragment(child.getFragment()))
                .collect(toImmutableSet());

        assertEquals(subPlan1Children, subPlan2Children);
    }

    private TableScanNode tableScan(String id, String... symbols)
    {
        List<VariableReferenceExpression> variables = Arrays.stream(symbols)
                .map(symbol -> new VariableReferenceExpression(Optional.empty(), symbol, BIGINT))
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
                Optional.empty(),
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

    private PlanNode project(String id, PlanNode source, VariableReferenceExpression variable, RowExpression expression)
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
                Optional.empty(),
                new PlanNodeId(id),
                source,
                ImmutableMap.of(new VariableReferenceExpression(Optional.empty(), "count", BIGINT), aggregation),
                singleGroupingSet(source.getOutputVariables()),
                ImmutableList.of(),
                AggregationNode.Step.FINAL,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }

    /**
     * EquiJoinClause is created from symbols in form of:
     * symbol[0] = symbol[1] AND symbol[2] = symbol[3] AND ...
     */
    private JoinNode join(String planNodeId, PlanNode left, PlanNode right, JoinDistributionType distributionType, String... symbols)
    {
        checkArgument(symbols.length % 2 == 0);
        ImmutableList.Builder<EquiJoinClause> criteria = ImmutableList.builder();

        for (int i = 0; i < symbols.length; i += 2) {
            criteria.add(new EquiJoinClause(new VariableReferenceExpression(Optional.empty(), symbols[i], BIGINT), new VariableReferenceExpression(Optional.empty(), symbols[i + 1], BIGINT)));
        }

        return new JoinNode(
                Optional.empty(),
                new PlanNodeId(planNodeId),
                JoinType.INNER,
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

    private SubPlan getFullFragmentedPlan(Plan plan)
    {
        return inTransaction(session -> planFragmenter.createSubPlans(session, plan, false, new PlanNodeIdAllocator(), WarningCollector.NOOP));
    }

    private PlanAndFragments getNextPlanAndFragments(IterativePlanFragmenter iterativePlanFragmenter, PlanNode node)
    {
        return iterativePlanFragmenter.createReadySubPlans(node);
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

    private static class TestingFragmentTracker
    {
        private final Set<PlanFragmentId> finishedFragments = new HashSet<>();

        public void addFinishedFragment(PlanFragmentId id)
        {
            finishedFragments.add(id);
        }

        public boolean isFragmentFinished(PlanFragmentId id)
        {
            return finishedFragments.contains(id);
        }
    }

    static class CanonicalTestFragment
    {
        // it's tricky to compare plans between fragments
        // because the remotes sources will be different
        // just make sure they have the same root node
        // for sanity checking
        private final Class<PlanNode> clazz;
        private final Set<VariableReferenceExpression> variables;
        private final PartitioningHandle partitioning;
        private final List<PlanNodeId> tableScanSchedulingOrder;
        private final List<Type> types;

        // can't compare the remoteSourceNodes themselves
        // because fragment numbering can differ,
        // so just ensure that there are the same number
        private final int numberOfRemoteSourceNodes;
        private final PartitioningScheme partitioningScheme;
        private final StageExecutionDescriptor stageExecutionDescriptor;
        private final boolean outputTableWriterFragment;
        private final Optional<StatsAndCosts> statsAndCosts;

        public CanonicalTestFragment(
                Class<PlanNode> clazz,
                Set<VariableReferenceExpression> variables,
                PartitioningHandle partitioning,
                List<PlanNodeId> tableScanSchedulingOrder,
                List<Type> types,
                int numberOfRemoteSourceNodes,
                PartitioningScheme partitioningScheme,
                StageExecutionDescriptor stageExecutionDescriptor,
                boolean outputTableWriterFragment,
                Optional<StatsAndCosts> statsAndCosts)
        {
            this.clazz = requireNonNull(clazz, "clazz is null");
            this.variables = ImmutableSet.copyOf(requireNonNull(variables, "variables is null"));
            this.partitioning = requireNonNull(partitioning, "partitioning is null");
            this.tableScanSchedulingOrder = ImmutableList.copyOf(requireNonNull(tableScanSchedulingOrder, "tableScanSchedulingOrder is null"));
            this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
            this.numberOfRemoteSourceNodes = numberOfRemoteSourceNodes;
            this.partitioningScheme = requireNonNull(partitioningScheme, "partitioningScheme is null");
            this.stageExecutionDescriptor = requireNonNull(stageExecutionDescriptor, "stageExecutionDescriptor is null");
            this.outputTableWriterFragment = outputTableWriterFragment;
            this.statsAndCosts = requireNonNull(statsAndCosts, "statsAndCosts is null");
        }

        public static CanonicalTestFragment toCanonicalTestFragment(PlanFragment planFragment)
        {
            return new CanonicalTestFragment(
                    (Class<PlanNode>) planFragment.getRoot().getClass(),
                    planFragment.getVariables(),
                    planFragment.getPartitioning(),
                    planFragment.getTableScanSchedulingOrder(),
                    planFragment.getTypes(),
                    planFragment.getRemoteSourceNodes().size(),
                    planFragment.getPartitioningScheme(),
                    planFragment.getStageExecutionDescriptor(),
                    planFragment.isOutputTableWriterFragment(),
                    planFragment.getStatsAndCosts());
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CanonicalTestFragment that = (CanonicalTestFragment) o;
            return numberOfRemoteSourceNodes == that.numberOfRemoteSourceNodes &&
                    outputTableWriterFragment == that.outputTableWriterFragment &&
                    clazz.equals(that.clazz) &&
                    variables.equals(that.variables) &&
                    partitioning.equals(that.partitioning) &&
                    tableScanSchedulingOrder.equals(that.tableScanSchedulingOrder) &&
                    types.equals(that.types) &&
                    partitioningScheme.equals(that.partitioningScheme) &&
                    stageExecutionDescriptor.equals(that.stageExecutionDescriptor) &&
                    statsAndCosts.equals(that.statsAndCosts);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(
                    clazz,
                    variables,
                    partitioning,
                    tableScanSchedulingOrder,
                    types,
                    numberOfRemoteSourceNodes,
                    partitioningScheme,
                    stageExecutionDescriptor,
                    outputTableWriterFragment,
                    statsAndCosts);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("clazz", clazz)
                    .add("variables", variables)
                    .add("partitioning", partitioning)
                    .add("tableScanSchedulingOrder", tableScanSchedulingOrder)
                    .add("types", types)
                    .add("numberOfRemoteSourceNodes", numberOfRemoteSourceNodes)
                    .add("partitioningScheme", partitioningScheme)
                    .add("stageExecutionDescriptor", stageExecutionDescriptor)
                    .add("outputTableWriterFragment", outputTableWriterFragment)
                    .add("statsAndCosts", statsAndCosts)
                    .toString();
        }
    }
}
