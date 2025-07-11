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

import com.facebook.presto.Session;
import com.facebook.presto.cost.StatsAndCosts;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.PartitioningMetadata;
import com.facebook.presto.metadata.TableLayout;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.MetadataDeleteNode;
import com.facebook.presto.spi.plan.OutputNode;
import com.facebook.presto.spi.plan.Partitioning;
import com.facebook.presto.spi.plan.PartitioningHandle;
import com.facebook.presto.spi.plan.PartitioningScheme;
import com.facebook.presto.spi.plan.PlanFragmentId;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.StageExecutionDescriptor;
import com.facebook.presto.spi.plan.TableFinishNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.TableWriterNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.ExplainAnalyzeNode;
import com.facebook.presto.sql.planner.plan.MergeProcessorNode;
import com.facebook.presto.sql.planner.plan.MergeWriterNode;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.facebook.presto.sql.planner.plan.SequenceNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.planner.plan.StatisticsWriterNode;
import com.facebook.presto.sql.planner.sanity.PlanChecker;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.isForceSingleNodeOutput;
import static com.facebook.presto.SystemSessionProperties.isSingleNodeExecutionEnabled;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.sql.TemporaryTableUtil.assignPartitioningVariables;
import static com.facebook.presto.sql.TemporaryTableUtil.assignTemporaryTableColumnNames;
import static com.facebook.presto.sql.TemporaryTableUtil.createTemporaryTableScan;
import static com.facebook.presto.sql.TemporaryTableUtil.createTemporaryTableWriteWithExchanges;
import static com.facebook.presto.sql.planner.BasePlanFragmenter.FragmentProperties;
import static com.facebook.presto.sql.planner.PlanFragmenterUtils.isCoordinatorOnlyDistribution;
import static com.facebook.presto.sql.planner.SchedulingOrderVisitor.scheduleOrder;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.COORDINATOR_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.isCompatibleSystemPartitioning;
import static com.facebook.presto.sql.planner.VariablesExtractor.extractOutputVariables;
import static com.facebook.presto.sql.planner.optimizations.PartitioningUtils.translateOutputLayout;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.REMOTE_MATERIALIZED;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.REMOTE_STREAMING;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static com.facebook.presto.sql.planner.planPrinter.PlanPrinter.jsonFragmentPlan;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Main rewriter that creates plan fragments
 */
public abstract class BasePlanFragmenter
        extends SimplePlanRewriter<FragmentProperties>
{
    private final Session session;
    private final Metadata metadata;
    private final PlanNodeIdAllocator idAllocator;
    private final VariableAllocator variableAllocator;
    private final StatsAndCosts statsAndCosts;
    private final PlanChecker planChecker;
    private final WarningCollector warningCollector;
    private final Set<PlanNodeId> outputTableWriterNodeIds;
    private final StatisticsAggregationPlanner statisticsAggregationPlanner;

    public BasePlanFragmenter(
            Session session,
            Metadata metadata,
            StatsAndCosts statsAndCosts,
            PlanChecker planChecker,
            WarningCollector warningCollector,
            PlanNodeIdAllocator idAllocator,
            VariableAllocator variableAllocator,
            Set<PlanNodeId> outputTableWriterNodeIds)
    {
        this.session = requireNonNull(session, "session is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.statsAndCosts = requireNonNull(statsAndCosts, "statsAndCosts is null");
        this.planChecker = requireNonNull(planChecker, "planChecker is null");
        this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
        this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
        this.variableAllocator = requireNonNull(variableAllocator, "variableAllocator is null");
        this.outputTableWriterNodeIds = ImmutableSet.copyOf(requireNonNull(outputTableWriterNodeIds, "outputTableWriterNodeIds is null"));
        this.statisticsAggregationPlanner = new StatisticsAggregationPlanner(variableAllocator, metadata.getFunctionAndTypeManager(), session);
    }

    public SubPlan buildRootFragment(PlanNode root, FragmentProperties properties)
    {
        return buildFragment(root, properties, new PlanFragmentId(PlanFragmenterUtils.ROOT_FRAGMENT_ID));
    }

    public abstract PlanFragmentId nextFragmentId();

    private SubPlan buildFragment(PlanNode root, FragmentProperties properties, PlanFragmentId fragmentId)
    {
        List<PlanNodeId> schedulingOrder = scheduleOrder(root);
        Preconditions.checkArgument(
                properties.getPartitionedSources().equals(ImmutableSet.copyOf(schedulingOrder)),
                "Expected scheduling order (%s) to contain an entry for all partitioned sources (%s)",
                schedulingOrder,
                properties.getPartitionedSources());

        Set<VariableReferenceExpression> fragmentVariableTypes = extractOutputVariables(root);
        Set<PlanNodeId> tableWriterNodeIds = PlanFragmenterUtils.getTableWriterNodeIds(root);
        boolean outputTableWriterFragment = tableWriterNodeIds.stream().anyMatch(outputTableWriterNodeIds::contains);
        if (outputTableWriterFragment) {
            verify(
                    outputTableWriterNodeIds.containsAll(tableWriterNodeIds),
                    "outputTableWriterNodeIds %s must include either all or none of tableWriterNodeIds %s",
                    outputTableWriterNodeIds,
                    tableWriterNodeIds);
        }

        PlanFragment fragment = new PlanFragment(
                fragmentId,
                root,
                fragmentVariableTypes,
                properties.getPartitioningHandle(),
                schedulingOrder,
                properties.getPartitioningScheme(),
                StageExecutionDescriptor.ungroupedExecution(),
                outputTableWriterFragment,
                Optional.of(statsAndCosts.getForSubplan(root)),
                Optional.of(jsonFragmentPlan(root, fragmentVariableTypes, statsAndCosts.getForSubplan(root), metadata.getFunctionAndTypeManager(), session)));

        planChecker.validatePlanFragment(fragment, session, metadata, warningCollector);

        return new SubPlan(fragment, properties.getChildren());
    }

    @Override
    public PlanNode visitOutput(OutputNode node, RewriteContext<FragmentProperties> context)
    {
        if (isForceSingleNodeOutput(session)) {
            context.get().setSingleNodeDistribution();
        }

        if (isSingleNodeExecutionEnabled(session)) {
            context.get().setSingleNodeDistribution();
        }

        return context.defaultRewrite(node, context.get());
    }

    @Override
    public PlanNode visitExplainAnalyze(ExplainAnalyzeNode node, RewriteContext<FragmentProperties> context)
    {
        context.get().setCoordinatorOnlyDistribution(node);
        return context.defaultRewrite(node, context.get());
    }

    @Override
    public PlanNode visitStatisticsWriterNode(StatisticsWriterNode node, RewriteContext<FragmentProperties> context)
    {
        context.get().setCoordinatorOnlyDistribution(node);
        return context.defaultRewrite(node, context.get());
    }

    @Override
    public PlanNode visitTableFinish(TableFinishNode node, RewriteContext<FragmentProperties> context)
    {
        context.get().setCoordinatorOnlyDistribution(node);
        return context.defaultRewrite(node, context.get());
    }

    @Override
    public PlanNode visitSequence(SequenceNode node, RewriteContext<FragmentProperties> context)
    {
        // To ensure that the execution order is maintained, we use an independent dependency graph.
        // This approach creates subgraphs sequentially, enhancing control over the execution flow. However, there are optimization opportunities:
        // 1. Can consider blocking only the CTEConsumer stages that are in a reading state.
        //    This approach sounds good on paper may not be ideal as it can block the entire query, leading to resource wastage since no progress can be made until the writing operations are complete.
        // 2. ToDo: Another improvement will be to schedule the execution of subgraphs based on their order in the overall execution plan instead of a topological sorting done here
        //  but that needs change to plan section framework for it to be able to handle the same child planSection.
        List<List<PlanNode>> independentCteProducerSubgraphs = node.getIndependentCteProducers();
        for (List<PlanNode> cteProducerSubgraph : independentCteProducerSubgraphs) {
            int cteProducerCount = cteProducerSubgraph.size();
            checkArgument(cteProducerCount >= 1, "CteProducer subgraph has 0 CTE producers");
            PlanNode source = cteProducerSubgraph.get(cteProducerCount - 1);
            FragmentProperties childProperties = new FragmentProperties(new PartitioningScheme(
                    Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()),
                    source.getOutputVariables()));
            SubPlan lastSubPlan = buildSubPlan(source, childProperties, context);
            for (int sourceIndex = cteProducerCount - 2; sourceIndex >= 0; sourceIndex--) {
                source = cteProducerSubgraph.get(sourceIndex);
                childProperties = new FragmentProperties(new PartitioningScheme(
                        Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()),
                        source.getOutputVariables()));
                childProperties.addChildren(ImmutableList.of(lastSubPlan));
                lastSubPlan = buildSubPlan(source, childProperties, context);
            }
            // This makes sure that the sectionedPlans generated in com.facebook.presto.execution.scheduler.StreamingPlanSection
            // are independent and thus could be scheduled concurrently
            context.get().addChildren(ImmutableList.of(lastSubPlan));
        }
        return node.getPrimarySource().accept(this, context);
    }

    @Override
    public PlanNode visitMetadataDelete(MetadataDeleteNode node, RewriteContext<FragmentProperties> context)
    {
        context.get().setCoordinatorOnlyDistribution(node);
        return context.defaultRewrite(node, context.get());
    }

    @Override
    public PlanNode visitTableScan(TableScanNode node, RewriteContext<FragmentProperties> context)
    {
        PartitioningHandle partitioning = metadata.getLayout(session, node.getTable())
                .getTablePartitioning()
                .map(TableLayout.TablePartitioning::getPartitioningHandle)
                .orElse(SOURCE_DISTRIBUTION);
        context.get().addSourceDistribution(node.getId(), partitioning, metadata, session);
        return context.defaultRewrite(node, context.get());
    }

    @Override
    public PlanNode visitTableWriter(TableWriterNode node, RewriteContext<FragmentProperties> context)
    {
        if (node.isSingleWriterPerPartitionRequired()) {
            context.get().setDistribution(node.getTablePartitioningScheme().get().getPartitioning().getHandle(), metadata, session);
        }
        return context.defaultRewrite(node, context.get());
    }

    @Override
    public PlanNode visitMergeWriter(MergeWriterNode node, RewriteContext<FragmentProperties> context)
    {
        if (node.getPartitioningScheme().isPresent()) {
            context.get().setDistribution(node.getPartitioningScheme().get().getPartitioning().getHandle(), metadata, session);
        }
        return context.defaultRewrite(node, context.get());
    }

    @Override
    public PlanNode visitMergeProcessor(MergeProcessorNode node, RewriteContext<FragmentProperties> context)
    {
        return context.defaultRewrite(node, context.get());
    }

    @Override
    public PlanNode visitValues(ValuesNode node, RewriteContext<FragmentProperties> context)
    {
        context.get().setSingleNodeDistribution();
        return context.defaultRewrite(node, context.get());
    }

    @Override
    public PlanNode visitExchange(ExchangeNode exchange, RewriteContext<FragmentProperties> context)
    {
        if (isSingleNodeExecutionEnabled(session)) {
            context.get().setSingleNodeDistribution();
        }

        switch (exchange.getScope()) {
            case LOCAL:
                return context.defaultRewrite(exchange, context.get());
            case REMOTE_STREAMING:
                return createRemoteStreamingExchange(exchange, context);
            case REMOTE_MATERIALIZED:
                return createRemoteMaterializedExchange(exchange, context);
            default:
                throw new IllegalArgumentException("Unexpected exchange scope: " + exchange.getScope());
        }
    }

    private PlanNode createRemoteStreamingExchange(ExchangeNode exchange, RewriteContext<FragmentProperties> context)
    {
        checkArgument(exchange.getScope() == REMOTE_STREAMING, "Unexpected exchange scope: %s", exchange.getScope());
        checkArgument(!exchange.getPartitioningScheme().isScaleWriters(), "task scaling for partitioned tables is not yet supported");

        PartitioningScheme partitioningScheme = exchange.getPartitioningScheme();

        setDistributionForExchange(exchange.getType(), partitioningScheme, context);

        ImmutableList.Builder<SubPlan> builder = ImmutableList.builder();
        for (int sourceIndex = 0; sourceIndex < exchange.getSources().size(); sourceIndex++) {
            FragmentProperties childProperties = new FragmentProperties(translateOutputLayout(partitioningScheme, exchange.getInputs().get(sourceIndex)));
            builder.add(buildSubPlan(exchange.getSources().get(sourceIndex), childProperties, context));
        }

        List<SubPlan> children = builder.build();
        context.get().addChildren(children);

        List<PlanFragmentId> childrenIds = children.stream()
                .map(SubPlan::getFragment)
                .map(PlanFragment::getId)
                .collect(toImmutableList());

        return new RemoteSourceNode(
                exchange.getSourceLocation(),
                exchange.getId(),
                exchange.getStatsEquivalentPlanNode(),
                childrenIds,
                exchange.getOutputVariables(),
                exchange.isEnsureSourceOrdering(),
                exchange.getOrderingScheme(),
                exchange.getType(),
                exchange.getPartitioningScheme().getEncoding());
    }

    protected void setDistributionForExchange(ExchangeNode.Type exchangeType, PartitioningScheme partitioningScheme, RewriteContext<FragmentProperties> context)
    {
        if (exchangeType == ExchangeNode.Type.GATHER) {
            context.get().setSingleNodeDistribution();
        }
        else if (exchangeType == ExchangeNode.Type.REPARTITION) {
            context.get().setDistribution(partitioningScheme.getPartitioning().getHandle(), metadata, session);
        }
    }

    private PlanNode createRemoteMaterializedExchange(ExchangeNode exchange, RewriteContext<FragmentProperties> context)
    {
        checkArgument(exchange.getType() == REPARTITION, "Unexpected exchange type: %s", exchange.getType());
        checkArgument(exchange.getScope() == REMOTE_MATERIALIZED, "Unexpected exchange scope: %s", exchange.getScope());

        checkArgument(!exchange.getPartitioningScheme().isScaleWriters(), "task scaling for partitioned tables is not yet supported");

        PartitioningScheme partitioningScheme = exchange.getPartitioningScheme();

        PartitioningHandle partitioningHandle = partitioningScheme.getPartitioning().getHandle();
        ConnectorId connectorId = partitioningHandle.getConnectorId()
                .orElseThrow(() -> new PrestoException(
                        NOT_SUPPORTED,
                        "The \"partitioning_provider_catalog\" session property must be set to enable the exchanges materialization. " +
                                "The catalog must support providing a custom partitioning and storing temporary tables."));

        Partitioning partitioning = partitioningScheme.getPartitioning();
        PartitioningVariableAssignments partitioningVariableAssignments = assignPartitioningVariables(variableAllocator, partitioning);
        Map<VariableReferenceExpression, ColumnMetadata> variableToColumnMap = assignTemporaryTableColumnNames(exchange.getOutputVariables(), partitioningVariableAssignments.getConstants().keySet());
        List<VariableReferenceExpression> partitioningVariables = partitioningVariableAssignments.getVariables();
        List<String> partitionColumns = partitioningVariables.stream()
                .map(variable -> variableToColumnMap.get(variable).getName())
                .collect(toImmutableList());
        PartitioningMetadata partitioningMetadata = new PartitioningMetadata(partitioningHandle, partitionColumns);

        TableHandle temporaryTableHandle;

        try {
            temporaryTableHandle = metadata.createTemporaryTable(
                    session,
                    connectorId.getCatalogName(),
                    ImmutableList.copyOf(variableToColumnMap.values()),
                    Optional.of(partitioningMetadata));
        }
        catch (PrestoException e) {
            if (e.getErrorCode().equals(NOT_SUPPORTED.toErrorCode())) {
                throw new PrestoException(
                        NOT_SUPPORTED,
                        format("Temporary table cannot be created in catalog \"%s\": %s", connectorId.getCatalogName(), e.getMessage()),
                        e);
            }
            throw e;
        }

        TableScanNode scan = createTemporaryTableScan(
                metadata,
                session,
                idAllocator,
                exchange.getSourceLocation(),
                temporaryTableHandle,
                exchange.getOutputVariables(),
                variableToColumnMap,
                Optional.of(partitioningMetadata),
                Optional.empty());

        checkArgument(
                !exchange.getPartitioningScheme().isReplicateNullsAndAny(),
                "materialized remote exchange is not supported when replicateNullsAndAny is needed");
        TableFinishNode write = createTemporaryTableWriteWithExchanges(
                metadata,
                session,
                idAllocator,
                variableAllocator,
                statisticsAggregationPlanner,
                scan.getSourceLocation(),
                temporaryTableHandle,
                variableToColumnMap,
                exchange.getOutputVariables(),
                exchange.getInputs(),
                exchange.getSources(),
                partitioningVariableAssignments.getConstants(),
                partitioningMetadata);

        FragmentProperties writeProperties = new FragmentProperties(new PartitioningScheme(
                Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()),
                write.getOutputVariables()));
        writeProperties.setCoordinatorOnlyDistribution(write);

        List<SubPlan> children = ImmutableList.of(buildSubPlan(write, writeProperties, context));
        context.get().addChildren(children);

        return visitTableScan(scan, context);
    }

    private SubPlan buildSubPlan(PlanNode node, FragmentProperties properties, RewriteContext<FragmentProperties> context)
    {
        PlanFragmentId planFragmentId = nextFragmentId();
        PlanNode child = context.rewrite(node, properties);
        return buildFragment(child, properties, planFragmentId);
    }

    public static class FragmentProperties
    {
        private final List<SubPlan> children = new ArrayList<>();

        private final PartitioningScheme partitioningScheme;

        private Optional<PartitioningHandle> partitioningHandle = Optional.empty();
        private final Set<PlanNodeId> partitionedSources = new HashSet<>();

        public FragmentProperties(PartitioningScheme partitioningScheme)
        {
            this.partitioningScheme = partitioningScheme;
        }

        public List<SubPlan> getChildren()
        {
            return children;
        }

        public FragmentProperties setSingleNodeDistribution()
        {
            if (partitioningHandle.isPresent() && partitioningHandle.get().isSingleNode()) {
                // already single node distribution
                return this;
            }

            checkState(!partitioningHandle.isPresent(),
                    "Cannot overwrite partitioning with %s (currently set to %s)",
                    SINGLE_DISTRIBUTION,
                    partitioningHandle);

            partitioningHandle = Optional.of(SINGLE_DISTRIBUTION);

            return this;
        }

        public FragmentProperties setDistribution(PartitioningHandle distribution, Metadata metadata, Session session)
        {
            if (!partitioningHandle.isPresent()) {
                partitioningHandle = Optional.of(distribution);
                return this;
            }

            PartitioningHandle currentPartitioning = this.partitioningHandle.get();

            if (isCompatibleSystemPartitioning(currentPartitioning, distribution)) {
                return this;
            }

            if (currentPartitioning.equals(SOURCE_DISTRIBUTION)) {
                this.partitioningHandle = Optional.of(distribution);
                return this;
            }

            // If already system SINGLE or COORDINATOR_ONLY, leave it as is (this is for single-node execution)
            if (currentPartitioning.isSingleNode()) {
                return this;
            }

            if (currentPartitioning.equals(distribution)) {
                return this;
            }

            Optional<PartitioningHandle> commonPartitioning = metadata.getCommonPartitioning(session, currentPartitioning, distribution);
            if (commonPartitioning.isPresent()) {
                partitioningHandle = commonPartitioning;
                return this;
            }

            if (metadata.isRefinedPartitioningOver(session, distribution, currentPartitioning)) {
                return this;
            }

            throw new IllegalStateException(format(
                    "Cannot set distribution to %s. Already set to %s",
                    distribution,
                    this.partitioningHandle));
        }

        public FragmentProperties setCoordinatorOnlyDistribution(PlanNode node)
        {
            checkArgument(isCoordinatorOnlyDistribution(node),
                    "PlanNode type %s doesn't support COORDINATOR_DISTRIBUTION", node.getClass());

            if (partitioningHandle.isPresent() && partitioningHandle.get().isCoordinatorOnly()) {
                // already single node distribution
                return this;
            }

            // only system SINGLE can be upgraded to COORDINATOR_ONLY
            checkState(!partitioningHandle.isPresent() || partitioningHandle.get().equals(SINGLE_DISTRIBUTION),
                    "Cannot overwrite partitioning with %s (currently set to %s)",
                    COORDINATOR_DISTRIBUTION,
                    partitioningHandle);

            partitioningHandle = Optional.of(COORDINATOR_DISTRIBUTION);

            return this;
        }

        public FragmentProperties addSourceDistribution(PlanNodeId source, PartitioningHandle distribution, Metadata metadata, Session session)
        {
            requireNonNull(source, "source is null");
            requireNonNull(distribution, "distribution is null");

            partitionedSources.add(source);
            return setDistribution(distribution, metadata, session);
        }

        public FragmentProperties addChildren(List<SubPlan> children)
        {
            this.children.addAll(children);

            return this;
        }

        public PartitioningScheme getPartitioningScheme()
        {
            return partitioningScheme;
        }

        public PartitioningHandle getPartitioningHandle()
        {
            return partitioningHandle.get();
        }

        public Set<PlanNodeId> getPartitionedSources()
        {
            return partitionedSources;
        }
    }

    public static class PartitioningVariableAssignments
    {
        private final List<VariableReferenceExpression> variables;
        private final Map<VariableReferenceExpression, RowExpression> constants;

        public PartitioningVariableAssignments(List<VariableReferenceExpression> variables, Map<VariableReferenceExpression, RowExpression> constants)
        {
            this.variables = ImmutableList.copyOf(requireNonNull(variables, "variables is null"));
            this.constants = ImmutableMap.copyOf(requireNonNull(constants, "constants is null"));
            checkArgument(
                    ImmutableSet.copyOf(variables).containsAll(constants.keySet()),
                    "partitioningVariables list must contain all partitioning variables including constants");
        }

        public List<VariableReferenceExpression> getVariables()
        {
            return variables;
        }

        public Map<VariableReferenceExpression, RowExpression> getConstants()
        {
            return constants;
        }
    }
}
