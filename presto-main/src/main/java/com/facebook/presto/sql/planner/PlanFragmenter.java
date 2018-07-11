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
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.TableLayout;
import com.facebook.presto.metadata.TableLayout.TablePartitioning;
import com.facebook.presto.spi.connector.ConnectorPartitionHandle;
import com.facebook.presto.spi.connector.ConnectorPartitioningHandle;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.ExplainAnalyzeNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.MetadataDeleteNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.planner.plan.TableFinishNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.isForceSingleNodeOutput;
import static com.facebook.presto.operator.PipelineExecutionStrategy.GROUPED_EXECUTION;
import static com.facebook.presto.operator.PipelineExecutionStrategy.UNGROUPED_EXECUTION;
import static com.facebook.presto.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static com.facebook.presto.sql.planner.SchedulingOrderVisitor.scheduleOrder;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.COORDINATOR_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.in;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Splits a logical plan into fragments that can be shipped and executed on distributed nodes
 */
public class PlanFragmenter
{
    private PlanFragmenter() {}

    public static SubPlan createSubPlans(Session session, Metadata metadata, NodePartitioningManager nodePartitioningManager, Plan plan, boolean forceSingleNode)
    {
        Fragmenter fragmenter = new Fragmenter(session, metadata, plan.getTypes());

        FragmentProperties properties = new FragmentProperties(new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), plan.getRoot().getOutputSymbols()));
        if (forceSingleNode || isForceSingleNodeOutput(session)) {
            properties = properties.setSingleNodeDistribution();
        }
        PlanNode root = SimplePlanRewriter.rewriteWith(fragmenter, plan.getRoot(), properties);

        SubPlan subPlan = fragmenter.buildRootFragment(root, properties);
        subPlan = analyzeGroupedExecution(session, metadata, nodePartitioningManager, subPlan);

        checkState(!isForceSingleNodeOutput(session) || subPlan.getFragment().getPartitioning().isSingleNode(), "Root of PlanFragment is not single node");
        subPlan.sanityCheck();

        return subPlan;
    }

    private static SubPlan analyzeGroupedExecution(Session session, Metadata metadata, NodePartitioningManager nodePartitioningManager, SubPlan subPlan)
    {
        PlanFragment fragment = subPlan.getFragment();
        GroupedExecutionProperties properties = fragment.getRoot().accept(new GroupedExecutionTagger(session, metadata, nodePartitioningManager), null);
        if (properties.isSubTreeUseful()) {
            fragment = fragment.withGroupedExecution(GROUPED_EXECUTION);
        }
        ImmutableList.Builder<SubPlan> result = ImmutableList.builder();
        for (SubPlan child : subPlan.getChildren()) {
            result.add(analyzeGroupedExecution(session, metadata, nodePartitioningManager, child));
        }
        return new SubPlan(fragment, result.build());
    }

    private static class Fragmenter
            extends SimplePlanRewriter<FragmentProperties>
    {
        private static final int ROOT_FRAGMENT_ID = 0;

        private final Session session;
        private final Metadata metadata;
        private final TypeProvider types;
        private int nextFragmentId = ROOT_FRAGMENT_ID + 1;

        public Fragmenter(Session session, Metadata metadata, TypeProvider types)
        {
            this.session = requireNonNull(session, "session is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.types = requireNonNull(types, "types is null");
        }

        public SubPlan buildRootFragment(PlanNode root, FragmentProperties properties)
        {
            return buildFragment(root, properties, new PlanFragmentId(String.valueOf(ROOT_FRAGMENT_ID)));
        }

        private PlanFragmentId nextFragmentId()
        {
            return new PlanFragmentId(String.valueOf(nextFragmentId++));
        }

        private SubPlan buildFragment(PlanNode root, FragmentProperties properties, PlanFragmentId fragmentId)
        {
            Set<Symbol> dependencies = SymbolsExtractor.extractOutputSymbols(root);

            List<PlanNodeId> schedulingOrder = scheduleOrder(root);
            boolean equals = properties.getPartitionedSources().equals(ImmutableSet.copyOf(schedulingOrder));
            checkArgument(equals, "Expected scheduling order (%s) to contain an entry for all partitioned sources (%s)", schedulingOrder, properties.getPartitionedSources());

            PlanFragment fragment = new PlanFragment(
                    fragmentId,
                    root,
                    Maps.filterKeys(types.allTypes(), in(dependencies)),
                    properties.getPartitioningHandle(),
                    schedulingOrder,
                    properties.getPartitioningScheme(),
                    UNGROUPED_EXECUTION);

            return new SubPlan(fragment, properties.getChildren());
        }

        @Override
        public PlanNode visitOutput(OutputNode node, RewriteContext<FragmentProperties> context)
        {
            if (isForceSingleNodeOutput(session)) {
                context.get().setSingleNodeDistribution();
            }

            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitExplainAnalyze(ExplainAnalyzeNode node, RewriteContext<FragmentProperties> context)
        {
            context.get().setCoordinatorOnlyDistribution();
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitTableFinish(TableFinishNode node, RewriteContext<FragmentProperties> context)
        {
            context.get().setCoordinatorOnlyDistribution();
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitMetadataDelete(MetadataDeleteNode node, RewriteContext<FragmentProperties> context)
        {
            context.get().setCoordinatorOnlyDistribution();
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, RewriteContext<FragmentProperties> context)
        {
            PartitioningHandle partitioning = node.getLayout()
                    .map(layout -> metadata.getLayout(session, layout))
                    .flatMap(TableLayout::getTablePartitioning)
                    .map(TablePartitioning::getPartitioningHandle)
                    .orElse(SOURCE_DISTRIBUTION);

            context.get().addSourceDistribution(node.getId(), partitioning);
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
            if (exchange.getScope() != REMOTE) {
                return context.defaultRewrite(exchange, context.get());
            }

            PartitioningScheme partitioningScheme = exchange.getPartitioningScheme();

            if (exchange.getType() == ExchangeNode.Type.GATHER) {
                context.get().setSingleNodeDistribution();
            }
            else if (exchange.getType() == ExchangeNode.Type.REPARTITION) {
                context.get().setDistribution(partitioningScheme.getPartitioning().getHandle());
            }

            ImmutableList.Builder<SubPlan> builder = ImmutableList.builder();
            for (int sourceIndex = 0; sourceIndex < exchange.getSources().size(); sourceIndex++) {
                FragmentProperties childProperties = new FragmentProperties(partitioningScheme.translateOutputLayout(exchange.getInputs().get(sourceIndex)));
                builder.add(buildSubPlan(exchange.getSources().get(sourceIndex), childProperties, context));
            }

            List<SubPlan> children = builder.build();
            context.get().addChildren(children);

            List<PlanFragmentId> childrenIds = children.stream()
                    .map(SubPlan::getFragment)
                    .map(PlanFragment::getId)
                    .collect(toImmutableList());

            return new RemoteSourceNode(exchange.getId(), childrenIds, exchange.getOutputSymbols(), exchange.getOrderingScheme());
        }

        private SubPlan buildSubPlan(PlanNode node, FragmentProperties properties, RewriteContext<FragmentProperties> context)
        {
            PlanFragmentId planFragmentId = nextFragmentId();
            PlanNode child = context.rewrite(node, properties);
            return buildFragment(child, properties, planFragmentId);
        }
    }

    private static class FragmentProperties
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

        public FragmentProperties setDistribution(PartitioningHandle distribution)
        {
            if (partitioningHandle.isPresent()) {
                chooseDistribution(distribution);
                return this;
            }

            partitioningHandle = Optional.of(distribution);
            return this;
        }

        private void chooseDistribution(PartitioningHandle distribution)
        {
            checkState(partitioningHandle.isPresent(), "No partitioning to choose from");

            if (partitioningHandle.get().equals(distribution) ||
                    partitioningHandle.get().isSingleNode() ||
                    isCompatibleSystemPartitioning(distribution)) {
                return;
            }
            if (partitioningHandle.get().equals(SOURCE_DISTRIBUTION)) {
                partitioningHandle = Optional.of(distribution);
                return;
            }
            throw new IllegalStateException(format(
                    "Cannot set distribution to %s. Already set to %s",
                    distribution,
                    partitioningHandle));
        }

        private boolean isCompatibleSystemPartitioning(PartitioningHandle distribution)
        {
            ConnectorPartitioningHandle currentHandle = partitioningHandle.get().getConnectorHandle();
            ConnectorPartitioningHandle distributionHandle = distribution.getConnectorHandle();
            if ((currentHandle instanceof SystemPartitioningHandle) &&
                    (distributionHandle instanceof SystemPartitioningHandle)) {
                return ((SystemPartitioningHandle) currentHandle).getPartitioning() ==
                        ((SystemPartitioningHandle) distributionHandle).getPartitioning();
            }
            return false;
        }

        public FragmentProperties setCoordinatorOnlyDistribution()
        {
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

        public FragmentProperties addSourceDistribution(PlanNodeId source, PartitioningHandle distribution)
        {
            requireNonNull(source, "source is null");
            requireNonNull(distribution, "distribution is null");

            partitionedSources.add(source);

            if (partitioningHandle.isPresent()) {
                PartitioningHandle currentPartitioning = partitioningHandle.get();
                if (!currentPartitioning.equals(distribution)) {
                    // If already system SINGLE or COORDINATOR_ONLY, leave it as is (this is for single-node execution)
                    checkState(
                            currentPartitioning.equals(SINGLE_DISTRIBUTION) || currentPartitioning.equals(COORDINATOR_DISTRIBUTION),
                            "Cannot overwrite distribution with %s (currently set to %s)",
                            distribution,
                            currentPartitioning);
                    return this;
                }
            }
            partitioningHandle = Optional.of(distribution);

            return this;
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

    private static class GroupedExecutionTagger
            extends PlanVisitor<GroupedExecutionProperties, Void>
    {
        private final Session session;
        private final Metadata metadata;
        private final NodePartitioningManager nodePartitioningManager;
        private final boolean groupedExecutionForAggregation;

        public GroupedExecutionTagger(Session session, Metadata metadata, NodePartitioningManager nodePartitioningManager)
        {
            this.session = requireNonNull(session, "session is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.nodePartitioningManager = requireNonNull(nodePartitioningManager, "nodePartitioningManager is null");
            this.groupedExecutionForAggregation = SystemSessionProperties.isGroupedExecutionForJoinEnabled(session);
        }

        @Override
        protected GroupedExecutionProperties visitPlan(PlanNode node, Void context)
        {
            if (node.getSources().isEmpty()) {
                return new GroupedExecutionProperties(false, false);
            }
            return processChildren(node);
        }

        @Override
        public GroupedExecutionProperties visitJoin(JoinNode node, Void context)
        {
            GroupedExecutionProperties left = node.getLeft().accept(this, null);
            GroupedExecutionProperties right = node.getRight().accept(this, null);

            if (!node.getDistributionType().isPresent()) {
                // This is possible when the optimizers is invoked with `forceSingleNode` set to true.
                return new GroupedExecutionProperties(false, false);
            }

            switch (node.getDistributionType().get()) {
                case REPLICATED:
                    // Broadcast join maintains partitioning for the left side.
                    // Right side of a broadcast is not capable of grouped execution because it always comes from a remote exchange.
                    checkState(!right.currentNodeCapable);
                    return left;
                case PARTITIONED:
                    if (left.currentNodeCapable && right.currentNodeCapable) {
                        return new GroupedExecutionProperties(true, true);
                    }
                    // right.subTreeUseful && !left.currentNodeCapable:
                    //   It's not particularly helpful to do grouped execution on the right side
                    //   because the benefit is likely cancelled out due to required buffering for hash build.
                    //   In theory, it could still be helpful (e.g. when the underlying aggregation's intermediate group state maybe larger than aggregation output).
                    //   However, this is not currently implemented. JoinBridgeDataManager need to support such a lifecycle.
                    // !right.currentNodeCapable:
                    //   The build/right side needs to buffer fully for this JOIN, but the probe/left side will still stream through.
                    //   As a result, there is no reason to change currentNodeCapable or subTreeUseful to false.
                    //
                    return left;
                default:
                    throw new UnsupportedOperationException("Unknown distribution type: " + node.getDistributionType());
            }
        }

        @Override
        public GroupedExecutionProperties visitAggregation(AggregationNode node, Void context)
        {
            GroupedExecutionProperties properties = processChildren(node);
            if (groupedExecutionForAggregation && properties.isCurrentNodeCapable()) {
                switch (node.getStep()) {
                    case SINGLE:
                    case FINAL:
                        return new GroupedExecutionProperties(true, true);
                    case PARTIAL:
                    case INTERMEDIATE:
                        return new GroupedExecutionProperties(true, properties.isSubTreeUseful());
                }
            }
            return new GroupedExecutionProperties(false, false);
        }

        @Override
        public GroupedExecutionProperties visitTableScan(TableScanNode node, Void context)
        {
            Optional<TablePartitioning> tablePartitioning = metadata.getLayout(session, node.getLayout().get()).getTablePartitioning();
            if (!tablePartitioning.isPresent()) {
                return new GroupedExecutionProperties(false, false);
            }
            List<ConnectorPartitionHandle> partitionHandles = nodePartitioningManager.listPartitionHandles(session, tablePartitioning.get().getPartitioningHandle());
            return new GroupedExecutionProperties(
                    !ImmutableList.of(NOT_PARTITIONED).equals(partitionHandles),
                    false);
        }

        private GroupedExecutionProperties processChildren(PlanNode node)
        {
            // Each fragment has a partitioning handle, which is derived from leaf nodes in the fragment.
            // Leaf nodes with different partitioning handle are not allowed to share a single fragment
            // (except for special cases as detailed in addSourceDistribution).
            // As a result, it is not necessary to check the compatibility between node.getSources because
            // they are guaranteed to be compatible.

            // * If any child is "not capable", return "not capable"
            // * When all children are capable ("capable and useful" or "capable but not useful")
            //   * if any child is "capable and useful", return "capable and useful"
            //   * if no children is "capable and useful", return "capable but not useful"
            boolean anyUseful = false;
            for (PlanNode source : node.getSources()) {
                GroupedExecutionProperties properties = source.accept(this, null);
                if (!properties.isCurrentNodeCapable()) {
                    return new GroupedExecutionProperties(false, false);
                }
                anyUseful |= properties.isSubTreeUseful();
            }
            return new GroupedExecutionProperties(true, anyUseful);
        }
    }

    private static class GroupedExecutionProperties
    {
        // currentNodeCapable:
        //   Whether grouped execution is possible with the current node.
        //   For example, a table scan is capable iff it supports addressable split discovery.
        // subTreeUseful:
        //   Whether grouped execution is beneficial in the current node, or any node below it.
        //   For example, a JOIN can benefit from grouped execution because build can be flushed early, reducing peak memory requirement.
        //
        // In the current implementation, subTreeUseful implies currentNodeCapable.
        // In theory, this doesn't have to be the case. Take an example where a GROUP BY feeds into the build side of a JOIN.
        // Even if JOIN cannot take advantage of grouped execution, it could still be beneficial to execute the GROUP BY with grouped execution
        // (e.g. when the underlying aggregation's intermediate group state may be larger than aggregation output).

        private final boolean currentNodeCapable;
        private final boolean subTreeUseful;

        public GroupedExecutionProperties(boolean currentNodeCapable, boolean subTreeUseful)
        {
            this.currentNodeCapable = currentNodeCapable;
            this.subTreeUseful = subTreeUseful;
            // Verify that `subTreeUseful` implies `currentNodeCapable`
            checkArgument(!subTreeUseful || currentNodeCapable);
        }

        public boolean isCurrentNodeCapable()
        {
            return currentNodeCapable;
        }

        public boolean isSubTreeUseful()
        {
            return subTreeUseful;
        }
    }
}
