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
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.TableLayout;
import com.facebook.presto.metadata.TableLayout.NodePartitioning;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.ExplainAnalyzeNode;
import com.facebook.presto.sql.planner.plan.IndexJoinNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.MetadataDeleteNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.planner.plan.TableFinishNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import static com.facebook.presto.sql.planner.SystemPartitioningHandle.COORDINATOR_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.in;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * Splits a logical plan into fragments that can be shipped and executed on distributed nodes
 */
public class PlanFragmenter
{
    private PlanFragmenter()
    {
    }

    public static SubPlan createSubPlans(Session session, Metadata metadata, Plan plan)
    {
        Fragmenter fragmenter = new Fragmenter(session, metadata, plan.getTypes());

        FragmentProperties properties = new FragmentProperties(new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), plan.getRoot().getOutputSymbols()))
                .setSingleNodeDistribution();
        PlanNode root = SimplePlanRewriter.rewriteWith(fragmenter, plan.getRoot(), properties);

        SubPlan result = fragmenter.buildRootFragment(root, properties);
        checkState(result.getFragment().getPartitioning().isSingleNode(), "Root of PlanFragment is not single node");
        result.sanityCheck();

        return result;
    }

    private static class Fragmenter
            extends SimplePlanRewriter<FragmentProperties>
    {
        private static final int ROOT_FRAGMENT_ID = 0;

        private final Session session;
        private final Metadata metadata;
        private final Map<Symbol, Type> types;
        private int nextFragmentId = ROOT_FRAGMENT_ID + 1;

        public Fragmenter(Session session, Metadata metadata, Map<Symbol, Type> types)
        {
            this.session = requireNonNull(session, "session is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.types = ImmutableMap.copyOf(requireNonNull(types, "types is null"));
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
            Set<Symbol> dependencies = SymbolExtractor.extract(root);

            List<PlanNodeId> schedulingOrder = new SchedulingOrderVisitor().getSchedulingOrder(root);
            boolean equals = properties.getPartitionedSources().equals(ImmutableSet.copyOf(schedulingOrder));
            checkArgument(equals, "Expected scheduling order (%s) to contain an entry for all partitioned sources (%s)", schedulingOrder, properties.getPartitionedSources());

            PlanFragment fragment = new PlanFragment(
                    fragmentId,
                    root,
                    Maps.filterKeys(types, in(dependencies)),
                    properties.getPartitioningHandle(),
                    schedulingOrder,
                    properties.getPartitioningScheme());

            return new SubPlan(fragment, properties.getChildren());
        }

        @Override
        public PlanNode visitOutput(OutputNode node, RewriteContext<FragmentProperties> context)
        {
            context.get().setSingleNodeDistribution(); // TODO: add support for distributed output

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
                    .flatMap(TableLayout::getNodePartitioning)
                    .map(NodePartitioning::getPartitioningHandle)
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

            ImmutableList.Builder<SubPlan> builder = ImmutableList.builder();
            if (exchange.getType() == ExchangeNode.Type.GATHER) {
                context.get().setSingleNodeDistribution();

                for (int i = 0; i < exchange.getSources().size(); i++) {
                    FragmentProperties childProperties = new FragmentProperties(partitioningScheme.translateOutputLayout(exchange.getInputs().get(i)));
                    builder.add(buildSubPlan(exchange.getSources().get(i), childProperties, context));
                }
            }
            else if (exchange.getType() == ExchangeNode.Type.REPARTITION) {
                context.get().setDistribution(partitioningScheme.getPartitioning().getHandle());

                FragmentProperties childProperties = new FragmentProperties(partitioningScheme.translateOutputLayout(Iterables.getOnlyElement(exchange.getInputs())));
                builder.add(buildSubPlan(Iterables.getOnlyElement(exchange.getSources()), childProperties, context));
            }
            else if (exchange.getType() == ExchangeNode.Type.REPLICATE) {
                FragmentProperties childProperties = new FragmentProperties(partitioningScheme.translateOutputLayout(Iterables.getOnlyElement(exchange.getInputs())));
                builder.add(buildSubPlan(Iterables.getOnlyElement(exchange.getSources()), childProperties, context));
            }

            List<SubPlan> children = builder.build();
            context.get().addChildren(children);

            List<PlanFragmentId> childrenIds = children.stream()
                    .map(SubPlan::getFragment)
                    .map(PlanFragment::getId)
                    .collect(toImmutableList());

            return new RemoteSourceNode(exchange.getId(), childrenIds, exchange.getOutputSymbols());
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
            if (partitioningHandle.isPresent() && !partitioningHandle.get().equals(distribution) && !partitioningHandle.get().equals(SOURCE_DISTRIBUTION)) {
                checkState(partitioningHandle.get().isSingleNode(),
                        "Cannot set distribution to %s. Already set to %s",
                        distribution,
                        partitioningHandle);
                return this;
            }
            partitioningHandle = Optional.of(distribution);

            return this;
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

    private static class SchedulingOrderVisitor
            extends PlanVisitor<Consumer<PlanNodeId>, Void>
    {
        public List<PlanNodeId> getSchedulingOrder(PlanNode node)
        {
            ImmutableList.Builder<PlanNodeId> schedulingOrder = ImmutableList.builder();
            node.accept(this, schedulingOrder::add);
            return schedulingOrder.build();
        }

        @Override
        protected Void visitPlan(PlanNode node, Consumer<PlanNodeId> schedulingOrder)
        {
            for (PlanNode source : node.getSources()) {
                source.accept(this, schedulingOrder);
            }
            return null;
        }

        @Override
        public Void visitJoin(JoinNode node, Consumer<PlanNodeId> schedulingOrder)
        {
            node.getRight().accept(this, schedulingOrder);
            node.getLeft().accept(this, schedulingOrder);
            return null;
        }

        @Override
        public Void visitSemiJoin(SemiJoinNode node, Consumer<PlanNodeId> schedulingOrder)
        {
            node.getFilteringSource().accept(this, schedulingOrder);
            node.getSource().accept(this, schedulingOrder);
            return null;
        }

        @Override
        public Void visitIndexJoin(IndexJoinNode node, Consumer<PlanNodeId> schedulingOrder)
        {
            node.getIndexSource().accept(this, schedulingOrder);
            node.getProbeSource().accept(this, schedulingOrder);
            return null;
        }

        @Override
        public Void visitTableScan(TableScanNode node, Consumer<PlanNodeId> schedulingOrder)
        {
            schedulingOrder.accept(node.getId());
            return null;
        }
    }
}
