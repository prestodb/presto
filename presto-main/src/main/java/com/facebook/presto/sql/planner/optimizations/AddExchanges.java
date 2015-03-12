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
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.operator.aggregation.InternalAggregationFunction;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.ChildReplacer;
import com.facebook.presto.sql.planner.plan.DistinctLimitNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.IndexJoinNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.MarkDistinctNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.RowNumberNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.TableCommitNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.TopNNode;
import com.facebook.presto.sql.planner.plan.TopNRowNumberNode;
import com.facebook.presto.sql.planner.plan.UnionNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.isBigQueryEnabled;
import static com.facebook.presto.sql.planner.plan.AggregationNode.Step.FINAL;
import static com.facebook.presto.sql.planner.plan.AggregationNode.Step.PARTIAL;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.gatheringExchange;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.partitionedExchange;
import static com.google.common.base.Preconditions.checkArgument;

public class AddExchanges
        extends PlanOptimizer
{
    private final Metadata metadata;
    private final boolean distributedIndexJoins;
    private final boolean distributedJoins;

    public AddExchanges(Metadata metadata, boolean distributedIndexJoins, boolean distributedJoins)
    {
        this.metadata = metadata;
        this.distributedIndexJoins = distributedIndexJoins;
        this.distributedJoins = distributedJoins;
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        boolean distributedJoinEnabled = SystemSessionProperties.isDistributedJoinEnabled(session, distributedJoins);
        PlanWithProperties result = plan.accept(new Rewriter(symbolAllocator, idAllocator, session, distributedIndexJoins, distributedJoinEnabled), Optional.empty());
        return result.getNode();
    }

    private class Rewriter
            extends PlanVisitor<Optional<Requirements>, PlanWithProperties>
    {
        private final SymbolAllocator allocator;
        private final PlanNodeIdAllocator idAllocator;
        private final Session session;
        private final boolean distributedIndexJoins;
        private final boolean distributedJoins;

        public Rewriter(SymbolAllocator allocator, PlanNodeIdAllocator idAllocator, Session session, boolean distributedIndexJoins, boolean distributedJoins)
        {
            this.allocator = allocator;
            this.idAllocator = idAllocator;
            this.session = session;
            this.distributedIndexJoins = distributedIndexJoins;
            this.distributedJoins = distributedJoins;
        }

        @Override
        protected PlanWithProperties visitPlan(PlanNode node, Optional<Requirements> preferredProperties)
        {
            // default behavior for nodes that have a single child and propagate child properties verbatim
            PlanWithProperties source = Iterables.getOnlyElement(node.getSources()).accept(this, preferredProperties);
            return new PlanWithProperties(ChildReplacer.replaceChildren(node, ImmutableList.of(source.getNode())), source.getProperties());
        }

        @Override
        public PlanWithProperties visitOutput(OutputNode node, Optional<Requirements> preferredProperties)
        {
            PlanWithProperties child = enforce(
                    Iterables.getOnlyElement(node.getSources()).accept(this, Optional.empty()),
                    Requirements.of(PartitioningProperties.unpartitioned()));

            return new PlanWithProperties(ChildReplacer.replaceChildren(node, ImmutableList.of(child.getNode())), child.getProperties());
        }

        @Override
        public PlanWithProperties visitAggregation(final AggregationNode node, Optional<Requirements> preferredProperties)
        {
            boolean decomposable = node.getFunctions()
                    .values().stream()
                    .map(metadata::getExactFunction)
                    .map(FunctionInfo::getAggregationFunction)
                    .allMatch(InternalAggregationFunction::isDecomposable);

            Requirements enforcedChildProperties;
            Optional<Requirements> preferredChildProperties;
            if (node.getGroupBy().isEmpty()) {
                // if this is not a group-by aggregation, prefer a partitioned plan so that we can perform partials in parallel
                preferredChildProperties = Optional.of(Requirements.of(PartitioningProperties.arbitrary()));
                enforcedChildProperties = Requirements.of(PartitioningProperties.unpartitioned());
            }
            else {
                enforcedChildProperties = Requirements.of(PartitioningProperties.partitioned(node.getGroupBy(), node.getHashSymbol()));
                preferredChildProperties = Optional.of(enforcedChildProperties);
            }

            PlanWithProperties source = node.getSource().accept(this, preferredChildProperties);
            if (source.getProperties().isUnpartitioned() || source.getProperties().isPartitionedOnKeys(node.getGroupBy())) {
                return rebaseAndPropagateChildProperties(node, source);
            }

            if (!decomposable) {
                return rebaseAndPropagateChildProperties(node, enforce(source, enforcedChildProperties));
            }

            // otherwise, add a partial and final with an exchange in between
            Map<Symbol, Symbol> masks = node.getMasks();

            Map<Symbol, FunctionCall> finalCalls = new HashMap<>();
            Map<Symbol, FunctionCall> intermediateCalls = new HashMap<>();
            Map<Symbol, Signature> intermediateFunctions = new HashMap<>();
            Map<Symbol, Symbol> intermediateMask = new HashMap<>();
            for (Map.Entry<Symbol, FunctionCall> entry : node.getAggregations().entrySet()) {
                Signature signature = node.getFunctions().get(entry.getKey());
                FunctionInfo function = metadata.getExactFunction(signature);

                Symbol intermediateSymbol = allocator.newSymbol(function.getName().getSuffix(), metadata.getType(function.getIntermediateType()));
                intermediateCalls.put(intermediateSymbol, entry.getValue());
                intermediateFunctions.put(intermediateSymbol, signature);
                if (masks.containsKey(entry.getKey())) {
                    intermediateMask.put(intermediateSymbol, masks.get(entry.getKey()));
                }

                // rewrite final aggregation in terms of intermediate function
                finalCalls.put(entry.getKey(), new FunctionCall(function.getName(), ImmutableList.<Expression>of(new QualifiedNameReference(intermediateSymbol.toQualifiedName()))));
            }

            PlanWithProperties partial = enforce(
                    new AggregationNode(
                            idAllocator.getNextId(),
                            source.getNode(),
                            node.getGroupBy(),
                            intermediateCalls,
                            intermediateFunctions,
                            intermediateMask,
                            PARTIAL,
                            node.getSampleWeight(),
                            node.getConfidence(),
                            node.getHashSymbol()),
                    source.getProperties(),
                    enforcedChildProperties);

            return new PlanWithProperties(
                    new AggregationNode(
                            node.getId(),
                            partial.getNode(),
                            node.getGroupBy(),
                            finalCalls,
                            node.getFunctions(),
                            ImmutableMap.of(),
                            FINAL,
                            Optional.empty(),
                            node.getConfidence(),
                            node.getHashSymbol()),
                    properties);
        }

        @Override
        public PlanWithProperties visitMarkDistinct(MarkDistinctNode node, Optional<Requirements> preferredProperties)
        {
            Requirements preferredChildProperties = Requirements.of(PartitioningProperties.partitioned(node.getDistinctSymbols(), node.getHashSymbol()));
            PlanWithProperties child = node.getSource().accept(this, Optional.of(preferredChildProperties));

            if (child.getProperties().isPartitioned() || isBigQueryEnabled(session, false)) {
                child = enforce(child, preferredChildProperties);
            }

            return rebaseAndPropagateChildProperties(node, child);
        }

        @Override
        public PlanWithProperties visitWindow(WindowNode node, Optional<Requirements> preferredProperties)
        {
            return planWindowFunction(node, node.getPartitionBy(), node.getHashSymbol());
        }

        @Override
        public PlanWithProperties visitRowNumber(RowNumberNode node, Optional<Requirements> preferredProperties)
        {
            return planWindowFunction(node, node.getPartitionBy(), node.getHashSymbol());
        }

        @Override
        public PlanWithProperties visitTopNRowNumber(TopNRowNumberNode node, Optional<Requirements> preferredProperties)
        {
            List<Symbol> partitionBy = node.getPartitionBy();
            Optional<Symbol> hashSymbol = node.getHashSymbol();

            PlanWithProperties child;
            if (partitionBy.isEmpty()) {
                child = planChild(node, Requirements.of(PartitioningProperties.arbitrary()));

                if (!child.getProperties().isUnpartitioned()) {
                    child = enforce(
                            new TopNRowNumberNode(
                                    idAllocator.getNextId(),
                                    child.getNode(),
                                    node.getPartitionBy(),
                                    node.getOrderBy(),
                                    node.getOrderings(),
                                    node.getRowNumberSymbol(),
                                    node.getMaxRowCountPerPartition(),
                                    true,
                                    node.getHashSymbol()),
                            child.getProperties(),
                            Requirements.of(PartitioningProperties.unpartitioned()));
                }
            }
            else {
                Requirements requirements = Requirements.of(PartitioningProperties.partitioned(partitionBy, hashSymbol));
                child = planChild(node, requirements);

                if (!child.getProperties().isUnpartitioned() && !child.getProperties().isPartitionedOnKeys(partitionBy)) {
                    child = enforce(
                            new TopNRowNumberNode(
                                    idAllocator.getNextId(),
                                    child.getNode(),
                                    node.getPartitionBy(),
                                    node.getOrderBy(),
                                    node.getOrderings(),
                                    node.getRowNumberSymbol(),
                                    node.getMaxRowCountPerPartition(),
                                    true,
                                    node.getHashSymbol()),
                            child.getProperties(),
                            requirements);
                }
            }

            return rebaseAndPropagateChildProperties(node, child);
        }

        @Override
        public PlanWithProperties visitTopN(TopNNode node, Optional<Requirements> preferredProperties)
        {
            PlanWithProperties child = Iterables.getOnlyElement(node.getSources()).accept(this, Optional.of(Requirements.of(PartitioningProperties.arbitrary())));

            if (!child.getProperties().isUnpartitioned()) {
                child = enforce(
                        new TopNNode(idAllocator.getNextId(), child.getNode(), node.getCount(), node.getOrderBy(), node.getOrderings(), true),
                        child.getProperties(),
                        Requirements.of(PartitioningProperties.unpartitioned()));
            }

            return rebaseAndPropagateChildProperties(node, child);
        }

        @Override
        public PlanWithProperties visitSort(SortNode node, Optional<Requirements> preferredProperties)
        {
            Requirements requirements = Requirements.of(PartitioningProperties.unpartitioned());
            return rebaseAndPropagateChildProperties(node, enforce(planChild(node, requirements), requirements));
        }

        @Override
        public PlanWithProperties visitLimit(LimitNode node, Optional<Requirements> preferredProperties)
        {
            PlanWithProperties child = planChild(node, Requirements.of(PartitioningProperties.arbitrary()));

            if (!child.getProperties().isUnpartitioned()) {
                child = enforce(
                        new LimitNode(idAllocator.getNextId(), child.getNode(), node.getCount()),
                        child.getProperties(),
                        Requirements.of(PartitioningProperties.unpartitioned()));
            }

            return rebaseAndPropagateChildProperties(node, child);
        }

        @Override
        public PlanWithProperties visitDistinctLimit(DistinctLimitNode node, Optional<Requirements> preferredProperties)
        {
            PlanWithProperties child = planChild(node, Requirements.of(PartitioningProperties.arbitrary()));

            if (!child.getProperties().isUnpartitioned()) {
                child = enforce(
                        new DistinctLimitNode(idAllocator.getNextId(), child.getNode(), node.getLimit(), node.getHashSymbol()),
                        child.getProperties(),
                        Requirements.of(PartitioningProperties.unpartitioned()));
            }

            return rebaseAndPropagateChildProperties(node, child);
        }

        @Override
        public PlanWithProperties visitTableScan(TableScanNode node, Optional<Requirements> preferredProperties)
        {
            return new PlanWithProperties(node, ActualProperties.of(PartitioningProperties.arbitrary(), PlacementProperties.source()));
        }

        @Override
        public PlanWithProperties visitValues(ValuesNode node, Optional<Requirements> preferredProperties)
        {
            return new PlanWithProperties(node, ActualProperties.of(PartitioningProperties.unpartitioned(), PlacementProperties.anywhere()));
        }

        @Override
        public PlanWithProperties visitTableCommit(TableCommitNode node, Optional<Requirements> preferredProperties)
        {
            Requirements requirements = Requirements.of(PartitioningProperties.unpartitioned(), PlacementProperties.coordinatorOnly());
            return rebaseAndPropagateChildProperties(node, enforce(planChild(node, requirements), requirements));
        }

        @Override
        public PlanWithProperties visitJoin(JoinNode node, Optional<Requirements> preferredProperties)
        {
            checkArgument(node.getType() != JoinNode.Type.RIGHT, "Expected RIGHT joins to be normalized to LEFT joins");

            List<Symbol> leftSymbols = Lists.transform(node.getCriteria(), JoinNode.EquiJoinClause::getLeft);
            List<Symbol> rightSymbols = Lists.transform(node.getCriteria(), JoinNode.EquiJoinClause::getRight);

            Optional<Symbol> leftHashSymbol = node.getLeftHashSymbol();
            Optional<Symbol> rightHashSymbol = node.getRightHashSymbol();

            Requirements leftRequirements = Requirements.of(PartitioningProperties.partitioned(leftSymbols, leftHashSymbol));
            Requirements rightRequirements = Requirements.of(PartitioningProperties.partitioned(rightSymbols, rightHashSymbol));

            PlanWithProperties left = node.getLeft().accept(this, Optional.of(leftRequirements));
            PlanWithProperties right = node.getRight().accept(this, Optional.of(rightRequirements));

            PlanNode rightNode;
            if (distributedJoins) {
                left = enforce(left, leftRequirements);
                rightNode = enforce(right, rightRequirements).getNode();
            }
            else {
                rightNode = new ExchangeNode(
                        idAllocator.getNextId(),
                        ExchangeNode.Type.REPLICATE,
                        ImmutableList.of(),
                        Optional.<Symbol>empty(),
                        ImmutableList.of(right.getNode()),
                        right.getNode().getOutputSymbols(),
                        ImmutableList.of(right.getNode().getOutputSymbols()));
            }

            return new PlanWithProperties(
                    new JoinNode(node.getId(),
                            node.getType(),
                            left.getNode(),
                            rightNode,
                            node.getCriteria(),
                            node.getLeftHashSymbol(),
                            node.getRightHashSymbol()),
                    left.getProperties());
        }

        @Override
        public PlanWithProperties visitSemiJoin(SemiJoinNode node, Optional<Requirements> preferredProperties)
        {
            PlanWithProperties source = node.getSource().accept(this, Optional.empty());
            PlanWithProperties filteringSource = node.getFilteringSource().accept(this, Optional.empty());

            // make filtering source match requirements of source
            PlanNode filteringSourceNode;
            if (source.getProperties().isPartitioned()) {
                filteringSourceNode = new ExchangeNode(
                        idAllocator.getNextId(),
                        ExchangeNode.Type.REPLICATE,
                        ImmutableList.of(),
                        Optional.<Symbol>empty(),
                        ImmutableList.of(filteringSource.getNode()),
                        filteringSource.getNode().getOutputSymbols(),
                        ImmutableList.of(filteringSource.getNode().getOutputSymbols()));
            }
            else {
                filteringSourceNode = enforce(filteringSource, Requirements.of(PartitioningProperties.unpartitioned()))
                        .getNode();
            }
            // TODO: add support for hash-partitioned semijoins

            return rebase(node, source.getProperties(), ImmutableList.of(source.getNode(), filteringSourceNode));
        }

        @Override
        public PlanWithProperties visitIndexJoin(IndexJoinNode node, Optional<Requirements> preferredProperties)
        {
            Requirements requirements = Requirements.of(PartitioningProperties.partitioned(Lists.transform(node.getCriteria(), IndexJoinNode.EquiJoinClause::getProbe), node.getProbeHashSymbol()));
            PlanWithProperties probeSource = node.getProbeSource().accept(this, Optional.of(requirements));

            if (distributedIndexJoins) {
                probeSource = enforce(probeSource, requirements);
            }

            // index side runs with the same partitioning/distribution strategy as the probe side, so don't insert exchanges
            return rebase(node, probeSource.getProperties(), ImmutableList.of(probeSource.getNode(), node.getIndexSource()));
        }

        @Override
        public PlanWithProperties visitUnion(UnionNode node, Optional<Requirements> preferredProperties)
        {
            // first, classify children into partitioned and unpartitioned
            List<PlanNode> unpartitionedChildren = new ArrayList<>();
            List<List<Symbol>> unpartitionedOutputLayouts = new ArrayList<>();

            List<PlanNode> partitionedChildren = new ArrayList<>();
            List<List<Symbol>> partitionedOutputLayouts = new ArrayList<>();

            List<PlanNode> sources = node.getSources();
            for (int i = 0; i < sources.size(); i++) {
                PlanWithProperties child = sources.get(i).accept(this, Optional.empty());
                if (child.getProperties().isUnpartitioned()) {
                    unpartitionedChildren.add(child.getNode());
                    unpartitionedOutputLayouts.add(node.sourceOutputLayout(i));
                }
                else {
                    partitionedChildren.add(child.getNode());
                    partitionedOutputLayouts.add(node.sourceOutputLayout(i));
                }
            }

            PlanNode result = null;
            if (!partitionedChildren.isEmpty()) {
                // add an exchange above partitioned inputs and fold it into the
                // set of unpartitioned inputs
                result = new ExchangeNode(
                        idAllocator.getNextId(),
                        ExchangeNode.Type.GATHER,
                        ImmutableList.of(),
                        Optional.<Symbol>empty(),
                        partitionedChildren,
                        node.getOutputSymbols(),
                        partitionedOutputLayouts);

                unpartitionedChildren.add(result);
                unpartitionedOutputLayouts.add(result.getOutputSymbols());
            }

            // if there's at least one unpartitioned input (including the exchange that might have been added in the
            // previous step), add a local union
            if (unpartitionedChildren.size() > 1) {
                ImmutableListMultimap.Builder<Symbol, Symbol> mappings = ImmutableListMultimap.builder();
                for (int i = 0; i < node.getOutputSymbols().size(); i++) {
                    for (List<Symbol> outputLayout : unpartitionedOutputLayouts) {
                        mappings.put(node.getOutputSymbols().get(i), outputLayout.get(i));
                    }
                }

                result = new UnionNode(node.getId(), unpartitionedChildren, mappings.build());
            }

            return new PlanWithProperties(result, ActualProperties.of(PartitioningProperties.unpartitioned(), PlacementProperties.anywhere()));
        }

        private PlanWithProperties planWindowFunction(PlanNode node, List<Symbol> partitionBy, Optional<Symbol> hashSymbol)
        {
            PlanWithProperties child;

            if (partitionBy.isEmpty()) {
                Requirements requirements = Requirements.of(PartitioningProperties.unpartitioned());
                child = enforce(planChild(node, requirements), requirements);
            }
            else {
                Requirements requirements = Requirements.of(PartitioningProperties.partitioned(partitionBy, hashSymbol));
                child = planChild(node, requirements);

                if (!child.getProperties().isUnpartitioned() && !child.getProperties().isPartitionedOnKeys(partitionBy)) {
                    child = enforce(child, requirements);
                }
            }

            return rebaseAndPropagateChildProperties(node, child);
        }

        private PlanWithProperties planChild(PlanNode node, Requirements preferredProperties)
        {
            return Iterables.getOnlyElement(node.getSources()).accept(this, Optional.of(preferredProperties));
        }

        private PlanWithProperties enforce(PlanWithProperties plan, Requirements requirements)
        {
            return enforce(plan.getNode(), plan.getProperties(), requirements);
        }

        private PlanWithProperties enforce(PlanNode node, ActualProperties properties, Requirements requirements)
        {
            if (requirements.isCoordinatorOnly() && !properties.isCoordinatorOnly()) {
                return new PlanWithProperties(
                        gatheringExchange(idAllocator.getNextId(), node),
                        ActualProperties.of(PartitioningProperties.unpartitioned(), PlacementProperties.coordinatorOnly()));
            }

            // req: unpartitioned, actual: unpartitioned
            if (requirements.isUnpartitioned() && properties.isUnpartitioned()) {
                return new PlanWithProperties(node, properties);
            }

            // req: partitioned, actual: partitioned on same keys or arbitrary
            if (requirements.isPartitioned() &&
                    properties.isPartitioned() &&
                    properties.getPartitioning().getKeys().equals(requirements.getPartitioning().get().getKeys())) {
                return new PlanWithProperties(node, properties);
            }

            // req: unpartitioned, actual: partitioned
            if (properties.isPartitioned() && requirements.isUnpartitioned()) {
                return new PlanWithProperties(
                        gatheringExchange(idAllocator.getNextId(), node),
                        ActualProperties.of(PartitioningProperties.unpartitioned(), PlacementProperties.anywhere()));
            }

            // req: partitioned[k], actual: partitioned[?] or unpartitioned
            if (requirements.isPartitionedOnKeys() &&
                    (properties.isUnpartitioned() || (properties.isPartitioned() && !properties.getPartitioning().getKeys().equals(requirements.getPartitioning().get().getKeys())))) {
                return new PlanWithProperties(
                        partitionedExchange(
                                idAllocator.getNextId(),
                                node,
                                requirements.getPartitioning().get().getKeys().get(),
                                requirements.getPartitioning().get().getHashSymbol()),
                        ActualProperties.of(requirements.getPartitioning().get(), PlacementProperties.anywhere()));
            }

            throw new UnsupportedOperationException(String.format("not supported: required %s, current %s", requirements, properties));
        }

        private PlanWithProperties rebaseAndPropagateChildProperties(PlanNode node, PlanWithProperties child)
        {
            PlanNode result = ChildReplacer.replaceChildren(node, ImmutableList.of(child.getNode()));
            return new PlanWithProperties(result, child.getProperties());
        }

        private PlanWithProperties rebase(PlanNode node, ActualProperties properties, List<PlanNode> children)
        {
            PlanNode result = ChildReplacer.replaceChildren(node, children);
            return new PlanWithProperties(result, properties);
        }
    }

    private static class PlanWithProperties
    {
        private final PlanNode node;
        private final ActualProperties properties;

        public PlanWithProperties(PlanNode node, ActualProperties properties)
        {
            this.node = node;
            this.properties = properties;
        }

        public PlanNode getNode()
        {
            return node;
        }

        public ActualProperties getProperties()
        {
            return properties;
        }
    }

    private static class Requirements
    {
        private final Optional<PartitioningProperties> partitioning;
        private final Optional<PlacementProperties> placement;

        private Requirements(Optional<PartitioningProperties> partitioning, Optional<PlacementProperties> placement)
        {
            this.partitioning = partitioning;
            this.placement = placement;
        }

        public static Requirements of(PartitioningProperties partitioning)
        {
            return new Requirements(Optional.of(partitioning), Optional.empty());
        }

        public static Requirements of(PartitioningProperties partitioning, PlacementProperties placement)
        {
            return new Requirements(Optional.of(partitioning), Optional.of(placement));
        }

        public Optional<PartitioningProperties> getPartitioning()
        {
            return partitioning;
        }

        @Override
        public String toString()
        {
            return "partitioning: " + (partitioning.isPresent() ? partitioning.get() : "*") +
                    "," +
                    "placement: " + (placement.isPresent() ? placement.get() : "*");
        }

        public boolean isCoordinatorOnly()
        {
            return placement.isPresent() && placement.get().getType() == PlacementProperties.Type.COORDINATOR_ONLY;
        }

        public boolean isUnpartitioned()
        {
            return partitioning.isPresent() && partitioning.get().getType() == PartitioningProperties.Type.UNPARTITIONED;
        }

        public boolean isPartitionedOnKeys()
        {
            return isPartitioned() && partitioning.get().getKeys().isPresent();
        }

        public boolean isPartitioned()
        {
            return partitioning.isPresent() && partitioning.get().getType() == PartitioningProperties.Type.PARTITIONED;
        }
    }

    private static class ActualProperties
    {
        // partitioning:
        //   partitioned: *, {k_i}
        //   unpartitioned

        // placement
        //   coordinator-only (=> unpartitioned)
        //   source
        //   anywhere

        private final PartitioningProperties partitioning;
        private final PlacementProperties placement;

        public ActualProperties(PartitioningProperties partitioning, PlacementProperties placement)
        {
            this.partitioning = partitioning;
            this.placement = placement;
        }

        public static ActualProperties of(PartitioningProperties partitioning, PlacementProperties placement)
        {
            return new ActualProperties(partitioning, placement);
        }

        public PartitioningProperties getPartitioning()
        {
            return partitioning;
        }

        public boolean isCoordinatorOnly()
        {
            return placement.getType() == PlacementProperties.Type.COORDINATOR_ONLY;
        }

        public boolean isPartitioned()
        {
            return partitioning.getType() == PartitioningProperties.Type.PARTITIONED;
        }

        public boolean isPartitionedOnKeys(List<Symbol> keys)
        {
            // partitioned on (k_1, k_2, ..., k_n) => partitioned on (k_1, k_2, ..., k_n, k_n+1, ...)
            return isPartitioned() &&
                    partitioning.getKeys().isPresent() &&
                    ImmutableSet.copyOf(keys).containsAll(partitioning.getKeys().get());
        }

        public boolean isUnpartitioned()
        {
            return partitioning.getType() == PartitioningProperties.Type.UNPARTITIONED;
        }

        @Override
        public String toString()
        {
            return "partitioning: " + partitioning + ", placement: " + placement;
        }
    }

    private static class PartitioningProperties
    {
        public enum Type
        {
            UNPARTITIONED,
            PARTITIONED
        }

        private final Type type;
        private final Optional<Symbol> hashSymbol;
        private final Optional<List<Symbol>> keys;

        public static PartitioningProperties arbitrary()
        {
            return new PartitioningProperties(Type.PARTITIONED);
        }

        public static PartitioningProperties unpartitioned()
        {
            return new PartitioningProperties(Type.UNPARTITIONED);
        }

        public static PartitioningProperties partitioned(List<Symbol> symbols, Optional<Symbol> hashSymbol)
        {
            return new PartitioningProperties(Type.PARTITIONED, symbols, hashSymbol);
        }

        private PartitioningProperties(Type type)
        {
            this.type = type;
            this.keys = Optional.empty();
            this.hashSymbol = Optional.empty();
        }

        private PartitioningProperties(Type type, List<Symbol> keys, Optional<Symbol> hashSymbol)
        {
            this.type = type;
            this.keys = Optional.of(keys);
            this.hashSymbol = hashSymbol;
        }

        public Type getType()
        {
            return type;
        }

        public Optional<List<Symbol>> getKeys()
        {
            return keys;
        }

        public Optional<Symbol> getHashSymbol()
        {
            return hashSymbol;
        }

        @Override
        public String toString()
        {
            if (type == Type.PARTITIONED) {
                return type.toString() + ": " + (keys.isPresent() ? keys.get() : "*");
            }

            return type.toString();
        }
    }

    private static class PlacementProperties
    {
        public enum Type
        {
            COORDINATOR_ONLY,
            SOURCE,
            ANY
        }

        private final Type type;

        public static PlacementProperties anywhere()
        {
            return new PlacementProperties(Type.ANY);
        }

        public static PlacementProperties source()
        {
            return new PlacementProperties(Type.SOURCE);
        }

        public static PlacementProperties coordinatorOnly()
        {
            return new PlacementProperties(Type.COORDINATOR_ONLY);
        }

        private PlacementProperties(Type type)
        {
            this.type = type;
        }

        public Type getType()
        {
            return type;
        }

        public String toString()
        {
            return type.toString();
        }
    }
}
