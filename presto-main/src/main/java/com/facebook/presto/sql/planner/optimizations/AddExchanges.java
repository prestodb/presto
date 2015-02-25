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
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

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
        PlanWithProperties result = plan.accept(new Rewriter(symbolAllocator, idAllocator, session, distributedIndexJoins, distributedJoinEnabled), null);
        return result.getNode();
    }

    private class Rewriter
            extends PlanVisitor<Void, PlanWithProperties>
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
        protected PlanWithProperties visitPlan(PlanNode node, Void context)
        {
            // default behavior for nodes that have a single child and propagate child properties verbatim
            PlanWithProperties source = Iterables.getOnlyElement(node.getSources()).accept(this, context);
            return propagateChildProperties(node, source);
        }

        @Override
        public PlanWithProperties visitOutput(OutputNode node, Void context)
        {
            return pushRequirementsToChild(node, Requirements.of(PartitioningProperties.unpartitioned()));
        }

        @Override
        public PlanWithProperties visitAggregation(final AggregationNode node, Void context)
        {
            boolean decomposable = node.getFunctions()
                    .values().stream()
                    .map(metadata::getExactFunction)
                    .map(FunctionInfo::getAggregationFunction)
                    .allMatch(InternalAggregationFunction::isDecomposable);

            if (!decomposable) {
                return pushRequirementsToChild(node, Requirements.of(PartitioningProperties.unpartitioned()));
            }

            // if child is unpartitioned or already partitioned on this node's group by keys
            // keep the current structure
            PlanWithProperties source = node.getSource().accept(this, context);
            if (source.getProperties().isUnpartitioned() ||
                    (!node.getGroupBy().isEmpty() && source.getProperties().isPartitionedOnKeys(node.getGroupBy()))) {
                return propagateChildProperties(node, source);
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

            return enforceWithPartial(
                    source,
                    computePartitioningRequirements(node.getGroupBy(), node.getHashSymbol()),
                    child -> new AggregationNode(
                            idAllocator.getNextId(),
                            child,
                            node.getGroupBy(),
                            intermediateCalls,
                            intermediateFunctions,
                            intermediateMask,
                            PARTIAL,
                            node.getSampleWeight(),
                            node.getConfidence(),
                            node.getHashSymbol()),
                    child -> new AggregationNode(
                            node.getId(),
                            child,
                            node.getGroupBy(),
                            finalCalls,
                            node.getFunctions(),
                            ImmutableMap.of(),
                            FINAL,
                            Optional.empty(),
                            node.getConfidence(),
                            node.getHashSymbol()));
        }

        @Override
        public PlanWithProperties visitMarkDistinct(MarkDistinctNode node, Void context)
        {
            PlanWithProperties child = node.getSource().accept(this, context);

            if (child.getProperties().isPartitioned() || isBigQueryEnabled(session, false)) {
                child = enforce(child, Requirements.of(PartitioningProperties.partitioned(node.getDistinctSymbols(), node.getHashSymbol())));
            }

            return propagateChildProperties(node, child);
        }

        @Override
        public PlanWithProperties visitWindow(WindowNode node, Void context)
        {
            return pushRequirementsToChild(node,
                    computePartitioningRequirements(node.getPartitionBy(), node.getHashSymbol()));
        }

        @Override
        public PlanWithProperties visitRowNumber(RowNumberNode node, Void context)
        {
            return pushRequirementsToChild(node,
                    computePartitioningRequirements(node.getPartitionBy(), node.getHashSymbol()));
        }

        @Override
        public PlanWithProperties visitTopNRowNumber(TopNRowNumberNode node, Void context)
        {
            return pushRequirementsToChildWithPartial(node,
                    computePartitioningRequirements(node.getPartitionBy(), node.getHashSymbol()),
                    child -> new TopNRowNumberNode(
                            idAllocator.getNextId(),
                            child,
                            node.getPartitionBy(),
                            node.getOrderBy(),
                            node.getOrderings(),
                            node.getRowNumberSymbol(),
                            node.getMaxRowCountPerPartition(),
                            true,
                            node.getHashSymbol()),
                    child -> new TopNRowNumberNode(
                            node.getId(),
                            child,
                            node.getPartitionBy(),
                            node.getOrderBy(),
                            node.getOrderings(),
                            node.getRowNumberSymbol(),
                            node.getMaxRowCountPerPartition(),
                            false,
                            node.getHashSymbol()));
        }

        @Override
        public PlanWithProperties visitTopN(TopNNode node, Void context)
        {
            return pushRequirementsToChildWithPartial(node,
                    Requirements.of(PartitioningProperties.unpartitioned()),
                    child -> new TopNNode(idAllocator.getNextId(), child, node.getCount(), node.getOrderBy(), node.getOrderings(), true),
                    child -> new TopNNode(node.getId(), child, node.getCount(), node.getOrderBy(), node.getOrderings(), false));
        }

        @Override
        public PlanWithProperties visitSort(SortNode node, Void context)
        {
            return pushRequirementsToChild(node, Requirements.of(PartitioningProperties.unpartitioned()));
        }

        @Override
        public PlanWithProperties visitLimit(LimitNode node, Void context)
        {
            return pushRequirementsToChildWithPartial(node,
                    Requirements.of(PartitioningProperties.unpartitioned()),
                    child -> new LimitNode(idAllocator.getNextId(), child, node.getCount()),
                    child -> ChildReplacer.replaceChildren(node, ImmutableList.of(child)));
        }

        @Override
        public PlanWithProperties visitDistinctLimit(DistinctLimitNode node, Void context)
        {
            return pushRequirementsToChildWithPartial(node,
                    Requirements.of(PartitioningProperties.unpartitioned()),
                    child -> new DistinctLimitNode(idAllocator.getNextId(), child, node.getLimit(), node.getHashSymbol()),
                    child -> ChildReplacer.replaceChildren(node, ImmutableList.of(child)));
        }

        @Override
        public PlanWithProperties visitTableScan(TableScanNode node, Void context)
        {
            return new PlanWithProperties(node, ActualProperties.of(PartitioningProperties.arbitrary(), PlacementProperties.source()));
        }

        @Override
        public PlanWithProperties visitValues(ValuesNode node, Void context)
        {
            return new PlanWithProperties(node, ActualProperties.of(PartitioningProperties.unpartitioned(), PlacementProperties.anywhere()));
        }

        @Override
        public PlanWithProperties visitTableCommit(TableCommitNode node, Void context)
        {
            return pushRequirementsToChild(node, Requirements.of(PartitioningProperties.unpartitioned(), PlacementProperties.coordinatorOnly()));
        }

        @Override
        public PlanWithProperties visitJoin(JoinNode node, Void context)
        {
            checkArgument(node.getType() != JoinNode.Type.RIGHT, "Expected RIGHT joins to be normalized to LEFT joins");

            PlanWithProperties left = node.getLeft().accept(this, context);
            PlanWithProperties right = node.getRight().accept(this, context);

            Optional<Symbol> leftHashSymbol = node.getLeftHashSymbol();
            Optional<Symbol> rightHashSymbol = node.getRightHashSymbol();

            List<Symbol> leftSymbols = Lists.transform(node.getCriteria(), JoinNode.EquiJoinClause::getLeft);
            List<Symbol> rightSymbols = Lists.transform(node.getCriteria(), JoinNode.EquiJoinClause::getRight);

            PlanNode rightNode;
            if (distributedJoins) {
                left = enforce(left, Requirements.of(PartitioningProperties.partitioned(leftSymbols, leftHashSymbol)));
                rightNode = enforce(right, Requirements.of(PartitioningProperties.partitioned(rightSymbols, rightHashSymbol))).getNode();
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
        public PlanWithProperties visitSemiJoin(SemiJoinNode node, Void context)
        {
            PlanWithProperties source = node.getSource().accept(this, context);
            PlanWithProperties filteringSource = node.getFilteringSource().accept(this, context);

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

            return withNewChildren(node, source.getProperties(), ImmutableList.of(source.getNode(), filteringSourceNode));
        }

        @Override
        public PlanWithProperties visitIndexJoin(IndexJoinNode node, Void context)
        {
            PlanWithProperties probeSource = node.getProbeSource().accept(this, context);

            if (distributedIndexJoins) {
                probeSource = enforce(probeSource, Requirements.of(PartitioningProperties.partitioned(Lists.transform(node.getCriteria(), IndexJoinNode.EquiJoinClause::getProbe), node.getProbeHashSymbol())));
            }

            // index side runs with the same partitioning/distribution strategy as the probe side, so don't insert exchanges
            return withNewChildren(node, probeSource.getProperties(), ImmutableList.of(probeSource.getNode(), node.getIndexSource()));
        }

        @Override
        public PlanWithProperties visitUnion(UnionNode node, Void context)
        {
            // first, classify children into partitioned and unpartitioned
            List<PlanNode> unpartitionedChildren = new ArrayList<>();
            List<List<Symbol>> unpartitionedOutputLayouts = new ArrayList<>();

            List<PlanNode> partitionedChildren = new ArrayList<>();
            List<List<Symbol>> partitionedOutputLayouts = new ArrayList<>();

            List<PlanNode> sources = node.getSources();
            for (int i = 0; i < sources.size(); i++) {
                PlanWithProperties child = sources.get(i).accept(this, context);
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

        private Requirements computePartitioningRequirements(List<Symbol> partitionKeys, Optional<Symbol> hashSymbol)
        {
            if (partitionKeys.isEmpty()) {
                return Requirements.of(PartitioningProperties.unpartitioned());
            }

            return Requirements.of(PartitioningProperties.partitioned(partitionKeys, hashSymbol));
        }

        /**
         * Require the child to produce the give properties. If an exchange is added to enforce them,
         * add a partial underneath it.
         *
         * So, a plan that looks like this A -> B will be rewritten either as F -> X -> P -> B or F -> B,
         * where F and P are computed by calling the provided functions makeFinal and makePartial.
         */
        private PlanWithProperties pushRequirementsToChildWithPartial(PlanNode node, Requirements requirements, Function<PlanNode, PlanNode> makePartial, Function<PlanNode, PlanNode> makeFinal)
        {
            PlanWithProperties child = Iterables.getOnlyElement(node.getSources())
                    .accept(this, null);

            return enforceWithPartial(child, requirements, makePartial, makeFinal);
        }

        private PlanWithProperties pushRequirementsToChild(PlanNode node, Requirements requirements)
        {
            PlanWithProperties source = Iterables.getOnlyElement(node.getSources())
                    .accept(this, null);

            return enforceWithPartial(
                    source,
                    requirements,
                    child -> child,
                    child -> ChildReplacer.replaceChildren(node, ImmutableList.of(child)));
        }

        /**
         * If the child is partitioned, push a partial computation, followed by an exchange and a final
         */
        private PlanWithProperties enforceWithPartial(
                PlanWithProperties child,
                Requirements requirements,
                Function<PlanNode, PlanNode> makePartial,
                Function<PlanNode, PlanNode> makeFinal)
        {
            PlanWithProperties enforced = enforce(child.getNode(), child.getProperties(), requirements, makePartial);
            return new PlanWithProperties(makeFinal.apply(enforced.getNode()), enforced.getProperties());
        }

        private PlanWithProperties enforce(PlanWithProperties plan, Requirements requirements)
        {
            return enforce(plan.getNode(), plan.getProperties(), requirements, child -> child);
        }

        private PlanWithProperties enforce(
                PlanNode node,
                ActualProperties properties,
                Requirements requirements,
                Function<PlanNode, PlanNode> makePartial)
        {
            if (requirements.isCoordinatorOnly() && !properties.isCoordinatorOnly()) {
                return new PlanWithProperties(
                        gatheringExchange(idAllocator.getNextId(), makePartial.apply(node)),
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
                        gatheringExchange(idAllocator.getNextId(), makePartial.apply(node)),
                        ActualProperties.of(PartitioningProperties.unpartitioned(), PlacementProperties.anywhere()));
            }

            // req: partitioned[k], actual: partitioned[?] or unpartitioned
            if (requirements.isPartitionedOnKeys() &&
                    (properties.isUnpartitioned() || (properties.isPartitioned() && !properties.getPartitioning().getKeys().equals(requirements.getPartitioning().get().getKeys())))) {
                return new PlanWithProperties(
                        partitionedExchange(
                                idAllocator.getNextId(),
                                makePartial.apply(node),
                                requirements.getPartitioning().get().getKeys().get(),
                                requirements.getPartitioning().get().getHashSymbol()),
                        ActualProperties.of(requirements.getPartitioning().get(), PlacementProperties.anywhere()));
            }

            throw new UnsupportedOperationException(String.format("not supported: required %s, current %s", requirements, properties));
        }

        private PlanWithProperties propagateChildProperties(PlanNode node, PlanWithProperties child)
        {
            PlanNode result = ChildReplacer.replaceChildren(node, ImmutableList.of(child.getNode()));
            return new PlanWithProperties(result, child.getProperties());
        }

        private PlanWithProperties withNewChildren(PlanNode node, ActualProperties properties, List<PlanNode> children)
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
            return isPartitioned() &&
                    partitioning.getKeys().isPresent() &&
                    partitioning.getKeys().get().equals(keys);
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
