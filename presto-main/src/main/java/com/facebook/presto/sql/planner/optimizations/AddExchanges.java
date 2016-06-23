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
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.metadata.TableLayoutResult;
import com.facebook.presto.operator.aggregation.InternalAggregationFunction;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.GroupingProperty;
import com.facebook.presto.spi.LocalProperty;
import com.facebook.presto.spi.SortingProperty;
import com.facebook.presto.spi.predicate.NullableValue;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.DomainTranslator;
import com.facebook.presto.sql.planner.ExpressionInterpreter;
import com.facebook.presto.sql.planner.LookupSymbolResolver;
import com.facebook.presto.sql.planner.Partitioning;
import com.facebook.presto.sql.planner.PartitioningScheme;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.ChildReplacer;
import com.facebook.presto.sql.planner.plan.DeleteNode;
import com.facebook.presto.sql.planner.plan.DistinctLimitNode;
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.ExplainAnalyzeNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.GroupIdNode;
import com.facebook.presto.sql.planner.plan.IndexJoinNode;
import com.facebook.presto.sql.planner.plan.IndexSourceNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.MarkDistinctNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.RowNumberNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.TableFinishNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.TableWriterNode;
import com.facebook.presto.sql.planner.plan.TopNNode;
import com.facebook.presto.sql.planner.plan.TopNRowNumberNode;
import com.facebook.presto.sql.planner.plan.UnionNode;
import com.facebook.presto.sql.planner.plan.UnnestNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.SetMultimap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.facebook.presto.SystemSessionProperties.isColocatedJoinEnabled;
import static com.facebook.presto.sql.ExpressionUtils.combineConjuncts;
import static com.facebook.presto.sql.ExpressionUtils.extractConjuncts;
import static com.facebook.presto.sql.ExpressionUtils.stripDeterministicConjuncts;
import static com.facebook.presto.sql.ExpressionUtils.stripNonDeterministicConjuncts;
import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.getExpressionTypes;
import static com.facebook.presto.sql.planner.FragmentTableScanCounter.hasMultipleSources;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_RANDOM_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.optimizations.ActualProperties.Global.partitionedOn;
import static com.facebook.presto.sql.planner.optimizations.ActualProperties.Global.singleStreamPartition;
import static com.facebook.presto.sql.planner.optimizations.LocalProperties.grouped;
import static com.facebook.presto.sql.planner.optimizations.ScalarQueryUtil.isScalar;
import static com.facebook.presto.sql.planner.plan.AggregationNode.Step.FINAL;
import static com.facebook.presto.sql.planner.plan.AggregationNode.Step.PARTIAL;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.GATHER;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.gatheringExchange;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.partitionedExchange;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.replicatedExchange;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.FULL;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.RIGHT;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableSet;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.stream.Collectors.toList;

public class AddExchanges
        implements PlanOptimizer
{
    private final SqlParser parser;
    private final Metadata metadata;

    public AddExchanges(Metadata metadata, SqlParser parser)
    {
        this.metadata = metadata;
        this.parser = parser;
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        PlanWithProperties result = plan.accept(new Rewriter(symbolAllocator, idAllocator, symbolAllocator, session), new Context(PreferredProperties.any(), false));
        return result.getNode();
    }

    private static class Context
    {
        private PreferredProperties preferredProperties;
        // For delete queries, the TableScan node that corresponds to the table being deleted on must be collocated with the Delete node.
        // Care must be taken so that Exchange node is not introduced between the two. For now, only SemiJoin may introduce it.
        private boolean downstreamIsDelete;

        Context(PreferredProperties preferredProperties, boolean downstreamIsDelete)
        {
            this.preferredProperties = preferredProperties;
            this.downstreamIsDelete = downstreamIsDelete;
        }

        Context withPreferredProperties(PreferredProperties preferredProperties)
        {
            return new Context(preferredProperties, downstreamIsDelete);
        }

        Context withHashPartitionedSemiJoinBanned(boolean hashPartitionedSemiJoinBanned)
        {
            return new Context(preferredProperties, hashPartitionedSemiJoinBanned);
        }

        PreferredProperties getPreferredProperties()
        {
            return preferredProperties;
        }

        boolean isDownstreamIsDelete()
        {
            return downstreamIsDelete;
        }
    }

    private class Rewriter
            extends PlanVisitor<Context, PlanWithProperties>
    {
        private final SymbolAllocator allocator;
        private final PlanNodeIdAllocator idAllocator;
        private final SymbolAllocator symbolAllocator;
        private final Session session;
        private final boolean distributedIndexJoins;
        private final boolean distributedJoins;
        private final boolean preferStreamingOperators;
        private final boolean redistributeWrites;

        public Rewriter(SymbolAllocator allocator, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator, Session session)
        {
            this.allocator = allocator;
            this.idAllocator = idAllocator;
            this.symbolAllocator = symbolAllocator;
            this.session = session;
            this.distributedJoins = SystemSessionProperties.isDistributedJoinEnabled(session);
            this.distributedIndexJoins = SystemSessionProperties.isDistributedIndexJoinEnabled(session);
            this.redistributeWrites = SystemSessionProperties.isRedistributeWrites(session);
            this.preferStreamingOperators = SystemSessionProperties.preferStreamingOperators(session);
        }

        @Override
        protected PlanWithProperties visitPlan(PlanNode node, Context context)
        {
            return rebaseAndDeriveProperties(node, planChild(node, context));
        }

        @Override
        public PlanWithProperties visitDelete(DeleteNode node, Context context)
        {
            // Delete operator does not work unless it is co-located with the corresponding TableScan.
            return rebaseAndDeriveProperties(node, planChild(node, context.withHashPartitionedSemiJoinBanned(true)));
        }

        @Override
        public PlanWithProperties visitProject(ProjectNode node, Context context)
        {
            Map<Symbol, Symbol> identities = computeIdentityTranslations(node.getAssignments());
            PreferredProperties translatedPreferred = context.getPreferredProperties().translate(symbol -> Optional.ofNullable(identities.get(symbol)));

            return rebaseAndDeriveProperties(node, planChild(node, context.withPreferredProperties(translatedPreferred)));
        }

        @Override
        public PlanWithProperties visitOutput(OutputNode node, Context context)
        {
            PlanWithProperties child = planChild(node, context.withPreferredProperties(PreferredProperties.any()));

            if (!child.getProperties().isSingleNode()) {
                child = withDerivedProperties(
                        gatheringExchange(idAllocator.getNextId(), REMOTE, child.getNode()),
                        child.getProperties());
            }

            return rebaseAndDeriveProperties(node, child);
        }

        @Override
        public PlanWithProperties visitEnforceSingleRow(EnforceSingleRowNode node, Context context)
        {
            PlanWithProperties child = planChild(node, context.withPreferredProperties(PreferredProperties.any()));

            if (!child.getProperties().isSingleNode()) {
                child = withDerivedProperties(
                        gatheringExchange(idAllocator.getNextId(), REMOTE, child.getNode()),
                        child.getProperties());
            }

            return rebaseAndDeriveProperties(node, child);
        }

        @Override
        public PlanWithProperties visitAggregation(AggregationNode node, Context context)
        {
            FunctionRegistry functionRegistry = metadata.getFunctionRegistry();
            boolean decomposable = node.getFunctions()
                    .values().stream()
                    .map(functionRegistry::getAggregateFunctionImplementation)
                    .allMatch(InternalAggregationFunction::isDecomposable);

            HashSet<Symbol> partitioningRequirement = new HashSet<>(node.getGroupingSets().get(0));
            for (int i = 1; i < node.getGroupingSets().size(); i++) {
                partitioningRequirement.retainAll(node.getGroupingSets().get(i));
            }

            PreferredProperties preferredProperties = PreferredProperties.any();
            if (!node.getGroupBy().isEmpty()) {
                preferredProperties = PreferredProperties.partitionedWithLocal(partitioningRequirement, grouped(node.getGroupBy()))
                        .mergeWithParent(context.getPreferredProperties());
            }

            PlanWithProperties child = planChild(node, context.withPreferredProperties(preferredProperties));

            if (child.getProperties().isSingleNode()) {
                // If already unpartitioned, just drop the single aggregation back on
                return rebaseAndDeriveProperties(node, child);
            }

            if (node.getGroupBy().isEmpty()) {
                if (decomposable) {
                    return splitAggregation(node, child, partial -> gatheringExchange(idAllocator.getNextId(), REMOTE, partial));
                }
                else {
                    child = withDerivedProperties(
                            gatheringExchange(idAllocator.getNextId(), REMOTE, child.getNode()),
                            child.getProperties());

                    return rebaseAndDeriveProperties(node, child);
                }
            }
            else {
                if (child.getProperties().isStreamPartitionedOn(partitioningRequirement)) {
                    return rebaseAndDeriveProperties(node, child);
                }
                else {
                    if (decomposable) {
                        Function<PlanNode, PlanNode> exchanger = null;
                        if (!child.getProperties().isNodePartitionedOn(partitioningRequirement)) {
                            exchanger = partial -> partitionedExchange(
                                    idAllocator.getNextId(),
                                    REMOTE,
                                    partial,
                                    node.getGroupBy(),
                                    node.getHashSymbol());
                        }
                        return splitAggregation(node, child, exchanger);
                    }
                    else {
                        child = withDerivedProperties(
                                partitionedExchange(idAllocator.getNextId(), REMOTE, child.getNode(), node.getGroupBy(), node.getHashSymbol()),
                                child.getProperties());
                        return rebaseAndDeriveProperties(node, child);
                    }
                }
            }
        }

        private PlanWithProperties splitAggregation(AggregationNode node, PlanWithProperties newChild, Function<PlanNode, PlanNode> exchanger)
        {
            // otherwise, add a partial and final with an exchange in between
            Map<Symbol, Symbol> masks = node.getMasks();

            Map<Symbol, FunctionCall> finalCalls = new HashMap<>();
            Map<Symbol, FunctionCall> intermediateCalls = new HashMap<>();
            Map<Symbol, Signature> intermediateFunctions = new HashMap<>();
            Map<Symbol, Symbol> intermediateMask = new HashMap<>();
            for (Map.Entry<Symbol, FunctionCall> entry : node.getAggregations().entrySet()) {
                Signature signature = node.getFunctions().get(entry.getKey());
                InternalAggregationFunction function = metadata.getFunctionRegistry().getAggregateFunctionImplementation(signature);

                Symbol intermediateSymbol = allocator.newSymbol(signature.getName(), function.getIntermediateType());
                intermediateCalls.put(intermediateSymbol, entry.getValue());
                intermediateFunctions.put(intermediateSymbol, signature);
                if (masks.containsKey(entry.getKey())) {
                    intermediateMask.put(intermediateSymbol, masks.get(entry.getKey()));
                }

                // rewrite final aggregation in terms of intermediate function
                finalCalls.put(entry.getKey(), new FunctionCall(QualifiedName.of(signature.getName()), ImmutableList.of(intermediateSymbol.toSymbolReference())));
            }

            PlanWithProperties partial = withDerivedProperties(
                    new AggregationNode(
                            idAllocator.getNextId(),
                            newChild.getNode(),
                            node.getGroupBy(),
                            intermediateCalls,
                            intermediateFunctions,
                            intermediateMask,
                            node.getGroupingSets(),
                            PARTIAL,
                            node.getSampleWeight(),
                            node.getConfidence(),
                            node.getHashSymbol()),
                    newChild.getProperties());

            PlanNode source = partial.getNode();
            if (exchanger != null) {
                source = exchanger.apply(source);
            }

            return withDerivedProperties(
                    new AggregationNode(
                            node.getId(),
                            source,
                            node.getGroupBy(),
                            finalCalls,
                            node.getFunctions(),
                            ImmutableMap.of(),
                            node.getGroupingSets(),
                            FINAL,
                            Optional.empty(),
                            node.getConfidence(),
                            node.getHashSymbol()),
                    deriveProperties(source, partial.getProperties()));
        }

        @Override
        public PlanWithProperties visitGroupId(GroupIdNode node, Context context)
        {
            PreferredProperties childPreference = context.getPreferredProperties().translate(translateGroupIdSymbols(node));
            PlanWithProperties child = planChild(node, context.withPreferredProperties(childPreference));
            return rebaseAndDeriveProperties(node, child);
        }

        private Function<Symbol, Optional<Symbol>> translateGroupIdSymbols(GroupIdNode node)
        {
            Map<Symbol, Symbol> invertedMappings = ImmutableBiMap.copyOf(node.getIdentityMappings()).inverse();
            List<Symbol> commonGroupingColumns = node.getCommonGroupingColumns();
            return symbol -> {
                if (invertedMappings.containsKey(symbol)) {
                    return Optional.of(invertedMappings.get(symbol));
                }

                if (commonGroupingColumns.contains(symbol)) {
                    return Optional.of(symbol);
                }

                return Optional.empty();
            };
        }

        @Override
        public PlanWithProperties visitMarkDistinct(MarkDistinctNode node, Context context)
        {
            PreferredProperties preferredChildProperties = PreferredProperties.partitionedWithLocal(ImmutableSet.copyOf(node.getDistinctSymbols()), grouped(node.getDistinctSymbols()))
                    .mergeWithParent(context.getPreferredProperties());
            PlanWithProperties child = node.getSource().accept(this, context.withPreferredProperties(preferredChildProperties));

            if (child.getProperties().isSingleNode() ||
                    !child.getProperties().isStreamPartitionedOn(node.getDistinctSymbols())) {
                child = withDerivedProperties(
                        partitionedExchange(
                                idAllocator.getNextId(),
                                REMOTE,
                                child.getNode(),
                                node.getDistinctSymbols(),
                                node.getHashSymbol()),
                        child.getProperties());
            }

            return rebaseAndDeriveProperties(node, child);
        }

        @Override
        public PlanWithProperties visitWindow(WindowNode node, Context context)
        {
            List<LocalProperty<Symbol>> desiredProperties = new ArrayList<>();
            if (!node.getPartitionBy().isEmpty()) {
                desiredProperties.add(new GroupingProperty<>(node.getPartitionBy()));
            }
            for (Symbol symbol : node.getOrderBy()) {
                desiredProperties.add(new SortingProperty<>(symbol, node.getOrderings().get(symbol)));
            }

            PlanWithProperties child = planChild(
                    node,
                    context.withPreferredProperties(
                            PreferredProperties.partitionedWithLocal(ImmutableSet.copyOf(node.getPartitionBy()), desiredProperties)
                                    .mergeWithParent(context.getPreferredProperties())));

            if (!child.getProperties().isStreamPartitionedOn(node.getPartitionBy())) {
                if (node.getPartitionBy().isEmpty()) {
                    child = withDerivedProperties(
                            gatheringExchange(idAllocator.getNextId(), REMOTE, child.getNode()),
                            child.getProperties());
                }
                else {
                    child = withDerivedProperties(
                            partitionedExchange(idAllocator.getNextId(), REMOTE, child.getNode(), node.getPartitionBy(), node.getHashSymbol()),
                            child.getProperties());
                }
            }

            return rebaseAndDeriveProperties(node, child);
        }

        @Override
        public PlanWithProperties visitRowNumber(RowNumberNode node, Context context)
        {
            if (node.getPartitionBy().isEmpty()) {
                PlanWithProperties child = planChild(node, context.withPreferredProperties(PreferredProperties.undistributed()));

                if (!child.getProperties().isSingleNode()) {
                    child = withDerivedProperties(
                            gatheringExchange(idAllocator.getNextId(), REMOTE, child.getNode()),
                            child.getProperties());
                }

                return rebaseAndDeriveProperties(node, child);
            }

            PlanWithProperties child = planChild(node, context.withPreferredProperties(
                    PreferredProperties.partitionedWithLocal(ImmutableSet.copyOf(node.getPartitionBy()), grouped(node.getPartitionBy()))
                            .mergeWithParent(context.getPreferredProperties())));

            // TODO: add config option/session property to force parallel plan if child is unpartitioned and window has a PARTITION BY clause
            if (!child.getProperties().isStreamPartitionedOn(node.getPartitionBy())) {
                child = withDerivedProperties(
                        partitionedExchange(
                                idAllocator.getNextId(),
                                REMOTE,
                                child.getNode(),
                                node.getPartitionBy(),
                                node.getHashSymbol()),
                        child.getProperties());
            }

            // TODO: streaming

            return rebaseAndDeriveProperties(node, child);
        }

        @Override
        public PlanWithProperties visitTopNRowNumber(TopNRowNumberNode node, Context context)
        {
            PreferredProperties preferredChildProperties;
            Function<PlanNode, PlanNode> addExchange;

            if (node.getPartitionBy().isEmpty()) {
                preferredChildProperties = PreferredProperties.any();
                addExchange = partial -> gatheringExchange(idAllocator.getNextId(), REMOTE, partial);
            }
            else {
                preferredChildProperties = PreferredProperties.partitionedWithLocal(ImmutableSet.copyOf(node.getPartitionBy()), grouped(node.getPartitionBy()))
                        .mergeWithParent(context.getPreferredProperties());
                addExchange = partial -> partitionedExchange(idAllocator.getNextId(), REMOTE, partial, node.getPartitionBy(), node.getHashSymbol());
            }

            PlanWithProperties child = planChild(node, context.withPreferredProperties(preferredChildProperties));
            if (!child.getProperties().isStreamPartitionedOn(node.getPartitionBy())) {
                // add exchange + push function to child
                child = withDerivedProperties(
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
                        child.getProperties());

                child = withDerivedProperties(addExchange.apply(child.getNode()), child.getProperties());
            }

            return rebaseAndDeriveProperties(node, child);
        }

        @Override
        public PlanWithProperties visitTopN(TopNNode node, Context context)
        {
            PlanWithProperties child = planChild(node, context.withPreferredProperties(PreferredProperties.any()));

            if (!child.getProperties().isSingleNode()) {
                child = withDerivedProperties(
                        new TopNNode(idAllocator.getNextId(), child.getNode(), node.getCount(), node.getOrderBy(), node.getOrderings(), true),
                        child.getProperties());

                child = withDerivedProperties(
                        gatheringExchange(idAllocator.getNextId(), REMOTE, child.getNode()),
                        child.getProperties());
            }

            return rebaseAndDeriveProperties(node, child);
        }

        @Override
        public PlanWithProperties visitSort(SortNode node, Context context)
        {
            PlanWithProperties child = planChild(node, context.withPreferredProperties(PreferredProperties.undistributed()));

            if (!child.getProperties().isSingleNode()) {
                child = withDerivedProperties(
                        gatheringExchange(idAllocator.getNextId(), REMOTE, child.getNode()),
                        child.getProperties());
            }

            return rebaseAndDeriveProperties(node, child);
        }

        @Override
        public PlanWithProperties visitLimit(LimitNode node, Context context)
        {
            PlanWithProperties child = planChild(node, context.withPreferredProperties(PreferredProperties.any()));

            if (!child.getProperties().isSingleNode()) {
                child = withDerivedProperties(
                        new LimitNode(idAllocator.getNextId(), child.getNode(), node.getCount(), true),
                        child.getProperties());

                child = withDerivedProperties(
                        gatheringExchange(idAllocator.getNextId(), REMOTE, child.getNode()),
                        child.getProperties());
            }

            return rebaseAndDeriveProperties(node, child);
        }

        @Override
        public PlanWithProperties visitDistinctLimit(DistinctLimitNode node, Context context)
        {
            PlanWithProperties child = planChild(node, context.withPreferredProperties(PreferredProperties.any()));

            if (!child.getProperties().isSingleNode()) {
                child = withDerivedProperties(
                        gatheringExchange(
                                idAllocator.getNextId(),
                                REMOTE,
                                new DistinctLimitNode(idAllocator.getNextId(), child.getNode(), node.getLimit(), true, node.getHashSymbol())),
                        child.getProperties());
            }

            return rebaseAndDeriveProperties(node, child);
        }

        @Override
        public PlanWithProperties visitFilter(FilterNode node, Context context)
        {
            if (node.getSource() instanceof TableScanNode) {
                return planTableScan((TableScanNode) node.getSource(), node.getPredicate(), context);
            }

            return rebaseAndDeriveProperties(node, planChild(node, context));
        }

        @Override
        public PlanWithProperties visitTableScan(TableScanNode node, Context context)
        {
            return planTableScan(node, BooleanLiteral.TRUE_LITERAL, context);
        }

        @Override
        public PlanWithProperties visitTableWriter(TableWriterNode node, Context context)
        {
            PlanWithProperties source = node.getSource().accept(this, context);

            Optional<PartitioningScheme> partitioningScheme = node.getPartitioningScheme();
            if (!partitioningScheme.isPresent() && redistributeWrites) {
                partitioningScheme = Optional.of(new PartitioningScheme(Partitioning.create(FIXED_RANDOM_DISTRIBUTION, ImmutableList.of()), source.getNode().getOutputSymbols()));
            }

            if (partitioningScheme.isPresent()) {
                source = withDerivedProperties(
                        partitionedExchange(
                                idAllocator.getNextId(),
                                REMOTE,
                                source.getNode(),
                                partitioningScheme.get()),
                        source.getProperties()
                );
            }
            return rebaseAndDeriveProperties(node, source);
        }

        private PlanWithProperties planTableScan(TableScanNode node, Expression predicate, Context context)
        {
            // don't include non-deterministic predicates
            Expression deterministicPredicate = stripNonDeterministicConjuncts(predicate);

            DomainTranslator.ExtractionResult decomposedPredicate = DomainTranslator.fromPredicate(
                    metadata,
                    session,
                    deterministicPredicate,
                    symbolAllocator.getTypes());

            TupleDomain<ColumnHandle> simplifiedConstraint = decomposedPredicate.getTupleDomain()
                    .transform(node.getAssignments()::get)
                    .intersect(node.getCurrentConstraint());

            Map<ColumnHandle, Symbol> assignments = ImmutableBiMap.copyOf(node.getAssignments()).inverse();

            Expression constraint = combineConjuncts(
                    deterministicPredicate,
                    DomainTranslator.toPredicate(node.getCurrentConstraint().transform(assignments::get)));

            // Layouts will be returned in order of the connector's preference
            List<TableLayoutResult> layouts = metadata.getLayouts(
                    session, node.getTable(),
                    new Constraint<>(simplifiedConstraint, bindings -> !shouldPrune(constraint, node.getAssignments(), bindings)),
                    Optional.of(node.getOutputSymbols().stream()
                            .map(node.getAssignments()::get)
                            .collect(toImmutableSet())));

            if (layouts.isEmpty()) {
                return new PlanWithProperties(
                        new ValuesNode(idAllocator.getNextId(), node.getOutputSymbols(), ImmutableList.of()),
                        ActualProperties.builder()
                                .global(singleStreamPartition())
                                .build());
            }

            // Filter out layouts that cannot supply all the required columns
            layouts = layouts.stream()
                    .filter(layoutHasAllNeededOutputs(node))
                    .collect(toList());
            checkState(!layouts.isEmpty(), "No usable layouts for %s", node);

            List<PlanWithProperties> possiblePlans = layouts.stream()
                    .map(layout -> {
                        TableScanNode tableScan = new TableScanNode(
                                node.getId(),
                                node.getTable(),
                                node.getOutputSymbols(),
                                node.getAssignments(),
                                Optional.of(layout.getLayout().getHandle()),
                                simplifiedConstraint.intersect(layout.getLayout().getPredicate()),
                                Optional.ofNullable(node.getOriginalConstraint()).orElse(predicate));

                        PlanWithProperties result = new PlanWithProperties(tableScan, deriveProperties(tableScan, ImmutableList.of()));

                        Expression resultingPredicate = combineConjuncts(
                                DomainTranslator.toPredicate(layout.getUnenforcedConstraint().transform(assignments::get)),
                                stripDeterministicConjuncts(predicate),
                                decomposedPredicate.getRemainingExpression());

                        if (!BooleanLiteral.TRUE_LITERAL.equals(resultingPredicate)) {
                            return withDerivedProperties(
                                    new FilterNode(idAllocator.getNextId(), result.getNode(), resultingPredicate),
                                    deriveProperties(tableScan, ImmutableList.of()));
                        }

                        return result;
                    })
                    .collect(toList());

            return pickPlan(possiblePlans, context);
        }

        private Predicate<TableLayoutResult> layoutHasAllNeededOutputs(TableScanNode node)
        {
            return layout -> !layout.getLayout().getColumns().isPresent()
                    || layout.getLayout().getColumns().get().containsAll(Lists.transform(node.getOutputSymbols(), node.getAssignments()::get));
        }

        /**
         * possiblePlans should be provided in layout preference order
         */
        private PlanWithProperties pickPlan(List<PlanWithProperties> possiblePlans, Context context)
        {
            checkArgument(!possiblePlans.isEmpty());

            if (preferStreamingOperators) {
                possiblePlans = new ArrayList<>(possiblePlans);
                Collections.sort(possiblePlans, Comparator.comparing(PlanWithProperties::getProperties, streamingExecutionPreference(context.getPreferredProperties()))); // stable sort; is Collections.min() guaranteed to be stable?
            }

            return possiblePlans.get(0);
        }

        private boolean shouldPrune(Expression predicate, Map<Symbol, ColumnHandle> assignments, Map<ColumnHandle, NullableValue> bindings)
        {
            List<Expression> conjuncts = extractConjuncts(predicate);
            IdentityHashMap<Expression, Type> expressionTypes = getExpressionTypes(session, metadata, parser, symbolAllocator.getTypes(), predicate);

            LookupSymbolResolver inputs = new LookupSymbolResolver(assignments, bindings);

            // If any conjuncts evaluate to FALSE or null, then the whole predicate will never be true and so the partition should be pruned
            for (Expression expression : conjuncts) {
                ExpressionInterpreter optimizer = ExpressionInterpreter.expressionOptimizer(expression, metadata, session, expressionTypes);
                Object optimized = optimizer.optimize(inputs);
                if (Boolean.FALSE.equals(optimized) || optimized == null || optimized instanceof NullLiteral) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public PlanWithProperties visitValues(ValuesNode node, Context context)
        {
            return new PlanWithProperties(
                    node,
                    ActualProperties.builder()
                            .global(singleStreamPartition())
                            .build());
        }

        @Override
        public PlanWithProperties visitExplainAnalyze(ExplainAnalyzeNode node, Context context)
        {
            PlanWithProperties child = planChild(node, context.withPreferredProperties(PreferredProperties.any()));

            // if the child is already a gathering exchange, don't add another
            if ((child.getNode() instanceof ExchangeNode) && ((ExchangeNode) child.getNode()).getType() == ExchangeNode.Type.GATHER) {
                return rebaseAndDeriveProperties(node, child);
            }

            // Always add an exchange because ExplainAnalyze should be in its own stage
            child = withDerivedProperties(
                    gatheringExchange(idAllocator.getNextId(), REMOTE, child.getNode()),
                    child.getProperties());

            return rebaseAndDeriveProperties(node, child);
        }

        @Override
        public PlanWithProperties visitTableFinish(TableFinishNode node, Context context)
        {
            PlanWithProperties child = planChild(node, context.withPreferredProperties(PreferredProperties.any()));

            // if the child is already a gathering exchange, don't add another
            if ((child.getNode() instanceof ExchangeNode) && ((ExchangeNode) child.getNode()).getType().equals(GATHER)) {
                return rebaseAndDeriveProperties(node, child);
            }

            if (!child.getProperties().isSingleNode() || !child.getProperties().isCoordinatorOnly()) {
                child = withDerivedProperties(
                        gatheringExchange(idAllocator.getNextId(), REMOTE, child.getNode()),
                        child.getProperties());
            }

            return rebaseAndDeriveProperties(node, child);
        }

        private <T> SetMultimap<T, T> createMapping(List<T> keys, List<T> values)
        {
            checkArgument(keys.size() == values.size(), "Inputs must have the same size");
            ImmutableSetMultimap.Builder<T, T> builder = ImmutableSetMultimap.builder();
            for (int i = 0; i < keys.size(); i++) {
                builder.put(keys.get(i), values.get(i));
            }
            return builder.build();
        }

        private <T> Function<T, Optional<T>> createTranslator(SetMultimap<T, T> inputToOutput)
        {
            return input -> inputToOutput.get(input).stream().findAny();
        }

        private <T> Function<T, T> createDirectTranslator(SetMultimap<T, T> inputToOutput)
        {
            return input -> inputToOutput.get(input).iterator().next();
        }

        @Override
        public PlanWithProperties visitJoin(JoinNode node, Context context)
        {
            List<Symbol> leftSymbols = Lists.transform(node.getCriteria(), JoinNode.EquiJoinClause::getLeft);
            List<Symbol> rightSymbols = Lists.transform(node.getCriteria(), JoinNode.EquiJoinClause::getRight);
            JoinNode.Type type = node.getType();

            PlanWithProperties left;
            PlanWithProperties right;

            boolean isCrossJoin = type == INNER && leftSymbols.isEmpty();
            if ((distributedJoins && !isCrossJoin && !isScalar(node.getRight())) || (type == FULL) || (type == RIGHT)) {
                // The implementation of full outer join only works if the data is hash partitioned. See LookupJoinOperators#buildSideOuterJoinUnvisitedPositions

                SetMultimap<Symbol, Symbol> rightToLeft = createMapping(rightSymbols, leftSymbols);
                SetMultimap<Symbol, Symbol> leftToRight = createMapping(leftSymbols, rightSymbols);

                left = node.getLeft().accept(this, context.withPreferredProperties(PreferredProperties.partitioned(ImmutableSet.copyOf(leftSymbols))));

                if (left.getProperties().isNodePartitionedOn(leftSymbols) && !(left.getProperties().isSingleNode() && distributedJoins)) {
                    Partitioning rightPartitioning = left.getProperties().translate(createTranslator(leftToRight)).getNodePartitioning().get();
                    right = node.getRight().accept(this, context.withPreferredProperties(PreferredProperties.partitioned(rightPartitioning)));
                    if (!right.getProperties().isNodePartitionedWith(left.getProperties(), rightToLeft::get)) {
                        right = withDerivedProperties(
                                partitionedExchange(idAllocator.getNextId(), REMOTE, right.getNode(), new PartitioningScheme(rightPartitioning, right.getNode().getOutputSymbols())),
                                right.getProperties());
                    }
                }
                else {
                    right = node.getRight().accept(this, context.withPreferredProperties(PreferredProperties.partitioned(ImmutableSet.copyOf(rightSymbols))));

                    if (right.getProperties().isNodePartitionedOn(rightSymbols) && !(right.getProperties().isSingleNode() && distributedJoins)) {
                        Partitioning leftPartitioning = right.getProperties().translate(createTranslator(rightToLeft)).getNodePartitioning().get();
                        left = withDerivedProperties(
                                partitionedExchange(idAllocator.getNextId(), REMOTE, left.getNode(), new PartitioningScheme(leftPartitioning, left.getNode().getOutputSymbols())),
                                left.getProperties());
                    }
                    else {
                        left = withDerivedProperties(
                                partitionedExchange(idAllocator.getNextId(), REMOTE, left.getNode(), leftSymbols, Optional.empty()),
                                left.getProperties());
                        right = withDerivedProperties(
                                partitionedExchange(idAllocator.getNextId(), REMOTE, right.getNode(), rightSymbols, Optional.empty()),
                                right.getProperties());
                    }
                }

                verify(left.getProperties().isNodePartitionedWith(right.getProperties(), leftToRight::get));

                // if colocated joins are disabled, force redistribute when using a custom partitioning
                if (!isColocatedJoinEnabled(session) && hasMultipleSources(left.getNode(), right.getNode())) {
                    Partitioning rightPartitioning = left.getProperties().translate(createTranslator(leftToRight)).getNodePartitioning().get();
                    right = withDerivedProperties(
                            partitionedExchange(idAllocator.getNextId(), REMOTE, right.getNode(), new PartitioningScheme(rightPartitioning, right.getNode().getOutputSymbols())),
                            right.getProperties());
                }
            }
            else {
                // Broadcast Join

                left = node.getLeft().accept(this, context.withPreferredProperties(PreferredProperties.any()));
                right = node.getRight().accept(this, context.withPreferredProperties(PreferredProperties.any()));

                if (left.getProperties().isSingleNode()) {
                    if (!right.getProperties().isSingleNode() ||
                            (!isColocatedJoinEnabled(session) && hasMultipleSources(left.getNode(), right.getNode()))) {
                        right = withDerivedProperties(
                                gatheringExchange(idAllocator.getNextId(), REMOTE, right.getNode()),
                                right.getProperties());
                    }
                }
                else {
                    right = withDerivedProperties(
                            replicatedExchange(idAllocator.getNextId(), REMOTE, right.getNode()),
                            right.getProperties());
                }
            }

            JoinNode result = new JoinNode(node.getId(),
                    type,
                    left.getNode(),
                    right.getNode(),
                    node.getCriteria(),
                    node.getFilter(),
                    node.getLeftHashSymbol(),
                    node.getRightHashSymbol());

            return new PlanWithProperties(result, deriveProperties(result, ImmutableList.of(left.getProperties(), right.getProperties())));
        }

        @Override
        public PlanWithProperties visitUnnest(UnnestNode node, Context context)
        {
            PreferredProperties translatedPreferred = context.getPreferredProperties().translate(symbol -> node.getReplicateSymbols().contains(symbol) ? Optional.of(symbol) : Optional.empty());

            return rebaseAndDeriveProperties(node, planChild(node, context.withPreferredProperties(translatedPreferred)));
        }

        @Override
        public PlanWithProperties visitSemiJoin(SemiJoinNode node, Context context)
        {
            PlanWithProperties source;
            PlanWithProperties filteringSource;

            if (distributedJoins && !context.isDownstreamIsDelete()) {
                List<Symbol> sourceSymbols = ImmutableList.of(node.getSourceJoinSymbol());
                List<Symbol> filteringSourceSymbols = ImmutableList.of(node.getFilteringSourceJoinSymbol());

                SetMultimap<Symbol, Symbol> sourceToFiltering = createMapping(sourceSymbols, filteringSourceSymbols);
                SetMultimap<Symbol, Symbol> filteringToSource = createMapping(filteringSourceSymbols, sourceSymbols);

                source = node.getSource().accept(this, context.withPreferredProperties(PreferredProperties.partitioned(ImmutableSet.copyOf(sourceSymbols))));

                if (source.getProperties().isNodePartitionedOn(sourceSymbols) && !(source.getProperties().isSingleNode() && distributedJoins)) {
                    Partitioning filteringPartitioning = source.getProperties().translate(createTranslator(sourceToFiltering)).getNodePartitioning().get();
                    filteringSource = node.getFilteringSource().accept(this, context.withPreferredProperties(PreferredProperties.partitionedWithNullsReplicated(filteringPartitioning)));
                    if (!source.getProperties().withReplicatedNulls(true).isNodePartitionedWith(filteringSource.getProperties(), sourceToFiltering::get)) {
                        filteringSource = withDerivedProperties(
                                partitionedExchange(idAllocator.getNextId(), REMOTE, filteringSource.getNode(), new PartitioningScheme(
                                        filteringPartitioning,
                                        filteringSource.getNode().getOutputSymbols(),
                                        Optional.empty(),
                                        true,
                                        Optional.empty())),
                                filteringSource.getProperties());
                    }
                }
                else {
                    filteringSource = node.getFilteringSource().accept(this, context.withPreferredProperties(PreferredProperties.partitionedWithNullsReplicated(ImmutableSet.copyOf(filteringSourceSymbols))));

                    if (filteringSource.getProperties().isNodePartitionedOn(filteringSourceSymbols, true) && !(filteringSource.getProperties().isSingleNode() && distributedJoins)) {
                        Partitioning sourcePartitioning = filteringSource.getProperties().translate(createTranslator(filteringToSource)).getNodePartitioning().get();
                        source = withDerivedProperties(
                                partitionedExchange(idAllocator.getNextId(), REMOTE, source.getNode(), new PartitioningScheme(sourcePartitioning, source.getNode().getOutputSymbols())),
                                source.getProperties());
                    }
                    else {
                        source = withDerivedProperties(
                                partitionedExchange(idAllocator.getNextId(), REMOTE, source.getNode(), sourceSymbols, Optional.empty()),
                                source.getProperties());
                        filteringSource = withDerivedProperties(
                                partitionedExchange(idAllocator.getNextId(), REMOTE, filteringSource.getNode(), filteringSourceSymbols, Optional.empty(), true),
                                filteringSource.getProperties());
                    }
                }

                verify(source.getProperties().withReplicatedNulls(true).isNodePartitionedWith(filteringSource.getProperties(), sourceToFiltering::get));

                // if colocated joins are disabled, force redistribute when using a custom partitioning
                if (!isColocatedJoinEnabled(session) && hasMultipleSources(source.getNode(), filteringSource.getNode())) {
                    Partitioning filteringPartitioning = source.getProperties().translate(createTranslator(sourceToFiltering)).getNodePartitioning().get();
                    filteringSource = withDerivedProperties(
                            partitionedExchange(idAllocator.getNextId(), REMOTE, filteringSource.getNode(), new PartitioningScheme(
                                    filteringPartitioning,
                                    filteringSource.getNode().getOutputSymbols(),
                                    Optional.empty(),
                                    true,
                                    Optional.empty())),
                            filteringSource.getProperties());
                }
            }
            else {
                source = node.getSource().accept(this, context.withPreferredProperties(PreferredProperties.any()));
                // Delete operator works fine even if TableScans on the filtering (right) side is not co-located with itself. It only cares about the corresponding TableScan,
                // which is always on the source (left) side. Therefore, hash-partitioned semi-join is always allowed on the filtering side.
                filteringSource = node.getFilteringSource().accept(this, context.withPreferredProperties(PreferredProperties.any()).withHashPartitionedSemiJoinBanned(false));

                // make filtering source match requirements of source
                if (source.getProperties().isSingleNode()) {
                    if (!filteringSource.getProperties().isSingleNode() ||
                            (!isColocatedJoinEnabled(session) && hasMultipleSources(source.getNode(), filteringSource.getNode()))) {
                        filteringSource = withDerivedProperties(
                                gatheringExchange(idAllocator.getNextId(), REMOTE, filteringSource.getNode()),
                                filteringSource.getProperties());
                    }
                }
                else {
                    filteringSource = withDerivedProperties(
                            replicatedExchange(idAllocator.getNextId(), REMOTE, filteringSource.getNode()),
                            filteringSource.getProperties());
                }
            }

            return rebaseAndDeriveProperties(node, ImmutableList.of(source, filteringSource));
        }

        @Override
        public PlanWithProperties visitIndexJoin(IndexJoinNode node, Context context)
        {
            List<Symbol> joinColumns = Lists.transform(node.getCriteria(), IndexJoinNode.EquiJoinClause::getProbe);

            // Only prefer grouping on join columns if no parent local property preferences
            List<LocalProperty<Symbol>> desiredLocalProperties = context.getPreferredProperties().getLocalProperties().isEmpty() ? grouped(joinColumns) : ImmutableList.of();

            PlanWithProperties probeSource = node.getProbeSource().accept(this, context.withPreferredProperties(
                    PreferredProperties.partitionedWithLocal(ImmutableSet.copyOf(joinColumns), desiredLocalProperties)
                            .mergeWithParent(context.getPreferredProperties())));
            ActualProperties probeProperties = probeSource.getProperties();

            PlanWithProperties indexSource = node.getIndexSource().accept(this, context.withPreferredProperties(PreferredProperties.any()));

            // TODO: allow repartitioning if unpartitioned to increase parallelism
            if (shouldRepartitionForIndexJoin(joinColumns, context.getPreferredProperties(), probeProperties)) {
                probeSource = withDerivedProperties(
                        partitionedExchange(idAllocator.getNextId(), REMOTE, probeSource.getNode(), joinColumns, node.getProbeHashSymbol()),
                        probeProperties);
            }

            // TODO: if input is grouped, create streaming join

            // index side is really a nested-loops plan, so don't add exchanges
            PlanNode result = ChildReplacer.replaceChildren(node, ImmutableList.of(probeSource.getNode(), node.getIndexSource()));
            return new PlanWithProperties(result, deriveProperties(result, ImmutableList.of(probeSource.getProperties(), indexSource.getProperties())));
        }

        private boolean shouldRepartitionForIndexJoin(List<Symbol> joinColumns, PreferredProperties parentPreferredProperties, ActualProperties probeProperties)
        {
            // See if distributed index joins are enabled
            if (!distributedIndexJoins) {
                return false;
            }

            // No point in repartitioning if the plan is not distributed
            if (probeProperties.isSingleNode()) {
                return false;
            }

            Optional<PreferredProperties.PartitioningProperties> parentPartitioningPreferences = parentPreferredProperties.getGlobalProperties()
                    .flatMap(PreferredProperties.Global::getPartitioningProperties);

            // Disable repartitioning if it would disrupt a parent's partitioning preference when streaming is enabled
            boolean parentAlreadyPartitionedOnChild = parentPartitioningPreferences
                    .map(partitioning -> probeProperties.isStreamPartitionedOn(partitioning.getPartitioningColumns()))
                    .orElse(false);
            if (preferStreamingOperators && parentAlreadyPartitionedOnChild) {
                return false;
            }

            // Otherwise, repartition if we need to align with the join columns
            if (!probeProperties.isStreamPartitionedOn(joinColumns)) {
                return true;
            }

            // If we are already partitioned on the join columns because the data has been forced effectively into one stream,
            // then we should repartition if that would make a difference (from the single stream state).
            return probeProperties.isEffectivelySingleStream() && probeProperties.isStreamRepartitionEffective(joinColumns);
        }

        @Override
        public PlanWithProperties visitIndexSource(IndexSourceNode node, Context context)
        {
            return new PlanWithProperties(
                    node,
                    ActualProperties.builder()
                            .global(singleStreamPartition())
                            .build());
        }

        private Function<Symbol, Optional<Symbol>> outputToInputTranslator(UnionNode node, int sourceIndex)
        {
            return symbol -> Optional.of(node.getSymbolMapping().get(symbol).get(sourceIndex));
        }

        private Partitioning selectUnionPartitioning(UnionNode node, Context context, PreferredProperties.PartitioningProperties parentPreference)
        {
            // Use the parent's requested partitioning if available
            if (parentPreference.getPartitioning().isPresent()) {
                return parentPreference.getPartitioning().get();
            }

            // Try planning the children to see if any of them naturally produce a partitioning (for now, just select the first)
            boolean nullsReplicated = parentPreference.isNullsReplicated();
            for (int sourceIndex = 0; sourceIndex < node.getSources().size(); sourceIndex++) {
                PreferredProperties.PartitioningProperties childPartitioning = parentPreference.translate(outputToInputTranslator(node, sourceIndex)).get();
                PreferredProperties childPreferred = PreferredProperties.builder()
                        .global(PreferredProperties.Global.distributed(childPartitioning.withNullsReplicated(nullsReplicated)))
                        .build();
                PlanWithProperties child = node.getSources().get(sourceIndex).accept(this, context.withPreferredProperties(childPreferred));
                if (child.getProperties().isNodePartitionedOn(childPartitioning.getPartitioningColumns(), nullsReplicated)) {
                    Function<Symbol, Optional<Symbol>> childToParent = createTranslator(createMapping(node.sourceOutputLayout(sourceIndex), node.getOutputSymbols()));
                    return child.getProperties().translate(childToParent).getNodePartitioning().get();
                }
            }

            // Otherwise, choose an arbitrary partitioning over the columns
            return Partitioning.create(FIXED_HASH_DISTRIBUTION, ImmutableList.copyOf(parentPreference.getPartitioningColumns()));
        }

        @Override
        public PlanWithProperties visitUnion(UnionNode node, Context context)
        {
            PreferredProperties parentPreference = context.getPreferredProperties();
            Optional<PreferredProperties.Global> parentGlobal = parentPreference.getGlobalProperties();
            if (parentGlobal.isPresent() && parentGlobal.get().isDistributed() && parentGlobal.get().getPartitioningProperties().isPresent()) {
                PreferredProperties.PartitioningProperties parentPartitioningPreference = parentGlobal.get().getPartitioningProperties().get();
                boolean nullsReplicated = parentPartitioningPreference.isNullsReplicated();
                Partitioning desiredParentPartitioning = selectUnionPartitioning(node, context, parentPartitioningPreference);

                ImmutableList.Builder<PlanNode> partitionedSources = ImmutableList.builder();
                ImmutableListMultimap.Builder<Symbol, Symbol> outputToSourcesMapping = ImmutableListMultimap.builder();

                for (int sourceIndex = 0; sourceIndex < node.getSources().size(); sourceIndex++) {
                    Partitioning childPartitioning = desiredParentPartitioning.translate(createDirectTranslator(createMapping(node.getOutputSymbols(), node.sourceOutputLayout(sourceIndex))));

                    PreferredProperties childPreferred = PreferredProperties.builder()
                            .global(PreferredProperties.Global.distributed(PreferredProperties.PartitioningProperties.partitioned(childPartitioning)
                                    .withNullsReplicated(nullsReplicated)))
                            .build();

                    PlanWithProperties source = node.getSources().get(sourceIndex).accept(this, context.withPreferredProperties(childPreferred));
                    if (!source.getProperties().isNodePartitionedOn(childPartitioning, nullsReplicated)) {
                        source = withDerivedProperties(
                                partitionedExchange(
                                        idAllocator.getNextId(),
                                        REMOTE,
                                        source.getNode(),
                                        new PartitioningScheme(
                                                childPartitioning,
                                                source.getNode().getOutputSymbols(),
                                                Optional.empty(),
                                                nullsReplicated,
                                                Optional.empty())),
                                source.getProperties());
                    }
                    partitionedSources.add(source.getNode());

                    for (int column = 0; column < node.getOutputSymbols().size(); column++) {
                        outputToSourcesMapping.put(node.getOutputSymbols().get(column), node.sourceOutputLayout(sourceIndex).get(column));
                    }
                }
                UnionNode newNode = new UnionNode(
                        node.getId(),
                        partitionedSources.build(),
                        outputToSourcesMapping.build(),
                        ImmutableList.copyOf(outputToSourcesMapping.build().keySet()));

                return new PlanWithProperties(
                        newNode,
                        ActualProperties.builder()
                                .global(partitionedOn(desiredParentPartitioning, Optional.of(desiredParentPartitioning)))
                                .build()
                                .withReplicatedNulls(parentPartitioningPreference.isNullsReplicated()));
            }

            // first, classify children into partitioned and unpartitioned
            List<PlanNode> unpartitionedChildren = new ArrayList<>();
            List<List<Symbol>> unpartitionedOutputLayouts = new ArrayList<>();

            List<PlanNode> partitionedChildren = new ArrayList<>();
            List<List<Symbol>> partitionedOutputLayouts = new ArrayList<>();

            List<PlanNode> sources = node.getSources();
            for (int i = 0; i < sources.size(); i++) {
                PlanWithProperties child = sources.get(i).accept(this, context.withPreferredProperties(PreferredProperties.any()));
                if (child.getProperties().isSingleNode()) {
                    unpartitionedChildren.add(child.getNode());
                    unpartitionedOutputLayouts.add(node.sourceOutputLayout(i));
                }
                else {
                    partitionedChildren.add(child.getNode());
                    // union may drop or duplicate symbols from the input so we must provide an exact mapping
                    partitionedOutputLayouts.add(node.sourceOutputLayout(i));
                }
            }

            PlanNode result = null;
            if (!partitionedChildren.isEmpty()) {
                // add an exchange above partitioned inputs and fold it into the
                // set of unpartitioned inputs
                // NOTE: this must provide the explicit imput mapping as unions may drop or duplicate symbols
                result = new ExchangeNode(
                        idAllocator.getNextId(),
                        GATHER,
                        REMOTE,
                        new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), node.getOutputSymbols()),
                        partitionedChildren,
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

                result = new UnionNode(node.getId(), unpartitionedChildren, mappings.build(), ImmutableList.copyOf(mappings.build().keySet()));
            }

            return new PlanWithProperties(
                    result,
                    ActualProperties.builder()
                            .global(singleStreamPartition())
                            .build());
        }

        private PlanWithProperties planChild(PlanNode node, Context context)
        {
            return getOnlyElement(node.getSources()).accept(this, context);
        }

        private PlanWithProperties rebaseAndDeriveProperties(PlanNode node, PlanWithProperties child)
        {
            return withDerivedProperties(
                    ChildReplacer.replaceChildren(node, ImmutableList.of(child.getNode())),
                    child.getProperties());
        }

        private PlanWithProperties rebaseAndDeriveProperties(PlanNode node, List<PlanWithProperties> children)
        {
            PlanNode result = ChildReplacer.replaceChildren(node, children.stream().map(PlanWithProperties::getNode).collect(toList()));
            return new PlanWithProperties(result, deriveProperties(result, children.stream().map(PlanWithProperties::getProperties).collect(toList())));
        }

        private PlanWithProperties withDerivedProperties(PlanNode node, ActualProperties inputProperties)
        {
            return new PlanWithProperties(node, deriveProperties(node, inputProperties));
        }

        private ActualProperties deriveProperties(PlanNode result, ActualProperties inputProperties)
        {
            return PropertyDerivations.deriveProperties(result, inputProperties, metadata, session, symbolAllocator.getTypes(), parser);
        }

        private ActualProperties deriveProperties(PlanNode result, List<ActualProperties> inputProperties)
        {
            return PropertyDerivations.deriveProperties(result, inputProperties, metadata, session, symbolAllocator.getTypes(), parser);
        }
    }

    private static Map<Symbol, Symbol> computeIdentityTranslations(Map<Symbol, Expression> assignments)
    {
        Map<Symbol, Symbol> outputToInput = new HashMap<>();
        for (Map.Entry<Symbol, Expression> assignment : assignments.entrySet()) {
            if (assignment.getValue() instanceof SymbolReference) {
                outputToInput.put(assignment.getKey(), Symbol.from(assignment.getValue()));
            }
        }
        return outputToInput;
    }

    @VisibleForTesting
    static Comparator<ActualProperties> streamingExecutionPreference(PreferredProperties preferred)
    {
        // Calculating the matches can be a bit expensive, so cache the results between comparisons
        LoadingCache<List<LocalProperty<Symbol>>, List<Optional<LocalProperty<Symbol>>>> matchCache = CacheBuilder.newBuilder()
                .build(new CacheLoader<List<LocalProperty<Symbol>>, List<Optional<LocalProperty<Symbol>>>>()
                {
                    @Override
                    public List<Optional<LocalProperty<Symbol>>> load(List<LocalProperty<Symbol>> actualProperties)
                    {
                        return LocalProperties.match(actualProperties, preferred.getLocalProperties());
                    }
                });

        return (actual1, actual2) -> {
            List<Optional<LocalProperty<Symbol>>> matchLayout1 = matchCache.getUnchecked(actual1.getLocalProperties());
            List<Optional<LocalProperty<Symbol>>> matchLayout2 = matchCache.getUnchecked(actual2.getLocalProperties());

            return ComparisonChain.start()
                    .compareTrueFirst(hasLocalOptimization(preferred.getLocalProperties(), matchLayout1), hasLocalOptimization(preferred.getLocalProperties(), matchLayout2))
                    .compareTrueFirst(meetsPartitioningRequirements(preferred, actual1), meetsPartitioningRequirements(preferred, actual2))
                    .compare(matchLayout1, matchLayout2, matchedLayoutPreference())
                    .result();
        };
    }

    private static <T> boolean hasLocalOptimization(List<LocalProperty<T>> desiredLayout, List<Optional<LocalProperty<T>>> matchResult)
    {
        checkArgument(desiredLayout.size() == matchResult.size());
        if (matchResult.isEmpty()) {
            return false;
        }
        // Optimizations can be applied if the first LocalProperty has been modified in the match in any way
        return !matchResult.get(0).equals(Optional.of(desiredLayout.get(0)));
    }

    private static boolean meetsPartitioningRequirements(PreferredProperties preferred, ActualProperties actual)
    {
        if (!preferred.getGlobalProperties().isPresent()) {
            return true;
        }
        PreferredProperties.Global preferredGlobal = preferred.getGlobalProperties().get();
        if (!preferredGlobal.isDistributed()) {
            return actual.isSingleNode();
        }
        if (!preferredGlobal.getPartitioningProperties().isPresent()) {
            return !actual.isSingleNode();
        }
        return actual.isStreamPartitionedOn(preferredGlobal.getPartitioningProperties().get().getPartitioningColumns());
    }

    // Prefer the match result that satisfied the most requirements
    private static <T> Comparator<List<Optional<LocalProperty<T>>>> matchedLayoutPreference()
    {
        return (matchLayout1, matchLayout2) -> {
            Iterator<Optional<LocalProperty<T>>> match1Iterator = matchLayout1.iterator();
            Iterator<Optional<LocalProperty<T>>> match2Iterator = matchLayout2.iterator();
            while (match1Iterator.hasNext() && match2Iterator.hasNext()) {
                Optional<LocalProperty<T>> match1 = match1Iterator.next();
                Optional<LocalProperty<T>> match2 = match2Iterator.next();
                if (match1.isPresent() && match2.isPresent()) {
                    return Integer.compare(match1.get().getColumns().size(), match2.get().getColumns().size());
                }
                else if (match1.isPresent()) {
                    return 1;
                }
                else if (match2.isPresent()) {
                    return -1;
                }
            }
            checkState(!match1Iterator.hasNext() && !match2Iterator.hasNext()); // Should be the same size
            return 0;
        };
    }

    @VisibleForTesting
    static class PlanWithProperties
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
}
