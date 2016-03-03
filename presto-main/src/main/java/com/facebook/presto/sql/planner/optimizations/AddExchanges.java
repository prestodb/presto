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
import com.facebook.presto.sql.planner.PartitionFunctionBinding;
import com.facebook.presto.sql.planner.PartitionFunctionBinding.PartitionFunctionArgumentBinding;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.ChildReplacer;
import com.facebook.presto.sql.planner.plan.DeleteNode;
import com.facebook.presto.sql.planner.plan.DistinctLimitNode;
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
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
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.SetMultimap;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.facebook.presto.sql.ExpressionUtils.combineConjuncts;
import static com.facebook.presto.sql.ExpressionUtils.extractConjuncts;
import static com.facebook.presto.sql.ExpressionUtils.stripDeterministicConjuncts;
import static com.facebook.presto.sql.ExpressionUtils.stripNonDeterministicConjuncts;
import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.getExpressionTypes;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_RANDOM_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.optimizations.ActualProperties.Global.partitionedOn;
import static com.facebook.presto.sql.planner.optimizations.ActualProperties.Global.singleStreamPartition;
import static com.facebook.presto.sql.planner.optimizations.LocalProperties.grouped;
import static com.facebook.presto.sql.planner.plan.AggregationNode.Step.FINAL;
import static com.facebook.presto.sql.planner.plan.AggregationNode.Step.PARTIAL;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.gatheringExchange;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.partitionedExchange;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.replicatedExchange;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.FULL;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.RIGHT;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableSet;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.stream.Collectors.toList;

public class AddExchanges
        extends PlanOptimizer
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
        boolean distributedJoinEnabled = SystemSessionProperties.isDistributedJoinEnabled(session);
        boolean distributedIndexJoinEnabled = SystemSessionProperties.isDistributedIndexJoinEnabled(session);
        boolean redistributeWrites = SystemSessionProperties.isRedistributeWrites(session);
        boolean preferStreamingOperators = SystemSessionProperties.preferStreamingOperators(session);
        PlanWithProperties result = plan.accept(new Rewriter(symbolAllocator, idAllocator, symbolAllocator, session, distributedIndexJoinEnabled, distributedJoinEnabled, preferStreamingOperators, redistributeWrites), new Context(PreferredProperties.any(), false));
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

        public Rewriter(SymbolAllocator allocator, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator, Session session, boolean distributedIndexJoins, boolean distributedJoins, boolean preferStreamingOperators, boolean redistributeWrites)
        {
            this.allocator = allocator;
            this.idAllocator = idAllocator;
            this.symbolAllocator = symbolAllocator;
            this.session = session;
            this.distributedIndexJoins = distributedIndexJoins;
            this.distributedJoins = distributedJoins;
            this.preferStreamingOperators = preferStreamingOperators;
            this.redistributeWrites = redistributeWrites;
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
                        gatheringExchange(idAllocator.getNextId(), child.getNode()),
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
                        gatheringExchange(idAllocator.getNextId(), child.getNode()),
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

            PreferredProperties preferredProperties = node.getGroupBy().isEmpty()
                    ? PreferredProperties.any()
                    : PreferredProperties.derivePreferences(context.getPreferredProperties(), ImmutableSet.copyOf(node.getGroupBy()), Optional.of(node.getGroupBy()), grouped(node.getGroupBy()));

            PlanWithProperties child = planChild(node, context.withPreferredProperties(preferredProperties));

            if (child.getProperties().isSingleNode()) {
                // If already unpartitioned, just drop the single aggregation back on
                return rebaseAndDeriveProperties(node, child);
            }

            if (node.getGroupBy().isEmpty()) {
                if (decomposable) {
                    return splitAggregation(node, child, partial -> gatheringExchange(idAllocator.getNextId(), partial));
                }
                else {
                    child = withDerivedProperties(
                            gatheringExchange(idAllocator.getNextId(), child.getNode()),
                            child.getProperties());

                    return rebaseAndDeriveProperties(node, child);
                }
            }
            else {
                if (child.getProperties().isNodePartitionedOn(node.getGroupBy())) {
                    return rebaseAndDeriveProperties(node, child);
                }
                else {
                    if (decomposable) {
                        Function<PlanNode, PlanNode> exchanger = null;
                        if (!child.getProperties().isNodePartitionedOn(node.getGroupBy())) {
                            exchanger = partial -> partitionedExchange(
                                    idAllocator.getNextId(),
                                    partial,
                                    node.getGroupBy(),
                                    node.getHashSymbol());
                        }
                        return splitAggregation(node, child, exchanger);
                    }
                    else {
                        child = withDerivedProperties(
                                partitionedExchange(idAllocator.getNextId(), child.getNode(), node.getGroupBy(), node.getHashSymbol()),
                                child.getProperties());
                        return rebaseAndDeriveProperties(node, child);
                    }
                }
            }
        }

        @NotNull
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
                finalCalls.put(entry.getKey(), new FunctionCall(QualifiedName.of(signature.getName()), ImmutableList.<Expression>of(new QualifiedNameReference(intermediateSymbol.toQualifiedName()))));
            }

            PlanWithProperties partial = withDerivedProperties(
                    new AggregationNode(
                            idAllocator.getNextId(),
                            newChild.getNode(),
                            node.getGroupBy(),
                            intermediateCalls,
                            intermediateFunctions,
                            intermediateMask,
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
                            FINAL,
                            Optional.empty(),
                            node.getConfidence(),
                            node.getHashSymbol()),
                    deriveProperties(source, partial.getProperties()));
        }

        @Override
        public PlanWithProperties visitMarkDistinct(MarkDistinctNode node, Context context)
        {
            PreferredProperties preferredChildProperties = PreferredProperties.derivePreferences(context.getPreferredProperties(), ImmutableSet.copyOf(node.getDistinctSymbols()), Optional.of(node.getDistinctSymbols()), grouped(node.getDistinctSymbols()));
            PlanWithProperties child = node.getSource().accept(this, context.withPreferredProperties(preferredChildProperties));

            if (child.getProperties().isSingleNode() ||
                    !child.getProperties().isStreamPartitionedOn(node.getDistinctSymbols())) {
                child = withDerivedProperties(
                        partitionedExchange(
                                idAllocator.getNextId(),
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
                    context.withPreferredProperties(PreferredProperties.derivePreferences(context.getPreferredProperties(), ImmutableSet.copyOf(node.getPartitionBy()), desiredProperties)));

            if (!child.getProperties().isStreamPartitionedOn(node.getPartitionBy())) {
                if (node.getPartitionBy().isEmpty()) {
                    child = withDerivedProperties(
                            gatheringExchange(idAllocator.getNextId(), child.getNode()),
                            child.getProperties());
                }
                else {
                    child = withDerivedProperties(
                            partitionedExchange(idAllocator.getNextId(), child.getNode(), node.getPartitionBy(), node.getHashSymbol()),
                            child.getProperties());
                }
            }

            Iterator<Optional<LocalProperty<Symbol>>> matchIterator = LocalProperties.match(child.getProperties().getLocalProperties(), desiredProperties).iterator();

            Set<Symbol> prePartitionedInputs = ImmutableSet.of();
            if (!node.getPartitionBy().isEmpty()) {
                Optional<LocalProperty<Symbol>> groupingRequirement = matchIterator.next();
                Set<Symbol> unPartitionedInputs = groupingRequirement.map(LocalProperty::getColumns).orElse(ImmutableSet.of());
                prePartitionedInputs = node.getPartitionBy().stream()
                        .filter(symbol -> !unPartitionedInputs.contains(symbol))
                        .collect(toImmutableSet());
            }

            int preSortedOrderPrefix = 0;
            if (prePartitionedInputs.equals(ImmutableSet.copyOf(node.getPartitionBy()))) {
                while (matchIterator.hasNext() && !matchIterator.next().isPresent()) {
                    preSortedOrderPrefix++;
                }
            }

            return withDerivedProperties(
                    new WindowNode(
                            node.getId(),
                            child.getNode(),
                            node.getPartitionBy(),
                            node.getOrderBy(),
                            node.getOrderings(),
                            node.getFrame(),
                            node.getWindowFunctions(),
                            node.getSignatures(),
                            node.getHashSymbol(),
                            prePartitionedInputs,
                            preSortedOrderPrefix),
                    child.getProperties());
        }

        @Override
        public PlanWithProperties visitRowNumber(RowNumberNode node, Context context)
        {
            if (node.getPartitionBy().isEmpty()) {
                PlanWithProperties child = planChild(node, context.withPreferredProperties(PreferredProperties.undistributed()));

                if (!child.getProperties().isSingleNode()) {
                    child = withDerivedProperties(
                            gatheringExchange(idAllocator.getNextId(), child.getNode()),
                            child.getProperties());
                }

                return rebaseAndDeriveProperties(node, child);
            }

            PlanWithProperties child = planChild(node, context.withPreferredProperties(PreferredProperties.derivePreferences(context.getPreferredProperties(), ImmutableSet.copyOf(node.getPartitionBy()), grouped(node.getPartitionBy()))));

            // TODO: add config option/session property to force parallel plan if child is unpartitioned and window has a PARTITION BY clause
            if (!child.getProperties().isStreamPartitionedOn(node.getPartitionBy())) {
                child = withDerivedProperties(
                        partitionedExchange(
                                idAllocator.getNextId(),
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
                addExchange = partial -> gatheringExchange(idAllocator.getNextId(), partial);
            }
            else {
                preferredChildProperties = PreferredProperties.derivePreferences(context.getPreferredProperties(), ImmutableSet.copyOf(node.getPartitionBy()), grouped(node.getPartitionBy()));
                addExchange = partial -> partitionedExchange(idAllocator.getNextId(), partial, node.getPartitionBy(), node.getHashSymbol());
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
                        gatheringExchange(idAllocator.getNextId(), child.getNode()),
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
                        gatheringExchange(idAllocator.getNextId(), child.getNode()),
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
                        new LimitNode(idAllocator.getNextId(), child.getNode(), node.getCount()),
                        child.getProperties());

                child = withDerivedProperties(
                        gatheringExchange(idAllocator.getNextId(), child.getNode()),
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
                                new DistinctLimitNode(idAllocator.getNextId(), child.getNode(), node.getLimit(), node.getHashSymbol())),
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

            Optional<PartitionFunctionBinding> partitionFunction = node.getPartitionFunction();
            if (!partitionFunction.isPresent() && redistributeWrites) {
                partitionFunction = Optional.of(new PartitionFunctionBinding(FIXED_RANDOM_DISTRIBUTION, source.getNode().getOutputSymbols(), ImmutableList.of()));
            }

            if (partitionFunction.isPresent()) {
                source = withDerivedProperties(
                        partitionedExchange(
                                idAllocator.getNextId(),
                                source.getNode(),
                                partitionFunction.get()),
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
        public PlanWithProperties visitTableFinish(TableFinishNode node, Context context)
        {
            PlanWithProperties child = planChild(node, context.withPreferredProperties(PreferredProperties.any()));

            // if the child is already a gathering exchange, don't add another
            if ((child.getNode() instanceof ExchangeNode) && ((ExchangeNode) child.getNode()).getType().equals(ExchangeNode.Type.GATHER)) {
                return rebaseAndDeriveProperties(node, child);
            }

            if (!child.getProperties().isSingleNode() || !child.getProperties().isCoordinatorOnly()) {
                child = withDerivedProperties(
                        gatheringExchange(idAllocator.getNextId(), child.getNode()),
                        child.getProperties());
            }

            return rebaseAndDeriveProperties(node, child);
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
            boolean joinWithNonScalar = node.getRight().accept(new IsScalarPlanVisitor(), null);
            if ((distributedJoins && !isCrossJoin && !joinWithNonScalar) || type == FULL || type == RIGHT) {
                // The implementation of full outer join only works if the data is hash partitioned. See LookupJoinOperators#buildSideOuterJoinUnvisitedPositions

                left = node.getLeft().accept(this, context.withPreferredProperties(PreferredProperties.hashPartitioned(leftSymbols)));
                right = node.getRight().accept(this, context.withPreferredProperties(PreferredProperties.hashPartitioned(rightSymbols)));

                if (!left.getProperties().isNodePartitionedOn(leftSymbols) || (distributedJoins && left.getProperties().isSingleNode())) {
                    left = withDerivedProperties(
                            partitionedExchange(idAllocator.getNextId(), left.getNode(), leftSymbols, node.getLeftHashSymbol()),
                            left.getProperties());
                }

                if (left.getProperties().isSingleNode()) {
                    // If necessary, gather right to the single-node
                    if (!right.getProperties().isSingleNode()) {
                        right = withDerivedProperties(
                                gatheringExchange(idAllocator.getNextId(), right.getNode()),
                                right.getProperties());
                    }
                }
                else {
                    // translate the partition arguments on the left symbols to the right symbols
                    SetMultimap<Symbol, Symbol> leftToRight = HashMultimap.create();
                    for (int i = 0; i < leftSymbols.size(); i++) {
                        leftToRight.put(leftSymbols.get(i), rightSymbols.get(i));
                    }

                    if (!left.getProperties().isNodePartitionedWith(right.getProperties(), leftToRight::get)) {
                        Function<Symbol, Optional<Symbol>> leftToRightTranslator = leftSymbol -> leftToRight.get(leftSymbol).stream().findAny();
                        Optional<List<PartitionFunctionArgumentBinding>> rightPartitionColumns = left.getProperties().translate(leftToRightTranslator).getNodePartitioningColumns();

                        verify(rightPartitionColumns.isPresent(), "Could not translate JOIN probe partitioning to build symbols");

                        PartitionFunctionBinding partitionFunction =  new PartitionFunctionBinding(
                                left.getProperties().getNodePartitioningHandle().get(),
                                node.getRight().getOutputSymbols(),
                                rightPartitionColumns.get(),
                                Optional.empty());

                        right = withDerivedProperties(
                                partitionedExchange(idAllocator.getNextId(), right.getNode(), partitionFunction),
                                right.getProperties());
                    }
                }
            }
            else {
                // Broadcast Join
                // It can only be INNER or LEFT here. Therefore, no flipping is necessary even though the below code assumes the node is not RIGHT.

                left = node.getLeft().accept(this, context.withPreferredProperties(PreferredProperties.any()));
                right = node.getRight().accept(this, context.withPreferredProperties(PreferredProperties.any()));

                if (left.getProperties().isSingleNode() && !right.getProperties().isSingleNode()) {
                    // force single-node join
                    // TODO: if inner join, flip order and do a broadcast join
                    right = withDerivedProperties(
                            gatheringExchange(idAllocator.getNextId(), right.getNode()),
                            right.getProperties());
                }
                else if (!left.getProperties().isSingleNode() &&
                        (!left.getProperties().isNodePartitionedOn(FIXED_HASH_DISTRIBUTION, leftSymbols) || !right.getProperties().isNodePartitionedOn(FIXED_HASH_DISTRIBUTION, rightSymbols))) {
                    right = withDerivedProperties(
                            replicatedExchange(idAllocator.getNextId(), right.getNode()),
                            right.getProperties());
                }
            }

            JoinNode result = new JoinNode(node.getId(),
                    type,
                    left.getNode(),
                    right.getNode(),
                    node.getCriteria(),
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

                source = node.getSource().accept(this, context.withPreferredProperties(PreferredProperties.hashPartitioned(sourceSymbols)));
                // Child will not satisfy hash with null replicate anyways, and repartition is always necessary.
                // Therefore, tell the filtering source pick whatever partition it likes.
                filteringSource = node.getFilteringSource().accept(this, context.withPreferredProperties(PreferredProperties.any()));

                // force partitioning if source isn't already partitioned on sourceSymbols
                if (!source.getProperties().isNodePartitionedOn(FIXED_HASH_DISTRIBUTION, sourceSymbols)) {
                    source = withDerivedProperties(
                            partitionedExchange(idAllocator.getNextId(), source.getNode(), sourceSymbols, node.getSourceHashSymbol()),
                            source.getProperties());
                }

                // The following statements would normally be written as: if (condition) { filteringSource = ...; }
                // However, the if-condition will always evaluate to true in this case because no externally-visible node produces partition with null replicate.
                // As a result, it is written as checkState instead.
                PartitionFunctionBinding partitionFunction = new PartitionFunctionBinding(
                        FIXED_HASH_DISTRIBUTION,
                        filteringSource.getNode().getOutputSymbols(),
                        filteringSourceSymbols.stream()
                                .map(PartitionFunctionArgumentBinding::new)
                                .collect(toImmutableList()),
                        node.getFilteringSourceHashSymbol(),
                        true,
                        Optional.empty());
                filteringSource = withDerivedProperties(
                        partitionedExchange(
                                idAllocator.getNextId(),
                                filteringSource.getNode(),
                                partitionFunction),
                        filteringSource.getProperties());
            }
            else {
                source = node.getSource().accept(this, context.withPreferredProperties(PreferredProperties.any()));
                // Delete operator works fine even if TableScans on the filtering (right) side is not co-located with itself. It only cares about the corresponding TableScan,
                // which is always on the source (left) side. Therefore, hash-partitioned semi-join is always allowed on the filtering side.
                filteringSource = node.getFilteringSource().accept(this, context.withPreferredProperties(PreferredProperties.any()).withHashPartitionedSemiJoinBanned(false));

                // make filtering source match requirements of source
                if (source.getProperties().isSingleNode()) {
                    filteringSource = withDerivedProperties(
                            gatheringExchange(idAllocator.getNextId(), filteringSource.getNode()),
                            filteringSource.getProperties());
                }
                else {
                    filteringSource = withDerivedProperties(
                            replicatedExchange(idAllocator.getNextId(), filteringSource.getNode()),
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

            PlanWithProperties probeSource = node.getProbeSource().accept(this, context.withPreferredProperties(PreferredProperties.derivePreferences(context.getPreferredProperties(), ImmutableSet.copyOf(joinColumns), desiredLocalProperties)));
            ActualProperties probeProperties = probeSource.getProperties();

            PlanWithProperties indexSource = node.getIndexSource().accept(this, context.withPreferredProperties(PreferredProperties.any()));

            // TODO: allow repartitioning if unpartitioned to increase parallelism
            if (shouldRepartitionForIndexJoin(joinColumns, context.getPreferredProperties(), probeProperties)) {
                probeSource = withDerivedProperties(
                        partitionedExchange(idAllocator.getNextId(), probeSource.getNode(), joinColumns, node.getProbeHashSymbol()),
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

            Optional<PreferredProperties.Partitioning> parentPartitioningPreferences = parentPreferredProperties.getGlobalProperties()
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

        @Override
        public PlanWithProperties visitUnion(UnionNode node, Context context)
        {
            if (!context.getPreferredProperties().getGlobalProperties().isPresent() || !context.getPreferredProperties().getGlobalProperties().get().isHashPartitioned()) {
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
                            ExchangeNode.Type.GATHER,
                            new PartitionFunctionBinding(SINGLE_DISTRIBUTION, node.getOutputSymbols(), ImmutableList.of()),
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

            // hash partition the sources
            List<Symbol> hashingColumns = context.getPreferredProperties().getGlobalProperties().get().getPartitioningProperties().get().getHashingOrder().get();

            ImmutableList.Builder<PlanNode> partitionedSources = ImmutableList.builder();
            ImmutableListMultimap.Builder<Symbol, Symbol> outputToSourcesMapping = ImmutableListMultimap.builder();

            for (int sourceIndex = 0; sourceIndex < node.getSources().size(); sourceIndex++) {
                ImmutableList.Builder<Symbol> hashColumnsBuilder = ImmutableList.builder();
                for (Symbol column : hashingColumns) {
                    hashColumnsBuilder.add(node.getSymbolMapping().get(column).get(sourceIndex));
                }
                List<Symbol> sourceHashColumns = hashColumnsBuilder.build();

                PlanWithProperties source = node.getSources().get(sourceIndex).accept(this, context.withPreferredProperties(PreferredProperties.hashPartitioned(sourceHashColumns)));
                if (!source.getProperties().isNodePartitionedOn(FIXED_HASH_DISTRIBUTION, sourceHashColumns)) {
                    source = withDerivedProperties(
                            partitionedExchange(
                                    idAllocator.getNextId(),
                                    source.getNode(),
                                    sourceHashColumns,
                                    Optional.empty()),
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

            List<PartitionFunctionArgumentBinding> hashArguments = hashingColumns.stream()
                    .map(PartitionFunctionArgumentBinding::new)
                    .collect(toImmutableList());

            return new PlanWithProperties(
                    newNode,
                    ActualProperties.builder()
                            .global(partitionedOn(FIXED_HASH_DISTRIBUTION, hashArguments, Optional.of(hashArguments)))
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
            if (assignment.getValue() instanceof QualifiedNameReference) {
                outputToInput.put(assignment.getKey(), Symbol.fromQualifiedName(((QualifiedNameReference) assignment.getValue()).getName()));
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

    private static final class IsScalarPlanVisitor
            extends PlanVisitor<Void, Boolean>
    {
        @Override
        protected Boolean visitPlan(PlanNode node, Void context)
        {
            return false;
        }

        @Override
        public Boolean visitEnforceSingleRow(EnforceSingleRowNode node, Void context)
        {
            return true;
        }

        @Override
        public Boolean visitProject(ProjectNode node, Void context)
        {
            return node.getSource().accept(this, null);
        }
    }
}
