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
import com.facebook.presto.metadata.TableLayoutResult;
import com.facebook.presto.operator.aggregation.InternalAggregationFunction;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.GroupingProperty;
import com.facebook.presto.spi.LocalProperty;
import com.facebook.presto.spi.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.DomainTranslator;
import com.facebook.presto.sql.planner.ExpressionInterpreter;
import com.facebook.presto.sql.planner.LookupSymbolResolver;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.ChildReplacer;
import com.facebook.presto.sql.planner.plan.DistinctLimitNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
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
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.facebook.presto.SystemSessionProperties.isBigQueryEnabled;
import static com.facebook.presto.sql.ExpressionUtils.combineConjuncts;
import static com.facebook.presto.sql.ExpressionUtils.extractConjuncts;
import static com.facebook.presto.sql.ExpressionUtils.stripDeterministicConjuncts;
import static com.facebook.presto.sql.ExpressionUtils.stripNonDeterministicConjuncts;
import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.getExpressionTypes;
import static com.facebook.presto.sql.planner.plan.AggregationNode.Step.FINAL;
import static com.facebook.presto.sql.planner.plan.AggregationNode.Step.PARTIAL;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.gatheringExchange;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.partitionedExchange;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableSet;
import static com.google.common.base.Preconditions.checkArgument;

public class AddExchanges
        extends PlanOptimizer
{
    private final SqlParser parser;
    private final Metadata metadata;
    private final boolean distributedIndexJoins;
    private final boolean distributedJoins;

    public AddExchanges(Metadata metadata, SqlParser parser, boolean distributedIndexJoins, boolean distributedJoins)
    {
        this.metadata = metadata;
        this.parser = parser;
        this.distributedIndexJoins = distributedIndexJoins;
        this.distributedJoins = distributedJoins;
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        boolean distributedJoinEnabled = SystemSessionProperties.isDistributedJoinEnabled(session, distributedJoins);
        PlanWithProperties result = plan.accept(new Rewriter(symbolAllocator, idAllocator, symbolAllocator, session, distributedIndexJoins, distributedJoinEnabled), PreferredProperties.any());
        return result.getNode();
    }

    private class Rewriter
            extends PlanVisitor<PreferredProperties, PlanWithProperties>
    {
        private final SymbolAllocator allocator;
        private final PlanNodeIdAllocator idAllocator;
        private final SymbolAllocator symbolAllocator;
        private final Session session;
        private final boolean distributedIndexJoins;
        private final boolean distributedJoins;

        public Rewriter(SymbolAllocator allocator, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator, Session session, boolean distributedIndexJoins, boolean distributedJoins)
        {
            this.allocator = allocator;
            this.idAllocator = idAllocator;
            this.symbolAllocator = symbolAllocator;
            this.session = session;
            this.distributedIndexJoins = distributedIndexJoins;
            this.distributedJoins = distributedJoins;
        }

        @Override
        protected PlanWithProperties visitPlan(PlanNode node, PreferredProperties preferred)
        {
            return rebaseAndDeriveProperties(node, planChild(node, preferred));
        }

        @Override
        public PlanWithProperties visitOutput(OutputNode node, PreferredProperties preferred)
        {
            PlanWithProperties child = planChild(node, PreferredProperties.any());

            if (child.getProperties().isPartitioned()) {
                child = withDerivedProperties(
                        gatheringExchange(idAllocator.getNextId(), child.getNode()),
                        child.getProperties());
            }

            return rebaseAndDeriveProperties(node, child);
        }

        @Override
        public PlanWithProperties visitAggregation(AggregationNode node, PreferredProperties preferred)
        {
            if (node.getGroupBy().isEmpty()) {
                return planAggregation(
                        node,
                        PreferredProperties.any(),
                        partial -> gatheringExchange(idAllocator.getNextId(), partial));
            }

            return planAggregation(
                    node,
                    determineChildPreferences(preferred, node.getGroupBy(), node.getGroupBy()),
                    partial -> partitionedExchange(idAllocator.getNextId(), partial, node.getGroupBy(), node.getHashSymbol()));
        }

        private PlanWithProperties planAggregation(AggregationNode node, PreferredProperties preferredChildProperties, Function<PlanNode, PlanNode> addExchange)
        {
            boolean decomposable = node.getFunctions()
                    .values().stream()
                    .map(metadata::getExactFunction)
                    .map(FunctionInfo::getAggregationFunction)
                    .allMatch(InternalAggregationFunction::isDecomposable);

            PlanWithProperties child = planChild(node, preferredChildProperties);
            if (!child.getProperties().isPartitionedOn(node.getGroupBy())) {
                if (!decomposable) {
                    child = withDerivedProperties(
                            addExchange.apply(child.getNode()),
                            child.getProperties());

                    return rebaseAndDeriveProperties(node, child);
                }

                return splitAggregation(node, child, addExchange);
            }

            return rebaseAndDeriveProperties(node, child);
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

            PlanNode exchange = exchanger.apply(partial.getNode());

            return withDerivedProperties(
                    new AggregationNode(
                            node.getId(),
                            exchange,
                            node.getGroupBy(),
                            finalCalls,
                            node.getFunctions(),
                            ImmutableMap.of(),
                            FINAL,
                            Optional.empty(),
                            node.getConfidence(),
                            node.getHashSymbol()),
                    deriveProperties(exchange, partial.getProperties()));
        }

        @Override
        public PlanWithProperties visitMarkDistinct(MarkDistinctNode node, PreferredProperties preferred)
        {
            PreferredProperties preferredChildProperties = determineChildPreferences(preferred, node.getDistinctSymbols(), node.getDistinctSymbols());
            PlanWithProperties child = node.getSource().accept(this, preferredChildProperties);

            if ((child.getProperties().isUnpartitioned() && isBigQueryEnabled(session, false)) ||
                    !child.getProperties().isPartitionedOn(node.getDistinctSymbols())) {
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
        public PlanWithProperties visitWindow(WindowNode node, PreferredProperties preferred)
        {
            PlanWithProperties child = planChild(node, determineChildPreferences(preferred, node.getPartitionBy(), node.getPartitionBy()));

            if (!child.getProperties().isPartitionedOn(node.getPartitionBy())) {
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
                            child.getProperties().getMaxGroupingSubset(node.getPartitionBy())),
                    child.getProperties());
        }

        @Override
        public PlanWithProperties visitRowNumber(RowNumberNode node, PreferredProperties preferred)
        {
            if (node.getPartitionBy().isEmpty()) {
                PlanWithProperties child = planChild(node, PreferredProperties.unpartitioned());

                if (child.getProperties().isPartitioned()) {
                    child = withDerivedProperties(
                            gatheringExchange(idAllocator.getNextId(), child.getNode()),
                            child.getProperties());
                }

                return rebaseAndDeriveProperties(node, child);
            }

            PlanWithProperties child = planChild(node, determineChildPreferences(preferred, node.getPartitionBy(), node.getPartitionBy()));

            // TODO: add config option/session property to force parallel plan if child is unpartitioned and window has a PARTITION BY clause
            if (!child.getProperties().isPartitionedOn(node.getPartitionBy())) {
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
        public PlanWithProperties visitTopNRowNumber(TopNRowNumberNode node, PreferredProperties preferred)
        {
            PreferredProperties preferredChildProperties;
            Function<PlanNode, PlanNode> addExchange;

            if (node.getPartitionBy().isEmpty()) {
                preferredChildProperties = PreferredProperties.any();
                addExchange = partial -> gatheringExchange(idAllocator.getNextId(), partial);
            }
            else {
                preferredChildProperties = determineChildPreferences(preferred, node.getPartitionBy(), node.getPartitionBy());
                addExchange = partial -> partitionedExchange(idAllocator.getNextId(), partial, node.getPartitionBy(), node.getHashSymbol());
            }

            PlanWithProperties child = planChild(node, preferredChildProperties);
            if (!child.getProperties().isPartitionedOn(node.getPartitionBy())) {
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
        public PlanWithProperties visitTopN(TopNNode node, PreferredProperties preferred)
        {
            PlanWithProperties child = planChild(node, PreferredProperties.any());

            if (child.getProperties().isPartitioned()) {
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
        public PlanWithProperties visitSort(SortNode node, PreferredProperties preferred)
        {
            PlanWithProperties child = planChild(node, PreferredProperties.unpartitioned());

            if (child.getProperties().isPartitioned()) {
                child = withDerivedProperties(
                        gatheringExchange(idAllocator.getNextId(), child.getNode()),
                        child.getProperties());
            }

            return rebaseAndDeriveProperties(node, child);
        }

        @Override
        public PlanWithProperties visitLimit(LimitNode node, PreferredProperties preferred)
        {
            PlanWithProperties child = planChild(node, PreferredProperties.any());

            if (child.getProperties().isPartitioned()) {
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
        public PlanWithProperties visitDistinctLimit(DistinctLimitNode node, PreferredProperties preferred)
        {
            PlanWithProperties child = planChild(node, PreferredProperties.any());

            if (child.getProperties().isPartitioned()) {
                child = withDerivedProperties(
                        new DistinctLimitNode(idAllocator.getNextId(), child.getNode(), node.getLimit(), node.getHashSymbol()),
                        child.getProperties());

                child = withDerivedProperties(
                        gatheringExchange(
                                idAllocator.getNextId(),
                                new DistinctLimitNode(idAllocator.getNextId(), child.getNode(), node.getLimit(), node.getHashSymbol())),
                        child.getProperties());
            }

            return rebaseAndDeriveProperties(node, child);
        }

        @Override
        public PlanWithProperties visitFilter(FilterNode node, PreferredProperties preferred)
        {
            if (node.getSource() instanceof TableScanNode) {
                return planTableScan((TableScanNode) node.getSource(), node.getPredicate(), preferred);
            }

            return rebaseAndDeriveProperties(node, planChild(node, preferred));
        }

        @Override
        public PlanWithProperties visitTableScan(TableScanNode node, PreferredProperties preferred)
        {
            return planTableScan(node, BooleanLiteral.TRUE_LITERAL, preferred);
        }

        private PlanWithProperties planTableScan(TableScanNode node, Expression predicate, PreferredProperties preferred)
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
                    DomainTranslator.toPredicate(
                            node.getCurrentConstraint().transform(assignments::get),
                            symbolAllocator.getTypes()));

            List<TableLayoutResult> layouts = metadata.getLayouts(
                    node.getTable(),
                    new Constraint<>(simplifiedConstraint, bindings -> !shouldPrune(constraint, node.getAssignments(), bindings)),
                    Optional.of(node.getOutputSymbols().stream()
                            .map(node.getAssignments()::get)
                            .collect(toImmutableSet()))
            );

            if (layouts.isEmpty()) {
                return new PlanWithProperties(
                        new ValuesNode(idAllocator.getNextId(), node.getOutputSymbols(), ImmutableList.of()),
                        ActualProperties.unpartitioned());
            }

            TableLayoutResult layout = pickLayout(layouts, preferred);

            Expression originalConstraint = node.getOriginalConstraint();
            if (originalConstraint == null) {
                originalConstraint = predicate;
            }

            TableScanNode tableScan = new TableScanNode(
                    node.getId(),
                    node.getTable(),
                    node.getOutputSymbols(),
                    node.getAssignments(),
                    Optional.of(layout.getLayout().getHandle()),
                    simplifiedConstraint.intersect(layout.getLayout().getPredicate()),
                    originalConstraint);

            PlanWithProperties result = new PlanWithProperties(tableScan, deriveProperties(tableScan, ImmutableList.of()));

            Expression resultingPredicate = combineConjuncts(
                    DomainTranslator.toPredicate(
                            layout.getUnenforcedConstraint().transform(assignments::get),
                            symbolAllocator.getTypes()),
                    stripDeterministicConjuncts(predicate),
                    decomposedPredicate.getRemainingExpression());

            if (!BooleanLiteral.TRUE_LITERAL.equals(resultingPredicate)) {
                return withDerivedProperties(
                        new FilterNode(idAllocator.getNextId(), result.getNode(), resultingPredicate),
                        deriveProperties(tableScan, ImmutableList.of()));
            }

            return result;
        }

        private TableLayoutResult pickLayout(List<TableLayoutResult> layouts, PreferredProperties preferred)
        {
            // TODO: for now, pick first available layout
            return layouts.get(0);
        }

        private boolean shouldPrune(Expression predicate, Map<Symbol, ColumnHandle> assignments, Map<ColumnHandle, ?> bindings)
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
        public PlanWithProperties visitValues(ValuesNode node, PreferredProperties preferred)
        {
            return new PlanWithProperties(node, ActualProperties.unpartitioned());
        }

        @Override
        public PlanWithProperties visitTableCommit(TableCommitNode node, PreferredProperties preferred)
        {
            PlanWithProperties child = planChild(node, PreferredProperties.any());
            if (child.getProperties().isPartitioned() || !child.getProperties().isCoordinatorOnly()) {
                child = withDerivedProperties(
                        gatheringExchange(idAllocator.getNextId(), child.getNode()),
                        child.getProperties());
            }

            return rebaseAndDeriveProperties(node, child);
        }

        @Override
        public PlanWithProperties visitJoin(JoinNode node, PreferredProperties preferred)
        {
            checkArgument(node.getType() != JoinNode.Type.RIGHT, "Expected RIGHT joins to be normalized to LEFT joins");

            List<Symbol> leftSymbols = Lists.transform(node.getCriteria(), JoinNode.EquiJoinClause::getLeft);
            List<Symbol> rightSymbols = Lists.transform(node.getCriteria(), JoinNode.EquiJoinClause::getRight);

            PlanWithProperties left = node.getLeft().accept(this, PreferredProperties.any());
            PlanWithProperties right = node.getRight().accept(this, PreferredProperties.any());

            if (distributedJoins) {
                // force partitioning
                if (!left.getProperties().isHashPartitionedOn(leftSymbols)) {
                    left = withDerivedProperties(
                            partitionedExchange(idAllocator.getNextId(), left.getNode(), leftSymbols, node.getLeftHashSymbol()),
                            left.getProperties());
                }

                if (!right.getProperties().isHashPartitionedOn(rightSymbols)) {
                    right = withDerivedProperties(
                            partitionedExchange(idAllocator.getNextId(), right.getNode(), rightSymbols, node.getRightHashSymbol()),
                            right.getProperties());
                }
            }
            else if (left.getProperties().isUnpartitioned() && right.getProperties().isPartitioned()) {
                // force single-node join
                // TODO: if inner join, flip order and do a broadcast join
                right = withDerivedProperties(gatheringExchange(idAllocator.getNextId(), right.getNode()), right.getProperties());
            }
            else if (left.getProperties().isPartitioned() && !(left.getProperties().isHashPartitionedOn(leftSymbols) && right.getProperties().isHashPartitionedOn(rightSymbols))) {
                right = withDerivedProperties(new ExchangeNode(
                                idAllocator.getNextId(),
                                ExchangeNode.Type.REPLICATE,
                                ImmutableList.of(),
                                Optional.<Symbol>empty(),
                                ImmutableList.of(right.getNode()),
                                right.getNode().getOutputSymbols(),
                                ImmutableList.of(right.getNode().getOutputSymbols())),
                        right.getProperties());
            }

            JoinNode result = new JoinNode(node.getId(),
                    node.getType(),
                    left.getNode(),
                    right.getNode(),
                    node.getCriteria(),
                    node.getLeftHashSymbol(),
                    node.getRightHashSymbol());

            return new PlanWithProperties(result, deriveProperties(result, ImmutableList.of(left.getProperties(), right.getProperties())));
        }

        @Override
        public PlanWithProperties visitSemiJoin(SemiJoinNode node, PreferredProperties preferred)
        {
            PlanWithProperties source = node.getSource().accept(this, PreferredProperties.any());
            PlanWithProperties filteringSource = node.getFilteringSource().accept(this, PreferredProperties.any());

            // make filtering source match requirements of source
            if (source.getProperties().isPartitioned()) {
                filteringSource = withDerivedProperties(
                        new ExchangeNode(
                                idAllocator.getNextId(),
                                ExchangeNode.Type.REPLICATE,
                                ImmutableList.of(),
                                Optional.<Symbol>empty(),
                                ImmutableList.of(filteringSource.getNode()),
                                filteringSource.getNode().getOutputSymbols(),
                                ImmutableList.of(filteringSource.getNode().getOutputSymbols())),
                        filteringSource.getProperties());
            }
            else {
                filteringSource = withDerivedProperties(
                        gatheringExchange(idAllocator.getNextId(), filteringSource.getNode()),
                        filteringSource.getProperties());
            }

            // TODO: add support for hash-partitioned semijoins

            return rebaseAndDeriveProperties(node, ImmutableList.of(source, filteringSource));
        }

        @Override
        public PlanWithProperties visitIndexJoin(IndexJoinNode node, PreferredProperties preferredProperties)
        {
            List<Symbol> joinColumns = Lists.transform(node.getCriteria(), IndexJoinNode.EquiJoinClause::getProbe);
            PlanWithProperties probeSource = node.getProbeSource().accept(this, determineChildPreferences(preferredProperties, joinColumns, joinColumns));

            if (distributedIndexJoins && !probeSource.getProperties().isPartitionedOn(joinColumns)) {
                probeSource = withDerivedProperties(
                        partitionedExchange(idAllocator.getNextId(), probeSource.getNode(), joinColumns, node.getProbeHashSymbol()),
                        probeSource.getProperties());
            }

            // TODO: if input is grouped, create streaming join

            // index side is really a nested-loops plan, so don't add exchanges
            PlanNode result = ChildReplacer.replaceChildren(node, ImmutableList.of(probeSource.getNode(), node.getIndexSource()));
            return new PlanWithProperties(result, deriveProperties(result, probeSource.getProperties()));
        }

        @Override
        public PlanWithProperties visitUnion(UnionNode node, PreferredProperties preferred)
        {
            // first, classify children into partitioned and unpartitioned
            List<PlanNode> unpartitionedChildren = new ArrayList<>();
            List<List<Symbol>> unpartitionedOutputLayouts = new ArrayList<>();

            List<PlanNode> partitionedChildren = new ArrayList<>();
            List<List<Symbol>> partitionedOutputLayouts = new ArrayList<>();

            List<PlanNode> sources = node.getSources();
            for (int i = 0; i < sources.size(); i++) {
                PlanWithProperties child = sources.get(i).accept(this, PreferredProperties.any());
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

            return new PlanWithProperties(result, ActualProperties.unpartitioned());
        }

        private PreferredProperties determineChildPreferences(PreferredProperties preferencesFromParent, List<Symbol> partitioningColumns, List<Symbol> groupingColumns)
        {
            // if the child plan can be grouped first by our requirements and then by our parent's,
            // it can satisfy both in one shot
            List<LocalProperty<Symbol>> local = ImmutableList.<LocalProperty<Symbol>>builder()
                    .add(new GroupingProperty<>(groupingColumns))
                    .addAll(preferencesFromParent.getLocalProperties())
                    .build();

            // if the child plan is partitioned by the common columns between our requirements and
            // our parent's, it can satisfy both in one shot
            Set<Symbol> partitioning = Sets.intersection(
                    preferencesFromParent.getPartitioningColumns().orElse(ImmutableSet.of()),
                    ImmutableSet.copyOf(partitioningColumns));

            // However, if there are no common columns, prefer our partitioning columns (to avoid ending up
            // with a non-parallel plan)
            if (partitioning.isEmpty()) {
                partitioning = ImmutableSet.copyOf(partitioningColumns);
            }

            return PreferredProperties.partitionedWithLocal(partitioning, local);
        }

        private PlanWithProperties planChild(PlanNode node, PreferredProperties preferred)
        {
            return Iterables.getOnlyElement(node.getSources()).accept(this, preferred);
        }

        private PlanWithProperties rebaseAndDeriveProperties(PlanNode node, PlanWithProperties child)
        {
            return withDerivedProperties(
                    ChildReplacer.replaceChildren(node, ImmutableList.of(child.getNode())),
                    child.getProperties());
        }

        private PlanWithProperties rebaseAndDeriveProperties(PlanNode node, List<PlanWithProperties> children)
        {
            PlanNode result = ChildReplacer.replaceChildren(node, children.stream().map(PlanWithProperties::getNode).collect(Collectors.toList()));
            return new PlanWithProperties(result, deriveProperties(result, children.stream().map(PlanWithProperties::getProperties).collect(Collectors.toList())));
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
}
