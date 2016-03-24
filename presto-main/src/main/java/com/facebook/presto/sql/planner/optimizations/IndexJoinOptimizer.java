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
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.ResolvedIndex;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.sql.planner.DomainTranslator;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.IndexJoinNode;
import com.facebook.presto.sql.planner.plan.IndexSourceNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.WindowFrame;
import com.google.common.base.Functions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.sql.ExpressionUtils.combineConjuncts;
import static com.facebook.presto.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableSet;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.in;
import static java.util.Objects.requireNonNull;

public class IndexJoinOptimizer
        extends PlanOptimizer
{
    private final Metadata metadata;

    public IndexJoinOptimizer(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, com.facebook.presto.spi.type.Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(session, "session is null");
        requireNonNull(types, "types is null");
        requireNonNull(symbolAllocator, "symbolAllocator is null");
        requireNonNull(idAllocator, "idAllocator is null");

        return SimplePlanRewriter.rewriteWith(new Rewriter(symbolAllocator, idAllocator, metadata, session), plan, null);
    }

    private static class Rewriter
            extends SimplePlanRewriter<Void>
    {
        private final SymbolAllocator symbolAllocator;
        private final PlanNodeIdAllocator idAllocator;
        private final Metadata metadata;
        private final Session session;

        private Rewriter(SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator, Metadata metadata, Session session)
        {
            this.symbolAllocator = requireNonNull(symbolAllocator, "symbolAllocator is null");
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.session = requireNonNull(session, "session is null");
        }

        @Override
        public PlanNode visitJoin(JoinNode node, RewriteContext<Void> context)
        {
            PlanNode leftRewritten = context.rewrite(node.getLeft());
            PlanNode rightRewritten = context.rewrite(node.getRight());

            if (!node.getCriteria().isEmpty()) { // Index join only possible with JOIN criteria
                List<Symbol> leftJoinSymbols = Lists.transform(node.getCriteria(), JoinNode.EquiJoinClause::getLeft);
                List<Symbol> rightJoinSymbols = Lists.transform(node.getCriteria(), JoinNode.EquiJoinClause::getRight);

                Optional<PlanNode> leftIndexCandidate = IndexSourceRewriter.rewriteWithIndex(
                        leftRewritten,
                        ImmutableSet.copyOf(leftJoinSymbols),
                        symbolAllocator,
                        idAllocator,
                        metadata,
                        session);
                if (leftIndexCandidate.isPresent()) {
                    // Sanity check that we can trace the path for the index lookup key
                    Map<Symbol, Symbol> trace = IndexKeyTracer.trace(leftIndexCandidate.get(), ImmutableSet.copyOf(leftJoinSymbols));
                    checkState(!trace.isEmpty() && leftJoinSymbols.containsAll(trace.keySet()));
                }

                Optional<PlanNode> rightIndexCandidate = IndexSourceRewriter.rewriteWithIndex(
                        rightRewritten,
                        ImmutableSet.copyOf(rightJoinSymbols),
                        symbolAllocator,
                        idAllocator,
                        metadata,
                        session);
                if (rightIndexCandidate.isPresent()) {
                    // Sanity check that we can trace the path for the index lookup key
                    Map<Symbol, Symbol> trace = IndexKeyTracer.trace(rightIndexCandidate.get(), ImmutableSet.copyOf(rightJoinSymbols));
                    checkState(!trace.isEmpty() && rightJoinSymbols.containsAll(trace.keySet()));
                }

                switch (node.getType()) {
                    case INNER:
                        // Prefer the right candidate over the left candidate
                        if (rightIndexCandidate.isPresent()) {
                            return new IndexJoinNode(idAllocator.getNextId(), IndexJoinNode.Type.INNER, leftRewritten, rightIndexCandidate.get(), createEquiJoinClause(leftJoinSymbols, rightJoinSymbols), Optional.empty(), Optional.empty());
                        }
                        else if (leftIndexCandidate.isPresent()) {
                            return new IndexJoinNode(idAllocator.getNextId(), IndexJoinNode.Type.INNER, rightRewritten, leftIndexCandidate.get(), createEquiJoinClause(rightJoinSymbols, leftJoinSymbols), Optional.empty(), Optional.empty());
                        }
                        break;

                    case LEFT:
                        if (rightIndexCandidate.isPresent()) {
                            return new IndexJoinNode(idAllocator.getNextId(), IndexJoinNode.Type.SOURCE_OUTER, leftRewritten, rightIndexCandidate.get(), createEquiJoinClause(leftJoinSymbols, rightJoinSymbols), Optional.empty(), Optional.empty());
                        }
                        break;

                    case RIGHT:
                        if (leftIndexCandidate.isPresent()) {
                            return new IndexJoinNode(idAllocator.getNextId(), IndexJoinNode.Type.SOURCE_OUTER, rightRewritten, leftIndexCandidate.get(), createEquiJoinClause(rightJoinSymbols, leftJoinSymbols), Optional.empty(), Optional.empty());
                        }
                        break;

                    case FULL:
                        break;

                    default:
                        throw new IllegalArgumentException("Unknown type: " + node.getType());
                }
            }

            if (leftRewritten != node.getLeft() || rightRewritten != node.getRight()) {
                return new JoinNode(node.getId(), node.getType(), leftRewritten, rightRewritten, node.getCriteria(), node.getLeftHashSymbol(), node.getRightHashSymbol());
            }
            return node;
        }

        private static List<IndexJoinNode.EquiJoinClause> createEquiJoinClause(List<Symbol> probeSymbols, List<Symbol> indexSymbols)
        {
            checkArgument(probeSymbols.size() == indexSymbols.size());
            ImmutableList.Builder<IndexJoinNode.EquiJoinClause> builder = ImmutableList.builder();
            for (int i = 0; i < probeSymbols.size(); i++) {
                builder.add(new IndexJoinNode.EquiJoinClause(probeSymbols.get(i), indexSymbols.get(i)));
            }
            return builder.build();
        }
    }

    private static Symbol referenceToSymbol(Expression expression)
    {
        checkArgument(expression instanceof QualifiedNameReference);
        return Symbol.fromQualifiedName(((QualifiedNameReference) expression).getName());
    }

    /**
     * Tries to rewrite a PlanNode tree with an IndexSource instead of a TableScan
     */
    private static class IndexSourceRewriter
            extends SimplePlanRewriter<IndexSourceRewriter.Context>
    {
        private final SymbolAllocator symbolAllocator;
        private final PlanNodeIdAllocator idAllocator;
        private final Metadata metadata;
        private final Session session;

        private IndexSourceRewriter(SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator, Metadata metadata, Session session)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.symbolAllocator = requireNonNull(symbolAllocator, "symbolAllocator is null");
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.session = requireNonNull(session, "session is null");
        }

        public static Optional<PlanNode> rewriteWithIndex(
                PlanNode planNode,
                Set<Symbol> lookupSymbols,
                SymbolAllocator symbolAllocator,
                PlanNodeIdAllocator idAllocator,
                Metadata metadata,
                Session session)
        {
            AtomicBoolean success = new AtomicBoolean();
            IndexSourceRewriter indexSourceRewriter = new IndexSourceRewriter(symbolAllocator, idAllocator, metadata, session);
            PlanNode rewritten = SimplePlanRewriter.rewriteWith(indexSourceRewriter, planNode, new Context(lookupSymbols, success));
            if (success.get()) {
                return Optional.of(rewritten);
            }
            return Optional.empty();
        }

        @Override
        public PlanNode visitPlan(PlanNode node, RewriteContext<Context> context)
        {
            // We don't know how to process this PlanNode in the context of an IndexJoin, so just give up by returning something
            return node;
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, RewriteContext<Context> context)
        {
            return planTableScan(node, BooleanLiteral.TRUE_LITERAL, context.get());
        }

        private PlanNode planTableScan(TableScanNode node, Expression predicate, Context context)
        {
            DomainTranslator.ExtractionResult decomposedPredicate = DomainTranslator.fromPredicate(
                    metadata,
                    session,
                    predicate,
                    symbolAllocator.getTypes());

            TupleDomain<ColumnHandle> simplifiedConstraint = decomposedPredicate.getTupleDomain()
                    .transform(node.getAssignments()::get)
                    .intersect(node.getCurrentConstraint());

            checkState(node.getOutputSymbols().containsAll(context.getLookupSymbols()));

            Set<ColumnHandle> lookupColumns = context.getLookupSymbols().stream()
                    .map(node.getAssignments()::get)
                    .collect(toImmutableSet());

            Set<ColumnHandle> outputColumns = node.getOutputSymbols().stream().map(node.getAssignments()::get).collect(toImmutableSet());

            Optional<ResolvedIndex> optionalResolvedIndex = metadata.resolveIndex(session, node.getTable(), lookupColumns, outputColumns, simplifiedConstraint);
            if (!optionalResolvedIndex.isPresent()) {
                // No index available, so give up by returning something
                return node;
            }
            ResolvedIndex resolvedIndex = optionalResolvedIndex.get();

            Map<ColumnHandle, Symbol> inverseAssignments = ImmutableBiMap.copyOf(node.getAssignments()).inverse();

            PlanNode source = new IndexSourceNode(
                    idAllocator.getNextId(),
                    resolvedIndex.getIndexHandle(),
                    node.getTable(),
                    context.getLookupSymbols(),
                    node.getOutputSymbols(),
                    node.getAssignments(),
                    simplifiedConstraint);

            Expression resultingPredicate = combineConjuncts(
                    DomainTranslator.toPredicate(resolvedIndex.getUnresolvedTupleDomain().transform(inverseAssignments::get)),
                    decomposedPredicate.getRemainingExpression());

            if (!resultingPredicate.equals(TRUE_LITERAL)) {
                // todo it is likely we end up with redundant filters here because the predicate push down has already been run... the fix is to run predicate push down again
                source = new FilterNode(idAllocator.getNextId(), source, resultingPredicate);
            }
            context.markSuccess();
            return source;
        }

        @Override
        public PlanNode visitProject(ProjectNode node, RewriteContext<Context> context)
        {
            // Rewrite the lookup symbols in terms of only the pre-projected symbols that have direct translations
            Set<Symbol> newLookupSymbols = context.get().getLookupSymbols().stream()
                    .map(node.getAssignments()::get)
                    .filter(QualifiedNameReference.class::isInstance)
                    .map(IndexJoinOptimizer::referenceToSymbol)
                    .collect(toImmutableSet());

            if (newLookupSymbols.isEmpty()) {
                return node;
            }

            return context.defaultRewrite(node, new Context(newLookupSymbols, context.get().getSuccess()));
        }

        @Override
        public PlanNode visitFilter(FilterNode node, RewriteContext<Context> context)
        {
            if (node.getSource() instanceof TableScanNode) {
                return planTableScan((TableScanNode) node.getSource(), node.getPredicate(), context.get());
            }

            return context.defaultRewrite(node, new Context(context.get().getLookupSymbols(), context.get().getSuccess()));
        }

        @Override
        public PlanNode visitWindow(WindowNode node, RewriteContext<Context> context)
        {
            if (!node.getWindowFunctions().values().stream()
                    .map(FunctionCall::getName)
                    .allMatch(metadata.getFunctionRegistry()::isAggregationFunction)) {
                return node;
            }

            // Don't need this restriction if we can prove that all order by symbols are deterministically produced
            if (!node.getOrderBy().isEmpty()) {
                return node;
            }

            // Only RANGE frame type currently supported for aggregation functions because it guarantees the
            // same value for each peer group.
            // ROWS frame type requires the ordering to be fully deterministic (e.g. deterministically sorted on all columns)
            if (node.getFrame().getType() != WindowFrame.Type.RANGE) {
                return node;
            }

            // Lookup symbols can only be passed through if they are part of the partitioning
            Set<Symbol> partitionByLookupSymbols = context.get().getLookupSymbols().stream()
                    .filter(node.getPartitionBy()::contains)
                    .collect(toImmutableSet());

            if (partitionByLookupSymbols.isEmpty()) {
                return node;
            }

            return context.defaultRewrite(node, new Context(partitionByLookupSymbols, context.get().getSuccess()));
        }

        @Override
        public PlanNode visitIndexSource(IndexSourceNode node, RewriteContext<Context> context)
        {
            throw new IllegalStateException("Should not be trying to generate an Index on something that has already been determined to use an Index");
        }

        @Override
        public PlanNode visitIndexJoin(IndexJoinNode node, RewriteContext<Context> context)
        {
            // Lookup symbols can only be passed through the probe side of an index join
            Set<Symbol> probeLookupSymbols = context.get().getLookupSymbols().stream()
                    .filter(node.getProbeSource().getOutputSymbols()::contains)
                    .collect(toImmutableSet());

            if (probeLookupSymbols.isEmpty()) {
                return node;
            }

            PlanNode rewrittenProbeSource = context.rewrite(node.getProbeSource(), new Context(probeLookupSymbols, context.get().getSuccess()));

            PlanNode source = node;
            if (rewrittenProbeSource != node.getProbeSource()) {
                source = new IndexJoinNode(node.getId(), node.getType(), rewrittenProbeSource, node.getIndexSource(), node.getCriteria(), node.getProbeHashSymbol(), node.getIndexHashSymbol());
            }

            return source;
        }

        @Override
        public PlanNode visitAggregation(AggregationNode node, RewriteContext<Context> context)
        {
            // Lookup symbols can only be passed through if they are part of the group by columns
            Set<Symbol> groupByLookupSymbols = context.get().getLookupSymbols().stream()
                    .filter(node.getGroupBy()::contains)
                    .collect(toImmutableSet());

            if (groupByLookupSymbols.isEmpty()) {
                return node;
            }

            return context.defaultRewrite(node, new Context(groupByLookupSymbols, context.get().getSuccess()));
        }

        @Override
        public PlanNode visitSort(SortNode node, RewriteContext<Context> context)
        {
            // Sort has no bearing when building an index, so just ignore the sort
            return context.rewrite(node.getSource(), context.get());
        }

        public static class Context
        {
            private final Set<Symbol> lookupSymbols;
            private final AtomicBoolean success;

            public Context(Set<Symbol> lookupSymbols, AtomicBoolean success)
            {
                checkArgument(!lookupSymbols.isEmpty(), "lookupSymbols can not be empty");
                this.lookupSymbols = ImmutableSet.copyOf(requireNonNull(lookupSymbols, "lookupSymbols is null"));
                this.success = requireNonNull(success, "success is null");
            }

            public Set<Symbol> getLookupSymbols()
            {
                return lookupSymbols;
            }

            public AtomicBoolean getSuccess()
            {
                return success;
            }

            public void markSuccess()
            {
                checkState(success.compareAndSet(false, true), "Can only have one success per context");
            }
        }
    }

    /**
     * Identify the mapping from the lookup symbols used at the top of the index plan to
     * the actual symbols produced by the IndexSource. Note that multiple top-level lookup symbols may share the same
     * underlying IndexSource symbol. Also note that lookup symbols that do not correspond to underlying index source symbols
     * will be omitted from the returned Map.
     */
    public static class IndexKeyTracer
    {
        public static Map<Symbol, Symbol> trace(PlanNode node, Set<Symbol> lookupSymbols)
        {
            return node.accept(new Visitor(), lookupSymbols);
        }

        private static class Visitor
                extends PlanVisitor<Set<Symbol>, Map<Symbol, Symbol>>
        {
            @Override
            protected Map<Symbol, Symbol> visitPlan(PlanNode node, Set<Symbol> lookupSymbols)
            {
                throw new UnsupportedOperationException("Node not expected to be part of Index pipeline: " + node);
            }

            @Override
            public Map<Symbol, Symbol> visitProject(ProjectNode node, Set<Symbol> lookupSymbols)
            {
                // Map from output Symbols to source Symbols
                Map<Symbol, Symbol> directSymbolTranslationOutputMap = Maps.transformValues(Maps.filterValues(node.getAssignments(), QualifiedNameReference.class::isInstance), IndexJoinOptimizer::referenceToSymbol);
                Map<Symbol, Symbol> outputToSourceMap = FluentIterable.from(lookupSymbols)
                        .filter(in(directSymbolTranslationOutputMap.keySet()))
                        .toMap(Functions.forMap(directSymbolTranslationOutputMap));
                checkState(!outputToSourceMap.isEmpty(), "No lookup symbols were able to pass through the projection");

                // Map from source Symbols to underlying index source Symbols
                Map<Symbol, Symbol> sourceToIndexMap = node.getSource().accept(this, ImmutableSet.copyOf(outputToSourceMap.values()));

                // Generate the Map the connects lookup symbols to underlying index source symbols
                Map<Symbol, Symbol> outputToIndexMap = Maps.transformValues(Maps.filterValues(outputToSourceMap, in(sourceToIndexMap.keySet())), Functions.forMap(sourceToIndexMap));
                return ImmutableMap.copyOf(outputToIndexMap);
            }

            @Override
            public Map<Symbol, Symbol> visitFilter(FilterNode node, Set<Symbol> lookupSymbols)
            {
                return node.getSource().accept(this, lookupSymbols);
            }

            @Override
            public Map<Symbol, Symbol> visitWindow(WindowNode node, Set<Symbol> lookupSymbols)
            {
                Set<Symbol> partitionByLookupSymbols = lookupSymbols.stream()
                        .filter(node.getPartitionBy()::contains)
                        .collect(toImmutableSet());
                checkState(!partitionByLookupSymbols.isEmpty(), "No lookup symbols were able to pass through the aggregation group by");
                return node.getSource().accept(this, partitionByLookupSymbols);
            }

            @Override
            public Map<Symbol, Symbol> visitIndexJoin(IndexJoinNode node, Set<Symbol> lookupSymbols)
            {
                Set<Symbol> probeLookupSymbols = lookupSymbols.stream()
                        .filter(node.getProbeSource().getOutputSymbols()::contains)
                        .collect(toImmutableSet());
                checkState(!probeLookupSymbols.isEmpty(), "No lookup symbols were able to pass through the index join probe source");
                return node.getProbeSource().accept(this, probeLookupSymbols);
            }

            @Override
            public Map<Symbol, Symbol> visitAggregation(AggregationNode node, Set<Symbol> lookupSymbols)
            {
                Set<Symbol> groupByLookupSymbols = lookupSymbols.stream()
                        .filter(node.getGroupBy()::contains)
                        .collect(toImmutableSet());
                checkState(!groupByLookupSymbols.isEmpty(), "No lookup symbols were able to pass through the aggregation group by");
                return node.getSource().accept(this, groupByLookupSymbols);
            }

            @Override
            public Map<Symbol, Symbol> visitSort(SortNode node, Set<Symbol> lookupSymbols)
            {
                return node.getSource().accept(this, lookupSymbols);
            }

            @Override
            public Map<Symbol, Symbol> visitIndexSource(IndexSourceNode node, Set<Symbol> lookupSymbols)
            {
                checkState(node.getLookupSymbols().equals(lookupSymbols), "lookupSymbols must be the same as IndexSource lookup symbols");
                return FluentIterable.from(lookupSymbols)
                        .toMap(Functions.<Symbol>identity());
            }
        }
    }
}
