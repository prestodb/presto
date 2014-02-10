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

import com.facebook.presto.index.IndexManager;
import com.facebook.presto.metadata.ColumnHandle;
import com.facebook.presto.metadata.ResolvedIndex;
import com.facebook.presto.spi.Session;
import com.facebook.presto.spi.TupleDomain;
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
import com.facebook.presto.sql.planner.plan.PlanNodeRewriter;
import com.facebook.presto.sql.planner.plan.PlanRewriter;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.sql.planner.plan.JoinNode.EquiJoinClause.leftGetter;
import static com.facebook.presto.sql.planner.plan.JoinNode.EquiJoinClause.rightGetter;
import static com.facebook.presto.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.not;

public class IndexJoinOptimizer
        extends PlanOptimizer
{
    private final IndexManager indexManager;

    public IndexJoinOptimizer(IndexManager indexManager)
    {
        this.indexManager = checkNotNull(indexManager, "indexManager is null");
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, com.facebook.presto.spi.type.Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        checkNotNull(plan, "plan is null");
        checkNotNull(session, "session is null");
        checkNotNull(types, "types is null");
        checkNotNull(symbolAllocator, "symbolAllocator is null");
        checkNotNull(idAllocator, "idAllocator is null");

        return PlanRewriter.rewriteWith(new Rewriter(symbolAllocator, idAllocator, indexManager), plan, null);
    }

    private static class Rewriter
            extends PlanNodeRewriter<Void>
    {
        private final IndexManager indexManager;
        private final SymbolAllocator symbolAllocator;
        private final PlanNodeIdAllocator idAllocator;

        private Rewriter(SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator, IndexManager indexManager)
        {
            this.symbolAllocator = checkNotNull(symbolAllocator, "symbolAllocator is null");
            this.idAllocator = checkNotNull(idAllocator, "idAllocator is null");
            this.indexManager = checkNotNull(indexManager, "indexManager is null");
        }

        @Override
        public PlanNode rewriteJoin(JoinNode node, Void context, PlanRewriter<Void> planRewriter)
        {
            PlanNode leftRewritten = planRewriter.rewrite(node.getLeft(), context);
            PlanNode rightRewritten = planRewriter.rewrite(node.getRight(), context);

            List<Symbol> leftJoinSymbols = Lists.transform(node.getCriteria(), leftGetter());
            List<Symbol> rightJoinSymbols = Lists.transform(node.getCriteria(), rightGetter());

            Optional<PlanNode> leftIndexCandidate = IndexSourceRewriter.rewriteWithIndex(
                    leftRewritten,
                    ImmutableSet.copyOf(leftJoinSymbols),
                    indexManager,
                    symbolAllocator,
                    idAllocator
            );
            if (leftIndexCandidate.isPresent()) {
                // Sanity check that we can trace the path for the index lookup key
                checkState(IndexKeyTracer.trace(leftIndexCandidate.get(), ImmutableSet.copyOf(leftJoinSymbols)).keySet().containsAll(leftJoinSymbols));
            }

            Optional<PlanNode> rightIndexCandidate = IndexSourceRewriter.rewriteWithIndex(
                    rightRewritten,
                    ImmutableSet.copyOf(rightJoinSymbols),
                    indexManager,
                    symbolAllocator,
                    idAllocator
            );
            if (rightIndexCandidate.isPresent()) {
                // Sanity check that we can trace the path for the index lookup key
                checkState(IndexKeyTracer.trace(rightIndexCandidate.get(), ImmutableSet.copyOf(rightJoinSymbols)).keySet().containsAll(rightJoinSymbols));
            }

            switch (node.getType()) {
                case INNER:
                    // Prefer the right candidate over the left candidate
                    if (rightIndexCandidate.isPresent()) {
                        return new IndexJoinNode(idAllocator.getNextId(), IndexJoinNode.Type.INNER, leftRewritten, rightIndexCandidate.get(), createEquiJoinClause(leftJoinSymbols, rightJoinSymbols));
                    }
                    else if (leftIndexCandidate.isPresent()) {
                        return new IndexJoinNode(idAllocator.getNextId(), IndexJoinNode.Type.INNER, rightRewritten, leftIndexCandidate.get(), createEquiJoinClause(rightJoinSymbols, leftJoinSymbols));
                    }
                    break;

                case LEFT:
                    if (rightIndexCandidate.isPresent()) {
                        return new IndexJoinNode(idAllocator.getNextId(), IndexJoinNode.Type.SOURCE_OUTER, leftRewritten, rightIndexCandidate.get(), createEquiJoinClause(leftJoinSymbols, rightJoinSymbols));
                    }
                    break;

                case RIGHT:
                    if (leftIndexCandidate.isPresent()) {
                        return new IndexJoinNode(idAllocator.getNextId(), IndexJoinNode.Type.SOURCE_OUTER, rightRewritten, leftIndexCandidate.get(), createEquiJoinClause(rightJoinSymbols, leftJoinSymbols));
                    }
                    break;

                default:
                    throw new IllegalArgumentException("Unknown type: " + node.getType());
            }

            if (leftRewritten != node.getLeft() || rightRewritten != node.getRight()) {
                return new JoinNode(node.getId(), node.getType(), leftRewritten, rightRewritten, node.getCriteria());
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

    private static Function<Expression, Symbol> symbolFromReferenceGetter()
    {
        return new Function<Expression, Symbol>()
        {
            @Override
            public Symbol apply(Expression expression)
            {
                checkArgument(expression instanceof QualifiedNameReference);
                return Symbol.fromQualifiedName(((QualifiedNameReference) expression).getName());
            }
        };
    }

    private static Predicate<Expression> instanceOfQualifiedNameReference()
    {
        return new Predicate<Expression>()
        {
            @Override
            public boolean apply(Expression expression)
            {
                return expression instanceof QualifiedNameReference;
            }
        };
    }

    /**
     * Tries to rewrite a PlanNode tree with an IndexSource instead of a TableScan
     */
    private static class IndexSourceRewriter
            extends PlanNodeRewriter<IndexSourceRewriter.Context>
    {
        private final IndexManager indexManager;
        private final SymbolAllocator symbolAllocator;
        private final PlanNodeIdAllocator idAllocator;

        private IndexSourceRewriter(IndexManager indexManager, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
        {
            this.symbolAllocator = checkNotNull(symbolAllocator, "symbolAllocator is null");
            this.idAllocator = checkNotNull(idAllocator, "idAllocator is null");
            this.indexManager = checkNotNull(indexManager, "indexManager is null");
        }

        public static Optional<PlanNode> rewriteWithIndex(
                PlanNode planNode,
                Set<Symbol> lookupSymbols,
                IndexManager indexManager,
                SymbolAllocator symbolAllocator,
                PlanNodeIdAllocator idAllocator)
        {
            AtomicBoolean success = new AtomicBoolean();
            IndexSourceRewriter indexSourceRewriter = new IndexSourceRewriter(indexManager, symbolAllocator, idAllocator);
            PlanNode rewritten = PlanRewriter.rewriteWith(indexSourceRewriter, planNode, new Context(lookupSymbols, success));
            if (success.get()) {
                return Optional.of(rewritten);
            }
            return Optional.absent();
        }

        @Override
        public PlanNode rewriteNode(PlanNode node, Context context, PlanRewriter<Context> planRewriter)
        {
            // We don't know how to process this PlanNode in the context of an IndexJoin, so just give up by returning something
            return node;
        }

        @Override
        public PlanNode rewriteTableScan(TableScanNode node, Context context, PlanRewriter<Context> planRewriter)
        {
            checkState(node.getOutputSymbols().containsAll(context.getLookupSymbols()));

            Set<ColumnHandle> lookupColumns = FluentIterable.from(context.getLookupSymbols())
                    .transform(Functions.forMap(node.getAssignments()))
                    .toSet();

            checkState(node.getGeneratedPartitions().isPresent(), "Predicate should have generated partitions before this optimizer");
            TupleDomain<ColumnHandle> tupleDomain = node.getGeneratedPartitions().get().getTupleDomainInput();
            Optional<ResolvedIndex> optionalResolvedIndex = indexManager.resolveIndex(node.getTable(), lookupColumns, tupleDomain);
            if (!optionalResolvedIndex.isPresent()) {
                // No index available, so give up by returning something
                return node;
            }
            ResolvedIndex resolvedIndex = optionalResolvedIndex.get();

            Map<ColumnHandle, Symbol> inverseAssignments = ImmutableBiMap.copyOf(node.getAssignments()).inverse();
            Expression unresolvedExpression = DomainTranslator.toPredicate(resolvedIndex.getUnresolvedTupleDomain(), inverseAssignments, symbolAllocator.getTypes());

            PlanNode source = new IndexSourceNode(
                    idAllocator.getNextId(),
                    resolvedIndex.getIndexHandle(),
                    node.getTable(),
                    context.getLookupSymbols(),
                    node.getOutputSymbols(),
                    node.getAssignments(),
                    tupleDomain);

            if (!unresolvedExpression.equals(TRUE_LITERAL)) {
                // todo it is likely we end up with redundant filters here because the predicate push down has already been run... the fix is to run predicate push down again
                source = new FilterNode(idAllocator.getNextId(), source, unresolvedExpression);
            }
            context.markSuccess();
            return source;
        }

        @Override
        public PlanNode rewriteProject(ProjectNode node, Context context, PlanRewriter<Context> planRewriter)
        {
            // All lookup symbols must be direct translations to underlying symbols
            if (FluentIterable.from(context.getLookupSymbols())
                    .transform(Functions.forMap(node.getOutputMap()))
                    .anyMatch(not(instanceOfQualifiedNameReference()))) {
                return node; // Give up if any of the lookup symbols don't have a simple translation
            }

            // Rewrite the lookup symbols in terms of pre-project symbols
            Set<Symbol> newLookupSymbols = FluentIterable.from(context.getLookupSymbols())
                    .transform(Functions.forMap(node.getOutputMap()))
                    .transform(symbolFromReferenceGetter())
                    .toSet();

            return planRewriter.defaultRewrite(node, new Context(newLookupSymbols, context.getSuccess()));
        }

        @Override
        public PlanNode rewriteFilter(FilterNode node, Context context, PlanRewriter<Context> planRewriter)
        {
            return planRewriter.defaultRewrite(node, new Context(context.getLookupSymbols(), context.getSuccess()));
        }

        @Override
        public PlanNode rewriteIndexSource(IndexSourceNode node, Context context, PlanRewriter<Context> planRewriter)
        {
            throw new IllegalStateException("Should not be trying to generate an Index on something that has already been determined to use an Index");
        }

        @Override
        public PlanNode rewriteIndexJoin(IndexJoinNode node, Context context, PlanRewriter<Context> planRewriter)
        {
            if (!node.getProbeSource().getOutputSymbols().containsAll(context.getLookupSymbols())) {
                // Can only pass through another IndexJoin if the lookup symbols all come from the probe side. Otherwise, give up.
                return node;
            }

            PlanNode rewrittenProbeSource = planRewriter.rewrite(node.getProbeSource(), new Context(context.getLookupSymbols(), context.getSuccess()));

            PlanNode source = node;
            if (rewrittenProbeSource != node.getProbeSource()) {
                source = new IndexJoinNode(node.getId(), node.getType(), rewrittenProbeSource, node.getIndexSource(), node.getCriteria());
            }

            return source;
        }

        @Override
        public PlanNode rewriteAggregation(AggregationNode node, Context context, PlanRewriter<Context> planRewriter)
        {
            if (!node.getGroupBy().containsAll(context.getLookupSymbols())) {
                // Every lookup symbol must be part of the group by for the index join to work. If not, then give up
                return node;
            }

            return planRewriter.defaultRewrite(node, new Context(context.getLookupSymbols(), context.getSuccess()));
        }

        @Override
        public PlanNode rewriteSort(SortNode node, Context context, PlanRewriter<Context> planRewriter)
        {
            // Sort has no bearing when building an index, so just ignore the sort
            return planRewriter.rewrite(node.getSource(), context);
        }

        public static class Context
        {
            private final Set<Symbol> lookupSymbols;
            private final AtomicBoolean success;

            public Context(Set<Symbol> lookupSymbols, AtomicBoolean success)
            {
                checkArgument(!lookupSymbols.isEmpty(), "lookupSymbols can not be empty");
                this.lookupSymbols = ImmutableSet.copyOf(checkNotNull(lookupSymbols, "lookupSymbols is null"));
                this.success = checkNotNull(success, "success is null");
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
     * the actual symbols produced by the IndexSource. Note: multiple top-level lookup symbols may share the same
     * underlying IndexSource symbol.
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
                Map<Symbol, Symbol> outputToSourceMap = FluentIterable.from(lookupSymbols)
                        .toMap(Functions.compose(symbolFromReferenceGetter(), Functions.forMap(node.getOutputMap())));

                // Map from source Symbols to underlying index Symbols
                Map<Symbol, Symbol> sourceToIndexMap = node.getSource().accept(this, ImmutableSet.copyOf(outputToSourceMap.values()));

                return FluentIterable.from(lookupSymbols)
                        .toMap(Functions.compose(Functions.forMap(sourceToIndexMap), Functions.forMap(outputToSourceMap)));
            }

            @Override
            public Map<Symbol, Symbol> visitFilter(FilterNode node, Set<Symbol> lookupSymbols)
            {
                return node.getSource().accept(this, lookupSymbols);
            }

            @Override
            public Map<Symbol, Symbol> visitIndexJoin(IndexJoinNode node, Set<Symbol> lookupSymbols)
            {
                checkState(node.getProbeSource().getOutputSymbols().containsAll(lookupSymbols), "lookupSymbols must be entirely part of IndexJoin probe");
                return node.getProbeSource().accept(this, lookupSymbols);
            }

            @Override
            public Map<Symbol, Symbol> visitAggregation(AggregationNode node, Set<Symbol> lookupSymbols)
            {
                checkState(node.getGroupBy().containsAll(lookupSymbols), "lookupSymbols must be entirely part of group by");
                return node.getSource().accept(this, lookupSymbols);
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
