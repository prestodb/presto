package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.sql.ExpressionUtils;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.analyzer.Type;
import com.facebook.presto.sql.planner.DependencyExtractor;
import com.facebook.presto.sql.planner.DeterminismEvaluator;
import com.facebook.presto.sql.planner.ExpressionInliner;
import com.facebook.presto.sql.planner.ExpressionInterpreter;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolResolver;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeRewriter;
import com.facebook.presto.sql.planner.plan.PlanRewriter;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.UnionNode;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.TreeRewriter;
import com.facebook.presto.util.MapTransformer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.sql.ExpressionUtils.and;
import static com.facebook.presto.sql.ExpressionUtils.combineConjuncts;
import static com.facebook.presto.sql.ExpressionUtils.extractConjuncts;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.Iterables.filter;

public class PredicatePushDown
        extends PlanOptimizer
{
    private final Metadata metadata;

    public PredicatePushDown(Metadata metadata)
    {
        this.metadata = checkNotNull(metadata, "metadata is null");
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types, PlanNodeIdAllocator idAllocator)
    {
        checkNotNull(plan, "plan is null");
        checkNotNull(session, "session is null");
        checkNotNull(types, "types is null");
        checkNotNull(idAllocator, "idAllocator is null");

        return PlanRewriter.rewriteWith(new Rewriter(idAllocator, metadata, session), plan, BooleanLiteral.TRUE_LITERAL);
    }

    private static class Rewriter
            extends PlanNodeRewriter<Expression>
    {
        private final PlanNodeIdAllocator idAllocator;
        private final Metadata metadata;
        private final Session session;

        private Rewriter(PlanNodeIdAllocator idAllocator, Metadata metadata, Session session)
        {
            this.idAllocator = checkNotNull(idAllocator, "idAllocator is null");
            this.metadata = checkNotNull(metadata, "metadata is null");
            this.session = checkNotNull(session, "session is null");
        }

        @Override
        public PlanNode rewriteNode(PlanNode node, Expression inheritedPredicate, PlanRewriter<Expression> planRewriter)
        {
            PlanNode rewrittenNode = planRewriter.defaultRewrite(node, BooleanLiteral.TRUE_LITERAL);
            if (!inheritedPredicate.equals(BooleanLiteral.TRUE_LITERAL)) {
                // Drop in a FilterNode b/c we cannot push our predicate down any further
                rewrittenNode = new FilterNode(idAllocator.getNextId(), rewrittenNode, inheritedPredicate);
            }
            return rewrittenNode;
        }

        @Override
        public PlanNode rewriteProject(ProjectNode node, Expression inheritedPredicate, PlanRewriter<Expression> planRewriter)
        {
            Expression inlinedPredicate = TreeRewriter.rewriteWith(new ExpressionInliner(node.getOutputMap()), inheritedPredicate);
            return planRewriter.defaultRewrite(node, inlinedPredicate);
        }

        @Override
        public PlanNode rewriteSort(SortNode node, Expression inheritedPredicate, PlanRewriter<Expression> planRewriter)
        {
            return planRewriter.defaultRewrite(node, inheritedPredicate);
        }

        @Override
        public PlanNode rewriteUnion(UnionNode node, Expression inheritedPredicate, PlanRewriter<Expression> planRewriter)
        {
            boolean modified = false;
            ImmutableList.Builder<PlanNode> builder = ImmutableList.builder();
            for (int i = 0; i < node.getSources().size(); i++) {
                Expression sourcePredicate = TreeRewriter.rewriteWith(new ExpressionInliner(node.sourceSymbolMap(i)), inheritedPredicate);
                PlanNode source = node.getSources().get(i);
                PlanNode rewrittenSource = planRewriter.rewrite(source, sourcePredicate);
                if (rewrittenSource != source) {
                    modified = true;
                }
                builder.add(rewrittenSource);
            }

            if (modified) {
                return new UnionNode(node.getId(), builder.build(), node.getSymbolMapping());
            }

            return node;
        }

        @Override
        public PlanNode rewriteFilter(FilterNode node, Expression inheritedPredicate, PlanRewriter<Expression> planRewriter)
        {
            Expression predicate = node.getPredicate();
            if (!inheritedPredicate.equals(BooleanLiteral.TRUE_LITERAL)) {
                predicate = and(predicate, inheritedPredicate);
            }
            return planRewriter.rewrite(node.getSource(), predicate);
        }

        // TODO: add predicate pull up for joins
        @Override
        public PlanNode rewriteJoin(JoinNode node, Expression inheritedPredicate, PlanRewriter<Expression> planRewriter)
        {
            if (node.getType() == JoinNode.Type.LEFT) {
                Set<Symbol> rightSymbols = ImmutableSet.copyOf(node.getRight().getOutputSymbols());
                for (Expression conjunct : extractConjuncts(inheritedPredicate)) {
                    if (DeterminismEvaluator.isDeterministic(conjunct)) {
                        // Ignore a conjunct for this test if we can not deterministically get responses from it
                        Object response = nullInputEvaluator(rightSymbols, conjunct);
                        if (response == null || BooleanLiteral.FALSE_LITERAL.equals(response)) {
                            // If there is a single conjunct that returns FALSE or NULL given all NULL inputs for right side symbols
                            // then this conjunct removes all effects of the outer join, and effectively turns this into an equivalent of an inner join.
                            // So, let's just rewrite this join as an INNER join
                            node = new JoinNode(node.getId(), JoinNode.Type.INNER, node.getLeft(), node.getRight(), node.getCriteria());
                            break;
                        }
                    }
                }
            }

            // Generate equality inferences
            EqualityInference.Builder inferenceBuilder = new EqualityInference.Builder();
            inferenceBuilder.addAllEqualities(filter(extractConjuncts(inheritedPredicate), simpleEquality()));
            EqualityInference inferenceWithoutJoin = inferenceBuilder.build(); // Equality inferences that do not factor in equi-join clauses

            for (JoinNode.EquiJoinClause equiJoinClause : node.getCriteria()) {
                inferenceBuilder.addEquality(equiJoinClause.getLeft(), equiJoinClause.getRight());
            }
            EqualityInference inferenceWithJoin = inferenceBuilder.build(); // Equality inferences that factor in equi-join clauses


            List<Expression> leftConjuncts = new ArrayList<>();
            List<Expression> rightConjuncts = new ArrayList<>();
            List<Expression> postJoinConjuncts = new ArrayList<>();

            // Organize all non-equality predicates by whether or not they can be pushed down to a particular side
            for (Expression conjunct : filter(extractConjuncts(inheritedPredicate), not(simpleEquality()))) {
                if (!DeterminismEvaluator.isDeterministic(conjunct)) {
                    // Not able to push a conjunct down through a join if it is not deterministic
                    postJoinConjuncts.add(conjunct);
                }
                else if (node.getType() == JoinNode.Type.INNER) {
                    Expression leftRewrittenConjunct = inferenceWithJoin.rewritePredicate(conjunct, in(node.getLeft().getOutputSymbols()));

                    if (leftRewrittenConjunct != null) {
                        leftConjuncts.add(leftRewrittenConjunct);
                    }

                    Expression rightRewrittenConjunct = inferenceWithJoin.rewritePredicate(conjunct, in(node.getRight().getOutputSymbols()));
                    if (rightRewrittenConjunct != null) {
                        rightConjuncts.add(rightRewrittenConjunct);
                    }

                    // Drop predicate after join if unable to push down to either side
                    if (leftRewrittenConjunct == null && rightRewrittenConjunct == null) {
                        postJoinConjuncts.add(conjunct);
                    }
                }
                else if (node.getType() == JoinNode.Type.LEFT) {
                    // Assuming that no predicate has turned this LEFT join into an INNER join
                    // We can only push down predicates bound to the left side

                    Set<Symbol> symbols = DependencyExtractor.extract(conjunct);
                    if (node.getLeft().getOutputSymbols().containsAll(symbols)) {
                        leftConjuncts.add(conjunct);

                        Expression rightRewrittenConjunct = inferenceWithJoin.rewritePredicate(conjunct, in(node.getRight().getOutputSymbols()));
                        if (rightRewrittenConjunct != null) {
                            rightConjuncts.add(rightRewrittenConjunct);
                        }
                    }
                    else {
                        postJoinConjuncts.add(conjunct);
                    }
                }
                else {
                    throw new UnsupportedOperationException("Unsupported join type: " + node.getType());
                }
            }

            // Add the equality predicates back in
            // First add the equalities scoped to each side of the join so that they can be pushed down as well
            leftConjuncts.addAll(inferenceWithJoin.scopedEqualityPredicates(in(node.getLeft().getOutputSymbols())));
            rightConjuncts.addAll(inferenceWithJoin.scopedEqualityPredicates(in(node.getRight().getOutputSymbols())));

            // Then add back the equalities that connect the left and right sides
            // ASSUMPTION: left output symbols are the complement of right output symbols
            // Make sure to not use the join clauses when computing the group bridge predicates b/c they will make outer joins incorrect and will already be evaluated
            // by the join clause
            postJoinConjuncts.addAll(inferenceWithoutJoin.scopeBridgePredicates(in(node.getLeft().getOutputSymbols())));

            PlanNode leftSource = planRewriter.rewrite(node.getLeft(), ExpressionUtils.combineConjuncts(leftConjuncts));
            PlanNode rightSource = planRewriter.rewrite(node.getRight(), ExpressionUtils.combineConjuncts(rightConjuncts));

            PlanNode output = node;
            if (leftSource != node.getLeft() || rightSource != node.getRight()) {
                output = new JoinNode(node.getId(), node.getType(), leftSource, rightSource, node.getCriteria());
            }
            if (!postJoinConjuncts.isEmpty()) {
                output = new FilterNode(idAllocator.getNextId(), output, and(postJoinConjuncts));
            }
            return output;
        }

        /**
         * Evaluates an expression's response to binding the specified input symbols to NULL
         */
        private Object nullInputEvaluator(final Collection<Symbol> nullSymbols, Expression expression)
        {
            return ExpressionInterpreter.expressionOptimizer(new SymbolResolver()
            {
                @Override
                public Object getValue(Symbol symbol)
                {
                    return nullSymbols.contains(symbol) ? null : new QualifiedNameReference(symbol.toQualifiedName());

                }
            }, metadata, session).process(expression, null);
        }

        @Override
        public PlanNode rewriteAggregation(AggregationNode node, Expression inheritedPredicate, PlanRewriter<Expression> planRewriter)
        {
            EqualityInference equalityInference = new EqualityInference.Builder()
                    .addAllEqualities(filter(extractConjuncts(inheritedPredicate), simpleEquality()))
                    .build();

            List<Expression> pushdownConjuncts = new ArrayList<>();
            List<Expression> postAggregationConjuncts = new ArrayList<>();

            // Sort non-equality predicates by those that can be pushed down and those that cannot
            for (Expression conjunct : filter(extractConjuncts(inheritedPredicate), not(simpleEquality()))) {
                Expression rewrittenConjunct = equalityInference.rewritePredicate(conjunct, in(node.getGroupBy()));
                if (rewrittenConjunct != null && DeterminismEvaluator.isDeterministic(rewrittenConjunct)) {
                    pushdownConjuncts.add(rewrittenConjunct);
                }
                else {
                    postAggregationConjuncts.add(conjunct);
                }
            }

            // Add the equality predicates back in
            pushdownConjuncts.addAll(equalityInference.scopedEqualityPredicates(in(node.getGroupBy())));
            postAggregationConjuncts.addAll(equalityInference.scopedEqualityPredicates(not(in(node.getGroupBy()))));
            postAggregationConjuncts.addAll(equalityInference.scopeBridgePredicates(in(node.getGroupBy())));

            PlanNode rewrittenSource = planRewriter.rewrite(node.getSource(), ExpressionUtils.combineConjuncts(pushdownConjuncts));

            PlanNode output = node;
            if (rewrittenSource != node.getSource()) {
                output = new AggregationNode(node.getId(), rewrittenSource, node.getGroupBy(), node.getAggregations(), node.getFunctions(), node.getStep());
            }
            if (!postAggregationConjuncts.isEmpty()) {
                output = new FilterNode(idAllocator.getNextId(), output, and(postAggregationConjuncts));
            }
            return output;
        }

        @Override
        public PlanNode rewriteTableScan(TableScanNode node, Expression inheritedPredicate, PlanRewriter<Expression> planRewriter)
        {
            // TODO: add predicate negotiation with connector so that we can dynamically filter out the parts of the expression that can be pre-processed
            // For now, assume that connectors will only be able to pre-process predicates on partition keys,
            // but also pass in the remaining predicates as a hint to the table scan (needed for certain
            // connectors like InformationSchema that need to know about upstream predicates).

            Set<Symbol> partitionSymbols = Sets.filter(MapTransformer.of(node.getAssignments())
                    .filterValues(partitionColumn(node.getTable()))
                    .immutableMap()
                    .keySet(),
                    in(node.getOutputSymbols()));

            EqualityInference equalityInference = new EqualityInference.Builder()
                    .addAllEqualities(filter(extractConjuncts(inheritedPredicate), simpleEquality()))
                    .build();

            List<Expression> partitionConjuncts = new ArrayList<>();
            List<Expression> postScanConjuncts = new ArrayList<>();

            // Sort non-equality predicates by those that can be applied exclusively to partition keys and those that cannot
            for (Expression conjunct : filter(extractConjuncts(inheritedPredicate), not(simpleEquality()))) {
                Expression rewrittenConjunct = equalityInference.rewritePredicate(conjunct, in(partitionSymbols));
                if (rewrittenConjunct != null && DeterminismEvaluator.isDeterministic(rewrittenConjunct)) {
                    partitionConjuncts.add(rewrittenConjunct);
                }
                else {
                    postScanConjuncts.add(conjunct);
                }
            }

            // Add the equality predicates back in
            partitionConjuncts.addAll(equalityInference.scopedEqualityPredicates(in(partitionSymbols)));
            postScanConjuncts.addAll(equalityInference.scopedEqualityPredicates(not(in(partitionSymbols))));
            postScanConjuncts.addAll(equalityInference.scopeBridgePredicates(in(partitionSymbols)));

            PlanNode output = node;
            if (!partitionConjuncts.isEmpty() || !postScanConjuncts.isEmpty()) {
                // Merge the partition conjuncts, but overwrite the upstream predicate hint
                if (!node.getPartitionPredicate().equals(BooleanLiteral.TRUE_LITERAL)) {
                    partitionConjuncts.add(node.getPartitionPredicate());
                }
                output = new TableScanNode(node.getId(), node.getTable(), node.getOutputSymbols(), node.getAssignments(), combineConjuncts(partitionConjuncts), combineConjuncts(postScanConjuncts));
            }
            if (!postScanConjuncts.isEmpty()) {
                output = new FilterNode(idAllocator.getNextId(), output, and(postScanConjuncts));
            }
            return output;
        }

        private Predicate<ColumnHandle> partitionColumn(final TableHandle tableHandle)
        {
            return new Predicate<ColumnHandle>()
            {
                @Override
                public boolean apply(ColumnHandle columnHandle)
                {
                    return metadata.getColumnMetadata(tableHandle, columnHandle).isPartitionKey();
                }
            };
        }
    }

    private static Predicate<Expression> simpleEquality()
    {
        return new Predicate<Expression>()
        {
            @Override
            public boolean apply(Expression expression)
            {
                if (expression instanceof ComparisonExpression) {
                    ComparisonExpression comparison = (ComparisonExpression) expression;
                    if (comparison.getType() == ComparisonExpression.Type.EQUAL) {
                        if (comparison.getLeft() instanceof QualifiedNameReference && comparison.getRight() instanceof QualifiedNameReference) {
                            // We should only consider equalities that have distinct left and right components
                            return !comparison.getLeft().equals(comparison.getRight());
                        }
                    }
                }
                return false;
            }
        };
    }

    @VisibleForTesting
    static class EqualityInference
    {
        private final SetMultimap<Symbol, Symbol> equalitySets; // Indexed by canonical Symbol
        private final Map<Symbol, Symbol> canonicalMap; // Map symbols to canonical symbol

        private EqualityInference(SetMultimap<Symbol, Symbol> equalitySets)
        {
            this.equalitySets = ImmutableSetMultimap.copyOf(equalitySets);

            ImmutableMap.Builder<Symbol, Symbol> builder = ImmutableMap.builder();
            for (Map.Entry<Symbol, Symbol> entry : equalitySets.entries()) {
                Symbol canonical = entry.getKey();
                Symbol symbol = entry.getValue();
                builder.put(symbol, canonical);
            }
            canonicalMap = builder.build();
        }

        /**
         * Returns a canonical symbol within the specified symbolScope that is equivalent to the
         * specified symbol.
         *
         * Example:
         *
         *   Equalities:
         *      a = b = c = d
         *      e = f
         *
         *   SymbolScope: c, d, f
         *
         *   Canonical Scoped Symbol:
         *      a => c
         *      b => c
         *      c => c
         *      d => c
         *      e => f
         *      f => f
         *      g => null
         */
        public Symbol canonicalScopedSymbol(Symbol symbol, Predicate<Symbol> symbolScope)
        {
            Symbol canonical = canonicalMap.get(symbol);
            if (canonical == null) {
                return null;
            }
            Iterable<Symbol> filtered = filter(equalitySets.get(canonical), symbolScope);
            if (Iterables.isEmpty(filtered)) {
                return null;
            }
            return Ordering.natural().min(filtered);
        }

        /**
         * Generates the equality expressions that bridge the equality sets that have been partitioned
         * by the specified symbolScope
         *
         * Example:
         *
         *   Equalities:
         *      a = b = c = d
         *      e = f = g
         *
         *   SymbolScope: c, d, f, g
         *
         *   Expressions:
         *      a = c
         *      e = f
         */
        public List<Expression> scopeBridgePredicates(Predicate<Symbol> symbolScope)
        {
            ImmutableList.Builder<Expression> builder = ImmutableList.builder();
            for (Map.Entry<Symbol, Collection<Symbol>> entry : equalitySets.asMap().entrySet()) {
                Symbol canonical = canonicalScopedSymbol(entry.getKey(), symbolScope);
                Symbol complementCanonical = canonicalScopedSymbol(entry.getKey(), not(symbolScope));
                if (canonical != null && complementCanonical != null) {
                    builder.add(new ComparisonExpression(ComparisonExpression.Type.EQUAL,
                            new QualifiedNameReference(canonical.toQualifiedName()),
                            new QualifiedNameReference(complementCanonical.toQualifiedName())));
                }
            }
            return builder.build();
        }

        /**
         * Generates the equality expressions that span the specified symbol scope
         *
         * Example:
         *
         *   Equalities:
         *      a = b = c = d
         *      e = f = g
         *
         *   SymbolScope: b, c, d, f, g
         *
         *   Expressions:
         *      b = c
         *      c = d
         *      f = g
         */
        public List<Expression> scopedEqualityPredicates(Predicate<Symbol> symbolScope)
        {
            ImmutableList.Builder<Expression> builder = ImmutableList.builder();
            for (Map.Entry<Symbol, Collection<Symbol>> entry : equalitySets.asMap().entrySet()) {
                Symbol canonical = canonicalScopedSymbol(entry.getKey(), symbolScope);
                if (canonical != null) {
                    for (Symbol symbol : filter(entry.getValue(), symbolScope)) {
                        if (!symbol.equals(canonical)) {
                            builder.add(new ComparisonExpression(ComparisonExpression.Type.EQUAL,
                                    new QualifiedNameReference(canonical.toQualifiedName()),
                                    new QualifiedNameReference(symbol.toQualifiedName())));
                        }
                    }
                }
            }
            return builder.build();
        }

        /**
         * Attempts to rewrite a predicate in terms of the symbols allowed by the symbol scope
         * given the known equalities. Returns null if unsuccessful.
         */
        public Expression rewritePredicate(Expression predicate, Predicate<Symbol> symbolScope)
        {
            ImmutableMap.Builder<Symbol, QualifiedNameReference> symbolRemap = ImmutableMap.builder();
            Set<Symbol> symbols = DependencyExtractor.extract(predicate);
            // Try to write symbols not in the scope in terms of those that are
            for (Symbol symbol : filter(symbols, not(symbolScope))) {
                Symbol canonicalSymbol = canonicalScopedSymbol(symbol, symbolScope);
                if (canonicalSymbol == null) {
                    return null;
                }
                symbolRemap.put(symbol, new QualifiedNameReference(canonicalSymbol.toQualifiedName()));
            }
            return TreeRewriter.rewriteWith(new ExpressionInliner(symbolRemap.build()), predicate);
        }

        @VisibleForTesting
        static class Builder
        {
            private final Map<Symbol, Symbol> map = new HashMap<>();
            private final Multimap<Symbol, Symbol> reverseMap = HashMultimap.create();

            public Builder addAllEqualities(Iterable<Expression> expressions)
            {
                for (Expression expression : expressions) {
                    addEquality(expression);
                }
                return this;
            }

            public Builder addEquality(Expression expression)
            {
                checkArgument(simpleEquality().apply(expression), "Expression must be a simple equality: " + expression);

                ComparisonExpression comparison = (ComparisonExpression) expression;
                addEquality(Symbol.fromQualifiedName(((QualifiedNameReference) comparison.getLeft()).getName()),
                        Symbol.fromQualifiedName(((QualifiedNameReference) comparison.getRight()).getName()));
                return this;
            }

            public Builder addEquality(Symbol symbol1, Symbol symbol2)
            {
                checkArgument(!symbol1.equals(symbol2), "need to provide equality between different symbols");

                Symbol left = canonicalize(symbol1);
                Symbol right = canonicalize(symbol2);

                int comparison = left.compareTo(right);
                if (comparison > 0) {
                    map.put(left, right);
                    reverseMap.put(right, left);
                }
                else if (comparison < 0) {
                    map.put(right, left);
                    reverseMap.put(left, right);
                }
                return this;
            }

            private Symbol canonicalize(Symbol symbol)
            {
                Symbol canonical = symbol;
                while (map.containsKey(canonical)) {
                    canonical = map.get(canonical);
                }
                return canonical;
            }

            private void collectEqualities(Symbol symbol, ImmutableList.Builder<Symbol> builder)
            {
                builder.add(symbol);
                for (Symbol childSymbol : reverseMap.get(symbol)) {
                    collectEqualities(childSymbol, builder);
                }
            }

            private Collection<Symbol> extractEqualSymbols(Symbol symbol)
            {
                ImmutableList.Builder<Symbol> builder = ImmutableList.builder();
                collectEqualities(canonicalize(symbol), builder);
                return builder.build();
            }

            public EqualityInference build()
            {
                HashSet<Symbol> seenCanonicals = new HashSet<>();
                ImmutableSetMultimap.Builder<Symbol, Symbol> builder = ImmutableSetMultimap.builder();
                for (Symbol symbol : map.keySet()) {
                    Symbol canonical = canonicalize(symbol);
                    if (!seenCanonicals.contains(canonical)) {
                        builder.putAll(canonical, extractEqualSymbols(canonical));
                        seenCanonicals.add(canonical);
                    }
                }
                return new EqualityInference(builder.build());
            }
        }
    }
}
