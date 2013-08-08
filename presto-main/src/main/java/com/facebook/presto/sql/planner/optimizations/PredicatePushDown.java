package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.sql.analyzer.ExpressionAnalyzer;
import com.facebook.presto.sql.analyzer.Field;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.analyzer.TupleDescriptor;
import com.facebook.presto.sql.analyzer.Type;
import com.facebook.presto.sql.planner.DependencyExtractor;
import com.facebook.presto.sql.planner.DeterminismEvaluator;
import com.facebook.presto.sql.planner.EffectivePredicateExtractor;
import com.facebook.presto.sql.planner.EqualityInference;
import com.facebook.presto.sql.planner.ExpressionInterpreter;
import com.facebook.presto.sql.planner.ExpressionSymbolInliner;
import com.facebook.presto.sql.planner.NoOpSymbolResolver;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
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
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.util.IterableTransformer;
import com.facebook.presto.util.MapTransformer;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.sql.ExpressionUtils.and;
import static com.facebook.presto.sql.ExpressionUtils.combineConjuncts;
import static com.facebook.presto.sql.ExpressionUtils.extractConjuncts;
import static com.facebook.presto.sql.ExpressionUtils.stripNonDeterministicConjuncts;
import static com.facebook.presto.sql.ExpressionUtils.symbolToQualifiedNameReference;
import static com.facebook.presto.sql.planner.DeterminismEvaluator.deterministic;
import static com.facebook.presto.sql.planner.DeterminismEvaluator.isDeterministic;
import static com.facebook.presto.sql.planner.EqualityInference.createEqualityInference;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;

public class PredicatePushDown
        extends PlanOptimizer
{
    private final Metadata metadata;

    public PredicatePushDown(Metadata metadata)
    {
        this.metadata = checkNotNull(metadata, "metadata is null");
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        checkNotNull(plan, "plan is null");
        checkNotNull(session, "session is null");
        checkNotNull(types, "types is null");
        checkNotNull(idAllocator, "idAllocator is null");

        return PlanRewriter.rewriteWith(new Rewriter(symbolAllocator, idAllocator, metadata, session), plan, BooleanLiteral.TRUE_LITERAL);
    }

    private static class Rewriter
            extends PlanNodeRewriter<Expression>
    {
        private final SymbolAllocator symbolAllocator;
        private final PlanNodeIdAllocator idAllocator;
        private final Metadata metadata;
        private final Session session;

        private Rewriter(SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator, Metadata metadata, Session session)
        {
            this.symbolAllocator = checkNotNull(symbolAllocator, "symbolAllocator is null");
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
            Expression inlinedPredicate = ExpressionTreeRewriter.rewriteWith(new ExpressionSymbolInliner(node.getOutputMap()), inheritedPredicate);
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
                Expression sourcePredicate = ExpressionTreeRewriter.rewriteWith(new ExpressionSymbolInliner(node.sourceSymbolMap(i)), inheritedPredicate);
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
            return planRewriter.rewrite(node.getSource(), combineConjuncts(node.getPredicate(), inheritedPredicate));
        }

        @Override
        public PlanNode rewriteJoin(JoinNode node, Expression inheritedPredicate, PlanRewriter<Expression> planRewriter)
        {
            // See if we can rewrite outer joins in terms of a plain inner join
            node = tryNormalizeToInnerJoin(node, inheritedPredicate);

            Expression leftEffectivePredicate = EffectivePredicateExtractor.extract(node.getLeft());
            Expression rightEffectivePredicate = EffectivePredicateExtractor.extract(node.getRight());
            Expression joinPredicate = extractJoinPredicate(node);

            Expression leftPredicate;
            Expression rightPredicate;
            Expression postJoinPredicate;
            Expression newJoinPredicate;

            switch (node.getType()) {
                case INNER:
                    InnerJoinPushDownResult innerJoinPushDownResult = processInnerJoin(inheritedPredicate,
                            leftEffectivePredicate,
                            rightEffectivePredicate,
                            joinPredicate,
                            node.getLeft().getOutputSymbols());
                    leftPredicate = innerJoinPushDownResult.getLeftPredicate();
                    rightPredicate = innerJoinPushDownResult.getRightPredicate();
                    postJoinPredicate = innerJoinPushDownResult.getPostJoinPredicate();
                    newJoinPredicate = innerJoinPushDownResult.getJoinPredicate();
                    break;
                case LEFT:
                    OuterJoinPushDownResult leftOuterJoinPushDownResult = processOuterJoin(inheritedPredicate,
                            leftEffectivePredicate,
                            rightEffectivePredicate,
                            joinPredicate,
                            node.getLeft().getOutputSymbols());
                    leftPredicate = leftOuterJoinPushDownResult.getOuterJoinPredicate();
                    rightPredicate = leftOuterJoinPushDownResult.getInnerJoinPredicate();
                    postJoinPredicate = leftOuterJoinPushDownResult.getPostJoinPredicate();
                    newJoinPredicate = joinPredicate; // Use the same as the original
                    break;
                case RIGHT:
                    OuterJoinPushDownResult rightOuterJoinPushDownResult = processOuterJoin(inheritedPredicate,
                            rightEffectivePredicate,
                            leftEffectivePredicate,
                            joinPredicate,
                            node.getRight().getOutputSymbols());
                    leftPredicate = rightOuterJoinPushDownResult.getInnerJoinPredicate();
                    rightPredicate = rightOuterJoinPushDownResult.getOuterJoinPredicate();
                    postJoinPredicate = rightOuterJoinPushDownResult.getPostJoinPredicate();
                    newJoinPredicate = joinPredicate; // Use the same as the original
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported join type: " + node.getType());
            }


            PlanNode leftSource = planRewriter.rewrite(node.getLeft(), leftPredicate);
            PlanNode rightSource = planRewriter.rewrite(node.getRight(), rightPredicate);

            PlanNode output = node;
            if (leftSource != node.getLeft() || rightSource != node.getRight() || !newJoinPredicate.equals(joinPredicate)) {
                List<JoinNode.EquiJoinClause> criteria = node.getCriteria();

                // Rewrite criteria and add projections if there is a new join predicate
                if (!newJoinPredicate.equals(joinPredicate)) {
                    // Create identity projections for all existing symbols
                    ImmutableMap.Builder<Symbol, Expression> leftProjections = ImmutableMap.builder();
                    leftProjections.putAll(IterableTransformer.<Symbol>on(node.getLeft().getOutputSymbols())
                            .toMap(symbolToQualifiedNameReference())
                            .map());
                    ImmutableMap.Builder<Symbol, Expression> rightProjections = ImmutableMap.builder();
                    rightProjections.putAll(IterableTransformer.<Symbol>on(node.getRight().getOutputSymbols())
                            .toMap(symbolToQualifiedNameReference())
                            .map());

                    // HACK! we don't support cross joins right now, so put in a simple fake join predicate instead if all of the join clauses got simplified out
                    // TODO: remove this code when cross join support is added
                    Iterable<Expression> simplifiedJoinConjuncts = transform(extractConjuncts(newJoinPredicate), simplifyExpressions());
                    simplifiedJoinConjuncts = filter(simplifiedJoinConjuncts, not(Predicates.<Expression>equalTo(BooleanLiteral.TRUE_LITERAL)));
                    if (Iterables.isEmpty(simplifiedJoinConjuncts)) {
                        simplifiedJoinConjuncts = ImmutableList.<Expression>of(new ComparisonExpression(ComparisonExpression.Type.EQUAL, new LongLiteral("0"), new LongLiteral("0")));
                    }

                    // Create new projections for the new join clauses
                    ImmutableList.Builder<JoinNode.EquiJoinClause> builder = ImmutableList.builder();
                    for (Expression conjunct : simplifiedJoinConjuncts) {
                        checkState(joinEqualityExpression(node.getLeft().getOutputSymbols()).apply(conjunct), "Expected join predicate to be a valid join equality");

                        ComparisonExpression equality = (ComparisonExpression) conjunct;

                        boolean alignedComparison = Iterables.all(DependencyExtractor.extractUnique(equality.getLeft()), in(node.getLeft().getOutputSymbols()));
                        Expression leftExpression = (alignedComparison) ? equality.getLeft() : equality.getRight();
                        Expression rightExpression = (alignedComparison) ? equality.getRight() : equality.getLeft();

                        Symbol leftSymbol = symbolAllocator.newSymbol(leftExpression, extractType(leftExpression));
                        leftProjections.put(leftSymbol, leftExpression);
                        Symbol rightSymbol = symbolAllocator.newSymbol(rightExpression, extractType(rightExpression));
                        rightProjections.put(rightSymbol, rightExpression);

                        builder.add(new JoinNode.EquiJoinClause(leftSymbol, rightSymbol));
                    }

                    leftSource = new ProjectNode(idAllocator.getNextId(), leftSource, leftProjections.build());
                    rightSource = new ProjectNode(idAllocator.getNextId(), rightSource, rightProjections.build());
                    criteria = builder.build();
                }
                output = new JoinNode(node.getId(), node.getType(), leftSource, rightSource, criteria);
            }
            if (!postJoinPredicate.equals(BooleanLiteral.TRUE_LITERAL)) {
                output = new FilterNode(idAllocator.getNextId(), output, postJoinPredicate);
            }
            return output;
        }

        private OuterJoinPushDownResult processOuterJoin(Expression inheritedPredicate, Expression outerEffectivePredicate, Expression innerEffectivePredicate, Expression joinPredicate, Collection<Symbol> outerSymbols)
        {
            checkArgument(Iterables.all(DependencyExtractor.extractUnique(outerEffectivePredicate), in(outerSymbols)), "outerEffectivePredicate must only contain symbols from outerSymbols");
            checkArgument(Iterables.all(DependencyExtractor.extractUnique(innerEffectivePredicate), not(in(outerSymbols))), "innerEffectivePredicate must not contain symbols from outerSymbols");

            ImmutableList.Builder<Expression> outerPushdownConjuncts = ImmutableList.builder();
            ImmutableList.Builder<Expression> innerPushdownConjuncts = ImmutableList.builder();
            ImmutableList.Builder<Expression> postJoinConjuncts = ImmutableList.builder();

            // Strip out non-deterministic conjuncts
            postJoinConjuncts.addAll(filter(extractConjuncts(inheritedPredicate), not(deterministic())));
            inheritedPredicate = stripNonDeterministicConjuncts(inheritedPredicate);

            outerEffectivePredicate = stripNonDeterministicConjuncts(outerEffectivePredicate);
            innerEffectivePredicate = stripNonDeterministicConjuncts(innerEffectivePredicate);
            joinPredicate = stripNonDeterministicConjuncts(joinPredicate);

            // Generate equality inferences
            EqualityInference inheritedInference = createEqualityInference(inheritedPredicate);
            EqualityInference outerInference = createEqualityInference(inheritedPredicate, outerEffectivePredicate);

            EqualityInference.EqualityPartition equalityPartition = inheritedInference.generateEqualitiesPartitionedBy(in(outerSymbols));
            Expression outerOnlyInheritedEqualities = combineConjuncts(equalityPartition.getScopeEqualities());
            EqualityInference potentialNullSymbolInference = createEqualityInference(outerOnlyInheritedEqualities, outerEffectivePredicate, innerEffectivePredicate, joinPredicate);
            EqualityInference potentialNullSymbolInferenceWithoutInnerInferred = createEqualityInference(outerOnlyInheritedEqualities, outerEffectivePredicate, joinPredicate);

            // Sort through conjuncts in inheritedPredicate that were not used for inference
            for (Expression conjunct : EqualityInference.nonInferrableConjuncts(inheritedPredicate)) {
                Expression outerRewritten = outerInference.rewriteExpression(conjunct, in(outerSymbols));
                if (outerRewritten != null) {
                    outerPushdownConjuncts.add(outerRewritten);

                    // A conjunct can only be pushed down into an inner side if it can be rewritten in terms of the outer side
                    Expression innerRewritten = potentialNullSymbolInference.rewriteExpression(outerRewritten, not(in(outerSymbols)));
                    if (innerRewritten != null) {
                        innerPushdownConjuncts.add(innerRewritten);
                    }
                }
                else {
                    postJoinConjuncts.add(conjunct);
                }
            }

            // See if we can push down any outer or join predicates to the inner side
            for (Expression conjunct : EqualityInference.nonInferrableConjuncts(and(outerEffectivePredicate, joinPredicate))) {
                Expression rewritten = potentialNullSymbolInference.rewriteExpression(conjunct, not(in(outerSymbols)));
                if (rewritten != null) {
                    innerPushdownConjuncts.add(rewritten);
                }
            }

            // TODO: consider adding join predicate optimizations to outer joins

            // Add the equalities from the inferences back in
            outerPushdownConjuncts.addAll(equalityPartition.getScopeEqualities());
            postJoinConjuncts.addAll(equalityPartition.getScopeComplementEqualities());
            postJoinConjuncts.addAll(equalityPartition.getScopeStraddlingEqualities());
            innerPushdownConjuncts.addAll(potentialNullSymbolInferenceWithoutInnerInferred.generateEqualitiesPartitionedBy(not(in(outerSymbols))).getScopeEqualities());

            return new OuterJoinPushDownResult(combineConjuncts(outerPushdownConjuncts.build()),
                    combineConjuncts(innerPushdownConjuncts.build()),
                    combineConjuncts(postJoinConjuncts.build()));
        }

        private static class OuterJoinPushDownResult
        {
            private final Expression outerJoinPredicate;
            private final Expression innerJoinPredicate;
            private final Expression postJoinPredicate;

            private OuterJoinPushDownResult(Expression outerJoinPredicate, Expression innerJoinPredicate, Expression postJoinPredicate)
            {
                this.outerJoinPredicate = outerJoinPredicate;
                this.innerJoinPredicate = innerJoinPredicate;
                this.postJoinPredicate = postJoinPredicate;
            }

            private Expression getOuterJoinPredicate()
            {
                return outerJoinPredicate;
            }

            private Expression getInnerJoinPredicate()
            {
                return innerJoinPredicate;
            }

            private Expression getPostJoinPredicate()
            {
                return postJoinPredicate;
            }
        }

        private InnerJoinPushDownResult processInnerJoin(Expression inheritedPredicate, Expression leftEffectivePredicate, Expression rightEffectivePredicate, Expression joinPredicate, Collection<Symbol> leftSymbols)
        {
            checkArgument(Iterables.all(DependencyExtractor.extractUnique(leftEffectivePredicate), in(leftSymbols)), "leftEffectivePredicate must only contain symbols from leftSymbols");
            checkArgument(Iterables.all(DependencyExtractor.extractUnique(rightEffectivePredicate), not(in(leftSymbols))), "rightEffectivePredicate must not contain symbols from leftSymbols");

            ImmutableList.Builder<Expression> leftPushDownConjuncts = ImmutableList.builder();
            ImmutableList.Builder<Expression> rightPushDownConjuncts = ImmutableList.builder();
            ImmutableList.Builder<Expression> joinConjuncts = ImmutableList.builder();

            // Strip out non-deterministic conjuncts
            joinConjuncts.addAll(filter(extractConjuncts(inheritedPredicate), not(deterministic())));
            inheritedPredicate = stripNonDeterministicConjuncts(inheritedPredicate);

            joinConjuncts.addAll(filter(extractConjuncts(joinPredicate), not(deterministic())));
            joinPredicate = stripNonDeterministicConjuncts(joinPredicate);

            leftEffectivePredicate = stripNonDeterministicConjuncts(leftEffectivePredicate);
            rightEffectivePredicate = stripNonDeterministicConjuncts(rightEffectivePredicate);

            // Generate equality inferences
            EqualityInference allInference = createEqualityInference(inheritedPredicate, leftEffectivePredicate, rightEffectivePredicate, joinPredicate);
            EqualityInference allInferenceWithoutLeftInferred = createEqualityInference(inheritedPredicate, rightEffectivePredicate, joinPredicate);
            EqualityInference allInferenceWithoutRightInferred = createEqualityInference(inheritedPredicate, leftEffectivePredicate, joinPredicate);

            // Sort through conjuncts in inheritedPredicate that were not used for inference
            for (Expression conjunct : EqualityInference.nonInferrableConjuncts(inheritedPredicate)) {
                Expression leftRewrittenConjunct = allInference.rewriteExpression(conjunct, in(leftSymbols));
                if (leftRewrittenConjunct != null) {
                    leftPushDownConjuncts.add(leftRewrittenConjunct);
                }

                Expression rightRewrittenConjunct = allInference.rewriteExpression(conjunct, not(in(leftSymbols)));
                if (rightRewrittenConjunct != null) {
                    rightPushDownConjuncts.add(rightRewrittenConjunct);
                }

                // Drop predicate after join only if unable to push down to either side
                if (leftRewrittenConjunct == null && rightRewrittenConjunct == null) {
                    joinConjuncts.add(conjunct);
                }
            }

            // See if we can push the right effective predicate to the left side
            for (Expression conjunct : EqualityInference.nonInferrableConjuncts(rightEffectivePredicate)) {
                Expression rewritten = allInference.rewriteExpression(conjunct, in(leftSymbols));
                if (rewritten != null) {
                    leftPushDownConjuncts.add(rewritten);
                }
            }

            // See if we can push the left effective predicate to the right side
            for (Expression conjunct : EqualityInference.nonInferrableConjuncts(leftEffectivePredicate)) {
                Expression rewritten = allInference.rewriteExpression(conjunct, not(in(leftSymbols)));
                if (rewritten != null) {
                    rightPushDownConjuncts.add(rewritten);
                }
            }

            // See if we can push any parts of the join predicates to either side
            for (Expression conjunct : EqualityInference.nonInferrableConjuncts(joinPredicate)) {
                Expression leftRewritten = allInference.rewriteExpression(conjunct, in(leftSymbols));
                if (leftRewritten != null) {
                    leftPushDownConjuncts.add(leftRewritten);
                }

                Expression rightRewritten = allInference.rewriteExpression(conjunct, not(in(leftSymbols)));
                if (rightRewritten != null) {
                    rightPushDownConjuncts.add(rightRewritten);
                }

                if (leftRewritten == null && rightRewritten == null) {
                    joinConjuncts.add(conjunct);
                }
            }

            // Add equalities from the inference back in
            leftPushDownConjuncts.addAll(allInferenceWithoutLeftInferred.generateEqualitiesPartitionedBy(in(leftSymbols)).getScopeEqualities());
            rightPushDownConjuncts.addAll(allInferenceWithoutRightInferred.generateEqualitiesPartitionedBy(not(in(leftSymbols))).getScopeEqualities());
            joinConjuncts.addAll(allInference.generateEqualitiesPartitionedBy(in(leftSymbols)).getScopeStraddlingEqualities()); // scope straddling equalities get dropped in as part of the join predicate

            // Since we only currently support equality in join conjuncts, factor out the non-equality conjuncts to a post-join filter
            List<Expression> joinConjunctsList = joinConjuncts.build();
            List<Expression> postJoinConjuncts = ImmutableList.copyOf(filter(joinConjunctsList, not(joinEqualityExpression(leftSymbols))));
            joinConjunctsList = ImmutableList.copyOf(filter(joinConjunctsList, joinEqualityExpression(leftSymbols)));

            return new InnerJoinPushDownResult(combineConjuncts(leftPushDownConjuncts.build()), combineConjuncts(rightPushDownConjuncts.build()), combineConjuncts(joinConjunctsList), combineConjuncts(postJoinConjuncts));
        }

        private static class InnerJoinPushDownResult
        {
            private final Expression leftPredicate;
            private final Expression rightPredicate;
            private final Expression joinPredicate;
            private final Expression postJoinPredicate;

            private InnerJoinPushDownResult(Expression leftPredicate, Expression rightPredicate, Expression joinPredicate, Expression postJoinPredicate)
            {
                this.leftPredicate = leftPredicate;
                this.rightPredicate = rightPredicate;
                this.joinPredicate = joinPredicate;
                this.postJoinPredicate = postJoinPredicate;
            }

            private Expression getLeftPredicate()
            {
                return leftPredicate;
            }

            private Expression getRightPredicate()
            {
                return rightPredicate;
            }

            private Expression getJoinPredicate()
            {
                return joinPredicate;
            }

            private Expression getPostJoinPredicate()
            {
                return postJoinPredicate;
            }
        }

        private static Expression extractJoinPredicate(JoinNode joinNode)
        {
            ImmutableList.Builder<Expression> builder = ImmutableList.builder();
            for (JoinNode.EquiJoinClause equiJoinClause : joinNode.getCriteria()) {
                builder.add(new ComparisonExpression(ComparisonExpression.Type.EQUAL,
                        new QualifiedNameReference(equiJoinClause.getLeft().toQualifiedName()),
                        new QualifiedNameReference(equiJoinClause.getRight().toQualifiedName())));
            }
            return combineConjuncts(builder.build());
        }

        // TODO: temporary addition to infer result type from expression. fix this with the new planner refactoring (martint)
        private Type extractType(Expression expression)
        {
            ExpressionAnalyzer expressionAnalyzer = new ExpressionAnalyzer(metadata);
            List<Field> fields = IterableTransformer.<Symbol>on(DependencyExtractor.extractUnique(expression))
                    .transform(new Function<Symbol, Field>()
                    {
                        @Override
                        public Field apply(Symbol symbol)
                        {
                            return Field.newUnqualified(symbol.getName(), symbolAllocator.getTypes().get(symbol));
                        }
                    })
                    .list();
            return expressionAnalyzer.analyze(expression, new TupleDescriptor(fields));
        }

        private JoinNode tryNormalizeToInnerJoin(JoinNode node, Expression inheritedPredicate)
        {
            Preconditions.checkArgument(EnumSet.of(JoinNode.Type.INNER, JoinNode.Type.RIGHT, JoinNode.Type.LEFT).contains(node.getType()), "Unsupported join type: %s", node.getType());

            if (node.getType() == JoinNode.Type.INNER ||
                    node.getType() == JoinNode.Type.LEFT && !canConvertOuterToInner(node.getRight().getOutputSymbols(), inheritedPredicate) ||
                    node.getType() == JoinNode.Type.RIGHT && !canConvertOuterToInner(node.getLeft().getOutputSymbols(), inheritedPredicate)) {
                return node;
            }
            return new JoinNode(node.getId(), JoinNode.Type.INNER, node.getLeft(), node.getRight(), node.getCriteria());
        }

        private boolean canConvertOuterToInner(List<Symbol> innerSymbolsForOuterJoin, Expression inheritedPredicate)
        {
            Set<Symbol> innerSymbols = ImmutableSet.copyOf(innerSymbolsForOuterJoin);
            for (Expression conjunct : extractConjuncts(inheritedPredicate)) {
                if (DeterminismEvaluator.isDeterministic(conjunct)) {
                    // Ignore a conjunct for this test if we can not deterministically get responses from it
                    Object response = nullInputEvaluator(innerSymbols, conjunct);
                    if (response == null || Boolean.FALSE.equals(response)) {
                        // If there is a single conjunct that returns FALSE or NULL given all NULL inputs for the inner side symbols of an outer join
                        // then this conjunct removes all effects of the outer join, and effectively turns this into an equivalent of an inner join.
                        // So, let's just rewrite this join as an INNER join
                        return true;
                    }
                }
            }
            return false;
        }

        // Temporary implementation for joins because the SimplifyExpressions optimizers can not run properly on join clauses
        private Function<Expression, Expression> simplifyExpressions()
        {
            return new Function<Expression, Expression>()
            {
                @Override
                public Expression apply(Expression expression)
                {
                    ExpressionInterpreter optimizer = ExpressionInterpreter.expressionOptimizer(NoOpSymbolResolver.INSTANCE, metadata, session);
                    return ExpressionInterpreter.toExpression(optimizer.process(expression, null));
                }
            };
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

        private static Predicate<Expression> joinEqualityExpression(final Collection<Symbol> leftSymbols)
        {
            return new Predicate<Expression>()
            {
                @Override
                public boolean apply(Expression expression)
                {
                    // At this point in time, our join predicates need to be deterministic
                    if (isDeterministic(expression) && expression instanceof ComparisonExpression) {
                        ComparisonExpression comparison = (ComparisonExpression) expression;
                        if (comparison.getType() == ComparisonExpression.Type.EQUAL) {
                            Set<Symbol> symbols1 = DependencyExtractor.extractUnique(comparison.getLeft());
                            Set<Symbol> symbols2 = DependencyExtractor.extractUnique(comparison.getRight());
                            return (Iterables.all(symbols1, in(leftSymbols)) && Iterables.all(symbols2, not(in(leftSymbols)))) ||
                                    (Iterables.all(symbols2, in(leftSymbols)) && Iterables.all(symbols1, not(in(leftSymbols))));
                        }
                    }
                    return false;
                }
            };
        }

        @Override
        public PlanNode rewriteAggregation(AggregationNode node, Expression inheritedPredicate, PlanRewriter<Expression> planRewriter)
        {
            EqualityInference equalityInference = createEqualityInference(inheritedPredicate);

            List<Expression> pushdownConjuncts = new ArrayList<>();
            List<Expression> postAggregationConjuncts = new ArrayList<>();

            // Strip out non-deterministic conjuncts
            postAggregationConjuncts.addAll(ImmutableList.copyOf(filter(extractConjuncts(inheritedPredicate), not(deterministic()))));
            inheritedPredicate = stripNonDeterministicConjuncts(inheritedPredicate);

            // Sort non-equality predicates by those that can be pushed down and those that cannot
            for (Expression conjunct : EqualityInference.nonInferrableConjuncts(inheritedPredicate)) {
                Expression rewrittenConjunct = equalityInference.rewriteExpression(conjunct, in(node.getGroupBy()));
                if (rewrittenConjunct != null) {
                    pushdownConjuncts.add(rewrittenConjunct);
                }
                else {
                    postAggregationConjuncts.add(conjunct);
                }
            }

            // Add the equality predicates back in
            EqualityInference.EqualityPartition equalityPartition = equalityInference.generateEqualitiesPartitionedBy(in(node.getGroupBy()));
            pushdownConjuncts.addAll(equalityPartition.getScopeEqualities());
            postAggregationConjuncts.addAll(equalityPartition.getScopeComplementEqualities());
            postAggregationConjuncts.addAll(equalityPartition.getScopeStraddlingEqualities());

            PlanNode rewrittenSource = planRewriter.rewrite(node.getSource(), combineConjuncts(pushdownConjuncts));

            PlanNode output = node;
            if (rewrittenSource != node.getSource()) {
                output = new AggregationNode(node.getId(), rewrittenSource, node.getGroupBy(), node.getAggregations(), node.getFunctions(), node.getStep());
            }
            if (!postAggregationConjuncts.isEmpty()) {
                output = new FilterNode(idAllocator.getNextId(), output, combineConjuncts(postAggregationConjuncts));
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

            EqualityInference equalityInference = createEqualityInference(inheritedPredicate);

            List<Expression> partitionConjuncts = new ArrayList<>();
            List<Expression> postScanConjuncts = new ArrayList<>();

            // Strip out non-deterministic conjuncts
            postScanConjuncts.addAll(ImmutableList.copyOf(filter(extractConjuncts(inheritedPredicate), not(deterministic()))));
            inheritedPredicate = stripNonDeterministicConjuncts(inheritedPredicate);

            // Sort non-equality predicates by those that can be applied exclusively to partition keys and those that cannot
            for (Expression conjunct : EqualityInference.nonInferrableConjuncts(inheritedPredicate)) {
                Expression rewrittenConjunct = equalityInference.rewriteExpression(conjunct, in(partitionSymbols));
                if (rewrittenConjunct != null) {
                    partitionConjuncts.add(rewrittenConjunct);
                }
                else {
                    postScanConjuncts.add(conjunct);
                }
            }

            // Add the equality predicates back in
            EqualityInference.EqualityPartition equalityPartition = equalityInference.generateEqualitiesPartitionedBy(in(partitionSymbols));
            partitionConjuncts.addAll(equalityPartition.getScopeEqualities());
            postScanConjuncts.addAll(equalityPartition.getScopeComplementEqualities());
            postScanConjuncts.addAll(equalityPartition.getScopeStraddlingEqualities());

            PlanNode output = node;
            if (!partitionConjuncts.isEmpty() || !postScanConjuncts.isEmpty()) {
                // Merge the partition conjuncts, but overwrite the upstream predicate hint
                if (!node.getPartitionPredicate().equals(BooleanLiteral.TRUE_LITERAL)) {
                    partitionConjuncts.add(node.getPartitionPredicate());
                }
                output = new TableScanNode(node.getId(), node.getTable(), node.getOutputSymbols(), node.getAssignments(), combineConjuncts(partitionConjuncts), combineConjuncts(postScanConjuncts));
            }
            if (!postScanConjuncts.isEmpty()) {
                output = new FilterNode(idAllocator.getNextId(), output, combineConjuncts(postScanConjuncts));
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
}
