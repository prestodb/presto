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
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.DeterminismEvaluator;
import com.facebook.presto.sql.planner.ExpressionInterpreter;
import com.facebook.presto.sql.planner.LiteralInterpreter;
import com.facebook.presto.sql.planner.NoOpSymbolResolver;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionRewriter;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.SymbolReference;
import com.facebook.presto.util.maps.IdentityLinkedHashMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.sql.ExpressionUtils.combinePredicates;
import static com.facebook.presto.sql.ExpressionUtils.extractPredicates;
import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.getExpressionTypes;
import static com.facebook.presto.sql.tree.BooleanLiteral.FALSE_LITERAL;
import static com.facebook.presto.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static com.facebook.presto.sql.tree.ComparisonExpressionType.IS_DISTINCT_FROM;
import static com.facebook.presto.sql.tree.LogicalBinaryExpression.Type.OR;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public class SimplifyExpressions
        implements PlanOptimizer
{
    private final Metadata metadata;
    private final SqlParser sqlParser;

    public SimplifyExpressions(Metadata metadata, SqlParser sqlParser)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(session, "session is null");
        requireNonNull(types, "types is null");
        requireNonNull(symbolAllocator, "symbolAllocator is null");
        requireNonNull(idAllocator, "idAllocator is null");
        return SimplePlanRewriter.rewriteWith(new Rewriter(metadata, sqlParser, session, types, idAllocator), plan);
    }

    private static class Rewriter
            extends SimplePlanRewriter<Void>
    {
        private final Metadata metadata;
        private final SqlParser sqlParser;
        private final Session session;
        private final Map<Symbol, Type> types;
        private final PlanNodeIdAllocator idAllocator;

        public Rewriter(Metadata metadata, SqlParser sqlParser, Session session, Map<Symbol, Type> types, PlanNodeIdAllocator idAllocator)
        {
            this.metadata = metadata;
            this.sqlParser = sqlParser;
            this.session = session;
            this.types = types;
            this.idAllocator = idAllocator;
        }

        @Override
        public PlanNode visitProject(ProjectNode node, RewriteContext<Void> context)
        {
            PlanNode source = context.rewrite(node.getSource());
            return new ProjectNode(node.getId(), source, node.getAssignments().rewrite(this::simplifyExpression));
        }

        @Override
        public PlanNode visitFilter(FilterNode node, RewriteContext<Void> context)
        {
            PlanNode source = context.rewrite(node.getSource());
            Expression simplified = simplifyExpression(node.getPredicate());
            if (simplified.equals(TRUE_LITERAL)) {
                return source;
            }
            // TODO: this needs to check whether the boolean expression coerces to false in a more general way.
            // E.g., simplify() not always produces a literal when the expression is constant
            else if (simplified.equals(FALSE_LITERAL) || simplified instanceof NullLiteral) {
                return new ValuesNode(idAllocator.getNextId(), node.getOutputSymbols(), ImmutableList.of());
            }
            return new FilterNode(node.getId(), source, simplified);
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, RewriteContext<Void> context)
        {
            Expression originalConstraint = null;
            if (node.getOriginalConstraint() != null) {
                originalConstraint = simplifyExpression(node.getOriginalConstraint());
            }
            return new TableScanNode(
                    node.getId(),
                    node.getTable(),
                    node.getOutputSymbols(),
                    node.getAssignments(),
                    node.getLayout(),
                    node.getCurrentConstraint(),
                    originalConstraint);
        }

        private Expression simplifyExpression(Expression expression)
        {
            if (expression instanceof SymbolReference) {
                return expression;
            }
            expression = ExpressionTreeRewriter.rewriteWith(new PushDownNegationsExpressionRewriter(), expression);
            expression = ExpressionTreeRewriter.rewriteWith(new ExtractCommonPredicatesExpressionRewriter(), expression, NodeContext.ROOT_NODE);
            IdentityLinkedHashMap<Expression, Type> expressionTypes = getExpressionTypes(session, metadata, sqlParser, types, expression, emptyList() /* parameters already replaced */);
            ExpressionInterpreter interpreter = ExpressionInterpreter.expressionOptimizer(expression, metadata, session, expressionTypes);
            return LiteralInterpreter.toExpression(interpreter.optimize(NoOpSymbolResolver.INSTANCE), expressionTypes.get(expression));
        }
    }

    private static class PushDownNegationsExpressionRewriter
            extends ExpressionRewriter<Void>
    {
        @Override
        public Expression rewriteNotExpression(NotExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            if (node.getValue() instanceof LogicalBinaryExpression) {
                LogicalBinaryExpression child = (LogicalBinaryExpression) node.getValue();
                List<Expression> predicates = extractPredicates(child);
                List<Expression> negatedPredicates = predicates.stream()
                        .map(predicate -> treeRewriter.rewrite((Expression) new NotExpression(predicate), context))
                        .collect(toImmutableList());
                return combinePredicates(child.getType().flip(), negatedPredicates);
            }
            else if (node.getValue() instanceof ComparisonExpression && ((ComparisonExpression) node.getValue()).getType() != IS_DISTINCT_FROM) {
                ComparisonExpression child = (ComparisonExpression) node.getValue();
                return new ComparisonExpression(
                        child.getType().negate(),
                        treeRewriter.rewrite(child.getLeft(), context),
                        treeRewriter.rewrite(child.getRight(), context));
            }
            else if (node.getValue() instanceof NotExpression) {
                NotExpression child = (NotExpression) node.getValue();
                return treeRewriter.rewrite(child.getValue(), context);
            }

            return new NotExpression(treeRewriter.rewrite(node.getValue(), context));
        }
    }

    private enum NodeContext
    {
        ROOT_NODE,
        NOT_ROOT_NODE;

        boolean isRootNode()
        {
            return this == ROOT_NODE;
        }
    }

    private static class ExtractCommonPredicatesExpressionRewriter
            extends ExpressionRewriter<NodeContext>
    {
        @Override
        public Expression rewriteExpression(Expression node, NodeContext context, ExpressionTreeRewriter<NodeContext> treeRewriter)
        {
            if (context.isRootNode()) {
                return treeRewriter.rewrite(node, NodeContext.NOT_ROOT_NODE);
            }

            return null;
        }

        @Override
        public Expression rewriteLogicalBinaryExpression(LogicalBinaryExpression node, NodeContext context, ExpressionTreeRewriter<NodeContext> treeRewriter)
        {
            Expression expression = combinePredicates(
                    node.getType(),
                    extractPredicates(node.getType(), node).stream()
                            .map(subExpression -> treeRewriter.rewrite(subExpression, NodeContext.NOT_ROOT_NODE))
                            .collect(toImmutableList()));

            if (!(expression instanceof LogicalBinaryExpression)) {
                return expression;
            }

            Expression simplified = extractCommonPredicates((LogicalBinaryExpression) expression);

            // Prefer AND LogicalBinaryExpression at the root if possible
            if (context.isRootNode() && simplified instanceof LogicalBinaryExpression && ((LogicalBinaryExpression) simplified).getType() == OR) {
                return distributeIfPossible((LogicalBinaryExpression) simplified);
            }

            return simplified;
        }

        private static Expression extractCommonPredicates(LogicalBinaryExpression node)
        {
            List<List<Expression>> subPredicates = getSubPredicates(node);

            Set<Expression> commonPredicates = ImmutableSet.copyOf(subPredicates.stream()
                    .map(ExtractCommonPredicatesExpressionRewriter::filterDeterministicPredicates)
                    .reduce(Sets::intersection)
                    .orElse(emptySet()));

            List<List<Expression>> uncorrelatedSubPredicates = subPredicates.stream()
                    .map(predicateList -> removeAll(predicateList, commonPredicates))
                    .collect(toImmutableList());

            LogicalBinaryExpression.Type flippedNodeType = node.getType().flip();

            List<Expression> uncorrelatedPredicates = uncorrelatedSubPredicates.stream()
                    .map(predicate -> combinePredicates(flippedNodeType, predicate))
                    .collect(toImmutableList());
            Expression combinedUncorrelatedPredicates = combinePredicates(node.getType(), uncorrelatedPredicates);

            return combinePredicates(flippedNodeType, ImmutableList.<Expression>builder()
                    .addAll(commonPredicates)
                    .add(combinedUncorrelatedPredicates)
                    .build());
        }

        private static List<List<Expression>> getSubPredicates(LogicalBinaryExpression expression)
        {
            return extractPredicates(expression.getType(), expression).stream()
                    .map(predicate -> predicate instanceof LogicalBinaryExpression ?
                            extractPredicates((LogicalBinaryExpression) predicate) : ImmutableList.of(predicate))
                    .collect(toImmutableList());
        }

        /**
         * Applies the boolean distributive property.
         *
         * For example:
         * ( A & B ) | ( C & D ) => ( A | C ) & ( A | D ) & ( B | C ) & ( B | D )
         *
         * Returns the original expression if the expression is non-deterministic or if the distribution will
         * expand the expression by too much.
         */
        private static Expression distributeIfPossible(LogicalBinaryExpression expression)
        {
            if (!DeterminismEvaluator.isDeterministic(expression)) {
                // Do not distribute boolean expressions if there are any non-deterministic elements
                // TODO: This can be optimized further if non-deterministic elements are not repeated
                return expression;
            }
            List<Set<Expression>> subPredicates = getSubPredicates(expression).stream()
                    .map(ImmutableSet::copyOf)
                    .collect(toList());

            int originalBaseExpressions = subPredicates.stream()
                    .mapToInt(Set::size)
                    .sum();

            int newBaseExpressions;
            try {
                newBaseExpressions = Math.multiplyExact(subPredicates.stream()
                        .mapToInt(Set::size)
                        .reduce(Math::multiplyExact)
                        .getAsInt(), subPredicates.size());
            }
            catch (ArithmeticException e) {
                // Integer overflow from multiplication means there are too many expressions
                return expression;
            }

            if (newBaseExpressions > originalBaseExpressions * 2) {
                // Do not distribute boolean expressions if it would create 2x more base expressions
                // (e.g. A, B, C, D from the above example). This is just an arbitrary heuristic to
                // avoid cross product expression explosion.
                return expression;
            }

            Set<List<Expression>> crossProduct = Sets.cartesianProduct(subPredicates);

            return combinePredicates(
                    expression.getType().flip(),
                    crossProduct.stream()
                            .map(expressions -> combinePredicates(expression.getType(), expressions))
                            .collect(toImmutableList()));
        }

        private static Set<Expression> filterDeterministicPredicates(List<Expression> predicates)
        {
            return predicates.stream()
                    .filter(DeterminismEvaluator::isDeterministic)
                    .collect(toSet());
        }

        private static <T> List<T> removeAll(Collection<T> collection, Collection<T> elementsToRemove)
        {
            return collection.stream()
                    .filter(element -> !elementsToRemove.contains(element))
                    .collect(toImmutableList());
        }
    }
}
