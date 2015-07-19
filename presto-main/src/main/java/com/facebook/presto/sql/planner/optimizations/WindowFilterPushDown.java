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
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.DependencyExtractor;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanRewriter;
import com.facebook.presto.sql.planner.plan.RowNumberNode;
import com.facebook.presto.sql.planner.plan.TopNRowNumberNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.DefaultExpressionTraversalVisitor;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Literal;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.getOnlyElement;

public class WindowFilterPushDown
        extends PlanOptimizer
{
    private static final Signature ROW_NUMBER_SIGNATURE = new Signature("row_number", StandardTypes.BIGINT, ImmutableList.<String>of());

    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        checkNotNull(plan, "plan is null");
        checkNotNull(session, "session is null");
        checkNotNull(types, "types is null");
        checkNotNull(symbolAllocator, "symbolAllocator is null");
        checkNotNull(idAllocator, "idAllocator is null");

        return PlanRewriter.rewriteWith(new Rewriter(idAllocator), plan, null);
    }

    private static class Rewriter
            extends PlanRewriter<Constraint>
    {
        private final PlanNodeIdAllocator idAllocator;

        private Rewriter(PlanNodeIdAllocator idAllocator)
        {
            this.idAllocator = checkNotNull(idAllocator, "idAllocator is null");
        }

        @Override
        public PlanNode visitWindow(WindowNode node, RewriteContext<Constraint> context)
        {
            if (canOptimizeWindowFunction(node)) {
                PlanNode rewrittenSource = context.rewrite(node.getSource(), null);
                Optional<Integer> limit = getLimit(node, context.get());
                if (node.getOrderBy().isEmpty()) {
                    return new RowNumberNode(idAllocator.getNextId(),
                            rewrittenSource,
                            node.getPartitionBy(),
                            getOnlyElement(node.getWindowFunctions().keySet()),
                            limit,
                            Optional.empty());
                }
                if (limit.isPresent()) {
                    return new TopNRowNumberNode(idAllocator.getNextId(),
                            rewrittenSource,
                            node.getPartitionBy(),
                            node.getOrderBy(),
                            node.getOrderings(),
                            getOnlyElement(node.getWindowFunctions().keySet()),
                            limit.get(),
                            false,
                            Optional.empty());
                }
            }
            return context.defaultRewrite(node);
        }

        private static Optional<Integer> getLimit(WindowNode node, Constraint filter)
        {
            if (filter == null || (!filter.getLimit().isPresent() && !filter.getFilterExpression().isPresent())) {
                return Optional.empty();
            }
            if (filter.getLimit().isPresent()) {
                return filter.getLimit();
            }
            if (filterContainsWindowFunctions(node, filter.getFilterExpression().get()) &&
                    filter.getFilterExpression().get() instanceof ComparisonExpression) {
                Symbol rowNumberSymbol = Iterables.getOnlyElement(node.getWindowFunctions().entrySet()).getKey();
                return WindowLimitExtractor.extract(filter.getFilterExpression().get(), rowNumberSymbol);
            }
            return Optional.empty();
        }

        private static boolean canOptimizeWindowFunction(WindowNode node)
        {
            if (node.getWindowFunctions().size() != 1) {
                return false;
            }
            Symbol rowNumberSymbol = getOnlyElement(node.getWindowFunctions().entrySet()).getKey();
            return isRowNumberSignature(node.getSignatures().get(rowNumberSymbol));
        }

        private static boolean filterContainsWindowFunctions(WindowNode node, Expression filterPredicate)
        {
            Set<Symbol> windowFunctionSymbols = node.getWindowFunctions().keySet();
            Sets.SetView<Symbol> commonSymbols = Sets.intersection(DependencyExtractor.extractUnique(filterPredicate), windowFunctionSymbols);
            return !commonSymbols.isEmpty();
        }

        @Override
        public PlanNode visitLimit(LimitNode node, RewriteContext<Constraint> context)
        {
            // Operators can handle MAX_VALUE rows per page, so do not optimize if count is greater than this value
            if (node.getCount() >= Integer.MAX_VALUE) {
                return context.defaultRewrite(node);
            }
            Constraint constraint = new Constraint(Optional.of((int) node.getCount()), Optional.empty());
            PlanNode rewrittenSource = context.rewrite(node.getSource(), constraint);

            if (rewrittenSource != node.getSource()) {
                if (rewrittenSource instanceof TopNRowNumberNode) {
                    return rewrittenSource;
                }
                return new LimitNode(idAllocator.getNextId(), rewrittenSource, node.getCount());
            }

            return context.defaultRewrite(node);
        }

        @Override
        public PlanNode visitFilter(FilterNode node, RewriteContext<Constraint> context)
        {
            PlanNode rewrittenSource = context.rewrite(node.getSource(), new Constraint(Optional.empty(), Optional.of(node.getPredicate())));
            if (rewrittenSource != node.getSource()) {
                if (rewrittenSource instanceof TopNRowNumberNode) {
                    return rewrittenSource;
                }
                return new FilterNode(idAllocator.getNextId(), rewrittenSource, node.getPredicate());
            }
            return context.defaultRewrite(node);
        }
    }

    private static boolean isRowNumberSignature(Signature signature)
    {
        return signature.equals(ROW_NUMBER_SIGNATURE);
    }

    private static class Constraint
    {
        private final Optional<Integer> limit;
        private final Optional<Expression> filterExpression;

        private Constraint(Optional<Integer> limit, Optional<Expression> filterExpression)
        {
            this.limit = limit;
            this.filterExpression = filterExpression;
        }

        public Optional<Integer> getLimit()
        {
            return limit;
        }

        public Optional<Expression> getFilterExpression()
        {
            return filterExpression;
        }
    }

    private static final class WindowLimitExtractor
    {
        private WindowLimitExtractor() {}

        public static Optional<Integer> extract(Expression expression, Symbol rowNumberSymbol)
        {
            Visitor visitor = new Visitor();
            Long limit = visitor.process(expression, rowNumberSymbol);
            if (limit == null || limit >= Integer.MAX_VALUE) {
                return Optional.empty();
            }

            return Optional.of(limit.intValue());
        }

        private static class Visitor
                extends DefaultExpressionTraversalVisitor<Long, Symbol>
        {
            @Override
            protected Long visitComparisonExpression(ComparisonExpression node, Symbol rowNumberSymbol)
            {
                Optional<QualifiedNameReference> reference = extractReference(node);
                Optional<Literal> literal = extractLiteral(node);
                if (!reference.isPresent() || !literal.isPresent()) {
                    return null;
                }
                if (!Symbol.fromQualifiedName(reference.get().getName()).equals(rowNumberSymbol)) {
                    return null;
                }

                long literalValue = extractValue(literal.get());
                if (node.getLeft() instanceof QualifiedNameReference && node.getRight() instanceof Literal) {
                    if (node.getType() == ComparisonExpression.Type.LESS_THAN_OR_EQUAL) {
                        return literalValue;
                    }
                    if (node.getType() == ComparisonExpression.Type.LESS_THAN) {
                        return literalValue - 1;
                    }
                }
                else if (node.getLeft() instanceof Literal && node.getRight() instanceof QualifiedNameReference) {
                    if (node.getType() == ComparisonExpression.Type.GREATER_THAN_OR_EQUAL) {
                        return literalValue;
                    }
                    if (node.getType() == ComparisonExpression.Type.GREATER_THAN) {
                        return literalValue - 1;
                    }
                }
                return null;
            }
        }

        private static Optional<QualifiedNameReference> extractReference(ComparisonExpression expression)
        {
            if (expression.getLeft() instanceof QualifiedNameReference) {
                return Optional.of((QualifiedNameReference) expression.getLeft());
            }
            if (expression.getRight() instanceof QualifiedNameReference) {
                return Optional.of((QualifiedNameReference) expression.getRight());
            }
            return Optional.empty();
        }

        private static Optional<Literal> extractLiteral(ComparisonExpression expression)
        {
            if (expression.getLeft() instanceof Literal) {
                return Optional.of((Literal) expression.getLeft());
            }
            if (expression.getRight() instanceof Literal) {
                return Optional.of((Literal) expression.getRight());
            }
            return Optional.empty();
        }

        private static long extractValue(Literal literal)
        {
            if (literal instanceof DoubleLiteral) {
                return (long) ((DoubleLiteral) literal).getValue();
            }
            if (literal instanceof LongLiteral) {
                return ((LongLiteral) literal).getValue();
            }
            throw new IllegalArgumentException("Row number compared to non numeric literal");
        }
    }
}
