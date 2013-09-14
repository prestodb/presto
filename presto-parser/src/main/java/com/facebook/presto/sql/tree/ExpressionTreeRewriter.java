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
package com.facebook.presto.sql.tree;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.Iterator;

public final class ExpressionTreeRewriter<C>
{
    private final ExpressionRewriter<C> rewriter;
    private final AstVisitor<Expression, ExpressionTreeRewriter.Context<C>> visitor;

    public static <C, T extends Expression> T rewriteWith(ExpressionRewriter<C> rewriter, T node)
    {
        return new ExpressionTreeRewriter<>(rewriter).rewrite(node, null);
    }

    public static <C, T extends Expression> T rewriteWith(ExpressionRewriter<C> rewriter, T node, C context)
    {
        return new ExpressionTreeRewriter<>(rewriter).rewrite(node, context);
    }

    public ExpressionTreeRewriter(ExpressionRewriter<C> rewriter)
    {
        this.rewriter = rewriter;
        this.visitor = new RewritingVisitor();
    }

    public <T extends Expression> T rewrite(T node, C context)
    {
        return (T) visitor.process(node, new Context<>(context, false));
    }

    /**
     * Invoke the default rewrite logic explicitly. Specifically, it skips the invocation of the expression rewriter for the provided node.
     */
    public <T extends Expression> T defaultRewrite(T node, C context)
    {
        return (T) visitor.process(node, new Context<>(context, true));
    }

    public static <C, T extends Expression> Function<Expression, T> rewriteFunction(final ExpressionRewriter<C> rewriter)
    {
        return new Function<Expression, T>()
        {
            @Override
            public T apply(Expression node)
            {
                return (T) rewriteWith(rewriter, node);
            }
        };
    }

    private class RewritingVisitor
            extends AstVisitor<Expression, ExpressionTreeRewriter.Context<C>>
    {
        @Override
        protected Expression visitExpression(Expression node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteExpression(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            throw new UnsupportedOperationException("not yet implemented: " + getClass().getSimpleName() + " for " + node.getClass().getName());
        }

        @Override
        protected Expression visitNegativeExpression(NegativeExpression node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteNegativeExpression(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Expression child = rewrite(node.getValue(), context.get());
            if (child != node.getValue()) {
                return new NegativeExpression(child);
            }

            return node;
        }

        @Override
        public Expression visitArithmeticExpression(ArithmeticExpression node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteArithmeticExpression(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Expression left = rewrite(node.getLeft(), context.get());
            Expression right = rewrite(node.getRight(), context.get());

            if (left != node.getLeft() || right != node.getRight()) {
                return new ArithmeticExpression(node.getType(), left, right);
            }

            return node;
        }

        @Override
        public Expression visitComparisonExpression(ComparisonExpression node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteComparisonExpression(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Expression left = rewrite(node.getLeft(), context.get());
            Expression right = rewrite(node.getRight(), context.get());

            if (left != node.getLeft() || right != node.getRight()) {
                return new ComparisonExpression(node.getType(), left, right);
            }

            return node;
        }

        @Override
        protected Expression visitBetweenPredicate(BetweenPredicate node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteBetweenPredicate(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Expression value = rewrite(node.getValue(), context.get());
            Expression min = rewrite(node.getMin(), context.get());
            Expression max = rewrite(node.getMax(), context.get());

            if (value != node.getValue() || min != node.getMin() || max != node.getMax()) {
                return new BetweenPredicate(value, min, max);
            }

            return node;
        }

        @Override
        public Expression visitLogicalBinaryExpression(LogicalBinaryExpression node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteLogicalBinaryExpression(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Expression left = rewrite(node.getLeft(), context.get());
            Expression right = rewrite(node.getRight(), context.get());

            if (left != node.getLeft() || right != node.getRight()) {
                return new LogicalBinaryExpression(node.getType(), left, right);
            }

            return node;
        }

        @Override
        public Expression visitNotExpression(NotExpression node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteNotExpression(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Expression value = rewrite(node.getValue(), context.get());

            if (value != node.getValue()) {
                return new NotExpression(value);
            }

            return node;
        }

        @Override
        protected Expression visitIsNullPredicate(IsNullPredicate node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteIsNullPredicate(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Expression value = rewrite(node.getValue(), context.get());

            if (value != node.getValue()) {
                return new IsNullPredicate(value);
            }

            return node;
        }

        @Override
        protected Expression visitIsNotNullPredicate(IsNotNullPredicate node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteIsNotNullPredicate(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Expression value = rewrite(node.getValue(), context.get());

            if (value != node.getValue()) {
                return new IsNotNullPredicate(value);
            }

            return node;
        }

        @Override
        protected Expression visitNullIfExpression(NullIfExpression node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteNullIfExpression(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Expression first = rewrite(node.getFirst(), context.get());
            Expression second = rewrite(node.getSecond(), context.get());

            if (first != node.getFirst() || second != node.getSecond()) {
                return new NullIfExpression(first, second);
            }

            return node;
        }

        @Override
        protected Expression visitIfExpression(IfExpression node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteIfExpression(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Expression condition = rewrite(node.getCondition(), context.get());
            Expression trueValue = rewrite(node.getTrueValue(), context.get());
            Expression falseValue = null;
            if (node.getFalseValue().isPresent()) {
                falseValue = rewrite(node.getFalseValue().get(), context.get());
            }

            if ((condition != node.getCondition()) || (trueValue != node.getTrueValue()) || (falseValue != node.getFalseValue().orNull())) {
                return new IfExpression(condition, trueValue, falseValue);
            }

            return node;
        }

        @Override
        protected Expression visitSearchedCaseExpression(SearchedCaseExpression node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteSearchedCaseExpression(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            ImmutableList.Builder<WhenClause> builder = ImmutableList.builder();
            for (WhenClause expression : node.getWhenClauses()) {
                builder.add(rewrite(expression, context.get()));
            }

            Expression defaultValue = null;
            if (node.getDefaultValue() != null) {
                defaultValue = rewrite(node.getDefaultValue(), context.get());
            }

            if (defaultValue != node.getDefaultValue() || !sameElements(node.getWhenClauses(), builder.build())) {
                return new SearchedCaseExpression(builder.build(), defaultValue);
            }

            return node;
        }

        @Override
        protected Expression visitSimpleCaseExpression(SimpleCaseExpression node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteSimpleCaseExpression(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Expression operand = rewrite(node.getOperand(), context.get());

            ImmutableList.Builder<WhenClause> builder = ImmutableList.builder();
            for (WhenClause expression : node.getWhenClauses()) {
                builder.add(rewrite(expression, context.get()));
            }

            Expression defaultValue = null;
            if (node.getDefaultValue() != null) {
                defaultValue = rewrite(node.getDefaultValue(), context.get());
            }

            if (operand != node.getOperand() || defaultValue != node.getDefaultValue() || !sameElements(node.getWhenClauses(), builder.build())) {
                return new SimpleCaseExpression(operand, builder.build(), defaultValue);
            }

            return node;
        }

        @Override
        protected Expression visitWhenClause(WhenClause node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteWhenClause(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Expression operand = rewrite(node.getOperand(), context.get());
            Expression result = rewrite(node.getResult(), context.get());

            if (operand != node.getOperand() || result != node.getResult()) {
                return new WhenClause(operand, result);
            }
            return node;
        }

        @Override
        protected Expression visitCoalesceExpression(CoalesceExpression node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteCoalesceExpression(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            ImmutableList.Builder<Expression> builder = ImmutableList.builder();
            for (Expression expression : node.getOperands()) {
                builder.add(rewrite(expression, context.get()));
            }

            if (!sameElements(node.getOperands(), builder.build())) {
                return new CoalesceExpression(builder.build());
            }

            return node;
        }

        @Override
        public Expression visitFunctionCall(FunctionCall node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteFunctionCall(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Window rewrittenWindow = node.getWindow().orNull();
            if (rewrittenWindow != null) {
                ImmutableList.Builder<Expression> partitionBy = ImmutableList.builder();
                for (Expression expression : rewrittenWindow.getPartitionBy()) {
                    partitionBy.add(rewrite(expression, context.get()));
                }

                // Since SortItem is not an Expression, but contains Expressions, just process the default rewrite inline with FunctionCall
                ImmutableList.Builder<SortItem> orderBy = ImmutableList.builder();
                for (SortItem sortItem : rewrittenWindow.getOrderBy()) {
                    Expression sortKey = rewrite(sortItem.getSortKey(), context.get());
                    if (sortItem.getSortKey() != sortKey) {
                        orderBy.add(new SortItem(sortKey, sortItem.getOrdering(), sortItem.getNullOrdering()));
                    }
                    else {
                        orderBy.add(sortItem);
                    }
                }

                // TODO: rewrite frame
                if (!sameElements(rewrittenWindow.getPartitionBy(), partitionBy.build()) ||
                        !sameElements(rewrittenWindow.getOrderBy(), orderBy.build())) {
                    rewrittenWindow = new Window(partitionBy.build(), orderBy.build(), rewrittenWindow.getFrame().orNull());
                }
            }

            ImmutableList.Builder<Expression> arguments = ImmutableList.builder();
            for (Expression expression : node.getArguments()) {
                arguments.add(rewrite(expression, context.get()));
            }

            if (!sameElements(node.getArguments(), arguments.build()) ||
                    (rewrittenWindow != node.getWindow().orNull())) {
                return new FunctionCall(node.getName(), rewrittenWindow, node.isDistinct(), arguments.build());
            }

            return node;
        }

        @Override
        public Expression visitLikePredicate(LikePredicate node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteLikePredicate(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Expression value = rewrite(node.getValue(), context.get());
            Expression pattern = rewrite(node.getPattern(), context.get());
            Expression escape = null;
            if (node.getEscape() != null) {
                escape = rewrite(node.getEscape(), context.get());
            }

            if (value != node.getValue() || pattern != node.getPattern() || escape != node.getEscape()) {
                return new LikePredicate(value, pattern, escape);
            }

            return node;
        }

        @Override
        public Expression visitInPredicate(InPredicate node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteInPredicate(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Expression value = rewrite(node.getValue(), context.get());
            Expression list = rewrite(node.getValueList(), context.get());

            if (node.getValue() != value || node.getValueList() != list) {
                return new InPredicate(value, list);
            }

            return node;
        }

        @Override
        protected Expression visitInListExpression(InListExpression node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteInListExpression(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            ImmutableList.Builder<Expression> builder = ImmutableList.builder();
            for (Expression expression : node.getValues()) {
                builder.add(rewrite(expression, context.get()));
            }

            if (!sameElements(node.getValues(), builder.build())) {
                return new InListExpression(builder.build());
            }

            return node;
        }

        @Override
        public Expression visitSubqueryExpression(SubqueryExpression node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteSubqueryExpression(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            // No default rewrite for SubqueryExpression since we do not want to traverse subqueries
            return node;
        }

        @Override
        public Expression visitLiteral(Literal node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteLiteral(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            return node;
        }

        @Override
        public Expression visitQualifiedNameReference(QualifiedNameReference node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteQualifiedNameReference(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            return node;
        }

        @Override
        protected Expression visitExtract(Extract node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteExtract(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Expression expression = rewrite(node.getExpression(), context.get());

            if (node.getExpression() != expression) {
                return new Extract(expression, node.getField());
            }

            return node;
        }

        @Override
        protected Expression visitCurrentTime(CurrentTime node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteCurrentTime(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            return node;
        }

        @Override
        public Expression visitCast(Cast node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteCast(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Expression expression = rewrite(node.getExpression(), context.get());

            if (node.getExpression() != expression) {
                return new Cast(expression, node.getType());
            }

            return node;
        }
    }

    public static class Context<C>
    {
        private final boolean defaultRewrite;
        private final C context;

        private Context(C context, boolean defaultRewrite)
        {
            this.context = context;
            this.defaultRewrite = defaultRewrite;
        }

        public C get()
        {
            return context;
        }

        public boolean isDefaultRewrite()
        {
            return defaultRewrite;
        }
    }

    @SuppressWarnings("ObjectEquality")
    private static <T> boolean sameElements(Iterable<? extends T> a, Iterable<? extends T> b)
    {
        if (Iterables.size(a) != Iterables.size(b)) {
            return false;
        }

        Iterator<? extends T> first = a.iterator();
        Iterator<? extends T> second = b.iterator();

        while (first.hasNext() && second.hasNext()) {
            if (first.next() != second.next()) {
                return false;
            }
        }

        return true;
    }
}
