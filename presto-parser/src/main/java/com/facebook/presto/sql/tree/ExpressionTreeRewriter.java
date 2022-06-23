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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;

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

    private List<Expression> rewrite(List<Expression> items, Context<C> context)
    {
        ImmutableList.Builder<Expression> builder = ImmutableList.builder();
        for (Expression expression : items) {
            builder.add(rewrite(expression, context.get()));
        }
        return builder.build();
    }

    @SuppressWarnings("unchecked")
    public <T extends Expression> T rewrite(T node, C context)
    {
        return (T) visitor.process(node, new Context<>(context, false));
    }

    /**
     * Invoke the default rewrite logic explicitly. Specifically, it skips the invocation of the expression rewriter for the provided node.
     */
    @SuppressWarnings("unchecked")
    public <T extends Expression> T defaultRewrite(T node, C context)
    {
        return (T) visitor.process(node, new Context<>(context, true));
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
        protected Expression visitRow(Row node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteRow(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            List<Expression> items = rewrite(node.getItems(), context);

            if (!sameElements(node.getItems(), items)) {
                return new Row(items);
            }

            return node;
        }

        @Override
        protected Expression visitArithmeticUnary(ArithmeticUnaryExpression node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteArithmeticUnary(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Expression child = rewrite(node.getValue(), context.get());
            if (child != node.getValue()) {
                return new ArithmeticUnaryExpression(node.getSign(), child);
            }

            return node;
        }

        @Override
        public Expression visitArithmeticBinary(ArithmeticBinaryExpression node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteArithmeticBinary(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Expression left = rewrite(node.getLeft(), context.get());
            Expression right = rewrite(node.getRight(), context.get());

            if (left != node.getLeft() || right != node.getRight()) {
                return new ArithmeticBinaryExpression(node.getOperator(), left, right);
            }

            return node;
        }

        @Override
        protected Expression visitArrayConstructor(ArrayConstructor node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteArrayConstructor(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            List<Expression> values = rewrite(node.getValues(), context);

            if (!sameElements(node.getValues(), values)) {
                return new ArrayConstructor(values);
            }

            return node;
        }

        @Override
        protected Expression visitAtTimeZone(AtTimeZone node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteAtTimeZone(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Expression value = rewrite(node.getValue(), context.get());
            Expression timeZone = rewrite(node.getTimeZone(), context.get());

            if (value != node.getValue() || timeZone != node.getTimeZone()) {
                return new AtTimeZone(value, timeZone);
            }

            return node;
        }

        @Override
        protected Expression visitSubscriptExpression(SubscriptExpression node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteSubscriptExpression(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Expression base = rewrite(node.getBase(), context.get());
            Expression index = rewrite(node.getIndex(), context.get());

            if (base != node.getBase() || index != node.getIndex()) {
                return new SubscriptExpression(base, index);
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
                return new ComparisonExpression(node.getOperator(), left, right);
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

            // It's common to have a large number of chained AND or OR. So we do it iteratively here to avoid stack overflow
            LogicalBinaryExpression.Operator operator = node.getOperator();
            Deque<LogicalBinaryExpression> nodesToVisit = new LinkedList<>();
            ImmutableList.Builder<Expression> childrenBuilder = ImmutableList.builder();
            LogicalBinaryExpression currentNode = node;

            // First collect all the left children that are the same operator for a left-leaning tree
            nodesToVisit.push(node);
            while (currentNode.getLeft() instanceof LogicalBinaryExpression && ((LogicalBinaryExpression) currentNode.getLeft()).getOperator() == operator) {
                nodesToVisit.push((LogicalBinaryExpression) currentNode.getLeft());
                currentNode = nodesToVisit.peek();
            }

            // Now rewrite the last child
            currentNode = nodesToVisit.pop();
            Expression rewrittenLeft = rewrite(currentNode.getLeft(), context.get());
            while (!nodesToVisit.isEmpty()) {
                Expression newRight = rewrite(currentNode.getRight(), context.get());
                if (newRight != currentNode.getRight() || rewrittenLeft != currentNode.getLeft()) {
                    rewrittenLeft = new LogicalBinaryExpression(operator, rewrittenLeft, newRight);
                }
                else {
                    rewrittenLeft = currentNode;
                }
                currentNode = nodesToVisit.pop();
            }

            // Now collect all the right children that are the same operator for a right-leaning tree
            nodesToVisit.push(node);
            currentNode = node;
            while (currentNode.getRight() instanceof LogicalBinaryExpression && ((LogicalBinaryExpression) currentNode.getRight()).getOperator() == operator) {
                nodesToVisit.push((LogicalBinaryExpression) currentNode.getRight());
                currentNode = nodesToVisit.peek();
            }

            // Now rewrite the last child
            currentNode = nodesToVisit.pop();
            Expression rewrittenRight = rewrite(currentNode.getRight(), context.get());
            while (!nodesToVisit.isEmpty()) {
                Expression newLeft = rewrite(currentNode.getLeft(), context.get());
                if (rewrittenRight != currentNode.getRight() || newLeft != currentNode.getLeft()) {
                    rewrittenRight = new LogicalBinaryExpression(operator, newLeft, rewrittenRight);
                }
                else {
                    rewrittenRight = currentNode;
                }
                currentNode = nodesToVisit.pop();
            }

            if (node.getLeft() != rewrittenLeft || node.getRight() != rewrittenRight) {
                return new LogicalBinaryExpression(operator, rewrittenLeft, rewrittenRight);
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

            if ((condition != node.getCondition()) || (trueValue != node.getTrueValue()) || (falseValue != node.getFalseValue().orElse(null))) {
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

            Optional<Expression> defaultValue = node.getDefaultValue()
                    .map(value -> rewrite(value, context.get()));

            if (!sameElements(node.getDefaultValue(), defaultValue) || !sameElements(node.getWhenClauses(), builder.build())) {
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

            Optional<Expression> defaultValue = node.getDefaultValue()
                    .map(value -> rewrite(value, context.get()));

            if (operand != node.getOperand() ||
                    !sameElements(node.getDefaultValue(), defaultValue) ||
                    !sameElements(node.getWhenClauses(), builder.build())) {
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

            List<Expression> operands = rewrite(node.getOperands(), context);

            if (!sameElements(node.getOperands(), operands)) {
                return new CoalesceExpression(operands);
            }

            return node;
        }

        @Override
        public Expression visitTryExpression(TryExpression node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteTryExpression(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Expression expression = rewrite(node.getInnerExpression(), context.get());

            if (node.getInnerExpression() != expression) {
                return new TryExpression(expression);
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

            Optional<Expression> filter = node.getFilter();
            if (filter.isPresent()) {
                Expression filterExpression = filter.get();
                Expression newFilterExpression = rewrite(filterExpression, context.get());
                filter = Optional.of(newFilterExpression);
            }

            Optional<Window> rewrittenWindow = node.getWindow();
            if (node.getWindow().isPresent()) {
                Window window = node.getWindow().get();

                List<Expression> partitionBy = rewrite(window.getPartitionBy(), context);

                Optional<OrderBy> orderBy = Optional.empty();
                if (window.getOrderBy().isPresent()) {
                    orderBy = Optional.of(rewriteOrderBy(window.getOrderBy().get(), context));
                }

                Optional<WindowFrame> rewrittenFrame = window.getFrame();
                if (rewrittenFrame.isPresent()) {
                    WindowFrame frame = rewrittenFrame.get();

                    FrameBound start = frame.getStart();
                    if (start.getValue().isPresent()) {
                        Expression value = rewrite(start.getValue().get(), context.get());
                        if (value != start.getValue().get()) {
                            start = new FrameBound(start.getType(), value);
                        }
                    }

                    Optional<FrameBound> rewrittenEnd = frame.getEnd();
                    if (rewrittenEnd.isPresent()) {
                        Optional<Expression> value = rewrittenEnd.get().getValue();
                        if (value.isPresent()) {
                            Expression rewrittenValue = rewrite(value.get(), context.get());
                            if (rewrittenValue != value.get()) {
                                rewrittenEnd = Optional.of(new FrameBound(rewrittenEnd.get().getType(), rewrittenValue));
                            }
                        }
                    }

                    if ((frame.getStart() != start) || !sameElements(frame.getEnd(), rewrittenEnd)) {
                        rewrittenFrame = Optional.of(new WindowFrame(frame.getType(), start, rewrittenEnd));
                    }
                }

                if (!sameElements(window.getPartitionBy(), partitionBy) ||
                        !sameElements(window.getOrderBy(), orderBy) ||
                        !sameElements(window.getFrame(), rewrittenFrame)) {
                    rewrittenWindow = Optional.of(new Window(partitionBy, orderBy, rewrittenFrame));
                }
            }

            List<Expression> arguments = rewrite(node.getArguments(), context);

            if (!sameElements(node.getArguments(), arguments) || !sameElements(rewrittenWindow, node.getWindow())
                    || !sameElements(filter, node.getFilter())) {
                return new FunctionCall(node.getName(), rewrittenWindow, filter, node.getOrderBy().map(orderBy -> rewriteOrderBy(orderBy, context)), node.isDistinct(), node.isIgnoreNulls(), arguments);
            }
            return node;
        }

        // Since OrderBy contains list of SortItems, we want to process each SortItem's key, which is an expression
        private OrderBy rewriteOrderBy(OrderBy orderBy, Context<C> context)
        {
            List<SortItem> rewrittenSortItems = rewriteSortItems(orderBy.getSortItems(), context);
            if (sameElements(orderBy.getSortItems(), rewrittenSortItems)) {
                return orderBy;
            }

            return new OrderBy(rewrittenSortItems);
        }

        private List<SortItem> rewriteSortItems(List<SortItem> sortItems, Context<C> context)
        {
            ImmutableList.Builder<SortItem> rewrittenSortItems = ImmutableList.builder();
            for (SortItem sortItem : sortItems) {
                Expression sortKey = rewrite(sortItem.getSortKey(), context.get());
                if (sortItem.getSortKey() != sortKey) {
                    rewrittenSortItems.add(new SortItem(sortKey, sortItem.getOrdering(), sortItem.getNullOrdering()));
                }
                else {
                    rewrittenSortItems.add(sortItem);
                }
            }
            return rewrittenSortItems.build();
        }

        @Override
        protected Expression visitLambdaExpression(LambdaExpression node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteLambdaExpression(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Expression body = rewrite(node.getBody(), context.get());
            if (body != node.getBody()) {
                return new LambdaExpression(node.getArguments(), body);
            }

            return node;
        }

        @Override
        protected Expression visitBindExpression(BindExpression node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteBindExpression(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            List<Expression> values = node.getValues().stream()
                    .map(value -> rewrite(value, context.get()))
                    .collect(toImmutableList());
            Expression function = rewrite(node.getFunction(), context.get());

            if (!sameElements(values, node.getValues()) || (function != node.getFunction())) {
                return new BindExpression(values, function);
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
            Optional<Expression> rewrittenEscape = node.getEscape()
                    .map(escape -> rewrite(escape, context.get()));

            if (value != node.getValue() || pattern != node.getPattern() || !sameElements(node.getEscape(), rewrittenEscape)) {
                return new LikePredicate(value, pattern, rewrittenEscape);
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

            List<Expression> values = rewrite(node.getValues(), context);

            if (!sameElements(node.getValues(), values)) {
                return new InListExpression(values);
            }

            return node;
        }

        @Override
        protected Expression visitExists(ExistsPredicate node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteExists(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Expression subquery = node.getSubquery();
            subquery = rewrite(subquery, context.get());

            if (subquery != node.getSubquery()) {
                return new ExistsPredicate(subquery);
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
        public Expression visitParameter(Parameter node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteParameter(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            return node;
        }

        @Override
        public Expression visitIdentifier(Identifier node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteIdentifier(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            return node;
        }

        @Override
        public Expression visitDereferenceExpression(DereferenceExpression node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteDereferenceExpression(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Expression base = rewrite(node.getBase(), context.get());
            if (base != node.getBase()) {
                return new DereferenceExpression(base, node.getField());
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
                return new Cast(expression, node.getType(), node.isSafe(), node.isTypeOnly());
            }

            return node;
        }

        @Override
        protected Expression visitFieldReference(FieldReference node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteFieldReference(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            return node;
        }

        @Override
        protected Expression visitSymbolReference(SymbolReference node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteSymbolReference(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            return node;
        }

        @Override
        protected Expression visitQuantifiedComparisonExpression(QuantifiedComparisonExpression node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteQuantifiedComparison(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Expression value = rewrite(node.getValue(), context.get());
            Expression subquery = rewrite(node.getSubquery(), context.get());

            if (node.getValue() != value || node.getSubquery() != subquery) {
                return new QuantifiedComparisonExpression(node.getOperator(), node.getQuantifier(), value, subquery);
            }

            return node;
        }

        @Override
        public Expression visitGroupingOperation(GroupingOperation node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteGroupingOperation(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            return node;
        }

        @Override
        protected Expression visitCurrentUser(CurrentUser node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteCurrentUser(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
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

    private static <T> boolean sameElements(Optional<T> a, Optional<T> b)
    {
        if (!a.isPresent() && !b.isPresent()) {
            return true;
        }
        else if (a.isPresent() != b.isPresent()) {
            return false;
        }

        return a.get() == b.get();
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
