package com.facebook.presto.sql.tree;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.Iterator;

public final class TreeRewriter<C>
{
    private final NodeRewriter<C> nodeRewriter;
    private final AstVisitor<Node, TreeRewriter.Context<C>> visitor;

    public static <C, T extends Node> T rewriteWith(NodeRewriter<C> rewriter, T node)
    {
        return new TreeRewriter<C>(rewriter).rewrite(node, null);
    }

    public TreeRewriter(NodeRewriter<C> nodeRewriter)
    {
        this.nodeRewriter = nodeRewriter;
        this.visitor = new RewritingVisitor();
    }

    public <T extends Node> T rewrite(T node, C context)
    {
        return (T) visitor.process(node, new Context<>(context, false));
    }

    /**
     * Invoke the default rewrite logic explicitly. Specifically, it skips the invocation of the node rewriter for the provided node.
     */
    public <T extends Node> T defaultRewrite(T node, C context)
    {
        return (T) visitor.process(node, new Context<>(context, true));
    }

    public static <C, T extends Node> Function<Node, T> rewriteFunction(final NodeRewriter<C> rewriter)
    {
        return new Function<Node, T>()
        {
            @Override
            public T apply(Node node)
            {
                return (T) rewriteWith(rewriter, node);
            }
        };
    }

    private class RewritingVisitor
            extends AstVisitor<Node, TreeRewriter.Context<C>>
    {
        @Override
        public Node visitNode(Node node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = nodeRewriter.rewriteNode(node, context.get(), TreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            throw new UnsupportedOperationException("not yet implemented: " + getClass().getSimpleName() + " for " + node.getClass().getName());
        }

        @Override
        public Node visitQuery(Query node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = nodeRewriter.rewriteQuery(node, context.get(), TreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            With with = null;
            if (node.getWith().isPresent()) {
                with = rewrite(node.getWith().get(), context.get());
            }

            Select select = rewrite(node.getSelect(), context.get());

            ImmutableList.Builder<Relation> from = ImmutableList.builder();
            for (Relation relation : node.getFrom()) {
                from.add(rewrite(relation, context.get()));
            }

            Expression where = null;
            if (node.getWhere().isPresent()) {
                where = rewrite(node.getWhere().get(), context.get());
            }

            ImmutableList.Builder<Expression> groupBy = ImmutableList.builder();
            for (Expression expression : node.getGroupBy()) {
                groupBy.add(rewrite(expression, context.get()));
            }

            Expression having = null;
            if (node.getHaving().isPresent()) {
                having = rewrite(node.getHaving().get(), context.get());
            }

            ImmutableList.Builder<SortItem> orderBy = ImmutableList.builder();
            for (SortItem sortItem : node.getOrderBy()) {
                orderBy.add(rewrite(sortItem, context.get()));
            }

            if ((with != node.getWith().orNull()) ||
                    (select != node.getSelect()) ||
                    !sameElements(node.getFrom(), from.build()) ||
                    where != node.getWhere().orNull() ||
                    !sameElements(node.getGroupBy(), groupBy.build()) ||
                    having != node.getHaving().orNull() ||
                    !sameElements(orderBy.build(), node.getOrderBy())) {

                return new Query(
                        Optional.fromNullable(with),
                        select,
                        from.build(),
                        Optional.fromNullable(where),
                        groupBy.build(),
                        Optional.fromNullable(having),
                        orderBy.build(),
                        node.getLimit());
            }

            return node;
        }

        @Override
        protected Node visitWith(With node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = nodeRewriter.rewriteWith(node, context.get(), TreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            ImmutableList.Builder<WithQuery> builder = ImmutableList.builder();
            for (WithQuery query : node.getQueries()) {
                builder.add(rewrite(query, context.get()));
            }

            if (!sameElements(node.getQueries(), builder.build())) {
                return new With(node.isRecursive(), builder.build());
            }

            return node;
        }

        @Override
        protected Node visitWithQuery(WithQuery node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = nodeRewriter.rewriteWithQuery(node, context.get(), TreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Query child = rewrite(node.getQuery(), context.get());
            if (child != node.getQuery()) {
                return new WithQuery(node.getName(), child, node.getColumnNames());
            }

            return node;
        }

        @Override
        public Node visitSelect(Select node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = nodeRewriter.rewriteSelect(node, context.get(), TreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            ImmutableList.Builder<Expression> builder = ImmutableList.builder();
            for (Expression expression : node.getSelectItems()) {
                builder.add(rewrite(expression, context.get()));
            }

            if (!sameElements(node.getSelectItems(), builder.build())) {
                return new Select(node.isDistinct(), builder.build());
            }

            return node;
        }

        @Override
        public Node visitAliasedRelation(AliasedRelation node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = nodeRewriter.rewriteAliasedRelation(node, context.get(), TreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Relation child = rewrite(node.getRelation(), context.get());
            if (child != node.getRelation()) {
                return new AliasedRelation(child, node.getAlias(), node.getColumnNames());
            }

            return node;
        }

        @Override
        public Node visitSubquery(Subquery node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = nodeRewriter.rewriteSubquery(node, context.get(), TreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Query child = rewrite(node.getQuery(), context.get());
            if (child != node.getQuery()) {
                return new Subquery(child);
            }

            return node;
        }

        @Override
        public Node visitAliasedExpression(AliasedExpression node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = nodeRewriter.rewriteAliasedExpression(node, context.get(), TreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Expression child = rewrite(node.getExpression(), context.get());
            if (child != node.getExpression()) {
                return new AliasedExpression(child, node.getAlias());
            }

            return node;
        }

        @Override
        protected Node visitNegativeExpression(NegativeExpression node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = nodeRewriter.rewriteNegativeExpression(node, context.get(), TreeRewriter.this);
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
        public Node visitArithmeticExpression(ArithmeticExpression node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = nodeRewriter.rewriteArithmeticExpression(node, context.get(), TreeRewriter.this);
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
        public Node visitComparisonExpression(ComparisonExpression node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = nodeRewriter.rewriteComparisonExpression(node, context.get(), TreeRewriter.this);
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
        protected Node visitBetweenPredicate(BetweenPredicate node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = nodeRewriter.rewriteBetweenPredicate(node, context.get(), TreeRewriter.this);
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
        public Node visitLogicalBinaryExpression(LogicalBinaryExpression node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = nodeRewriter.rewriteLogicalBinaryExpression(node, context.get(), TreeRewriter.this);
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
        public Node visitNotExpression(NotExpression node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = nodeRewriter.rewriteNotExpression(node, context.get(), TreeRewriter.this);
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
        protected Node visitIsNullPredicate(IsNullPredicate node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = nodeRewriter.rewriteIsNullPredicate(node, context.get(), TreeRewriter.this);
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
        protected Node visitIsNotNullPredicate(IsNotNullPredicate node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = nodeRewriter.rewriteIsNotNullPredicate(node, context.get(), TreeRewriter.this);
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
        protected Node visitNullIfExpression(NullIfExpression node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = nodeRewriter.rewriteNullIfExpression(node, context.get(), TreeRewriter.this);
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
        protected Node visitIfExpression(IfExpression node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = nodeRewriter.rewriteIfExpression(node, context.get(), TreeRewriter.this);
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
        protected Node visitSearchedCaseExpression(SearchedCaseExpression node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = nodeRewriter.rewriteSearchedCaseExpression(node, context.get(), TreeRewriter.this);
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
        protected Node visitSimpleCaseExpression(SimpleCaseExpression node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = nodeRewriter.rewriteSimpleCaseExpression(node, context.get(), TreeRewriter.this);
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
        protected Node visitWhenClause(WhenClause node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = nodeRewriter.rewriteWhenClause(node, context.get(), TreeRewriter.this);
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
        protected Node visitCoalesceExpression(CoalesceExpression node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = nodeRewriter.rewriteCoalesceExpression(node, context.get(), TreeRewriter.this);
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
        public Node visitFunctionCall(FunctionCall node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = nodeRewriter.rewriteFunctionCall(node, context.get(), TreeRewriter.this);
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

                ImmutableList.Builder<SortItem> orderBy = ImmutableList.builder();
                for (SortItem sortItem : rewrittenWindow.getOrderBy()) {
                    orderBy.add(rewrite(sortItem, context.get()));
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
        protected Node visitSortItem(SortItem node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = nodeRewriter.rewriteSortItem(node, context.get(), TreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Expression sortKey = rewrite(node.getSortKey(), context.get());

            if (node.getSortKey() != sortKey) {
                return new SortItem(sortKey, node.getOrdering(), node.getNullOrdering());
            }

            return node;
        }

        @Override
        public Node visitLikePredicate(LikePredicate node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = nodeRewriter.rewriteLikePredicate(node, context.get(), TreeRewriter.this);
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
        public Node visitInPredicate(InPredicate node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = nodeRewriter.rewriteInPredicate(node, context.get(), TreeRewriter.this);
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
        protected Node visitInListExpression(InListExpression node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = nodeRewriter.rewriteInListExpression(node, context.get(), TreeRewriter.this);
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
        public Node visitSubqueryExpression(SubqueryExpression node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = nodeRewriter.rewriteSubqueryExpression(node, context.get(), TreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Query child = rewrite(node.getQuery(), context.get());
            if (child != node.getQuery()) {
                return new SubqueryExpression(child);
            }

            return node;
        }

        @Override
        public Node visitAllColumns(AllColumns node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = nodeRewriter.rewriteAllColumns(node, context.get(), TreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            return node;
        }

        @Override
        public Node visitTable(Table node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = nodeRewriter.rewriteTable(node, context.get(), TreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            return node;
        }

        @Override
        public Node visitLiteral(Literal node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = nodeRewriter.rewriteLiteral(node, context.get(), TreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            return node;
        }

        @Override
        public Node visitQualifiedNameReference(QualifiedNameReference node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = nodeRewriter.rewriteQualifiedNameReference(node, context.get(), TreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            return node;
        }

        @Override
        protected Node visitExtract(Extract node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = nodeRewriter.rewriteExtract(node, context.get(), TreeRewriter.this);
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
        protected Node visitCurrentTime(CurrentTime node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = nodeRewriter.rewriteCurrentTime(node, context.get(), TreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            return node;
        }

        @Override
        public Node visitCast(Cast node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = nodeRewriter.rewriteCast(node, context.get(), TreeRewriter.this);
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
        private boolean defaultRewrite;
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
