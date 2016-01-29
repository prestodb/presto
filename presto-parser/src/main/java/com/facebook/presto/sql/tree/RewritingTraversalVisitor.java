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
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Optional;

public abstract class RewritingTraversalVisitor<C>
        extends AstVisitor<Node, C>
{
    @Override
    protected Node visitExtract(Extract node, C context)
    {
        Expression expression = (Expression) process(node.getExpression(), context);
        return new Extract(node.getLocation(), expression, node.getField());
    }

    @Override
    protected Node visitCast(Cast node, C context)
    {
        Expression expression = (Expression) process(node.getExpression(), context);
        return new Cast(node.getLocation(), expression, node.getType(), node.isSafe(), node.isTypeOnly());
    }

    @Override
    protected Node visitArithmeticBinary(ArithmeticBinaryExpression node, C context)
    {
        Expression expression1 = (Expression) process(node.getLeft(), context);
        Expression expression2 = (Expression) process(node.getRight(), context);

        return new ArithmeticBinaryExpression(node.getLocation(), node.getType(), expression1, expression2);
    }

    @Override
    protected Node visitBetweenPredicate(BetweenPredicate node, C context)
    {
        Expression value = (Expression) process(node.getValue(), context);
        Expression min = (Expression) process(node.getMin(), context);
        Expression max = (Expression) process(node.getMax(), context);

        return new BetweenPredicate(node.getLocation(), value, min, max);
    }

    @Override
    protected Node visitCoalesceExpression(CoalesceExpression node, C context)
    {
        List<Expression> operands = processExpressionList(node.getOperands(), context);

        return new CoalesceExpression(node.getLocation(), operands);
    }

    private List<Expression> processExpressionList(List<Expression> expressions, C context)
    {
        return ImmutableList.copyOf(Lists.transform(expressions, (value -> (Expression) process(value, context))));
    }

    @Override
    protected Node visitArrayConstructor(ArrayConstructor node, C context)
    {
        List<Expression> values = processExpressionList(node.getValues(), context);
        return new ArrayConstructor(node.getLocation(), values);
    }

    @Override
    protected Node visitSubscriptExpression(SubscriptExpression node, C context)
    {
        SubscriptExpression base = (SubscriptExpression) process(node.getBase(), context);
        Expression index = (Expression) process(node.getIndex(), context);

        return new SubscriptExpression(node.getLocation(), base, index);
    }

    @Override
    protected Node visitComparisonExpression(ComparisonExpression node, C context)
    {
        Expression left = (Expression) process(node.getLeft(), context);
        Expression right = (Expression) process(node.getRight(), context);

        return new ComparisonExpression(node.getLocation(), node.getType(), left, right);
    }

    @Override
    protected Node visitQuery(Query node, C context)
    {
        Optional<With> with = Optional.empty();
        if (node.getWith().isPresent()) {
            with = Optional.of((With) process(node.getWith().get(), context));
        }

        QueryBody queryBody = (QueryBody) process(node.getQueryBody(), context);

        List<SortItem> orderBy = ImmutableList.copyOf(Lists.transform(node.getOrderBy(), (value -> (SortItem) process(value, context))));
        return new Query(node.getLocation(), with, queryBody, orderBy, node.getLimit(), node.getApproximate());
    }

    @Override
    protected Node visitWith(With node, C context)
    {
        List<WithQuery> queries = ImmutableList.copyOf(Lists.transform(node.getQueries(), (value -> (WithQuery) process(value, context))));
        return new With(node.getLocation(), node.isRecursive(), queries);
    }

    @Override
    protected Node visitWithQuery(WithQuery node, C context)
    {
        Query query = (Query) process(node.getQuery(), context);
        return new WithQuery(node.getLocation(), node.getName(), query, node.getColumnNames());
    }

    @Override
    protected Node visitSelect(Select node, C context)
    {
        List<SelectItem> selectItems = ImmutableList.copyOf(Lists.transform(node.getSelectItems(), (value -> (SelectItem) process(value, context))));
        return new Select(node.getLocation(), node.isDistinct(), selectItems);
    }

    @Override
    protected Node visitSingleColumn(SingleColumn node, C context)
    {
        Expression expression = (Expression) process(node.getExpression(), context);

        return new SingleColumn(node.getLocation(), expression, node.getAlias());
    }

    @Override
    protected Node visitWhenClause(WhenClause node, C context)
    {
        Expression operand = (Expression) process(node.getOperand(), context);
        Expression result = (Expression) process(node.getResult(), context);

        return new WhenClause(node.getLocation(), operand, result);
    }

    @Override
    protected Node visitInPredicate(InPredicate node, C context)
    {
        Expression value = (Expression) process(node.getValue(), context);
        Expression valueList = (Expression) process(node.getValueList(), context);

        return new InPredicate(node.getLocation(), value, valueList);
    }

    @Override
    protected Node visitFunctionCall(FunctionCall node, C context)
    {
        List<Expression> arguments = processExpressionList(node.getArguments(), context);

        Optional<Window> window = Optional.empty();
        if (node.getWindow().isPresent()) {
            window = Optional.of((Window) process(node.getWindow().get(), context));
        }

        return new FunctionCall(node.getLocation(), node.getName(), window, node.isDistinct(), arguments);
    }

    @Override
    protected Node visitDereferenceExpression(DereferenceExpression node, C context)
    {
        Expression base = (Expression) process(node.getBase(), context);
        return new DereferenceExpression(node.getLocation(), base, node.getFieldName());
    }

    @Override
    public Node visitWindow(Window node, C context)
    {
        List<Expression> partitionBy = processExpressionList(node.getPartitionBy(), context);

        List<SortItem> orderBy = ImmutableList.copyOf(Lists.transform(node.getOrderBy(), (value -> (SortItem) process(value, context))));

        Optional<WindowFrame> frame = Optional.empty();
        if (node.getFrame().isPresent()) {
            frame = Optional.of((WindowFrame) process(node.getFrame().get(), context));
        }

        return new Window(node.getLocation(), partitionBy, orderBy, frame);
    }

    @Override
    public Node visitWindowFrame(WindowFrame node, C context)
    {
        FrameBound start = (FrameBound) process(node.getStart(), context);
        Optional<FrameBound> end = Optional.empty();
        if (node.getEnd().isPresent()) {
            end = Optional.of((FrameBound) process(node.getEnd().get(), context));
        }

        return new WindowFrame(node.getLocation(), node.getType(), start, end);
    }

    @Override
    public Node visitFrameBound(FrameBound node, C context)
    {
        Expression value = null;
        if (node.getValue().isPresent()) {
            value = (Expression) process(node.getValue().get(), context);
        }

        return new FrameBound(node.getLocation(), node.getType(), value);
    }

    @Override
    protected Node visitSimpleCaseExpression(SimpleCaseExpression node, C context)
    {
        Expression operand = (Expression) process(node.getOperand(), context);
        List<WhenClause> whenClauses = ImmutableList.copyOf(Lists.transform(node.getWhenClauses(), (value -> (WhenClause) process(value, context))));

        Optional<Expression> defaultValue = Optional.empty();
        if (node.getDefaultValue().isPresent()) {
            defaultValue = Optional.of((Expression) process(node.getDefaultValue().get(), context));
        }
        node.getDefaultValue()
                .ifPresent(value -> process(value, context));

        return new SimpleCaseExpression(node.getLocation(), operand, whenClauses, defaultValue);
    }

    @Override
    protected Node visitInListExpression(InListExpression node, C context)
    {
        List<Expression> values = processExpressionList(node.getValues(), context);

        return new InListExpression(node.getLocation(), values);
    }

    @Override
    protected Node visitNullIfExpression(NullIfExpression node, C context)
    {
        Expression first = (Expression) process(node.getFirst(), context);
        Expression second = (Expression) process(node.getSecond(), context);

        return new NullIfExpression(node.getLocation(), first, second);
    }

    @Override
    protected Node visitIfExpression(IfExpression node, C context)
    {
        Expression condition = (Expression) process(node.getCondition(), context);
        Expression trueValue = (Expression) process(node.getTrueValue(), context);
        Expression falseValue = null;
        if (node.getFalseValue().isPresent()) {
            falseValue = (Expression) process(node.getFalseValue().get(), context);
        }

        return new IfExpression(node.getLocation(), condition, trueValue, falseValue);
    }

    @Override
    protected Node visitArithmeticUnary(ArithmeticUnaryExpression node, C context)
    {
        Expression value = (Expression) process(node.getValue(), context);
        return new ArithmeticUnaryExpression(node.getLocation(), node.getSign(), value);
    }

    @Override
    protected Node visitNotExpression(NotExpression node, C context)
    {
        Expression value = (Expression) process(node.getValue(), context);
        return new NotExpression(node.getLocation(), value);
    }

    @Override
    protected Node visitSearchedCaseExpression(SearchedCaseExpression node, C context)
    {
        List<WhenClause> whenClauses = ImmutableList.copyOf(Lists.transform(node.getWhenClauses(), (value -> (WhenClause) process(value, context))));

        Optional<Expression> defaultValue = Optional.empty();
        if (node.getDefaultValue().isPresent()) {
            defaultValue = Optional.of((Expression) process(node.getDefaultValue().get(), context));
        }

        return new SearchedCaseExpression(node.getLocation(), whenClauses, defaultValue);
    }

    @Override
    protected Node visitLikePredicate(LikePredicate node, C context)
    {
        Expression value = (Expression) process(node.getValue(), context);
        Expression pattern = (Expression) process(node.getPattern(), context);
        Expression escape = null;
        if (node.getEscape() != null) {
            escape = (Expression) process(node.getEscape(), context);
        }

        return new LikePredicate(node.getLocation(), value, pattern, escape);
    }

    @Override
    protected Node visitIsNotNullPredicate(IsNotNullPredicate node, C context)
    {
        Expression value = (Expression) process(node.getValue(), context);
        return new IsNotNullPredicate(node.getLocation(), value);
    }

    @Override
    protected Node visitIsNullPredicate(IsNullPredicate node, C context)
    {
        Expression value = (Expression) process(node.getValue(), context);
        return new IsNullPredicate(node.getLocation(), value);
    }

    @Override
    protected Node visitLogicalBinaryExpression(LogicalBinaryExpression node, C context)
    {
        Expression left = (Expression) process(node.getLeft(), context);
        Expression right = (Expression) process(node.getRight(), context);

        return new LogicalBinaryExpression(node.getLocation(), node.getType(), left, right);
    }

    @Override
    protected Node visitSubqueryExpression(SubqueryExpression node, C context)
    {
        Query query = (Query) process(node.getQuery(), context);
        return new SubqueryExpression(node.getLocation(), query);
    }

    @Override
    protected Node visitSortItem(SortItem node, C context)
    {
        Expression sortKey = (Expression) process(node.getSortKey(), context);
        return new SortItem(node.getLocation(), sortKey, node.getOrdering(), node.getNullOrdering());
    }

    @Override
    protected Node visitQuerySpecification(QuerySpecification node, C context)
    {
        Select select = (Select) process(node.getSelect(), context);

        Optional<Relation> from = Optional.empty();
        if (node.getFrom().isPresent()) {
            from = Optional.of((Relation) process(node.getFrom().get(), context));
        }

        Optional<Expression> where = Optional.empty();
        if (node.getWhere().isPresent()) {
            where = Optional.of((Expression) process(node.getWhere().get(), context));
        }

        Optional<GroupBy> groupBy = Optional.empty();
        if (node.getGroupBy().isPresent()) {
            GroupBy original = node.getGroupBy().get();
            List<GroupingElement> groupingElements = ImmutableList.copyOf(Lists.transform(original.getGroupingElements(), (value -> (GroupingElement) process(value, context))));
            groupBy = Optional.of(new GroupBy(original.getLocation(), original.isDistinct(), groupingElements));
        }

        Optional<Expression> having = Optional.empty();
        if (node.getHaving().isPresent()) {
            having = Optional.of((Expression) process(node.getHaving().get(), context));
        }

        List<SortItem> orderBy = ImmutableList.copyOf(Lists.transform(node.getOrderBy(), (value -> (SortItem) process(value, context))));

        return new QuerySpecification(node.getLocation(), select, from, where, groupBy, having, orderBy, node.getLimit());
    }

    @Override
    protected Node visitUnion(Union node, C context)
    {
        List<Relation> relations = ImmutableList.copyOf(Lists.transform(node.getRelations(), (value -> (Relation) process(value, context))));
        return new Union(node.getLocation(), relations, node.isDistinct());
    }

    @Override
    protected Node visitIntersect(Intersect node, C context)
    {
        List<Relation> relations = ImmutableList.copyOf(Lists.transform(node.getRelations(), (value -> (Relation) process(value, context))));
        return new Intersect(node.getLocation(), relations, node.isDistinct());
    }

    @Override
    protected Node visitExcept(Except node, C context)
    {
        Relation left = (Relation) process(node.getLeft(), context);
        Relation right = (Relation) process(node.getRight(), context);
        return new Except(node.getLocation(), left, right, node.isDistinct());
    }

    @Override
    protected Node visitValues(Values node, C context)
    {
        List<Expression> rows = processExpressionList(node.getRows(), context);
        return new Values(node.getLocation(), rows);
    }

    @Override
    protected Node visitRow(Row node, C context)
    {
        List<Expression> items = processExpressionList(node.getItems(), context);
        return new Row(node.getLocation(), items);
    }

    @Override
    protected Node visitTableSubquery(TableSubquery node, C context)
    {
        Query query = (Query) process(node.getQuery(), context);
        return new TableSubquery(node.getLocation(), query);
    }

    @Override
    protected Node visitAliasedRelation(AliasedRelation node, C context)
    {
        Relation relation = (Relation) process(node.getRelation(), context);
        return new AliasedRelation(node.getLocation(), relation, node.getAlias(), node.getColumnNames());
    }

    @Override
    protected Node visitSampledRelation(SampledRelation node, C context)
    {
        Relation relation = (Relation) process(node.getRelation(), context);
        Expression sampledPercentage = (Expression) process(node.getSamplePercentage(), context);
        Optional<List<Expression>> columnsToStratifyOn = Optional.empty();
        if (node.getColumnsToStratifyOn().isPresent()) {
            columnsToStratifyOn = Optional.of(processExpressionList(node.getColumnsToStratifyOn().get(), context));
        }
        return new SampledRelation(node.getLocation(), relation, node.getType(), sampledPercentage, node.isRescaled(), columnsToStratifyOn);
    }

    @Override
    protected Node visitJoin(Join node, C context)
    {
        Relation left = (Relation) process(node.getLeft(), context);
        Relation right = (Relation) process(node.getRight(), context);

        Optional<JoinCriteria> criteria = node.getCriteria();
        if (criteria.isPresent() && criteria.get() instanceof JoinOn) {
            Expression expression = (Expression) process(((JoinOn) criteria.get()).getExpression(), context);
            criteria = Optional.of(new JoinOn(expression));
        }

        return new Join(node.getLocation(), node.getType(), left, right, criteria);
    }

    @Override
    protected Node visitUnnest(Unnest node, C context)
    {
        List<Expression> expression = processExpressionList(node.getExpressions(), context);

        return new Unnest(node.getLocation(), expression, node.isWithOrdinality());
    }

    @Override
    protected Node visitNode(Node node, C context)
    {
        return node;
    }

    @Override
    protected Node visitExpression(Expression node, C context)
    {
        return visitNode(node, context);
    }

    @Override
    protected Node visitInsert(Insert node, C context)
    {
        Query query = (Query) process(node.getQuery(), context);
        return new Insert(node.getLocation(), node.getColumns(), node.getTarget(), query);
    }

    @Override
    protected Node visitSimpleGroupBy(SimpleGroupBy node, C context)
    {
        List<Expression> groupByExpressions = ImmutableList.copyOf(Lists.transform(node.getColumnExpressions(), (value -> (Expression) process(value, context))));
        return new SimpleGroupBy(node.getLocation(), groupByExpressions);
    }
}
