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

public abstract class DefaultTraversalVisitor<R, C>
        extends AstVisitor<R, C>
{
    @Override
    protected R visitExtract(Extract node, C context)
    {
        return process(node.getExpression(), context);
    }

    @Override
    protected R visitCast(Cast node, C context)
    {
        return process(node.getExpression(), context);
    }

    @Override
    protected R visitArithmeticBinary(ArithmeticBinaryExpression node, C context)
    {
        process(node.getLeft(), context);
        process(node.getRight(), context);

        return null;
    }

    @Override
    protected R visitBetweenPredicate(BetweenPredicate node, C context)
    {
        process(node.getValue(), context);
        process(node.getMin(), context);
        process(node.getMax(), context);

        return null;
    }

    @Override
    protected R visitCoalesceExpression(CoalesceExpression node, C context)
    {
        for (Expression operand : node.getOperands()) {
            process(operand, context);
        }

        return null;
    }

    @Override
    protected R visitAtTimeZone(AtTimeZone node, C context)
    {
        process(node.getValue(), context);
        process(node.getTimeZone(), context);

        return null;
    }

    @Override
    protected R visitArrayConstructor(ArrayConstructor node, C context)
    {
        for (Expression expression : node.getValues()) {
            process(expression, context);
        }

        return null;
    }

    @Override
    protected R visitSubscriptExpression(SubscriptExpression node, C context)
    {
        process(node.getBase(), context);
        process(node.getIndex(), context);

        return null;
    }

    @Override
    protected R visitComparisonExpression(ComparisonExpression node, C context)
    {
        process(node.getLeft(), context);
        process(node.getRight(), context);

        return null;
    }

    @Override
    protected R visitQuery(Query node, C context)
    {
        if (node.getWith().isPresent()) {
            process(node.getWith().get(), context);
        }
        process(node.getQueryBody(), context);
        for (SortItem sortItem : node.getOrderBy()) {
            process(sortItem, context);
        }

        return null;
    }

    @Override
    protected R visitWith(With node, C context)
    {
        for (WithQuery query : node.getQueries()) {
            process(query, context);
        }

        return null;
    }

    @Override
    protected R visitWithQuery(WithQuery node, C context)
    {
        return process(node.getQuery(), context);
    }

    @Override
    protected R visitSelect(Select node, C context)
    {
        for (SelectItem item : node.getSelectItems()) {
            process(item, context);
        }

        return null;
    }

    @Override
    protected R visitSingleColumn(SingleColumn node, C context)
    {
        process(node.getExpression(), context);

        return null;
    }

    @Override
    protected R visitWhenClause(WhenClause node, C context)
    {
        process(node.getOperand(), context);
        process(node.getResult(), context);

        return null;
    }

    @Override
    protected R visitInPredicate(InPredicate node, C context)
    {
        process(node.getValue(), context);
        process(node.getValueList(), context);

        return null;
    }

    @Override
    protected R visitFunctionCall(FunctionCall node, C context)
    {
        for (Expression argument : node.getArguments()) {
            process(argument, context);
        }

        if (node.getWindow().isPresent()) {
            process(node.getWindow().get(), context);
        }

        return null;
    }

    @Override
    protected R visitDereferenceExpression(DereferenceExpression node, C context)
    {
        process(node.getBase(), context);
        return null;
    }

    @Override
    public R visitWindow(Window node, C context)
    {
        for (Expression expression : node.getPartitionBy()) {
            process(expression, context);
        }

        for (SortItem sortItem : node.getOrderBy()) {
            process(sortItem.getSortKey(), context);
        }

        if (node.getFrame().isPresent()) {
            process(node.getFrame().get(), context);
        }

        return null;
    }

    @Override
    public R visitWindowFrame(WindowFrame node, C context)
    {
        process(node.getStart(), context);
        if (node.getEnd().isPresent()) {
            process(node.getEnd().get(), context);
        }

        return null;
    }

    @Override
    public R visitFrameBound(FrameBound node, C context)
    {
        if (node.getValue().isPresent()) {
            process(node.getValue().get(), context);
        }

        return null;
    }

    @Override
    protected R visitSimpleCaseExpression(SimpleCaseExpression node, C context)
    {
        process(node.getOperand(), context);
        for (WhenClause clause : node.getWhenClauses()) {
            process(clause, context);
        }

        node.getDefaultValue()
                .ifPresent(value -> process(value, context));

        return null;
    }

    @Override
    protected R visitInListExpression(InListExpression node, C context)
    {
        for (Expression value : node.getValues()) {
            process(value, context);
        }

        return null;
    }

    @Override
    protected R visitNullIfExpression(NullIfExpression node, C context)
    {
        process(node.getFirst(), context);
        process(node.getSecond(), context);

        return null;
    }

    @Override
    protected R visitIfExpression(IfExpression node, C context)
    {
        process(node.getCondition(), context);
        process(node.getTrueValue(), context);
        if (node.getFalseValue().isPresent()) {
            process(node.getFalseValue().get(), context);
        }

        return null;
    }

    @Override
    protected R visitTryExpression(TryExpression node, C context)
    {
        process(node.getInnerExpression(), context);
        return null;
    }

    @Override
    protected R visitArithmeticUnary(ArithmeticUnaryExpression node, C context)
    {
        return process(node.getValue(), context);
    }

    @Override
    protected R visitNotExpression(NotExpression node, C context)
    {
        return process(node.getValue(), context);
    }

    @Override
    protected R visitSearchedCaseExpression(SearchedCaseExpression node, C context)
    {
        for (WhenClause clause : node.getWhenClauses()) {
            process(clause, context);
        }
        node.getDefaultValue()
                .ifPresent(value -> process(value, context));

        return null;
    }

    @Override
    protected R visitLikePredicate(LikePredicate node, C context)
    {
        process(node.getValue(), context);
        process(node.getPattern(), context);
        if (node.getEscape() != null) {
            process(node.getEscape(), context);
        }

        return null;
    }

    @Override
    protected R visitIsNotNullPredicate(IsNotNullPredicate node, C context)
    {
        return process(node.getValue(), context);
    }

    @Override
    protected R visitIsNullPredicate(IsNullPredicate node, C context)
    {
        return process(node.getValue(), context);
    }

    @Override
    protected R visitLogicalBinaryExpression(LogicalBinaryExpression node, C context)
    {
        process(node.getLeft(), context);
        process(node.getRight(), context);

        return null;
    }

    @Override
    protected R visitSubqueryExpression(SubqueryExpression node, C context)
    {
        return process(node.getQuery(), context);
    }

    @Override
    protected R visitSortItem(SortItem node, C context)
    {
        return process(node.getSortKey(), context);
    }

    @Override
    protected R visitQuerySpecification(QuerySpecification node, C context)
    {
        process(node.getSelect(), context);
        if (node.getFrom().isPresent()) {
            process(node.getFrom().get(), context);
        }
        if (node.getWhere().isPresent()) {
            process(node.getWhere().get(), context);
        }
        if (node.getGroupBy().isPresent()) {
            for (GroupingElement groupingElement : node.getGroupBy().get().getGroupingElements()) {
                process(groupingElement, context);
            }
        }
        if (node.getHaving().isPresent()) {
            process(node.getHaving().get(), context);
        }
        for (SortItem sortItem : node.getOrderBy()) {
            process(sortItem, context);
        }
        return null;
    }

    @Override
    protected R visitSetOperation(SetOperation node, C context)
    {
        for (Relation relation : node.getRelations()) {
            process(relation, context);
        }
        return null;
    }

    @Override
    protected R visitValues(Values node, C context)
    {
        for (Expression row : node.getRows()) {
            process(row, context);
        }
        return null;
    }

    @Override
    protected R visitRow(Row node, C context)
    {
        for (Expression expression : node.getItems()) {
            process(expression, context);
        }
        return null;
    }

    @Override
    protected R visitTableSubquery(TableSubquery node, C context)
    {
        return process(node.getQuery(), context);
    }

    @Override
    protected R visitAliasedRelation(AliasedRelation node, C context)
    {
        return process(node.getRelation(), context);
    }

    @Override
    protected R visitSampledRelation(SampledRelation node, C context)
    {
        process(node.getRelation(), context);
        process(node.getSamplePercentage(), context);
        if (node.getColumnsToStratifyOn().isPresent()) {
            for (Expression expression : node.getColumnsToStratifyOn().get()) {
                process(expression, context);
            }
        }
        return null;
    }

    @Override
    protected R visitJoin(Join node, C context)
    {
        process(node.getLeft(), context);
        process(node.getRight(), context);

        node.getCriteria()
                .filter(criteria -> criteria instanceof JoinOn)
                .map(criteria -> process(((JoinOn) criteria).getExpression(), context));

        return null;
    }

    @Override
    protected R visitUnnest(Unnest node, C context)
    {
        for (Expression expression : node.getExpressions()) {
            process(expression, context);
        }

        return null;
    }
}
