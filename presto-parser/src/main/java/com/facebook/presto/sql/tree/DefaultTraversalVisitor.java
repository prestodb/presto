package com.facebook.presto.sql.tree;

public class DefaultTraversalVisitor<R, C>
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
    protected R visitArithmeticExpression(ArithmeticExpression node, C context)
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
    protected R visitComparisonExpression(ComparisonExpression node, C context)
    {
        process(node.getLeft(), context);
        process(node.getRight(), context);

        return null;
    }

    @Override
    protected R visitQuery(Query node, C context)
    {
        process(node.getSelect(), context);
        for (Relation relation : node.getFrom()) {
            process(relation, context);
        }
        if (node.getWhere().isPresent()) {
            process(node.getWhere().get(), context);
        }
        for (Expression expression : node.getGroupBy()) {
            process(expression, context);
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
    protected R visitSelect(Select node, C context)
    {
        for (Expression item : node.getSelectItems()) {
            process(item, context);
        }

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
    protected R visitAliasedExpression(AliasedExpression node, C context)
    {
        return process(node.getExpression(), context);
    }

    @Override
    protected R visitSimpleCaseExpression(SimpleCaseExpression node, C context)
    {
        process(node.getOperand(), context);
        for (WhenClause clause : node.getWhenClauses()) {
            process(clause, context);
        }
        if (node.getDefaultValue() != null) {
            process(node.getDefaultValue(), context);
        }

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
    protected R visitNegativeExpression(NegativeExpression node, C context)
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
        if (node.getDefaultValue() != null) {
            process(node.getDefaultValue(), context);
        }

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
    protected R visitSubquery(Subquery node, C context)
    {
        return process(node.getQuery(), context);
    }

    @Override
    protected R visitAliasedRelation(AliasedRelation node, C context)
    {
        return process(node.getRelation(), context);
    }

    @Override
    protected R visitJoin(Join node, C context)
    {
        process(node.getLeft(), context);
        process(node.getRight(), context);

        return null;
    }
}
