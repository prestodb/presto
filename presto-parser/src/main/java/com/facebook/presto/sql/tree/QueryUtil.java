package com.facebook.presto.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;

public class QueryUtil
{
    public static Expression nameReference(String name)
    {
        return new QualifiedNameReference(QualifiedName.of(name));
    }

    public static Expression aliasedName(String name, String alias)
    {
        return new AliasedExpression(nameReference(name), alias);
    }

    public static Expression aliasedExpression(Expression expression, String alias)
    {
        return new AliasedExpression(expression, alias);
    }

    public static Select selectList(Expression... expressions)
    {
        return new Select(false, ImmutableList.copyOf(expressions));
    }

    public static Select selectAll(List<Expression> expressions)
    {
        return new Select(false, expressions);
    }

    public static List<Relation> table(QualifiedName name)
    {
        return ImmutableList.<Relation>of(new Table(name));
    }

    public static SortItem ascending(String name)
    {
        return new SortItem(nameReference(name), SortItem.Ordering.ASCENDING, SortItem.NullOrdering.UNDEFINED);
    }

    public static Expression logicalAnd(Expression left, Expression right)
    {
        return new LogicalBinaryExpression(LogicalBinaryExpression.Type.AND, left, right);
    }

    public static Expression equal(Expression left, Expression right)
    {
        return new ComparisonExpression(ComparisonExpression.Type.EQUAL, left, right);
    }

    public static Expression caseWhen(Expression operand, Expression result)
    {
        return new SearchedCaseExpression(ImmutableList.of(new WhenClause(operand, result)), null);
    }

    public static Expression functionCall(String name, Expression... arguments)
    {
        return new FunctionCall(new QualifiedName(name), ImmutableList.copyOf(arguments));
    }
}
