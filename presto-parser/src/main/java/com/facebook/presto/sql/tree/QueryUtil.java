package com.facebook.presto.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;

public class QueryUtil
{
    public static Expression nameReference(String name)
    {
        return new QualifiedNameReference(QualifiedName.of(name));
    }

    public static SelectItem aliasedName(String name, String alias)
    {
        return new SingleColumn(nameReference(name), alias);
    }

    public static Select selectList(Expression... expressions)
    {
        ImmutableList.Builder<SelectItem> items = ImmutableList.builder();
        for (Expression expression : expressions) {
            items.add(new SingleColumn(expression));
        }
        return new Select(false, items.build());
    }

    public static Select selectList(SelectItem... items)
    {
        return new Select(false, ImmutableList.copyOf(items));
    }

    public static Select selectAll(List<SelectItem> items)
    {
        return new Select(false, items);
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
