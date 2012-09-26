package com.facebook.presto.sql.tree;

import com.google.common.base.Objects;

public class SelectItemExpression
        extends SelectItem
{
    private final Expression expression;
    private final String alias;

    public SelectItemExpression(Expression expression, String alias)
    {
        this.expression = expression;
        this.alias = alias;
    }

    @Override
    public boolean isAllColumns()
    {
        return false;
    }

    @Override
    public QualifiedName getAllColumnsName()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression getExpression()
    {
        return expression;
    }

    @Override
    public String getAlias()
    {
        return alias;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("expression", expression)
                .add("alias", alias)
                .toString();
    }
}
