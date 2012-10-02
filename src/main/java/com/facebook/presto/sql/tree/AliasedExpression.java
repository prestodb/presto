package com.facebook.presto.sql.tree;

import com.google.common.base.Objects;

public class AliasedExpression
        extends Expression
{
    private final Expression expression;
    private final String alias;

    public AliasedExpression(Expression expression, String alias)
    {
        this.expression = expression;
        this.alias = alias;
    }

    public Expression getExpression()
    {
        return expression;
    }

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
