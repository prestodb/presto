package com.facebook.presto.sql.tree;

import com.google.common.base.Objects;

public class JoinOn
        extends JoinCriteria
{
    private final Expression expression;

    public JoinOn(Expression expression)
    {
        this.expression = expression;
    }

    public Expression getExpression()
    {
        return expression;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .addValue(expression)
                .toString();
    }
}
