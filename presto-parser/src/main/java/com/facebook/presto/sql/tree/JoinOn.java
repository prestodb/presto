package com.facebook.presto.sql.tree;

import com.google.common.base.Objects;

import static com.google.common.base.Preconditions.checkNotNull;

public class JoinOn
        extends JoinCriteria
{
    private final Expression expression;

    public JoinOn(Expression expression)
    {
        this.expression = checkNotNull(expression, "expression is null");
    }

    public Expression getExpression()
    {
        return expression;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        JoinOn o = (JoinOn) obj;
        return Objects.equal(expression, o.expression);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(expression);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .addValue(expression)
                .toString();
    }
}
