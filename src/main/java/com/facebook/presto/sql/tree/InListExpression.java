package com.facebook.presto.sql.tree;

import com.google.common.base.Objects;

import java.util.List;

public class InListExpression
        extends Expression
{
    private final List<Expression> values;

    public InListExpression(List<Expression> values)
    {
        this.values = values;
    }

    public List<Expression> getValues()
    {
        return values;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitInListExpression(this, context);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .addValue(values)
                .toString();
    }
}
