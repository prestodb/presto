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

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        InListExpression that = (InListExpression) o;

        if (!values.equals(that.values)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return values.hashCode();
    }
}
