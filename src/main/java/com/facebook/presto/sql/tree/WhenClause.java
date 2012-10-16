package com.facebook.presto.sql.tree;

import com.google.common.base.Objects;

public class WhenClause
        extends Expression
{
    private final Expression operand;
    private final Expression result;

    public WhenClause(Expression operand, Expression result)
    {
        this.operand = operand;
        this.result = result;
    }

    public Expression getOperand()
    {
        return operand;
    }

    public Expression getResult()
    {
        return result;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitWhenClause(this, context);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("operand", operand)
                .add("result", result)
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

        WhenClause that = (WhenClause) o;

        if (!operand.equals(that.operand)) {
            return false;
        }
        if (!result.equals(that.result)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result1 = operand.hashCode();
        result1 = 31 * result1 + result.hashCode();
        return result1;
    }
}
