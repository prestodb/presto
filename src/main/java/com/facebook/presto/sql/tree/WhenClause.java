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
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("operand", operand)
                .add("result", result)
                .toString();
    }
}
