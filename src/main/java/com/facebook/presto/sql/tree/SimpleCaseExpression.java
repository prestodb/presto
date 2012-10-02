package com.facebook.presto.sql.tree;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

import java.util.List;

public class SimpleCaseExpression
        extends Expression
{
    private final Expression operand;
    private final List<WhenClause> whenClauses;
    private final Expression defaultValue;

    public SimpleCaseExpression(Expression operand, List<WhenClause> whenClauses, Expression defaultValue)
    {
        this.operand = operand;
        this.whenClauses = ImmutableList.copyOf(whenClauses);
        this.defaultValue = defaultValue;
    }

    public Expression getOperand()
    {
        return operand;
    }

    public List<WhenClause> getWhenClauses()
    {
        return whenClauses;
    }

    public Expression getDefaultValue()
    {
        return defaultValue;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitSimpleCaseExpression(this, context);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("operand", operand)
                .add("whenClauses", whenClauses)
                .add("defaultValue", defaultValue)
                .toString();
    }
}
