package com.facebook.presto.sql.tree;

import com.google.common.base.Objects;

public class ComparisonExpression
        extends Expression
{
    public enum Type
    {
        EQUAL("="),
        NOT_EQUAL("!="),
        LESS_THAN("<"),
        LESS_THAN_OR_EQUAL("<="),
        GREATER_THAN(">"),
        GREATER_THAN_OR_EQUAL(">=");
        private final String value;

        Type(String value)
        {
            this.value = value;
        }

        public String getValue()
        {
            return value;
        }
    }

    private final Type type;
    private final Expression left;
    private final Expression right;

    public ComparisonExpression(Type type, Expression left, Expression right)
    {
        this.type = type;
        this.left = left;
        this.right = right;
    }

    public Type getType()
    {
        return type;
    }

    public Expression getLeft()
    {
        return left;
    }

    public Expression getRight()
    {
        return right;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitComparisonExpression(this, context);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("type", type)
                .add("left", left)
                .add("right", right)
                .toString();
    }
}
