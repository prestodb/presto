package com.facebook.presto.sql.tree;

import com.google.common.base.Objects;

public class ArithmeticExpression
        extends Expression
{
    public enum Type
    {
        ADD, SUBTRACT, MULTIPLY, DIVIDE, MODULUS
    }

    private final Type type;
    private final Expression left;
    private final Expression right;

    public ArithmeticExpression(Type type, Expression left, Expression right)
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
        return visitor.visitArithmeticExpression(this, context);
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
