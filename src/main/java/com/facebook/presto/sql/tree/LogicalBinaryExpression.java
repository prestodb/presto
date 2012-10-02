package com.facebook.presto.sql.tree;

import com.google.common.base.Objects;

public class LogicalBinaryExpression
        extends Expression
{
    public enum Type
    {
        AND, OR
    }

    private final Type type;
    private final Expression left;
    private final Expression right;

    public LogicalBinaryExpression(Type type, Expression left, Expression right)
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
        return visitor.visitLogicalBinaryExpression(this, context);
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

    public static LogicalBinaryExpression and(Expression left, Expression right)
    {
        return new LogicalBinaryExpression(Type.AND, left, right);
    }

    public static LogicalBinaryExpression or(Expression left, Expression right)
    {
        return new LogicalBinaryExpression(Type.OR, left, right);
    }
}
