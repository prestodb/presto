package com.facebook.presto.sql.tree;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

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
        Preconditions.checkNotNull(type, "type is null");
        Preconditions.checkNotNull(left, "left is null");
        Preconditions.checkNotNull(right, "right is null");

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

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        LogicalBinaryExpression that = (LogicalBinaryExpression) o;

        if (!left.equals(that.left)) {
            return false;
        }
        if (!right.equals(that.right)) {
            return false;
        }
        if (type != that.type) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = type.hashCode();
        result = 31 * result + left.hashCode();
        result = 31 * result + right.hashCode();
        return result;
    }
}
