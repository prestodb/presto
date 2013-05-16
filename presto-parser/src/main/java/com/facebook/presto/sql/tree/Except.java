package com.facebook.presto.sql.tree;

import com.google.common.base.Objects;

public class Except
    extends SetOperation
{
    private final Relation left;
    private final Relation right;
    private final boolean distinct;

    public Except(Relation left, Relation right, boolean distinct)
    {
        this.left = left;
        this.right = right;
        this.distinct = distinct;
    }

    public Relation getLeft()
    {
        return left;
    }

    public Relation getRight()
    {
        return right;
    }

    public boolean isDistinct()
    {
        return distinct;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("left", left)
                .add("right", right)
                .add("distinct", distinct)
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

        Except except = (Except) o;

        if (!left.equals(except.left)) {
            return false;
        }
        if (!right.equals(except.right)) {
            return false;
        }
        if (distinct != except.distinct) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(left, right, distinct);
    }
}
