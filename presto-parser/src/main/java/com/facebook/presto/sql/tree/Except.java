package com.facebook.presto.sql.tree;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

public class Except
        extends SetOperation
{
    private final Relation left;
    private final Relation right;
    private final boolean distinct;

    public Except(Relation left, Relation right, boolean distinct)
    {
        Preconditions.checkNotNull(left, "left is null");
        Preconditions.checkNotNull(right, "right is null");

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
        throw new UnsupportedOperationException("EXCEPT not yet supported");
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
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        Except o = (Except) obj;
        return Objects.equal(left, o.left) &&
                Objects.equal(right, o.right) &&
                Objects.equal(distinct, o.distinct);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(left, right, distinct);
    }
}
