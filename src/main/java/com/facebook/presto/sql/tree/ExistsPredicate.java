package com.facebook.presto.sql.tree;

import com.google.common.base.Objects;

public class ExistsPredicate
        extends Expression
{
    private final Query subquery;

    public ExistsPredicate(Query subquery)
    {
        this.subquery = subquery;
    }

    public Query getSubquery()
    {
        return subquery;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitExists(this, context);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .addValue(subquery)
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

        ExistsPredicate that = (ExistsPredicate) o;

        if (!subquery.equals(that.subquery)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return subquery.hashCode();
    }
}
