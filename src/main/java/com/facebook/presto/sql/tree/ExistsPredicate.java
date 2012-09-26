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
    public String toString()
    {
        return Objects.toStringHelper(this)
                .addValue(subquery)
                .toString();
    }
}
