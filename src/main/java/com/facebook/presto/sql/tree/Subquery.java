package com.facebook.presto.sql.tree;

import com.google.common.base.Objects;

public class Subquery
        extends Relation
{
    private final Query query;

    public Subquery(Query query)
    {
        this.query = query;
    }

    public Query getQuery()
    {
        return query;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .addValue(query)
                .toString();
    }
}
