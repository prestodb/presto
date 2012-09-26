package com.facebook.presto.sql.tree;

import com.google.common.base.Objects;

public class SubqueryTable
        extends TablePrimary
{
    private final Query query;

    public SubqueryTable(Query query, TableCorrelation correlation)
    {
        super(correlation);
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
                .add("query", query)
                .add("correlation", getCorrelation())
                .omitNullValues()
                .toString();
    }
}
