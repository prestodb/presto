package com.facebook.presto.sql.tree;

import com.google.common.base.Objects;

public class TableSubquery
        extends Relation
{
    private final Query query;

    public TableSubquery(Query query)
    {
        this.query = query;
    }

    public Query getQuery()
    {
        return query;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitTableSubquery(this, context);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .addValue(query)
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

        TableSubquery tableSubquery = (TableSubquery) o;

        if (!query.equals(tableSubquery.query)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return query.hashCode();
    }
}
