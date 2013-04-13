package com.facebook.presto.sql.tree;

import com.google.common.base.Objects;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class WithQuery
        extends Node
{
    private final String name;
    private final Query query;
    private final List<String> columnNames;

    public WithQuery(String name, Query query, List<String> columnNames)
    {
        this.name = checkNotNull(name, "name is null");
        this.query = checkNotNull(query, "query is null");
        this.columnNames = columnNames;
    }

    public String getName()
    {
        return name;
    }

    public Query getQuery()
    {
        return query;
    }

    public List<String> getColumnNames()
    {
        return columnNames;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitWithQuery(this, context);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("name", name)
                .add("query", query)
                .add("columnNames", columnNames)
                .omitNullValues()
                .toString();
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(name, query, columnNames);
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
        WithQuery o = (WithQuery) obj;
        return Objects.equal(name, o.name) &&
                Objects.equal(query, o.query) &&
                Objects.equal(columnNames, o.columnNames);
    }
}
