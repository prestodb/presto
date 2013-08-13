package com.facebook.presto.sql.tree;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class ShowPartitions
        extends Statement
{
    private final QualifiedName table;
    private final Optional<Expression> where;
    private final List<SortItem> orderBy;
    private final Optional<String> limit;

    public ShowPartitions(QualifiedName table, Optional<Expression> where, List<SortItem> orderBy, Optional<String> limit)
    {
        this.table = checkNotNull(table, "table is null");
        this.where = checkNotNull(where, "where is null");
        this.orderBy = ImmutableList.copyOf(checkNotNull(orderBy, "orderBy is null"));
        this.limit = checkNotNull(limit, "limit is null");
    }

    public QualifiedName getTable()
    {
        return table;
    }

    public Optional<Expression> getWhere()
    {
        return where;
    }

    public List<SortItem> getOrderBy()
    {
        return orderBy;
    }

    public Optional<String> getLimit()
    {
        return limit;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitShowPartitions(this, context);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(table, where, orderBy, limit);
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
        ShowPartitions o = (ShowPartitions) obj;
        return Objects.equal(table, o.table) &&
                Objects.equal(where, o.where) &&
                Objects.equal(orderBy, o.orderBy) &&
                Objects.equal(limit, o.limit);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("table", table)
                .add("where", where)
                .add("orderBy", orderBy)
                .add("limit", limit)
                .toString();
    }
}
