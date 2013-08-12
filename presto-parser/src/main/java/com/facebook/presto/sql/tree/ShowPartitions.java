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

    public ShowPartitions(QualifiedName table, Optional<Expression> where, List<SortItem> orderBy)
    {
        this.table = checkNotNull(table, "table is null");
        this.where = checkNotNull(where, "where is null");
        this.orderBy = ImmutableList.copyOf(checkNotNull(orderBy, "orderBy is null"));
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

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitShowPartitions(this, context);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(table, where, orderBy);
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
                Objects.equal(orderBy, o.orderBy);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("table", table)
                .add("where", where)
                .add("orderBy", orderBy)
                .toString();
    }
}
