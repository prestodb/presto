package com.facebook.presto.sql.tree;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import java.util.List;

public class Query
        extends Statement
{
    private final With with;
    private final Select select;
    private final List<Relation> from;
    private final Expression where;
    private final List<Expression> groupBy;
    private final Expression having;
    private final List<SortItem> orderBy;
    private final String limit;

    public Query(
            With with,
            Select select,
            List<Relation> from,
            Expression where,
            List<Expression> groupBy,
            Expression having,
            List<SortItem> orderBy,
            String limit)
    {
        Preconditions.checkNotNull(select, "select is null");
        Preconditions.checkNotNull(from, "from is null");
        Preconditions.checkArgument(!from.isEmpty(), "from is empty");
        Preconditions.checkNotNull(groupBy, "groupBy is null");
        Preconditions.checkNotNull(orderBy, "orderBy is null");

        this.with = with;
        this.select = select;
        this.from = from;
        this.where = where;
        this.groupBy = groupBy;
        this.having = having;
        this.orderBy = orderBy;
        this.limit = limit;
    }

    public With getWith()
    {
        return with;
    }

    public Select getSelect()
    {
        return select;
    }

    public List<Relation> getFrom()
    {
        return from;
    }

    public Expression getWhere()
    {
        return where;
    }

    public List<Expression> getGroupBy()
    {
        return groupBy;
    }

    public Expression getHaving()
    {
        return having;
    }

    public List<SortItem> getOrderBy()
    {
        return orderBy;
    }

    public String getLimit()
    {
        return limit;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitQuery(this, context);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("with", with)
                .add("select", select)
                .add("from", from)
                .add("where", where)
                .add("groupBy", groupBy)
                .add("having", having)
                .add("orderBy", orderBy)
                .add("limit", limit)
                .toString();
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(with, select, from, where, groupBy, having, orderBy, limit);
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
        Query o = (Query) obj;
        return Objects.equal(with, o.with) &&
                Objects.equal(select, o.select) &&
                Objects.equal(from, o.from) &&
                Objects.equal(where, o.where) &&
                Objects.equal(groupBy, o.groupBy) &&
                Objects.equal(having, o.having) &&
                Objects.equal(orderBy, o.orderBy) &&
                Objects.equal(limit, o.limit);
    }
}
