package com.facebook.presto.sql.tree;

import com.google.common.base.Objects;

import java.util.List;

public class Query
        extends Statement
{
    private final Select select;
    private final List<Relation> from;
    private final Expression where;
    private final List<Expression> groupBy;
    private final Expression having;
    private final List<SortItem> orderBy;
    private final String limit;

    public Query(
            Select select,
            List<Relation> from,
            Expression where,
            List<Expression> groupBy,
            Expression having,
            List<SortItem> orderBy,
            String limit)
    {
        this.select = select;
        this.from = from;
        this.where = where;
        this.groupBy = groupBy;
        this.having = having;
        this.orderBy = orderBy;
        this.limit = limit;
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
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("select", select)
                .add("from", from)
                .add("where", where)
                .add("groupBy", groupBy)
                .add("having", having)
                .add("orderBy", orderBy)
                .add("limit", limit)
                .toString();
    }
}
