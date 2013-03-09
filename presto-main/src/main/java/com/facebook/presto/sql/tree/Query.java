package com.facebook.presto.sql.tree;

import com.google.common.base.Objects;
import com.google.common.base.Optional;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class Query
        extends Statement
{
    private final Select select;
    private final List<Relation> from;
    private final Optional<Expression> where;
    private final List<Expression> groupBy;
    private final Optional<Expression> having;
    private final List<SortItem> orderBy;
    private final Optional<String> limit;

    public Query(
            Select select,
            List<Relation> from,
            Optional<Expression> where,
            List<Expression> groupBy,
            Optional<Expression> having,
            List<SortItem> orderBy,
            Optional<String> limit)
    {
        checkNotNull(select, "select is null");
        checkNotNull(from, "from is null");
        checkArgument(!from.isEmpty(), "from is empty");
        checkNotNull(groupBy, "groupBy is null");
        checkNotNull(orderBy, "orderBy is null");
        checkNotNull(where, "where is null");
        checkNotNull(where, "having is null");
        checkNotNull(where, "limit is null");

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

    public Optional<Expression> getWhere()
    {
        return where;
    }

    public List<Expression> getGroupBy()
    {
        return groupBy;
    }

    public Optional<Expression> getHaving()
    {
        return having;
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
        return visitor.visitQuery(this, context);
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

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Query query = (Query) o;

        if (!from.equals(query.from)) {
            return false;
        }
        if (!groupBy.equals(query.groupBy)) {
            return false;
        }
        if (having != null ? !having.equals(query.having) : query.having != null) {
            return false;
        }
        if (limit != null ? !limit.equals(query.limit) : query.limit != null) {
            return false;
        }
        if (!orderBy.equals(query.orderBy)) {
            return false;
        }
        if (!select.equals(query.select)) {
            return false;
        }
        if (where != null ? !where.equals(query.where) : query.where != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = select.hashCode();
        result = 31 * result + from.hashCode();
        result = 31 * result + (where != null ? where.hashCode() : 0);
        result = 31 * result + groupBy.hashCode();
        result = 31 * result + (having != null ? having.hashCode() : 0);
        result = 31 * result + orderBy.hashCode();
        result = 31 * result + (limit != null ? limit.hashCode() : 0);
        return result;
    }
}
