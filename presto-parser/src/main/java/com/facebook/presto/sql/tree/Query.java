package com.facebook.presto.sql.tree;

import com.google.common.base.Objects;
import com.google.common.base.Optional;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class Query
        extends Statement
{
    private final Optional<With> with;
    private final Select select;
    private final List<Relation> from;
    private final Optional<Expression> where;
    private final List<Expression> groupBy;
    private final Optional<Expression> having;
    private final List<SortItem> orderBy;
    private final Optional<String> limit;

    public Query(
            Optional<With> with,
            Select select,
            List<Relation> from,
            Optional<Expression> where,
            List<Expression> groupBy,
            Optional<Expression> having,
            List<SortItem> orderBy,
            Optional<String> limit)
    {
        checkNotNull(with, "with is null");
        checkNotNull(select, "select is null");
        checkNotNull(from, "from is null");
        checkArgument(!from.isEmpty(), "from is empty");
        checkNotNull(groupBy, "groupBy is null");
        checkNotNull(orderBy, "orderBy is null");
        checkNotNull(where, "where is null");
        checkNotNull(having, "having is null");
        checkNotNull(limit, "limit is null");

        this.with = with;
        this.select = select;
        this.from = from;
        this.where = where;
        this.groupBy = groupBy;
        this.having = having;
        this.orderBy = orderBy;
        this.limit = limit;
    }

    public Optional<With> getWith()
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
                .add("with", with.orNull())
                .add("select", select)
                .add("from", from)
                .add("where", where.orNull())
                .add("groupBy", groupBy)
                .add("having", having.orNull())
                .add("orderBy", orderBy)
                .add("limit", limit.orNull())
                .omitNullValues()
                .toString();
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

    @Override
    public int hashCode()
    {
        return Objects.hashCode(with, select, from, where, groupBy, having, orderBy, limit);
    }
}
