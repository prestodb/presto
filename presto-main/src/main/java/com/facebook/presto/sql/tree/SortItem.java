package com.facebook.presto.sql.tree;

import com.google.common.base.Function;
import com.google.common.base.Objects;

import javax.annotation.Nullable;

public class SortItem
    extends Node
{
    public enum Ordering
    {
        ASCENDING, DESCENDING
    }

    public enum NullOrdering
    {
        FIRST, LAST, UNDEFINED
    }

    private final Expression sortKey;
    private final Ordering ordering;
    private final NullOrdering nullOrdering;

    public SortItem(Expression sortKey, Ordering ordering, NullOrdering nullOrdering)
    {
        this.ordering = ordering;
        this.sortKey = sortKey;
        this.nullOrdering = nullOrdering;
    }

    public Expression getSortKey()
    {
        return sortKey;
    }

    public Ordering getOrdering()
    {
        return ordering;
    }

    public NullOrdering getNullOrdering()
    {
        return nullOrdering;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitSortItem(this, context);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("sortKey", sortKey)
                .add("ordering", ordering)
                .add("nullOrdering", nullOrdering)
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

        SortItem sortItem = (SortItem) o;

        if (nullOrdering != sortItem.nullOrdering) {
            return false;
        }
        if (ordering != sortItem.ordering) {
            return false;
        }
        if (!sortKey.equals(sortItem.sortKey)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = sortKey.hashCode();
        result = 31 * result + (ordering != null ? ordering.hashCode() : 0);
        result = 31 * result + (nullOrdering != null ? nullOrdering.hashCode() : 0);
        return result;
    }

    public static Function<SortItem, Expression> sortKeyGetter()
    {
        return new Function<SortItem, Expression>()
        {
            @Override
            public Expression apply(SortItem input)
            {
                return input.getSortKey();
            }
        };
    }
}
