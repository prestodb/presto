package com.facebook.presto.sql.tree;

import com.google.common.base.Objects;

public class SortItem
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
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("sortKey", sortKey)
                .add("ordering", ordering)
                .add("nullOrdering", nullOrdering)
                .toString();
    }
}
