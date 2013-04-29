package com.facebook.presto.sql.tree;

public abstract class JoinCriteria
{
    // Force subclasses to have a proper equals and hashcode implementation
    @Override
    public abstract boolean equals(Object obj);

    @Override
    public abstract int hashCode();

    @Override
    public abstract String toString();
}
