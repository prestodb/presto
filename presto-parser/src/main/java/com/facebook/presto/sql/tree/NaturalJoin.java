package com.facebook.presto.sql.tree;

import com.google.common.base.Objects;

public class NaturalJoin
        extends JoinCriteria
{
    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        return (obj != null) && (getClass() == obj.getClass());
    }

    @Override
    public int hashCode()
    {
        return 0;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this).toString();
    }
}
