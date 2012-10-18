package com.facebook.presto.sql.tree;

import com.google.common.base.Objects;

public class NaturalJoin
        extends JoinCriteria
{
    @Override
    public String toString()
    {
        return Objects.toStringHelper(this).toString();
    }
}
