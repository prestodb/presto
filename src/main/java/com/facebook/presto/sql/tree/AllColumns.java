package com.facebook.presto.sql.tree;

import com.google.common.base.Objects;

public class AllColumns
        extends Expression
{
    private final QualifiedName prefix;

    public AllColumns()
    {
        this(null);
    }

    public AllColumns(QualifiedName prefix)
    {
        this.prefix = prefix;
    }

    public QualifiedName getPrefix()
    {
        return prefix;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("name", prefix)
                .toString();
    }
}
