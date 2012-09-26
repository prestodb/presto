package com.facebook.presto.sql.tree;

import com.google.common.base.Objects;

public class NamedTable
        extends TablePrimary
{
    private final QualifiedName name;

    public NamedTable(QualifiedName name, TableCorrelation correlation)
    {
        super(correlation);
        this.name = name;
    }

    public QualifiedName getName()
    {
        return name;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("name", name)
                .add("correlation", getCorrelation())
                .omitNullValues()
                .toString();
    }
}
