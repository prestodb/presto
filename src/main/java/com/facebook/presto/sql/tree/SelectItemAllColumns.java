package com.facebook.presto.sql.tree;

import com.google.common.base.Objects;

public class SelectItemAllColumns
        extends SelectItem
{
    private final QualifiedName name;

    public SelectItemAllColumns(QualifiedName name)
    {
        this.name = name;
    }

    @Override
    public boolean isAllColumns()
    {
        return true;
    }

    @Override
    public QualifiedName getAllColumnsName()
    {
        return name;
    }

    @Override
    public Expression getExpression()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getAlias()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("name", name)
                .toString();
    }
}
