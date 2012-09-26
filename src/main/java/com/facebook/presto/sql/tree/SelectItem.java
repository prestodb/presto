package com.facebook.presto.sql.tree;

public abstract class SelectItem
{
    public abstract boolean isAllColumns();

    public abstract QualifiedName getAllColumnsName();

    public abstract Expression getExpression();

    public abstract String getAlias();

    @Override
    public abstract String toString();
}
