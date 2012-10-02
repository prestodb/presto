package com.facebook.presto.sql.tree;

import com.google.common.base.Objects;

public class Table
        extends Relation
{
    private final QualifiedName name;

    public Table(QualifiedName name)
    {
        this.name = name;
    }

    public QualifiedName getName()
    {
        return name;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitTable(this, context);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .addValue(name)
                .toString();
    }
}
