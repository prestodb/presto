package com.facebook.presto.sql.tree;

import com.google.common.base.Objects;
import com.google.common.collect.Iterables;

public class QualifiedNameReference
        extends Expression
{
    private final QualifiedName name;

    public QualifiedNameReference(QualifiedName name)
    {
        this.name = name;
    }

    public QualifiedName getName()
    {
        return name;
    }

    public QualifiedName getSuffix()
    {
        return QualifiedName.of(Iterables.getLast(name.getParts()));
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitQualifiedNameReference(this, context);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .addValue(name)
                .toString();
    }
}
