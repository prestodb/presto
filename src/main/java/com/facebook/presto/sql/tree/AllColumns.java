package com.facebook.presto.sql.tree;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

public class AllColumns
        extends Expression
{
    private final Optional<QualifiedName> prefix;

    public AllColumns()
    {
        prefix = Optional.absent();
    }

    public AllColumns(QualifiedName prefix)
    {
        Preconditions.checkNotNull(prefix, "prefix is null");
        this.prefix = Optional.of(prefix);
    }

    public Optional<QualifiedName> getPrefix()
    {
        return prefix;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitAllColumns(this, context);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("name", prefix)
                .toString();
    }
}
