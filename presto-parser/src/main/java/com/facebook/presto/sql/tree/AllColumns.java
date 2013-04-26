package com.facebook.presto.sql.tree;

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
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AllColumns that = (AllColumns) o;

        if (prefix != null ? !prefix.equals(that.prefix) : that.prefix != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return prefix != null ? prefix.hashCode() : 0;
    }
}
