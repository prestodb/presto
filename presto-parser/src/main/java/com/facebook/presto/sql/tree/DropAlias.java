package com.facebook.presto.sql.tree;

import com.google.common.base.Objects;

import static com.google.common.base.Preconditions.checkNotNull;

public class DropAlias
        extends Statement
{
    private final QualifiedName remote;

    public DropAlias(QualifiedName remote)
    {
        this.remote = checkNotNull(remote, "remote is null");
    }

    public QualifiedName getRemote()
    {
        return remote;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitDropAlias(this, context);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(remote);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        else if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        DropAlias o = (DropAlias) obj;
        return Objects.equal(remote, o.remote);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("remote", remote)
                .toString();
    }
}
