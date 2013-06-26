package com.facebook.presto.sql.tree;

import com.google.common.base.Objects;

import static com.google.common.base.Preconditions.checkNotNull;

public class RefreshMaterializedView
        extends Statement
{
    private final QualifiedName name;

    public RefreshMaterializedView(QualifiedName name)
    {
        this.name = checkNotNull(name, "name is null");
    }

    public QualifiedName getName()
    {
        return name;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitRefreshMaterializedView(this, context);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(name);
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
        RefreshMaterializedView o = (RefreshMaterializedView) obj;
        return Objects.equal(name, o.name);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("name", name)
                .toString();
    }
}
