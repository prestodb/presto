package com.facebook.presto.sql.tree;

import com.google.common.base.Objects;
import com.google.common.base.Optional;

import static com.google.common.base.Preconditions.checkNotNull;

public class CreateMaterializedView
        extends Statement
{
    private final QualifiedName name;
    private final Query tableDefinition;
    private final Optional<String> refresh;

    public CreateMaterializedView(QualifiedName name, Optional<String> refresh, Query tableDefinition)
    {
        this.name = checkNotNull(name, "name is null");
        this.refresh = checkNotNull(refresh, "refresh is null");
        this.tableDefinition = checkNotNull(tableDefinition, "tableDefinition is null");
    }

    public QualifiedName getName()
    {
        return name;
    }

    public Optional<String> getRefresh()
    {
        return refresh;
    }

    public Query getTableDefinition()
    {
        return tableDefinition;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitCreateMaterializedView(this, context);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(name, tableDefinition);
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
        CreateMaterializedView o = (CreateMaterializedView) obj;
        return Objects.equal(name, o.name)
                && Objects.equal(refresh, o.refresh)
                && Objects.equal(tableDefinition, o.tableDefinition);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("name", name)
                .add("refresh", refresh)
                .add("tableDefinition", tableDefinition)
                .toString();
    }
}
