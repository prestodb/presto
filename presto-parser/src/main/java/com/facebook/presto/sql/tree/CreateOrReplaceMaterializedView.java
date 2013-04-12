package com.facebook.presto.sql.tree;

import com.google.common.base.Objects;

import static com.google.common.base.Preconditions.checkNotNull;

public class CreateOrReplaceMaterializedView
        extends Statement
{
    private final QualifiedName name;
    private final Query tableDefinition;

    public CreateOrReplaceMaterializedView(QualifiedName name, Query tableDefinition)
    {
        this.name = checkNotNull(name, "name is null");
        this.tableDefinition = checkNotNull(tableDefinition, "tableDefinition is null");
    }

    public QualifiedName getName()
    {
        return name;
    }

    public Query getTableDefinition()
    {
        return tableDefinition;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitCreateOrReplaceMaterializedView(this, context);
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
        CreateOrReplaceMaterializedView o = (CreateOrReplaceMaterializedView) obj;
        return Objects.equal(name, o.name)
            && Objects.equal(tableDefinition, o.tableDefinition);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
            .add("name", name)
            .add("tableDefinition", tableDefinition)
            .toString();
    }
}
