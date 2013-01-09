package com.facebook.presto.sql.tree;

import com.google.common.base.Objects;

public class ShowTables
        extends Statement
{
    private final QualifiedName schema;

    public ShowTables(QualifiedName schema)
    {
        this.schema = schema;
    }

    public QualifiedName getSchema()
    {
        return schema;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitShowTables(this, context);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(schema);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        ShowTables o = (ShowTables) obj;
        return Objects.equal(schema, o.schema);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("schema", schema)
                .toString();
    }
}
