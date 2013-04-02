package com.facebook.presto.sql.tree;

import com.google.common.base.Objects;

public class ShowTables
        extends Statement
{
    private final QualifiedName schema;
    private final String likePattern;

    public ShowTables(QualifiedName schema, String likePattern)
    {
        this.schema = schema;
        this.likePattern = likePattern;
    }

    public QualifiedName getSchema()
    {
        return schema;
    }

    public String getLikePattern()
    {
        return likePattern;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitShowTables(this, context);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(schema, likePattern);
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
        return Objects.equal(schema, o.schema) &&
                Objects.equal(likePattern, o.likePattern);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("schema", schema)
                .add("likePattern", likePattern)
                .toString();
    }
}
