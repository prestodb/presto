package com.facebook.presto.sql.tree;

import com.google.common.base.Objects;

public class DropTable
        extends Statement
{
    private final QualifiedName tableName;

    public DropTable(QualifiedName tableName)
    {
        this.tableName = tableName;
    }

    public QualifiedName getTableName()
    {
        return tableName;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitStatement(this, context);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(tableName);
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
        DropTable o = (DropTable) obj;
        return Objects.equal(tableName, o.tableName);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
            .add("tableName", tableName)
            .toString();
    }
}
