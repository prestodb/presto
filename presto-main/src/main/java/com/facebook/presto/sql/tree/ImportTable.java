package com.facebook.presto.sql.tree;

import com.google.common.base.Objects;

public class ImportTable
        extends Statement
{
    private final QualifiedName srcTable;
    private final QualifiedName dstTable;

    public ImportTable(QualifiedName srcTable, QualifiedName dstTable)
    {
        this.srcTable = srcTable;
        this.dstTable = dstTable;
    }

    public QualifiedName getSrcTable()
    {
        return srcTable;
    }

    public QualifiedName getDstTable()
    {
        return dstTable;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitStatement(this, context);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(srcTable, dstTable);
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
        ImportTable o = (ImportTable) obj;
        return Objects.equal(srcTable, o.srcTable)
                && Objects.equal(dstTable, o.dstTable);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("srcTable", srcTable)
                .add("dstTable", dstTable)
                .toString();
    }
}
