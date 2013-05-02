package com.facebook.presto.sql.tree;

public abstract class QueryBody
    extends Relation
{
    @Override
    public abstract int hashCode();

    @Override
    public abstract boolean equals(Object obj);

    @Override
    public abstract String toString();

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitQueryBody(this, context);
    }
}
