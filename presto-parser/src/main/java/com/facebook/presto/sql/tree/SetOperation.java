package com.facebook.presto.sql.tree;

public abstract class SetOperation
        extends QueryBody
{
    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitSetOperation(this, context);
    }
}
