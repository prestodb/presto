package com.facebook.presto.sql.tree;

public abstract class Statement
    extends Node
{
    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitStatement(this, context);
    }
}
