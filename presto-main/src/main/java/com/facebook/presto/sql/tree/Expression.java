package com.facebook.presto.sql.tree;

public abstract class Expression
    extends Node
{
    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitExpression(this, context);
    }
}
