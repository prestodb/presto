package com.facebook.presto.sql.tree;

public abstract class Node
{
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitNode(this, context);
    }

}
