package com.facebook.presto.sql.tree;

import com.facebook.presto.sql.ExpressionFormatter;

public abstract class Expression
    extends Node
{
    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitExpression(this, context);
    }

    public final String toString()
    {
        return ExpressionFormatter.toString(this);
    }
}
