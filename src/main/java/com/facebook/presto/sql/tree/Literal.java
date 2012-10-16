package com.facebook.presto.sql.tree;

public abstract class Literal
    extends Expression
{
    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitLiteral(this, context);
    }
}
