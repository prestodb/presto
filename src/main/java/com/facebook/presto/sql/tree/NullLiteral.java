package com.facebook.presto.sql.tree;

import com.google.common.base.Objects;

public class NullLiteral
        extends Literal
{
    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitNullLiteral(this, context);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this).toString();
    }
}
