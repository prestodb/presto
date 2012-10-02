package com.facebook.presto.sql.tree;

import com.google.common.base.Objects;

public class SubqueryExpression
        extends Expression
{
    private final Query query;

    public SubqueryExpression(Query query)
    {
        this.query = query;
    }

    public Query getQuery()
    {
        return query;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitSubqueryExpression(this, context);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .addValue(query)
                .toString();
    }
}
