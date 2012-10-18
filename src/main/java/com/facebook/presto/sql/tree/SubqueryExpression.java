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

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SubqueryExpression that = (SubqueryExpression) o;

        if (!query.equals(that.query)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return query.hashCode();
    }
}
