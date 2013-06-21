package com.facebook.presto.sql.tree;

import com.google.common.base.Objects;

import static com.google.common.base.Preconditions.checkNotNull;

public class Explain
        extends Statement
{
    private final Query query;

    public Explain(Query query)
    {
        this.query = checkNotNull(query, "query is null");
    }

    public Query getQuery()
    {
        return query;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitExplain(this, context);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(query);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        Explain o = (Explain) obj;
        return Objects.equal(query, o.query);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("query", query)
                .toString();
    }
}
