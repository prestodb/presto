package com.facebook.presto.sql.tree;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class Explain
        extends Statement
{
    private final Query query;
    private final List<ExplainOption> options;

    public Explain(Query query, List<ExplainOption> options)
    {
        this.query = checkNotNull(query, "query is null");
        if (options == null) {
            this.options = ImmutableList.of();
        }
        else {
            this.options = ImmutableList.copyOf(options);
        }
    }

    public Query getQuery()
    {
        return query;
    }

    public List<ExplainOption> getOptions()
    {
        return options;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitExplain(this, context);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(query, options);
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
        return Objects.equal(query, o.query) &&
                Objects.equal(options, o.options);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("query", query)
                .add("options", options)
                .toString();
    }
}
