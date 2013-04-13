package com.facebook.presto.sql.tree;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class With
        extends Node
{
    private final boolean recursive;
    private final List<WithQuery> queries;

    public With(boolean recursive, List<WithQuery> queries)
    {
        checkNotNull(queries, "queries is null");
        checkArgument(!queries.isEmpty(), "queries is empty");

        this.recursive = recursive;
        this.queries = ImmutableList.copyOf(queries);
    }

    public boolean isRecursive()
    {
        return recursive;
    }

    public List<WithQuery> getQueries()
    {
        return queries;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitWith(this, context);
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
        With o = (With) obj;
        return Objects.equal(recursive, o.recursive) &&
                Objects.equal(queries, o.queries);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(recursive, queries);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("recursive", recursive)
                .add("queries", queries)
                .toString();
    }
}
