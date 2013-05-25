package com.facebook.presto.sql.tree;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

public class Intersect
    extends SetOperation
{
    private final List<Relation> relations;
    private final boolean distinct;

    public Intersect(List<Relation> relations, boolean distinct)
    {
        Preconditions.checkNotNull(relations, "relations is null");

        this.relations = ImmutableList.copyOf(relations);
        this.distinct = distinct;
    }

    public List<Relation> getRelations()
    {
        return relations;
    }

    public boolean isDistinct()
    {
        return distinct;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitIntersect(this, context);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("relations", relations)
                .add("distinct", distinct)
                .toString();
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
        Intersect o = (Intersect) obj;
        return Objects.equal(relations, o.relations) &&
                Objects.equal(distinct, o.distinct);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(relations, distinct);
    }
}
