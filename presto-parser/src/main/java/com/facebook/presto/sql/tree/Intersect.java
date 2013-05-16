package com.facebook.presto.sql.tree;

import com.google.common.base.Objects;

import java.util.List;

public class Intersect
    extends SetOperation
{
    private final List<Relation> relations;
    private final boolean distinct;

    public Intersect(List<Relation> relations, boolean distinct)
    {
        this.relations = relations;
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
        throw new UnsupportedOperationException();
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
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Intersect intersect = (Intersect) o;

        if (!relations.equals(intersect.relations)) {
            return false;
        }
        if (distinct != intersect.distinct) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(relations, distinct);
    }
}
