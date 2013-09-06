package com.facebook.presto.sql.tree;

import com.google.common.base.Objects;
import static com.google.common.base.Preconditions.checkNotNull;

public class SampledRelation
        extends Relation
{
    public enum Type
    {
        BERNOULLI,
        SYSTEM
    }

    private final Relation relation;
    private final Type type;
    private final Expression samplePercentage;

    public SampledRelation(Relation relation, Type type, Expression samplePercentage)
    {
        this.relation = checkNotNull(relation, "relation is null");
        this.type = checkNotNull(type, "type is null");
        this.samplePercentage = checkNotNull(samplePercentage, "samplePercentage is null");
    }

    public Relation getRelation()
    {
        return relation;
    }

    public Type getType()
    {
        return type;
    }

    public Expression getSamplePercentage()
    {
        return samplePercentage;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitSampledRelation(this, context);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("relation", relation)
                .add("type", type)
                .add("samplePercentage", samplePercentage)
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
        SampledRelation that = (SampledRelation) o;
        return Objects.equal(relation, that.relation) &&
                Objects.equal(type, that.type) &&
                Objects.equal(samplePercentage, that.samplePercentage);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(relation, type, samplePercentage);
    }
}
