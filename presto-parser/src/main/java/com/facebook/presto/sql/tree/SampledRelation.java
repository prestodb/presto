/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

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
        this(Optional.empty(), relation, type, samplePercentage);
    }

    public SampledRelation(NodeLocation location, Relation relation, Type type, Expression samplePercentage)
    {
        this(Optional.of(location), relation, type, samplePercentage);
    }

    private SampledRelation(Optional<NodeLocation> location, Relation relation, Type type, Expression samplePercentage)
    {
        super(location);
        this.relation = requireNonNull(relation, "relation is null");
        this.type = requireNonNull(type, "type is null");
        this.samplePercentage = requireNonNull(samplePercentage, "samplePercentage is null");
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
    public List<Node> getChildren()
    {
        return ImmutableList.of(relation, samplePercentage);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
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
        return Objects.equals(relation, that.relation) &&
                Objects.equals(type, that.type) &&
                Objects.equals(samplePercentage, that.samplePercentage);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(relation, type, samplePercentage);
    }
}
