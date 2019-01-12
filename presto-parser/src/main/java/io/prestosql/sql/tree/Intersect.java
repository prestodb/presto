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

public class Intersect
        extends SetOperation
{
    private final List<Relation> relations;

    public Intersect(List<Relation> relations, boolean distinct)
    {
        this(Optional.empty(), relations, distinct);
    }

    public Intersect(NodeLocation location, List<Relation> relations, boolean distinct)
    {
        this(Optional.of(location), relations, distinct);
    }

    private Intersect(Optional<NodeLocation> location, List<Relation> relations, boolean distinct)
    {
        super(location, distinct);
        requireNonNull(relations, "relations is null");

        this.relations = ImmutableList.copyOf(relations);
    }

    public List<Relation> getRelations()
    {
        return relations;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitIntersect(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return relations;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("relations", relations)
                .add("distinct", isDistinct())
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
        return Objects.equals(relations, o.relations) &&
                Objects.equals(isDistinct(), o.isDistinct());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(relations, isDistinct());
    }
}
