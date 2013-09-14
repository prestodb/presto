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

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

public class Union
        extends SetOperation
{
    private final List<Relation> relations;
    private final boolean distinct;

    public Union(List<Relation> relations, boolean distinct)
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
        return visitor.visitUnion(this, context);
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
        Union o = (Union) obj;
        return Objects.equal(relations, o.relations) &&
                Objects.equal(distinct, o.distinct);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(relations, distinct);
    }
}
