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
package io.prestosql.sql.tree;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;

public class ShowStats
        extends Statement
{
    private final Relation relation;

    @VisibleForTesting
    public ShowStats(Relation relation)
    {
        this(Optional.empty(), relation);
    }

    public ShowStats(Optional<NodeLocation> location, Relation relation)
    {
        super(location);
        this.relation = relation;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitShowStats(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.of(relation);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(relation);
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
        ShowStats o = (ShowStats) obj;
        return Objects.equals(relation, o.relation);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("query", relation)
                .toString();
    }

    public Relation getRelation()
    {
        return relation;
    }
}
