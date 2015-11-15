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

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class Join
        extends Relation
{
    public Join(Type type, Relation left, Relation right, Optional<JoinCriteria> criteria)
    {
        this(Optional.empty(), type, left, right, criteria);
    }

    public Join(NodeLocation location, Type type, Relation left, Relation right, Optional<JoinCriteria> criteria)
    {
        this(Optional.of(location), type, left, right, criteria);
    }

    private Join(Optional<NodeLocation> location, Type type, Relation left, Relation right, Optional<JoinCriteria> criteria)
    {
        super(location);
        requireNonNull(left, "left is null");
        requireNonNull(right, "right is null");
        if ((type == Type.CROSS) || (type == Type.IMPLICIT)) {
            checkArgument(!criteria.isPresent(), "%s join cannot have join criteria", type);
        }
        else {
            checkArgument(criteria.isPresent(), "No join criteria specified");
        }

        this.type = type;
        this.left = left;
        this.right = right;
        this.criteria = criteria;
    }

    public enum Type
    {
        CROSS, INNER, LEFT, RIGHT, FULL, IMPLICIT
    }

    private final Type type;
    private final Relation left;
    private final Relation right;
    private final Optional<JoinCriteria> criteria;

    public Type getType()
    {
        return type;
    }

    public Relation getLeft()
    {
        return left;
    }

    public Relation getRight()
    {
        return right;
    }

    public Optional<JoinCriteria> getCriteria()
    {
        return criteria;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitJoin(this, context);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("type", type)
                .add("left", left)
                .add("right", right)
                .add("criteria", criteria)
                .omitNullValues()
                .toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if ((o == null) || (getClass() != o.getClass())) {
            return false;
        }
        Join join = (Join) o;
        return (type == join.type) &&
                Objects.equals(left, join.left) &&
                Objects.equals(right, join.right) &&
                Objects.equals(criteria, join.criteria);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(type, left, right, criteria);
    }
}
