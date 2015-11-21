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
import static java.util.Objects.requireNonNull;

public class Except
        extends SetOperation
{
    private final Relation left;
    private final Relation right;
    private final boolean distinct;

    public Except(Relation left, Relation right, boolean distinct)
    {
        this(Optional.empty(), left, right, distinct);
    }

    public Except(NodeLocation location, Relation left, Relation right, boolean distinct)
    {
        this(Optional.of(location), left, right, distinct);
    }

    private Except(Optional<NodeLocation> location, Relation left, Relation right, boolean distinct)
    {
        super(location);
        requireNonNull(left, "left is null");
        requireNonNull(right, "right is null");

        this.left = left;
        this.right = right;
        this.distinct = distinct;
    }

    public Relation getLeft()
    {
        return left;
    }

    public Relation getRight()
    {
        return right;
    }

    public boolean isDistinct()
    {
        return distinct;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitExcept(this, context);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("left", left)
                .add("right", right)
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
        Except o = (Except) obj;
        return Objects.equals(left, o.left) &&
                Objects.equals(right, o.right) &&
                Objects.equals(distinct, o.distinct);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(left, right, distinct);
    }
}
