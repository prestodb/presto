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

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class Except
        extends SetOperation
{
    private final Relation left;
    private final Relation right;

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
        super(location, distinct);
        requireNonNull(left, "left is null");
        requireNonNull(right, "right is null");

        this.left = left;
        this.right = right;
    }

    public Relation getLeft()
    {
        return left;
    }

    public Relation getRight()
    {
        return right;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitExcept(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of(left, right);
    }

    @Override
    public List<Relation> getRelations()
    {
        return ImmutableList.of(left, right);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("left", left)
                .add("right", right)
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
        Except o = (Except) obj;
        return Objects.equals(left, o.left) &&
                Objects.equals(right, o.right) &&
                Objects.equals(isDistinct(), o.isDistinct());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(left, right, isDistinct());
    }
}
