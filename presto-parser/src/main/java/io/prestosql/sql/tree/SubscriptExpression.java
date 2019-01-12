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

import static java.util.Objects.requireNonNull;

public class SubscriptExpression
        extends Expression
{
    private final Expression base;
    private final Expression index;

    public SubscriptExpression(Expression base, Expression index)
    {
        this(Optional.empty(), base, index);
    }

    public SubscriptExpression(NodeLocation location, Expression base, Expression index)
    {
        this(Optional.of(location), base, index);
    }

    private SubscriptExpression(Optional<NodeLocation> location, Expression base, Expression index)
    {
        super(location);
        this.base = requireNonNull(base, "base is null");
        this.index = requireNonNull(index, "index is null");
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitSubscriptExpression(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of(base, index);
    }

    public Expression getBase()
    {
        return base;
    }

    public Expression getIndex()
    {
        return index;
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

        SubscriptExpression that = (SubscriptExpression) o;

        return Objects.equals(this.base, that.base) && Objects.equals(this.index, that.index);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(base, index);
    }
}
