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

public final class CallArgument
        extends Node
{
    private final Optional<String> name;
    private final Expression value;

    public CallArgument(Expression value)
    {
        this(Optional.empty(), Optional.empty(), value);
    }

    public CallArgument(NodeLocation location, Expression value)
    {
        this(Optional.of(location), Optional.empty(), value);
    }

    public CallArgument(String name, Expression value)
    {
        this(Optional.empty(), Optional.of(name), value);
    }

    public CallArgument(NodeLocation location, String name, Expression value)
    {
        this(Optional.of(location), Optional.of(name), value);
    }

    public CallArgument(Optional<NodeLocation> location, Optional<String> name, Expression value)
    {
        super(location);
        this.name = requireNonNull(name, "name is null");
        this.value = requireNonNull(value, "value is null");
    }

    public Optional<String> getName()
    {
        return name;
    }

    public Expression getValue()
    {
        return value;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitCallArgument(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of(value);
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
        CallArgument o = (CallArgument) obj;
        return Objects.equals(name, o.name) &&
                Objects.equals(value, o.value);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, value);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name.orElse(null))
                .add("value", value)
                .omitNullValues()
                .toString();
    }
}
