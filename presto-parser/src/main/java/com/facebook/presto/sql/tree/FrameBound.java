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

public class FrameBound
        extends Node
{
    public enum Type
    {
        UNBOUNDED_PRECEDING,
        PRECEDING,
        CURRENT_ROW,
        FOLLOWING,
        UNBOUNDED_FOLLOWING
    }

    private final Type type;
    private final Optional<Expression> value;
    private final Optional<Expression> originalValue;

    public FrameBound(Type type)
    {
        this(Optional.empty(), type);
    }

    public FrameBound(NodeLocation location, Type type)
    {
        this(Optional.of(location), type);
    }

    public FrameBound(Type type, Expression value, Expression originalValue)
    {
        this(Optional.empty(), type, value, originalValue);
    }

    private FrameBound(Optional<NodeLocation> location, Type type)
    {
        this(location, type, null, null);
    }

    public FrameBound(NodeLocation location, Type type, Expression value)
    {
        this(Optional.of(location), type, value, value);
    }

    private FrameBound(Optional<NodeLocation> location, Type type, Expression value, Expression originalValue)
    {
        super(location);
        this.type = requireNonNull(type, "type is null");
        this.value = Optional.ofNullable(value);
        this.originalValue = Optional.ofNullable(originalValue);
    }

    public Type getType()
    {
        return type;
    }

    public Optional<Expression> getValue()
    {
        return value;
    }

    public Optional<Expression> getOriginalValue()
    {
        return originalValue;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitFrameBound(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        ImmutableList.Builder<Node> nodes = ImmutableList.builder();
        value.ifPresent(nodes::add);
        return nodes.build();
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
        FrameBound o = (FrameBound) obj;
        return Objects.equals(type, o.type) &&
                Objects.equals(value, o.value);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(type, value);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("type", type)
                .add("value", value)
                .toString();
    }
}
