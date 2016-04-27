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

import static java.util.Objects.requireNonNull;

public class MapConstructor
    extends Expression
{
    public static final String MAP_CONSTRUCTOR = "MAP";
    private final List<Expression> keys;
    private final List<Expression> values;

    public MapConstructor(List<Expression> keys, List<Expression> values)
    {
        this(Optional.empty(), keys, values);
    }

    public MapConstructor(NodeLocation location, List<Expression> keys, List<Expression> values)
    {
        this(Optional.of(location), keys, values);
    }

    private MapConstructor(Optional<NodeLocation> location, List<Expression> keys, List<Expression> values)
    {
        super(location);
        requireNonNull(keys, "keys is null");
        requireNonNull(values, "values is null");
        this.keys = ImmutableList.copyOf(keys);
        this.values = ImmutableList.copyOf(values);
    }

    public List<Expression> getKeys()
    {
        return keys;
    }

    public List<Expression> getValues()
    {
        return values;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitMapConstructor(this, context);
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

        MapConstructor that = (MapConstructor) o;
        return Objects.equals(keys, that.keys) && Objects.equals(values, that.values);
    }

    @Override
    public int hashCode()
    {
        return keys.hashCode() + values.hashCode();
    }
}
