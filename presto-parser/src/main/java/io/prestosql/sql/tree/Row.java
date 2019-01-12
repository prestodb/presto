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

public final class Row
        extends Expression
{
    private final List<Expression> items;

    public Row(List<Expression> items)
    {
        this(Optional.empty(), items);
    }

    public Row(NodeLocation location, List<Expression> items)
    {
        this(Optional.of(location), items);
    }

    private Row(Optional<NodeLocation> location, List<Expression> items)
    {
        super(location);
        requireNonNull(items, "items is null");
        this.items = ImmutableList.copyOf(items);
    }

    public List<Expression> getItems()
    {
        return items;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitRow(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return items;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(items);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        Row other = (Row) obj;
        return Objects.equals(this.items, other.items);
    }
}
