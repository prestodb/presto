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

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class TableFunctionArgument
        extends Node
{
    private final Optional<Identifier> name;
    private final Node value;

    public TableFunctionArgument(NodeLocation location, Optional<Identifier> name, Node value)
    {
        super(Optional.of(location));
        this.name = requireNonNull(name, "name is null");
        requireNonNull(value, "value is null");
        checkArgument(value instanceof TableFunctionTableArgument || value instanceof TableFunctionDescriptorArgument || value instanceof Expression);
        this.value = value;
    }

    public Optional<Identifier> getName()
    {
        return name;
    }

    public Node getValue()
    {
        return value;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitTableFunctionArgument(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.of(value);
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

        TableFunctionArgument other = (TableFunctionArgument) o;
        return Objects.equals(name, other.name) &&
                Objects.equals(value, other.value);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, value);
    }

    @Override
    public String toString()
    {
        return name.map(identifier -> identifier + " => ").orElse("") + value;
    }

    @Override
    public boolean shallowEquals(Node o)
    {
        if (!sameClass(this, o)) {
            return false;
        }

        return Objects.equals(name, ((TableFunctionArgument) o).name);
    }
}
