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

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class DescriptorField
        extends Node
{
    private final Identifier name;
    private final Optional<String> type;

    public DescriptorField(NodeLocation location, Identifier name, Optional<String> type)
    {
        super(Optional.of(location));
        this.name = requireNonNull(name, "name is null");
        this.type = requireNonNull(type, "type is null");
    }

    public Identifier getName()
    {
        return name;
    }

    public Optional<String> getType()
    {
        return type;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitDescriptorField(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return Collections.emptyList();
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
        DescriptorField field = (DescriptorField) o;
        return Objects.equals(name, field.name) &&
                Objects.equals(type, (field.type));
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, type);
    }

    @Override
    public String toString()
    {
        return type.map(dataType -> name + " " + dataType).orElse(name.toString());
    }

    @Override
    public boolean shallowEquals(Node o)
    {
        if (!sameClass(this, o)) {
            return false;
        }

        return Objects.equals(name, ((DescriptorField) o).name);
    }
}
