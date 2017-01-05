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
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class CreateTable
        extends Statement
{
    private final QualifiedName name;
    private final List<TableElement> elements;
    private final boolean notExists;
    private final Map<String, Expression> properties;

    public CreateTable(QualifiedName name, List<TableElement> elements, boolean notExists, Map<String, Expression> properties)
    {
        this(Optional.empty(), name, elements, notExists, properties);
    }

    public CreateTable(NodeLocation location, QualifiedName name, List<TableElement> elements, boolean notExists, Map<String, Expression> properties)
    {
        this(Optional.of(location), name, elements, notExists, properties);
    }

    private CreateTable(Optional<NodeLocation> location, QualifiedName name, List<TableElement> elements, boolean notExists, Map<String, Expression> properties)
    {
        super(location);
        this.name = requireNonNull(name, "table is null");
        this.elements = ImmutableList.copyOf(requireNonNull(elements, "elements is null"));
        this.notExists = notExists;
        this.properties = ImmutableMap.copyOf(requireNonNull(properties, "properties is null"));
    }

    public QualifiedName getName()
    {
        return name;
    }

    public List<TableElement> getElements()
    {
        return elements;
    }

    public boolean isNotExists()
    {
        return notExists;
    }

    public Map<String, Expression> getProperties()
    {
        return properties;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitCreateTable(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.<Node>builder()
                .addAll(elements)
                .addAll(properties.values())
                .build();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, elements, notExists, properties);
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
        CreateTable o = (CreateTable) obj;
        return Objects.equals(name, o.name) &&
                Objects.equals(elements, o.elements) &&
                Objects.equals(notExists, o.notExists) &&
                Objects.equals(properties, o.properties);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("elements", elements)
                .add("notExists", notExists)
                .add("properties", properties)
                .toString();
    }
}
