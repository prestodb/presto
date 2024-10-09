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

public class SetProperties
        extends Statement
{
    public enum Type
    {
        TABLE
    }

    private final Type type;
    private final QualifiedName name;
    private final List<Property> properties;

    public SetProperties(Type type, QualifiedName name, List<Property> properties)
    {
        this(Optional.empty(), type, name, properties);
    }

    public SetProperties(NodeLocation location, Type type, QualifiedName name, List<Property> properties)
    {
        this(Optional.of(location), type, name, properties);
    }

    private SetProperties(Optional<NodeLocation> location, Type type, QualifiedName name, List<Property> properties)
    {
        super(location);
        this.type = requireNonNull(type, "type is null");
        this.name = requireNonNull(name, "name is null");
        this.properties = ImmutableList.copyOf(requireNonNull(properties, "properties is null"));
    }

    public Type getType()
    {
        return type;
    }

    public QualifiedName getName()
    {
        return name;
    }

    public List<Property> getProperties()
    {
        return properties;
    }

    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitSetProperties(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.of();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(type, name, properties);
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
        SetProperties o = (SetProperties) obj;
        return type == o.type &&
                Objects.equals(name, o.name) &&
                Objects.equals(properties, o.properties);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("type", type)
                .add("name", name)
                .add("properties", properties)
                .toString();
    }
}
