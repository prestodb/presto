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
    private final QualifiedName tableName;
    private final List<Property> properties;
    private final boolean tableExists;

    public SetProperties(Type type, QualifiedName name, List<Property> properties, boolean tableExists)
    {
        this(Optional.empty(), type, name, properties, tableExists);
    }

    public SetProperties(NodeLocation location, Type type, QualifiedName name, List<Property> properties, boolean tableExists)
    {
        this(Optional.of(location), type, name, properties, tableExists);
    }

    private SetProperties(Optional<NodeLocation> location, Type type, QualifiedName name, List<Property> properties, boolean tableExists)
    {
        super(location);
        this.type = requireNonNull(type, "type is null");
        this.tableName = requireNonNull(name, "name is null");
        this.properties = ImmutableList.copyOf(requireNonNull(properties, "properties is null"));
        this.tableExists = tableExists;
    }

    public Type getType()
    {
        return type;
    }

    public boolean isTableExists()
    {
        return tableExists;
    }

    public QualifiedName getTableName()
    {
        return tableName;
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
        return Objects.hash(type, tableName, properties, tableExists);
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
                Objects.equals(tableName, o.tableName) &&
                Objects.equals(properties, o.properties) &&
                Objects.equals(tableExists, o.tableExists);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("type", type)
                .add("name", tableName)
                .add("properties", properties)
                .add("tableExists", tableExists)
                .toString();
    }
}
