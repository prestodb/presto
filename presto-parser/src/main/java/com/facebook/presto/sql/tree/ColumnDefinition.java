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

import com.facebook.presto.common.SourceLocation;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class ColumnDefinition
        extends TableElement
{
    private final Identifier name;
    private final String type;
    private final boolean nullable;
    private final List<Property> properties;
    private final Optional<String> comment;

    public ColumnDefinition(Identifier name, String type, boolean nullable, List<Property> properties, Optional<String> comment)
    {
        this(Optional.empty(), name, type, nullable, properties, comment);
    }

    public ColumnDefinition(SourceLocation location, Identifier name, String type, boolean nullable, List<Property> properties, Optional<String> comment)
    {
        this(Optional.of(location), name, type, nullable, properties, comment);
    }
    private ColumnDefinition(Optional<SourceLocation> location, Identifier name, String type, boolean nullable, List<Property> properties, Optional<String> comment)
    {
        super(location);
        this.name = requireNonNull(name, "name is null");
        this.type = requireNonNull(type, "type is null");
        this.nullable = nullable;
        this.properties = requireNonNull(properties, "properties is null");
        this.comment = requireNonNull(comment, "comment is null");
    }

    public Identifier getName()
    {
        return name;
    }

    public String getType()
    {
        return type;
    }

    public boolean isNullable()
    {
        return nullable;
    }

    public List<Property> getProperties()
    {
        return properties;
    }

    public Optional<String> getComment()
    {
        return comment;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitColumnDefinition(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of();
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
        ColumnDefinition o = (ColumnDefinition) obj;
        return Objects.equals(this.name, o.name) &&
                Objects.equals(this.type, o.type) &&
                this.nullable == o.nullable &&
                Objects.equals(properties, o.properties) &&
                Objects.equals(this.comment, o.comment);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, type, properties, comment, nullable);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("type", type)
                .add("nullable", nullable)
                .add("properties", properties)
                .add("comment", comment)
                .toString();
    }
}
