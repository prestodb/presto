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

import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class CreateSchema
        extends Statement
{
    private final QualifiedName schemaName;
    private final boolean notExists;
    private final Map<String, Expression> properties;

    public CreateSchema(QualifiedName schemaName, boolean notExists, Map<String, Expression> properties)
    {
        this(Optional.empty(), schemaName, notExists, properties);
    }

    public CreateSchema(NodeLocation location, QualifiedName schemaName, boolean notExists, Map<String, Expression> properties)
    {
        this(Optional.of(location), schemaName, notExists, properties);
    }

    private CreateSchema(Optional<NodeLocation> location, QualifiedName schemaName, boolean notExists, Map<String, Expression> properties)
    {
        super(location);
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.notExists = notExists;
        this.properties = ImmutableMap.copyOf(requireNonNull(properties, "properties is null"));
    }

    public QualifiedName getSchemaName()
    {
        return schemaName;
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
        return visitor.visitCreateSchema(this, context);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaName, notExists, properties);
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
        CreateSchema o = (CreateSchema) obj;
        return Objects.equals(schemaName, o.schemaName) &&
                Objects.equals(notExists, o.notExists) &&
                Objects.equals(properties, o.properties);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("schemaName", schemaName)
                .add("notExists", notExists)
                .add("properties", properties)
                .toString();
    }
}
