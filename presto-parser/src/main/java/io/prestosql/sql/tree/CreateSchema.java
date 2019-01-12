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

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class CreateSchema
        extends Statement
{
    private final QualifiedName schemaName;
    private final boolean notExists;
    private final List<Property> properties;

    public CreateSchema(QualifiedName schemaName, boolean notExists, List<Property> properties)
    {
        this(Optional.empty(), schemaName, notExists, properties);
    }

    public CreateSchema(NodeLocation location, QualifiedName schemaName, boolean notExists, List<Property> properties)
    {
        this(Optional.of(location), schemaName, notExists, properties);
    }

    private CreateSchema(Optional<NodeLocation> location, QualifiedName schemaName, boolean notExists, List<Property> properties)
    {
        super(location);
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.notExists = notExists;
        this.properties = ImmutableList.copyOf(requireNonNull(properties, "properties is null"));
    }

    public QualifiedName getSchemaName()
    {
        return schemaName;
    }

    public boolean isNotExists()
    {
        return notExists;
    }

    public List<Property> getProperties()
    {
        return properties;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitCreateSchema(this, context);
    }

    @Override
    public List<Property> getChildren()
    {
        return properties;
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
