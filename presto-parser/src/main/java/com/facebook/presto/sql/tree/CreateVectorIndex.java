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

public class CreateVectorIndex
        extends Statement
{
    private final Identifier indexName;
    private final QualifiedName tableName;
    private final List<Identifier> columns;
    private final Optional<Expression> updatingFor;
    private final List<Property> properties;

    public CreateVectorIndex(
            Identifier indexName,
            QualifiedName tableName,
            List<Identifier> columns,
            Optional<Expression> updatingFor,
            List<Property> properties)
    {
        this(Optional.empty(), indexName, tableName, columns, updatingFor, properties);
    }

    public CreateVectorIndex(
            NodeLocation location,
            Identifier indexName,
            QualifiedName tableName,
            List<Identifier> columns,
            Optional<Expression> updatingFor,
            List<Property> properties)
    {
        this(Optional.of(location), indexName, tableName, columns, updatingFor, properties);
    }

    private CreateVectorIndex(
            Optional<NodeLocation> location,
            Identifier indexName,
            QualifiedName tableName,
            List<Identifier> columns,
            Optional<Expression> updatingFor,
            List<Property> properties)
    {
        super(location);
        this.indexName = requireNonNull(indexName, "indexName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
        this.updatingFor = requireNonNull(updatingFor, "updatingFor is null");
        this.properties = ImmutableList.copyOf(requireNonNull(properties, "properties is null"));
    }

    public Identifier getIndexName()
    {
        return indexName;
    }

    public QualifiedName getTableName()
    {
        return tableName;
    }

    public List<Identifier> getColumns()
    {
        return columns;
    }

    public Optional<Expression> getUpdatingFor()
    {
        return updatingFor;
    }

    public List<Property> getProperties()
    {
        return properties;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitCreateVectorIndex(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        ImmutableList.Builder<Node> children = ImmutableList.builder();
        children.add(indexName);
        children.addAll(columns);
        updatingFor.ifPresent(children::add);
        children.addAll(properties);
        return children.build();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(indexName, tableName, columns, updatingFor, properties);
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
        CreateVectorIndex o = (CreateVectorIndex) obj;
        return Objects.equals(indexName, o.indexName) &&
                Objects.equals(tableName, o.tableName) &&
                Objects.equals(columns, o.columns) &&
                Objects.equals(updatingFor, o.updatingFor) &&
                Objects.equals(properties, o.properties);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("indexName", indexName)
                .add("tableName", tableName)
                .add("columns", columns)
                .add("updatingFor", updatingFor)
                .add("properties", properties)
                .toString();
    }
}
