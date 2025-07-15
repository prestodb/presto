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
package com.facebook.presto.connector.tvf;

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class MockConnectorTableHandle
        implements ConnectorTableHandle
{
    private final SchemaTableName tableName;
    private final TupleDomain<ColumnHandle> constraint;
    private final Optional<List<ColumnHandle>> columns;

    public MockConnectorTableHandle(SchemaTableName tableName)
    {
        this(tableName, TupleDomain.all(), Optional.empty());
    }

    @JsonCreator
    public MockConnectorTableHandle(
            @JsonProperty SchemaTableName tableName,
            @JsonProperty("constraint") TupleDomain<ColumnHandle> constraint,
            @JsonProperty("columns") Optional<List<ColumnHandle>> columns)
    {
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.constraint = requireNonNull(constraint, "constraint is null");
        requireNonNull(columns, "columns is null");
        this.columns = columns.map(ImmutableList::copyOf);
    }

    @JsonProperty
    public SchemaTableName getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getConstraint()
    {
        return constraint;
    }

    @JsonProperty
    public Optional<List<ColumnHandle>> getColumns()
    {
        return columns;
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
        MockConnectorTableHandle other = (MockConnectorTableHandle) o;
        return Objects.equals(tableName, other.tableName) &&
                Objects.equals(constraint, other.constraint) &&
                Objects.equals(columns, other.columns);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(tableName, constraint, columns);
    }

    @Override
    public String toString()
    {
        return tableName.toString();
    }
}
