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

public class TestTVFConnectorTableHandle
        implements ConnectorTableHandle
{
    // These are example fields for a Connector's Table Handle.
    // For other examples, see TpchTableHandle or any other implementations
    // of ConnectorTableHandle.
    private final SchemaTableName tableName;
    private final Optional<List<ColumnHandle>> columns;
    private final TupleDomain<ColumnHandle> constraint;

    public TestTVFConnectorTableHandle(SchemaTableName tableName)
    {
        this(tableName, Optional.empty(), TupleDomain.all());
    }

    @JsonCreator
    public TestTVFConnectorTableHandle(
            @JsonProperty SchemaTableName tableName,
            @JsonProperty("columns") Optional<List<ColumnHandle>> columns,
            @JsonProperty("constraint") TupleDomain<ColumnHandle> constraint)
    {
        this.tableName = requireNonNull(tableName, "tableName is null");
        requireNonNull(columns, "columns is null");
        this.columns = columns.map(ImmutableList::copyOf);
        this.constraint = requireNonNull(constraint, "constraint is null");
    }

    @JsonProperty
    public SchemaTableName getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public Optional<List<ColumnHandle>> getColumns()
    {
        return columns;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getConstraint()
    {
        return constraint;
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
        TestTVFConnectorTableHandle other = (TestTVFConnectorTableHandle) o;
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
