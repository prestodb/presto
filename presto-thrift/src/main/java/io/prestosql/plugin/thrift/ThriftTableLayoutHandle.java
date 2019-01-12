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
package io.prestosql.plugin.thrift;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorTableLayoutHandle;
import io.prestosql.spi.predicate.TupleDomain;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class ThriftTableLayoutHandle
        implements ConnectorTableLayoutHandle
{
    private final String schemaName;
    private final String tableName;
    private final Optional<Set<ColumnHandle>> columns;
    private final TupleDomain<ColumnHandle> constraint;

    @JsonCreator
    public ThriftTableLayoutHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("columns") Optional<Set<ColumnHandle>> columns,
            @JsonProperty("constraint") TupleDomain<ColumnHandle> constraint)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.columns = requireNonNull(columns, "columns is null").map(ImmutableSet::copyOf);
        this.constraint = requireNonNull(constraint, "constraint is null");
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public Optional<Set<ColumnHandle>> getColumns()
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
        ThriftTableLayoutHandle other = (ThriftTableLayoutHandle) o;
        return schemaName.equals(other.schemaName)
                && tableName.equals(other.tableName)
                && columns.equals(other.columns)
                && constraint.equals(other.constraint);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaName, tableName, columns, constraint);
    }

    @Override
    public String toString()
    {
        return schemaName + ":" + tableName;
    }
}
