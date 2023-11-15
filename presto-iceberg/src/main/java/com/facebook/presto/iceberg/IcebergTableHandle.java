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
package com.facebook.presto.iceberg;

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class IcebergTableHandle
        implements ConnectorTableHandle
{
    private final String schemaName;
    private final IcebergTableName tableName;
    private final TupleDomain<IcebergColumnHandle> predicate;
    private final boolean snapshotSpecified;
    private final Optional<String> tableSchemaJson;

    @JsonCreator
    public IcebergTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") IcebergTableName tableName,
            @JsonProperty("snapshotSpecified") boolean snapshotSpecified,
            @JsonProperty("predicate") TupleDomain<IcebergColumnHandle> predicate,
            @JsonProperty("tableSchemaJson") Optional<String> tableSchemaJson)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.snapshotSpecified = snapshotSpecified;
        this.predicate = requireNonNull(predicate, "predicate is null");
        this.tableSchemaJson = requireNonNull(tableSchemaJson, "tableSchemaJson is null");
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public IcebergTableName getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public boolean isSnapshotSpecified()
    {
        return snapshotSpecified;
    }

    @JsonProperty
    public TupleDomain<IcebergColumnHandle> getPredicate()
    {
        return predicate;
    }

    @JsonProperty
    public Optional<String> getTableSchemaJson()
    {
        return tableSchemaJson;
    }

    public SchemaTableName getSchemaTableName()
    {
        return new SchemaTableName(schemaName, tableName.getTableName());
    }

    public SchemaTableName getSchemaTableNameWithType()
    {
        return new SchemaTableName(schemaName, tableName.getTableNameWithType());
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

        IcebergTableHandle that = (IcebergTableHandle) o;
        return Objects.equals(schemaName, that.schemaName) &&
                Objects.equals(tableName, that.tableName) &&
                snapshotSpecified == that.snapshotSpecified &&
                Objects.equals(predicate, that.predicate) &&
                Objects.equals(tableSchemaJson, that.tableSchemaJson);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaName, tableName, predicate, snapshotSpecified, tableSchemaJson);
    }

    @Override
    public String toString()
    {
        return tableName.toString();
    }
}
