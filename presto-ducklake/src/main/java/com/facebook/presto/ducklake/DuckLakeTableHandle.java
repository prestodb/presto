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
package com.facebook.presto.ducklake;

import com.facebook.presto.ducklake.catalog.DuckLakeTable;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * Identifies a DuckLake table at a resolved snapshot and carries {@code schema}, the JSON
 * snapshot of its columns and partition fields built by {@code SchemaBuilder}, so that workers
 * never need to query the catalog database to know the table's shape. This plays the role {@code
 * tableSchemaJson} plays on {@code IcebergTableHandle}. {@code tablePath} is the resolved table
 * directory ({@code DuckLakeTable.getPath()}), carried here so the split manager does not need
 * another catalog round trip.
 */
public class DuckLakeTableHandle
        implements ConnectorTableHandle
{
    private final String schemaName;
    private final DuckLakeTableName tableName;
    private final long tableId;
    private final long schemaId;
    private final String tablePath;
    private final PrestoDuckLakeSchema schema;

    @JsonCreator
    public DuckLakeTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") DuckLakeTableName tableName,
            @JsonProperty("tableId") long tableId,
            @JsonProperty("schemaId") long schemaId,
            @JsonProperty("tablePath") String tablePath,
            @JsonProperty("schema") PrestoDuckLakeSchema schema)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.tableId = tableId;
        this.schemaId = schemaId;
        this.tablePath = requireNonNull(tablePath, "tablePath is null");
        this.schema = requireNonNull(schema, "schema is null");
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public DuckLakeTableName getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public long getTableId()
    {
        return tableId;
    }

    @JsonProperty
    public long getSchemaId()
    {
        return schemaId;
    }

    @JsonProperty
    public String getTablePath()
    {
        return tablePath;
    }

    @JsonProperty
    public PrestoDuckLakeSchema getSchema()
    {
        return schema;
    }

    public SchemaTableName getSchemaTableName()
    {
        return new SchemaTableName(schemaName, tableName.getTableName());
    }

    /**
     * The snapshot id resolved for this table by the metadata layer. Always present by the time a
     * table handle reaches the split manager or page source provider.
     *
     * @throws IllegalStateException if the snapshot id has not been resolved
     */
    public long getSnapshotId()
    {
        return tableName.getSnapshotId()
                .orElseThrow(() -> new IllegalStateException("Snapshot id has not been resolved for table " + getSchemaTableName()));
    }

    /**
     * Rebuilds the {@link DuckLakeTable} this handle was resolved from, for catalog calls (for
     * example {@code DuckLakeCatalog.listDataFiles}) that take one.
     */
    public DuckLakeTable toDuckLakeTable()
    {
        return new DuckLakeTable(tableId, schemaId, tableName.getTableName(), tablePath);
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
        DuckLakeTableHandle that = (DuckLakeTableHandle) o;
        return tableId == that.tableId &&
                schemaId == that.schemaId &&
                schemaName.equals(that.schemaName) &&
                tableName.equals(that.tableName) &&
                tablePath.equals(that.tablePath) &&
                schema.equals(that.schema);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaName, tableName, tableId, schemaId, tablePath, schema);
    }

    @Override
    public String toString()
    {
        return getSchemaTableName() + ":" + tableName;
    }
}
