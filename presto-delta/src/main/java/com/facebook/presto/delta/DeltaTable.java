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
package com.facebook.presto.delta;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

public class DeltaTable
{
    /**
     * Value returned by {@link #getColumnMappingMode()} when the source table
     * has no {@code delta.columnMapping.mode} configuration (or set it to
     * {@code none}). Kept as a constant so the protocol string stays the same
     * on both sides of the Java / C++ boundary.
     */
    public static final String COLUMN_MAPPING_MODE_NONE = "none";

    private final String schemaName;
    private final String tableName;
    private final String tableLocation;
    private final Optional<Long> snapshotId;
    private final List<DeltaColumn> columns;
    private final String columnMappingMode;

    /**
     * Data file format type (eg. Parquet, ORC etc).
     */
    public enum DataFormat
    {
        PARQUET,
        ORC;
    }

    @JsonCreator
    public DeltaTable(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("tableLocation") String tableLocation,
            @JsonProperty("snapshotId") Optional<Long> snapshotId,
            @JsonProperty("columns") List<DeltaColumn> columns,
            @JsonProperty("columnMappingMode") String columnMappingMode)
    {
        checkArgument(!isNullOrEmpty(schemaName), "schemaName is null or is empty");
        checkArgument(!isNullOrEmpty(tableName), "tableName is null or is empty");
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.tableLocation = requireNonNull(tableLocation, "tableLocation is null");
        this.snapshotId = requireNonNull(snapshotId, "snapshotId is null");
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
        this.columnMappingMode = columnMappingMode == null || columnMappingMode.isEmpty()
                ? COLUMN_MAPPING_MODE_NONE
                : columnMappingMode;
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
    public String getTableLocation()
    {
        return tableLocation;
    }

    @JsonProperty
    public Optional<Long> getSnapshotId()
    {
        return snapshotId;
    }

    @JsonProperty
    public List<DeltaColumn> getColumns()
    {
        return columns;
    }

    /**
     * Delta {@code delta.columnMapping.mode} value ({@code "none"}, {@code
     * "name"}, or {@code "id"}). Populated from the snapshot metadata by
     * {@link DeltaClient#getTable}. Kept as a string here so the protocol
     * layer can forward it without an enum on the wire.
     */
    @JsonProperty
    public String getColumnMappingMode()
    {
        return columnMappingMode;
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

        DeltaTable that = (DeltaTable) o;
        return Objects.equals(schemaName, that.schemaName) &&
                Objects.equals(tableName, that.tableName) &&
                Objects.equals(tableLocation, that.tableLocation) &&
                Objects.equals(snapshotId, that.snapshotId) &&
                Objects.equals(columns, that.columns) &&
                Objects.equals(columnMappingMode, that.columnMappingMode);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaName, tableName, tableLocation, snapshotId, columns, columnMappingMode);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("table", schemaName + "." + tableName)
                .add("location", tableLocation)
                .add("snapshotId", snapshotId)
                .add("columns", columns)
                .add("columnMappingMode", columnMappingMode)
                .toString();
    }
}
