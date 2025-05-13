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

import com.facebook.presto.hive.BaseHiveTableHandle;
import com.facebook.presto.spi.ConnectorDeleteTableHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class IcebergTableHandle
        extends BaseHiveTableHandle
            implements ConnectorDeleteTableHandle
{
    private final IcebergTableName icebergTableName;
    private final boolean snapshotSpecified;
    private final Optional<String> outputPath;
    private final Optional<Map<String, String>> storageProperties;
    private final Optional<String> tableSchemaJson;
    private final Optional<Set<Integer>> partitionFieldIds;
    private final Optional<Set<Integer>> equalityFieldIds;
    private final List<SortField> sortOrder;
    private final List<IcebergColumnHandle> updatedColumns;

    @JsonCreator
    public IcebergTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("icebergTableName") IcebergTableName icebergTableName,
            @JsonProperty("snapshotSpecified") boolean snapshotSpecified,
            @JsonProperty("outputPath") Optional<String> outputPath,
            @JsonProperty("storageProperties") Optional<Map<String, String>> storageProperties,
            @JsonProperty("tableSchemaJson") Optional<String> tableSchemaJson,
            @JsonProperty("partitionFieldIds") Optional<Set<Integer>> partitionFieldIds,
            @JsonProperty("equalityFieldIds") Optional<Set<Integer>> equalityFieldIds,
            @JsonProperty("sortOrder") List<SortField> sortOrder,
            @JsonProperty("updatedColumns") List<IcebergColumnHandle> updatedColumns)
    {
        super(schemaName, icebergTableName.getTableName());

        this.icebergTableName = requireNonNull(icebergTableName, "tableName is null");
        this.snapshotSpecified = snapshotSpecified;
        this.outputPath = requireNonNull(outputPath, "filePrefix is null");
        this.storageProperties = requireNonNull(storageProperties, "storageProperties is null");
        this.tableSchemaJson = requireNonNull(tableSchemaJson, "tableSchemaJson is null");
        this.partitionFieldIds = requireNonNull(partitionFieldIds, "partitionFieldIds is null");
        this.equalityFieldIds = requireNonNull(equalityFieldIds, "equalityFieldIds is null");
        this.sortOrder = ImmutableList.copyOf(requireNonNull(sortOrder, "sortOrder is null"));
        this.updatedColumns = requireNonNull(updatedColumns, "updatedColumns is null");
    }

    @JsonProperty
    public IcebergTableName getIcebergTableName()
    {
        return icebergTableName;
    }

    @JsonProperty
    public boolean isSnapshotSpecified()
    {
        return snapshotSpecified;
    }

    @JsonProperty
    public List<SortField> getSortOrder()
    {
        return sortOrder;
    }

    @JsonProperty
    public Optional<String> getTableSchemaJson()
    {
        return tableSchemaJson;
    }

    @JsonProperty
    public Optional<String> getOutputPath()
    {
        return outputPath;
    }

    @JsonProperty
    public Optional<Map<String, String>> getStorageProperties()
    {
        return storageProperties;
    }

    @JsonProperty
    public Optional<Set<Integer>> getPartitionSpecId()
    {
        return partitionFieldIds;
    }

    @JsonProperty
    public Optional<Set<Integer>> getEqualityFieldIds()
    {
        return equalityFieldIds;
    }

    @JsonProperty
    public List<IcebergColumnHandle> getUpdatedColumns()
    {
        return updatedColumns;
    }

    public IcebergTableHandle withUpdatedColumns(List<IcebergColumnHandle> updatedColumns)
    {
        return new IcebergTableHandle(
                getSchemaName(),
                icebergTableName,
                snapshotSpecified,
                outputPath,
                storageProperties,
                tableSchemaJson,
                partitionFieldIds,
                equalityFieldIds,
                sortOrder,
                updatedColumns);
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
        return Objects.equals(getSchemaName(), that.getSchemaName()) &&
                Objects.equals(icebergTableName, that.icebergTableName) &&
                snapshotSpecified == that.snapshotSpecified &&
                Objects.equals(sortOrder, that.sortOrder) &&
                Objects.equals(tableSchemaJson, that.tableSchemaJson) &&
                Objects.equals(equalityFieldIds, that.equalityFieldIds);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getSchemaName(), icebergTableName, sortOrder, snapshotSpecified, tableSchemaJson, equalityFieldIds);
    }

    @Override
    public String toString()
    {
        return icebergTableName.toString();
    }
}
