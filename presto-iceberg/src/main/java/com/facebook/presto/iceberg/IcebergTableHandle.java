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
import com.facebook.presto.hive.BaseHiveTableHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class IcebergTableHandle
        extends BaseHiveTableHandle
{
    private final IcebergTableName icebergTableName;
    private final TupleDomain<IcebergColumnHandle> predicate;
    private final boolean snapshotSpecified;
    private final Optional<String> tableSchemaJson;
    private final Optional<Set<Integer>> partitionFieldIds;
    private final Optional<Set<Integer>> equalityFieldIds;

    @JsonCreator
    public IcebergTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("icebergTableName") IcebergTableName icebergTableName,
            @JsonProperty("snapshotSpecified") boolean snapshotSpecified,
            @JsonProperty("predicate") TupleDomain<IcebergColumnHandle> predicate,
            @JsonProperty("tableSchemaJson") Optional<String> tableSchemaJson,
            @JsonProperty("partitionFieldIds") Optional<Set<Integer>> partitionFieldIds,
            @JsonProperty("equalityFieldIds") Optional<Set<Integer>> equalityFieldIds)
    {
        super(schemaName, icebergTableName.getTableName());

        this.icebergTableName = requireNonNull(icebergTableName, "tableName is null");
        this.snapshotSpecified = snapshotSpecified;
        this.predicate = requireNonNull(predicate, "predicate is null");
        this.tableSchemaJson = requireNonNull(tableSchemaJson, "tableSchemaJson is null");
        this.partitionFieldIds = requireNonNull(partitionFieldIds, "partitionFieldIds is null");
        this.equalityFieldIds = requireNonNull(equalityFieldIds, "equalityFieldIds is null");
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
    public TupleDomain<IcebergColumnHandle> getPredicate()
    {
        return predicate;
    }

    @JsonProperty
    public Optional<String> getTableSchemaJson()
    {
        return tableSchemaJson;
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
                Objects.equals(predicate, that.predicate) &&
                Objects.equals(tableSchemaJson, that.tableSchemaJson) &&
                Objects.equals(equalityFieldIds, that.equalityFieldIds);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getSchemaName(), icebergTableName, predicate, snapshotSpecified, tableSchemaJson, equalityFieldIds);
    }

    @Override
    public String toString()
    {
        return icebergTableName.toString();
    }
}
