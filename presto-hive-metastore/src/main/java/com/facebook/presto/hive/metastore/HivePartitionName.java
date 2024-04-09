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
package com.facebook.presto.hive.metastore;

import com.facebook.presto.hive.PartitionNameWithVersion;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.hive.metastore.HiveTableName.hiveTableName;
import static com.facebook.presto.hive.metastore.MetastoreUtil.toPartitionValues;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

@Immutable
public class HivePartitionName
{
    private final HiveTableName hiveTableName;
    private final List<String> partitionValues;
    private final Optional<PartitionNameWithVersion> partitionNameWithVersion;

    @JsonCreator
    public HivePartitionName(
            @JsonProperty("hiveTableName") HiveTableName hiveTableName,
            @JsonProperty("partitionValues") List<String> partitionValues,
            @JsonProperty("partitionNameWithVersion") Optional<PartitionNameWithVersion> partitionNameWithVersion)
    {
        this.hiveTableName = requireNonNull(hiveTableName, "hiveTableName is null");
        this.partitionValues = ImmutableList.copyOf(requireNonNull(partitionValues, "partitionValues is null"));
        this.partitionNameWithVersion = requireNonNull(partitionNameWithVersion, "partitionNameWithVersion is null");
    }

    public static HivePartitionName hivePartitionName(HiveTableName hiveTableName, PartitionNameWithVersion partitionName)
    {
        return new HivePartitionName(hiveTableName, toPartitionValues(partitionName.getPartitionName()), Optional.of(partitionName));
    }

    public static HivePartitionName hivePartitionName(HiveTableName hiveTableName, String partitionName)
    {
        return new HivePartitionName(hiveTableName, toPartitionValues(partitionName), Optional.of(new PartitionNameWithVersion(partitionName, Optional.empty())));
    }

    public static HivePartitionName hivePartitionName(String databaseName, String tableName, PartitionNameWithVersion partitionName)
    {
        return hivePartitionName(hiveTableName(databaseName, tableName), partitionName);
    }

    public static HivePartitionName hivePartitionName(String databaseName, String tableName, String partitionName)
    {
        return hivePartitionName(hiveTableName(databaseName, tableName), partitionName);
    }

    public static HivePartitionName hivePartitionName(String databaseName, String tableName, List<String> partitionValues)
    {
        return new HivePartitionName(hiveTableName(databaseName, tableName), partitionValues, Optional.empty());
    }

    @JsonProperty
    public HiveTableName getHiveTableName()
    {
        return hiveTableName;
    }

    @JsonProperty
    public List<String> getPartitionValues()
    {
        return partitionValues;
    }

    @JsonProperty
    public Optional<PartitionNameWithVersion> getPartitionNameWithVersion()
    {
        return partitionNameWithVersion;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("hiveTableName", hiveTableName)
                .add("partitionValues", partitionValues)
                .add("partitionNameWithVersion", partitionNameWithVersion)
                .toString();
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

        HivePartitionName other = (HivePartitionName) o;
        return Objects.equals(hiveTableName, other.hiveTableName) &&
                Objects.equals(partitionValues, other.partitionValues) &&
                Objects.equals(partitionNameWithVersion, other.partitionNameWithVersion);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(hiveTableName, partitionValues, partitionNameWithVersion);
    }
}
