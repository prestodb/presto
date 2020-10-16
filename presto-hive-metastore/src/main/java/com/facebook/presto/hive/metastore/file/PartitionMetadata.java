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
package com.facebook.presto.hive.metastore.file;

import com.facebook.presto.hive.HiveBucketProperty;
import com.facebook.presto.hive.HiveStorageFormat;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.HiveColumnStatistics;
import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.PartitionStatistics;
import com.facebook.presto.hive.metastore.PartitionWithStatistics;
import com.facebook.presto.hive.metastore.Storage;
import com.facebook.presto.hive.metastore.StorageFormat;
import com.facebook.presto.hive.metastore.Table;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.hive.metastore.MetastoreUtil.updateStatisticsParameters;
import static com.facebook.presto.hive.metastore.PrestoTableType.EXTERNAL_TABLE;
import static com.facebook.presto.hive.metastore.StorageFormat.VIEW_STORAGE_FORMAT;
import static com.google.common.base.MoreObjects.firstNonNull;
import static java.util.Objects.requireNonNull;

public class PartitionMetadata
{
    private final List<Column> columns;
    private final Map<String, String> parameters;

    private final Optional<HiveStorageFormat> storageFormat;
    private final Optional<HiveBucketProperty> bucketProperty;
    private final Map<String, String> storageParameters;
    private final Map<String, String> serdeParameters;

    private final Optional<String> externalLocation;

    private final Map<String, HiveColumnStatistics> columnStatistics;
    private final boolean eligibleToIgnore;
    private final boolean sealedPartition;

    @JsonCreator
    public PartitionMetadata(
            @JsonProperty("columns") List<Column> columns,
            @JsonProperty("parameters") Map<String, String> parameters,
            @JsonProperty("storageFormat") Optional<HiveStorageFormat> storageFormat,
            @JsonProperty("bucketProperty") Optional<HiveBucketProperty> bucketProperty,
            @JsonProperty("storageParameters") Map<String, String> storageParameters,
            @JsonProperty("serdeParameters") Map<String, String> serdeParameters,
            @JsonProperty("externalLocation") Optional<String> externalLocation,
            @JsonProperty("columnStatistics") Map<String, HiveColumnStatistics> columnStatistics,
            @JsonProperty("eligibleToIgnore") boolean eligibleToIgnore,
            @JsonProperty("sealedPartition") boolean sealedPartition)
    {
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
        this.parameters = ImmutableMap.copyOf(requireNonNull(parameters, "parameters is null"));

        this.storageFormat = requireNonNull(storageFormat, "storageFormat is null");
        this.bucketProperty = requireNonNull(bucketProperty, "bucketProperty is null");
        this.storageParameters = ImmutableMap.copyOf(firstNonNull(storageParameters, ImmutableMap.of()));
        this.serdeParameters = requireNonNull(serdeParameters, "serdeParameters is null");

        this.externalLocation = requireNonNull(externalLocation, "externalLocation is null");
        this.columnStatistics = ImmutableMap.copyOf(requireNonNull(columnStatistics, "columnStatistics is null"));
        this.eligibleToIgnore = eligibleToIgnore;
        this.sealedPartition = sealedPartition;
    }

    public PartitionMetadata(Table table, PartitionWithStatistics partitionWithStatistics)
    {
        Partition partition = partitionWithStatistics.getPartition();
        PartitionStatistics statistics = partitionWithStatistics.getStatistics();

        this.columns = partition.getColumns();
        this.parameters = updateStatisticsParameters(partition.getParameters(), statistics.getBasicStatistics());

        StorageFormat tableFormat = partition.getStorage().getStorageFormat();
        storageFormat = Arrays.stream(HiveStorageFormat.values())
                .filter(format -> tableFormat.equals(StorageFormat.fromHiveStorageFormat(format)))
                .findFirst();

        if (table.getTableType().equals(EXTERNAL_TABLE)) {
            externalLocation = Optional.of(partition.getStorage().getLocation());
        }
        else {
            externalLocation = Optional.empty();
        }

        bucketProperty = partition.getStorage().getBucketProperty();
        storageParameters = partition.getStorage().getParameters();
        serdeParameters = partition.getStorage().getSerdeParameters();
        columnStatistics = ImmutableMap.copyOf(statistics.getColumnStatistics());
        eligibleToIgnore = partition.isEligibleToIgnore();
        sealedPartition = partition.isSealedPartition();
    }

    @JsonProperty
    public List<Column> getColumns()
    {
        return columns;
    }

    @JsonProperty
    public Map<String, String> getParameters()
    {
        return parameters;
    }

    @JsonProperty
    public Optional<HiveStorageFormat> getStorageFormat()
    {
        return storageFormat;
    }

    @JsonProperty
    public Optional<HiveBucketProperty> getBucketProperty()
    {
        return bucketProperty;
    }

    @JsonProperty
    public Map<String, String> getStorageParameters()
    {
        return storageParameters;
    }

    @JsonProperty
    public Map<String, String> getSerdeParameters()
    {
        return serdeParameters;
    }

    @JsonProperty
    public Optional<String> getExternalLocation()
    {
        return externalLocation;
    }

    @JsonProperty
    public Map<String, HiveColumnStatistics> getColumnStatistics()
    {
        return columnStatistics;
    }

    @JsonProperty
    public boolean isEligibleToIgnore()
    {
        return eligibleToIgnore;
    }

    @JsonProperty
    public boolean isSealedPartition()
    {
        return sealedPartition;
    }

    public PartitionMetadata withParameters(Map<String, String> parameters)
    {
        return new PartitionMetadata(columns, parameters, storageFormat, bucketProperty, storageParameters, serdeParameters, externalLocation, columnStatistics, eligibleToIgnore, sealedPartition);
    }

    public PartitionMetadata withColumnStatistics(Map<String, HiveColumnStatistics> columnStatistics)
    {
        return new PartitionMetadata(columns, parameters, storageFormat, bucketProperty, storageParameters, serdeParameters, externalLocation, columnStatistics, eligibleToIgnore, sealedPartition);
    }

    public Partition toPartition(String databaseName, String tableName, List<String> values, String location)
    {
        return new Partition(
                databaseName,
                tableName,
                values,
                Storage.builder()
                        .setLocation(externalLocation.orElse(location))
                        .setStorageFormat(storageFormat.map(StorageFormat::fromHiveStorageFormat).orElse(VIEW_STORAGE_FORMAT))
                        .setBucketProperty(bucketProperty)
                        .setSerdeParameters(serdeParameters)
                        .setParameters(parameters)
                        .build(),
                columns,
                parameters,
                Optional.empty(),
                eligibleToIgnore,
                sealedPartition);
    }
}
