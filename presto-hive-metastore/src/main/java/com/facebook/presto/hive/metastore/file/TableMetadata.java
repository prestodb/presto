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
import com.facebook.presto.hive.metastore.PrestoTableType;
import com.facebook.presto.hive.metastore.Storage;
import com.facebook.presto.hive.metastore.StorageFormat;
import com.facebook.presto.hive.metastore.Table;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.hive.HiveStorageFormat.getHiveStorageFormat;
import static com.facebook.presto.hive.metastore.PrestoTableType.EXTERNAL_TABLE;
import static com.facebook.presto.hive.metastore.StorageFormat.VIEW_STORAGE_FORMAT;
import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class TableMetadata
{
    private final String owner;
    private final PrestoTableType tableType;
    private final List<Column> dataColumns;
    private final List<Column> partitionColumns;
    private final Map<String, String> parameters;

    private final StorageFormat storageFormat;
    private final Optional<HiveBucketProperty> bucketProperty;
    private final Map<String, String> storageParameters;
    private final Map<String, String> serdeParameters;

    private final Optional<String> externalLocation;

    private final Optional<String> viewOriginalText;
    private final Optional<String> viewExpandedText;

    private final Map<String, HiveColumnStatistics> columnStatistics;

    @JsonCreator
    public TableMetadata(
            @JsonProperty("owner") String owner,
            @JsonProperty("tableType") PrestoTableType tableType,
            @JsonProperty("dataColumns") List<Column> dataColumns,
            @JsonProperty("partitionColumns") List<Column> partitionColumns,
            @JsonProperty("parameters") Map<String, String> parameters,
            @JsonDeserialize(using = StorageFormatCompatDeserializer.class)
            @JsonProperty("storageFormat") StorageFormat storageFormat,
            @JsonProperty("bucketProperty") Optional<HiveBucketProperty> bucketProperty,
            @JsonProperty("storageParameters") Map<String, String> storageParameters,
            @JsonProperty("serdeParameters") Map<String, String> serdeParameters,
            @JsonProperty("externalLocation") Optional<String> externalLocation,
            @JsonProperty("viewOriginalText") Optional<String> viewOriginalText,
            @JsonProperty("viewExpandedText") Optional<String> viewExpandedText,
            @JsonProperty("columnStatistics") Map<String, HiveColumnStatistics> columnStatistics)
    {
        this.owner = requireNonNull(owner, "owner is null");
        this.tableType = requireNonNull(tableType, "tableType is null");
        this.dataColumns = ImmutableList.copyOf(requireNonNull(dataColumns, "dataColumns is null"));
        this.partitionColumns = ImmutableList.copyOf(requireNonNull(partitionColumns, "partitionColumns is null"));
        this.parameters = ImmutableMap.copyOf(requireNonNull(parameters, "parameters is null"));
        this.storageParameters = ImmutableMap.copyOf(firstNonNull(storageParameters, ImmutableMap.of()));
        this.storageFormat = storageFormat == null ? VIEW_STORAGE_FORMAT : storageFormat;
        this.bucketProperty = requireNonNull(bucketProperty, "bucketProperty is null");
        this.serdeParameters = requireNonNull(serdeParameters, "serdeParameters is null");
        this.externalLocation = requireNonNull(externalLocation, "externalLocation is null");
        if (tableType.equals(EXTERNAL_TABLE)) {
            checkArgument(externalLocation.isPresent(), "External location is required for external tables");
        }
        else {
            checkArgument(!externalLocation.isPresent(), "External location is only allowed for external tables");
        }

        this.viewOriginalText = requireNonNull(viewOriginalText, "viewOriginalText is null");
        this.viewExpandedText = requireNonNull(viewExpandedText, "viewExpandedText is null");
        this.columnStatistics = ImmutableMap.copyOf(requireNonNull(columnStatistics, "columnStatistics is null"));
        checkArgument(partitionColumns.isEmpty() || columnStatistics.isEmpty(), "column statistics cannot be set for partitioned table");
    }

    @Deprecated
    public TableMetadata(
            String owner,
            PrestoTableType tableType,
            List<Column> dataColumns,
            List<Column> partitionColumns,
            Map<String, String> parameters,
            Optional<HiveStorageFormat> storageFormat,
            Optional<HiveBucketProperty> bucketProperty,
            Map<String, String> storageParameters,
            Map<String, String> serdeParameters,
            Optional<String> externalLocation,
            Optional<String> viewOriginalText,
            Optional<String> viewExpandedText,
            Map<String, HiveColumnStatistics> columnStatistics)
    {
        this(
                owner,
                tableType,
                dataColumns,
                partitionColumns,
                parameters,
                storageFormat.map(StorageFormat::fromHiveStorageFormat).orElse(VIEW_STORAGE_FORMAT),
                bucketProperty,
                storageParameters,
                serdeParameters,
                externalLocation,
                viewOriginalText,
                viewExpandedText,
                columnStatistics);
    }

    public TableMetadata(Table table)
    {
        this(table, ImmutableMap.of());
    }

    public TableMetadata(Table table, Map<String, HiveColumnStatistics> columnStatistics)
    {
        owner = table.getOwner();
        tableType = table.getTableType();
        dataColumns = table.getDataColumns();
        partitionColumns = table.getPartitionColumns();
        parameters = table.getParameters();

        storageFormat = table.getStorage().getStorageFormat();
        bucketProperty = table.getStorage().getBucketProperty();
        storageParameters = table.getStorage().getParameters();
        serdeParameters = table.getStorage().getSerdeParameters();

        if (tableType.equals(EXTERNAL_TABLE)) {
            externalLocation = Optional.of(table.getStorage().getLocation());
        }
        else {
            externalLocation = Optional.empty();
        }

        viewOriginalText = table.getViewOriginalText();
        viewExpandedText = table.getViewExpandedText();
        this.columnStatistics = ImmutableMap.copyOf(requireNonNull(columnStatistics, "columnStatistics is null"));
    }

    @JsonProperty
    public String getOwner()
    {
        return owner;
    }

    @JsonProperty
    public PrestoTableType getTableType()
    {
        return tableType;
    }

    @JsonProperty
    public List<Column> getDataColumns()
    {
        return dataColumns;
    }

    @JsonProperty
    public List<Column> getPartitionColumns()
    {
        return partitionColumns;
    }

    public Optional<Column> getColumn(String name)
    {
        for (Column partitionColumn : partitionColumns) {
            if (partitionColumn.getName().equals(name)) {
                return Optional.of(partitionColumn);
            }
        }
        for (Column dataColumn : dataColumns) {
            if (dataColumn.getName().equals(name)) {
                return Optional.of(dataColumn);
            }
        }
        return Optional.empty();
    }

    @JsonProperty
    public Map<String, String> getParameters()
    {
        return parameters;
    }

    @Deprecated
    @JsonIgnore
    public Optional<HiveStorageFormat> getStorageFormat()
    {
        return getHiveStorageFormat(storageFormat);
    }

    @JsonProperty("storageFormat")
    public StorageFormat getTableStorageFormat()
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
    public Optional<String> getViewOriginalText()
    {
        return viewOriginalText;
    }

    @JsonProperty
    public Optional<String> getViewExpandedText()
    {
        return viewExpandedText;
    }

    @JsonProperty
    public Map<String, HiveColumnStatistics> getColumnStatistics()
    {
        return columnStatistics;
    }

    public TableMetadata withDataColumns(List<Column> dataColumns)
    {
        return new TableMetadata(
                owner,
                tableType,
                dataColumns,
                partitionColumns,
                parameters,
                storageFormat,
                bucketProperty,
                storageParameters,
                serdeParameters,
                externalLocation,
                viewOriginalText,
                viewExpandedText,
                columnStatistics);
    }

    public TableMetadata withParameters(Map<String, String> parameters)
    {
        return new TableMetadata(
                owner,
                tableType,
                dataColumns,
                partitionColumns,
                parameters,
                storageFormat,
                bucketProperty,
                storageParameters,
                serdeParameters,
                externalLocation,
                viewOriginalText,
                viewExpandedText,
                columnStatistics);
    }

    public TableMetadata withColumnStatistics(Map<String, HiveColumnStatistics> columnStatistics)
    {
        return new TableMetadata(
                owner,
                tableType,
                dataColumns,
                partitionColumns,
                parameters,
                storageFormat,
                bucketProperty,
                storageParameters,
                serdeParameters,
                externalLocation,
                viewOriginalText,
                viewExpandedText,
                columnStatistics);
    }

    public Table toTable(String databaseName, String tableName, String location)
    {
        return new Table(
                databaseName,
                tableName,
                owner,
                tableType,
                Storage.builder()
                        .setLocation(externalLocation.orElse(location))
                        .setStorageFormat(storageFormat)
                        .setBucketProperty(bucketProperty)
                        .setParameters(storageParameters)
                        .setSerdeParameters(serdeParameters)
                        .build(),
                dataColumns,
                partitionColumns,
                parameters,
                viewOriginalText,
                viewExpandedText);
    }
}
