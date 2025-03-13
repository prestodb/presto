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
package com.facebook.presto.hive;

import com.facebook.presto.common.experimental.ThriftSerializable;
import com.facebook.presto.common.experimental.auto_gen.ThriftHiveCompressionCodec;
import com.facebook.presto.common.experimental.auto_gen.ThriftHiveStorageFormat;
import com.facebook.presto.common.experimental.auto_gen.ThriftHiveWritableTableHandle;
import com.facebook.presto.hive.metastore.HivePageSinkMetadata;
import com.facebook.presto.hive.metastore.SortingColumn;
import com.facebook.presto.hive.util.TypeInfoRegister;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import org.apache.thrift.TBase;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.hive.metastore.HivePageSinkMetadata.createHivePageSinkMetadata;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static java.util.Objects.requireNonNull;

public class HiveWritableTableHandle
        implements ThriftSerializable
{
    private final String schemaName;
    private final String tableName;
    private final List<HiveColumnHandle> inputColumns;
    private final HivePageSinkMetadata pageSinkMetadata;
    private final LocationHandle locationHandle;
    private final Optional<HiveBucketProperty> bucketProperty;
    private final List<SortingColumn> preferredOrderingColumns;
    private final HiveStorageFormat tableStorageFormat;
    private final HiveStorageFormat partitionStorageFormat;
    private final HiveStorageFormat actualStorageFormat;
    private final HiveCompressionCodec compressionCodec;
    private final Optional<EncryptionInformation> encryptionInformation;

    private final TypeInfoRegister typeInfoRegister = new TypeInfoRegister();

    public HiveWritableTableHandle(ThriftHiveWritableTableHandle thriftHandle)
    {
        this(
                thriftHandle.getSchemaName(),
                thriftHandle.getTableName(),
                thriftHandle.getInputColumns().stream().map(HiveColumnHandle::new).collect(Collectors.toList()),
                createHivePageSinkMetadata(thriftHandle.getPageSinkMetadata()),
                new LocationHandle(thriftHandle.getLocationHandle()),
                thriftHandle.getBucketProperty().map(HiveBucketProperty::new),
                thriftHandle.getPreferredOrderingColumns().stream().map(SortingColumn::new).collect(Collectors.toList()),
                HiveStorageFormat.valueOf(thriftHandle.getTableStorageFormat().name()),
                HiveStorageFormat.valueOf(thriftHandle.getPartitionStorageFormat().name()),
                HiveStorageFormat.valueOf(thriftHandle.getActualStorageFormat().name()),
                HiveCompressionCodec.valueOf(thriftHandle.getCompressionCodec().name()),
                thriftHandle.getEncryptionInformation().map(EncryptionInformation::new));
    }

    @Override
    public TBase toThrift()
    {
        ThriftHiveWritableTableHandle thriftHandle = new ThriftHiveWritableTableHandle(
                schemaName, tableName,
                inputColumns.stream().map(HiveColumnHandle::toThrift).collect(Collectors.toList()),
                pageSinkMetadata.toThrift(),
                locationHandle.toThrift(),
                preferredOrderingColumns.stream().map(SortingColumn::toThrift).collect(Collectors.toList()),
                ThriftHiveStorageFormat.valueOf(tableStorageFormat.name()),
                ThriftHiveStorageFormat.valueOf(partitionStorageFormat.name()),
                ThriftHiveStorageFormat.valueOf(actualStorageFormat.name()),
                ThriftHiveCompressionCodec.valueOf(compressionCodec.name()));
        bucketProperty.ifPresent(property -> thriftHandle.setBucketProperty(property.toThrift()));
        encryptionInformation.ifPresent(info -> thriftHandle.setEncryptionInformation(info.toThrift()));
        return thriftHandle;
    }

    public HiveWritableTableHandle(
            String schemaName,
            String tableName,
            List<HiveColumnHandle> inputColumns,
            HivePageSinkMetadata pageSinkMetadata,
            LocationHandle locationHandle,
            Optional<HiveBucketProperty> bucketProperty,
            List<SortingColumn> preferredOrderingColumns,
            HiveStorageFormat tableStorageFormat,
            HiveStorageFormat partitionStorageFormat,
            HiveStorageFormat actualStorageFormat,
            HiveCompressionCodec compressionCodec,
            Optional<EncryptionInformation> encryptionInformation)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.inputColumns = ImmutableList.copyOf(requireNonNull(inputColumns, "inputColumns is null"));
        this.pageSinkMetadata = requireNonNull(pageSinkMetadata, "pageSinkMetadata is null");
        this.locationHandle = requireNonNull(locationHandle, "locationHandle is null");
        this.bucketProperty = requireNonNull(bucketProperty, "bucketProperty is null");
        this.preferredOrderingColumns = requireNonNull(preferredOrderingColumns, "preferredOrderingColumns is null");
        this.tableStorageFormat = requireNonNull(tableStorageFormat, "tableStorageFormat is null");
        this.partitionStorageFormat = requireNonNull(partitionStorageFormat, "partitionStorageFormat is null");
        this.actualStorageFormat = requireNonNull(actualStorageFormat, "actualStorageFormat is null");
        this.compressionCodec = requireNonNull(compressionCodec, "compressionCodec is null");
        this.encryptionInformation = requireNonNull(encryptionInformation, "encryptionInformation is null");

//        this.schemaName = schemaName;
//        this.tableName = tableName;
//        this.inputColumns = ImmutableList.copyOf(inputColumns);
//        this.pageSinkMetadata = pageSinkMetadata;
//        this.locationHandle = locationHandle;
//        this.bucketProperty = bucketProperty;
//        this.preferredOrderingColumns = preferredOrderingColumns;
//        this.tableStorageFormat = tableStorageFormat;
//        this.partitionStorageFormat = partitionStorageFormat;
//        this.encryptionInformation = encryptionInformation;
//        this.actualStorageFormat = actualStorageFormat;
//        this.compressionCodec = compressionCodec;

        if (!compressionCodec.isSupportedStorageFormat(actualStorageFormat)) {
            throw new PrestoException(GENERIC_USER_ERROR, String.format("%s compression is not supported with %s", compressionCodec.name(), actualStorageFormat.name()));
        }
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
    public List<HiveColumnHandle> getInputColumns()
    {
        return inputColumns;
    }

    @JsonIgnore
    public SchemaTableName getSchemaTableName()
    {
        return new SchemaTableName(schemaName, tableName);
    }

    @JsonProperty
    public HivePageSinkMetadata getPageSinkMetadata()
    {
        return pageSinkMetadata;
    }

    @JsonProperty
    public LocationHandle getLocationHandle()
    {
        return locationHandle;
    }

    @JsonProperty
    public Optional<HiveBucketProperty> getBucketProperty()
    {
        return bucketProperty;
    }

    @JsonProperty
    public List<SortingColumn> getPreferredOrderingColumns()
    {
        return preferredOrderingColumns;
    }

    /* Use {@link #getActualStorageFormat()}*/
    @Deprecated
    @JsonProperty
    public HiveStorageFormat getTableStorageFormat()
    {
        return tableStorageFormat;
    }

    /* Use {@link #getActualStorageFormat()}*/
    @Deprecated
    @JsonProperty
    public HiveStorageFormat getPartitionStorageFormat()
    {
        return partitionStorageFormat;
    }

    /**
     * The actualStorageFormat is the real storage format that gets used later in the pipeline.
     * It could be either representing tableStorageFormat, or partitionStorageFormat.
     */
    @JsonProperty
    public HiveStorageFormat getActualStorageFormat()
    {
        return actualStorageFormat;
    }

    @JsonProperty
    public HiveCompressionCodec getCompressionCodec()
    {
        return compressionCodec;
    }

    @JsonProperty
    public Optional<EncryptionInformation> getEncryptionInformation()
    {
        return encryptionInformation;
    }

    @Override
    public String toString()
    {
        return schemaName + "." + tableName;
    }
}
