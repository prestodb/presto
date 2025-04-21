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

import com.facebook.presto.common.experimental.FbThriftUtils;
import com.facebook.presto.common.experimental.ThriftSerializationRegistry;
import com.facebook.presto.common.experimental.auto_gen.ThriftConnectorOutputTableHandle;
import com.facebook.presto.common.experimental.auto_gen.ThriftHiveOutputTableHandle;
import com.facebook.presto.common.experimental.auto_gen.ThriftHiveWritableTableHandle;
import com.facebook.presto.hive.metastore.HivePageSinkMetadata;
import com.facebook.presto.hive.metastore.SortingColumn;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class HiveOutputTableHandle
        extends HiveWritableTableHandle
        implements ConnectorOutputTableHandle
{
    private final List<String> partitionedBy;
    private final String tableOwner;
    private final Map<String, String> additionalTableParameters;

    static {
        ThriftSerializationRegistry.registerSerializer(HiveOutputTableHandle.class, HiveOutputTableHandle::toThrift, null);
        ThriftSerializationRegistry.registerDeserializer(HiveOutputTableHandle.class, ThriftHiveOutputTableHandle.class, HiveOutputTableHandle::deserialize, null);
    }

    public HiveOutputTableHandle(ThriftHiveOutputTableHandle thriftHandle)
    {
        this(thriftHandle.getHiveWritableTableHandle().getSchemaName(),
                thriftHandle.getHiveWritableTableHandle().getTableName(),
                thriftHandle.getHiveWritableTableHandle().getInputColumns().stream().map(HiveColumnHandle::new).collect(Collectors.toList()),
                new HivePageSinkMetadata(thriftHandle.getHiveWritableTableHandle().getPageSinkMetadata()),
                new LocationHandle(thriftHandle.getHiveWritableTableHandle().getLocationHandle()),
                HiveStorageFormat.valueOf(thriftHandle.getHiveWritableTableHandle().getTableStorageFormat().name()),
                HiveStorageFormat.valueOf(thriftHandle.getHiveWritableTableHandle().getPartitionStorageFormat().name()),
                HiveStorageFormat.valueOf(thriftHandle.getHiveWritableTableHandle().getActualStorageFormat().name()),
                HiveCompressionCodec.valueOf(thriftHandle.getHiveWritableTableHandle().getCompressionCodec().name()),
                thriftHandle.getPartitionedBy(),
                Optional.ofNullable(thriftHandle.getHiveWritableTableHandle().getBucketProperty()).map(HiveBucketProperty::new),
                thriftHandle.getHiveWritableTableHandle().getPreferredOrderingColumns().stream().map(SortingColumn::new).collect(Collectors.toList()),
                thriftHandle.getTableOwner(),
                thriftHandle.getAdditionalTableParameters(),
                Optional.ofNullable(thriftHandle.getHiveWritableTableHandle().getEncryptionInformation()).map(EncryptionInformation::new));
    }

    @JsonCreator
    public HiveOutputTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("inputColumns") List<HiveColumnHandle> inputColumns,
            @JsonProperty("pageSinkMetadata") HivePageSinkMetadata pageSinkMetadata,
            @JsonProperty("locationHandle") LocationHandle locationHandle,
            @JsonProperty("tableStorageFormat") HiveStorageFormat tableStorageFormat,
            @JsonProperty("partitionStorageFormat") HiveStorageFormat partitionStorageFormat,
            @JsonProperty("actualStorageFormat") HiveStorageFormat actualStorageFormat,
            @JsonProperty("compressionCodec") HiveCompressionCodec compressionCodec,
            @JsonProperty("partitionedBy") List<String> partitionedBy,
            @JsonProperty("bucketProperty") Optional<HiveBucketProperty> bucketProperty,
            @JsonProperty("preferredOrderingColumns") List<SortingColumn> preferredOrderingColumns,
            @JsonProperty("tableOwner") String tableOwner,
            @JsonProperty("additionalTableParameters") Map<String, String> additionalTableParameters,
            @JsonProperty("encryptionInformation") Optional<EncryptionInformation> encryptionInformation)
    {
        super(
                schemaName,
                tableName,
                inputColumns,
                pageSinkMetadata,
                locationHandle,
                bucketProperty,
                preferredOrderingColumns,
                tableStorageFormat,
                partitionStorageFormat,
                actualStorageFormat,
                compressionCodec,
                encryptionInformation);

        this.partitionedBy = ImmutableList.copyOf(requireNonNull(partitionedBy, "partitionedBy is null"));
        this.tableOwner = requireNonNull(tableOwner, "tableOwner is null");
        this.additionalTableParameters = ImmutableMap.copyOf(requireNonNull(additionalTableParameters, "additionalTableParameters is null"));
    }

    @JsonProperty
    public List<String> getPartitionedBy()
    {
        return partitionedBy;
    }

    @JsonProperty
    public String getTableOwner()
    {
        return tableOwner;
    }

    @JsonProperty
    public Map<String, String> getAdditionalTableParameters()
    {
        return additionalTableParameters;
    }

    @Override
    public ThriftHiveOutputTableHandle toThrift()
    {
        ThriftHiveWritableTableHandle thriftBase = (ThriftHiveWritableTableHandle) super.toThrift();
        return new ThriftHiveOutputTableHandle(partitionedBy, tableOwner, additionalTableParameters, thriftBase);
    }

    @Override
    public ThriftConnectorOutputTableHandle toThriftInterface()
    {
        return ThriftConnectorOutputTableHandle.builder()
                .setType(getImplementationType())
                .setSerializedOutputTableHandle(FbThriftUtils.serialize(this.toThrift()))
                .build();
    }

    public static byte[] serialize(HiveOutputTableHandle handle)
    {
        return FbThriftUtils.serialize(handle.toThrift());
    }

    public static HiveOutputTableHandle deserialize(byte[] bytes)
    {
        return new HiveOutputTableHandle(FbThriftUtils.deserialize(ThriftHiveOutputTableHandle.class, bytes));
    }
}
