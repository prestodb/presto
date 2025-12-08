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

import com.facebook.presto.hive.HiveCompressionCodec;
import com.facebook.presto.spi.ConnectorDistributedProcedureHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class IcebergDistributedProcedureHandle
        extends IcebergWritableTableHandle
        implements ConnectorDistributedProcedureHandle
{
    private final IcebergTableLayoutHandle tableLayoutHandle;
    private final Map<String, String> relevantData;

    @JsonCreator
    public IcebergDistributedProcedureHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") IcebergTableName tableName,
            @JsonProperty("schema") PrestoIcebergSchema schema,
            @JsonProperty("partitionSpec") PrestoIcebergPartitionSpec partitionSpec,
            @JsonProperty("inputColumns") List<IcebergColumnHandle> inputColumns,
            @JsonProperty("outputPath") String outputPath,
            @JsonProperty("fileFormat") FileFormat fileFormat,
            @JsonProperty("compressionCodec") HiveCompressionCodec compressionCodec,
            @JsonProperty("storageProperties") Map<String, String> storageProperties,
            @JsonProperty("tableLayoutHandle") IcebergTableLayoutHandle tableLayoutHandle,
            @JsonProperty("relevantData") Map<String, String> relevantData)
    {
        super(
                schemaName,
                tableName,
                schema,
                partitionSpec,
                inputColumns,
                outputPath,
                fileFormat,
                compressionCodec,
                storageProperties,
                ImmutableList.of());
        this.tableLayoutHandle = requireNonNull(tableLayoutHandle, "tableLayoutHandle is null");
        this.relevantData = requireNonNull(relevantData, "relevantData is null");
    }

    @JsonProperty
    public IcebergTableLayoutHandle getTableLayoutHandle()
    {
        return tableLayoutHandle;
    }

    @JsonProperty
    public Map<String, String> getRelevantData()
    {
        return relevantData;
    }
}
