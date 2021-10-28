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

import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import org.apache.iceberg.FileFormat;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class IcebergWritableTableHandle
        implements ConnectorInsertTableHandle, ConnectorOutputTableHandle
{
    private final String schemaName;
    private final String tableName;
    private final String schemaAsJson;
    private final String partitionSpecAsJson;
    private final List<IcebergColumnHandle> inputColumns;
    private final String outputPath;
    private final FileFormat fileFormat;
    private final Map<String, String> storageProperties;

    @JsonCreator
    public IcebergWritableTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("schemaAsJson") String schemaAsJson,
            @JsonProperty("partitionSpecAsJson") String partitionSpecAsJson,
            @JsonProperty("inputColumns") List<IcebergColumnHandle> inputColumns,
            @JsonProperty("outputPath") String outputPath,
            @JsonProperty("fileFormat") FileFormat fileFormat,
            @JsonProperty("storageProperties") Map<String, String> storageProperties)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.schemaAsJson = requireNonNull(schemaAsJson, "schemaAsJson is null");
        this.partitionSpecAsJson = requireNonNull(partitionSpecAsJson, "partitionSpecAsJson is null");
        this.inputColumns = ImmutableList.copyOf(requireNonNull(inputColumns, "inputColumns is null"));
        this.outputPath = requireNonNull(outputPath, "filePrefix is null");
        this.fileFormat = requireNonNull(fileFormat, "fileFormat is null");
        this.storageProperties = requireNonNull(storageProperties, "storageProperties is null");
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
    public String getSchemaAsJson()
    {
        return schemaAsJson;
    }

    @JsonProperty
    public String getPartitionSpecAsJson()
    {
        return partitionSpecAsJson;
    }

    @JsonProperty
    public List<IcebergColumnHandle> getInputColumns()
    {
        return inputColumns;
    }

    @JsonProperty
    public String getOutputPath()
    {
        return outputPath;
    }

    @JsonProperty
    public FileFormat getFileFormat()
    {
        return fileFormat;
    }

    @JsonProperty
    public Map<String, String> getStorageProperties()
    {
        return storageProperties;
    }

    @Override
    public String toString()
    {
        return schemaName + "." + tableName;
    }
}
