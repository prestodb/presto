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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class IcebergWritableTableHandle
{
    private final String schemaName;
    private final IcebergTableName tableName;
    private final PrestoIcebergSchema schema;
    private final PrestoIcebergPartitionSpec partitionSpec;
    private final List<IcebergColumnHandle> inputColumns;
    private final String outputPath;
    private final FileFormat fileFormat;
    private final HiveCompressionCodec compressionCodec;
    private final Map<String, String> storageProperties;
    private final List<SortField> sortOrder;

    public IcebergWritableTableHandle(
            String schemaName,
            IcebergTableName tableName,
            PrestoIcebergSchema schema,
            PrestoIcebergPartitionSpec partitionSpec,
            List<IcebergColumnHandle> inputColumns,
            String outputPath,
            FileFormat fileFormat,
            HiveCompressionCodec compressionCodec,
            Map<String, String> storageProperties,
            List<SortField> sortOrder)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.partitionSpec = requireNonNull(partitionSpec, "partitionSpec is null");
        this.inputColumns = ImmutableList.copyOf(requireNonNull(inputColumns, "inputColumns is null"));
        this.outputPath = requireNonNull(outputPath, "filePrefix is null");
        this.fileFormat = requireNonNull(fileFormat, "fileFormat is null");
        this.compressionCodec = requireNonNull(compressionCodec, "compressionCodec is null");
        this.storageProperties = requireNonNull(storageProperties, "storageProperties is null");
        this.sortOrder = ImmutableList.copyOf(requireNonNull(sortOrder, "sortOrder is null"));
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public IcebergTableName getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public PrestoIcebergSchema getSchema()
    {
        return schema;
    }

    @JsonProperty
    public PrestoIcebergPartitionSpec getPartitionSpec()
    {
        return partitionSpec;
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
    public HiveCompressionCodec getCompressionCodec()
    {
        return compressionCodec;
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

    @JsonProperty
    public List<SortField> getSortOrder()
    {
        return sortOrder;
    }
}
