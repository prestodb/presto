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
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

public class IcebergInsertTableHandle
        extends IcebergWritableTableHandle
        implements ConnectorInsertTableHandle
{
    @JsonCreator
    public IcebergInsertTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") IcebergTableName tableName,
            @JsonProperty("schema") PrestoIcebergSchema schema,
            @JsonProperty("partitionSpec") PrestoIcebergPartitionSpec partitionSpec,
            @JsonProperty("inputColumns") List<IcebergColumnHandle> inputColumns,
            @JsonProperty("outputPath") String outputPath,
            @JsonProperty("fileFormat") FileFormat fileFormat,
            @JsonProperty("compressionCodec") HiveCompressionCodec compressionCodec,
            @JsonProperty("storageProperties") Map<String, String> storageProperties,
            @JsonProperty("sortOrder") List<SortField> sortOrder)
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
                sortOrder);
    }
}
