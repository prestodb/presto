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

import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.netflix.iceberg.FileFormat;

import java.util.List;

public class IcebergInsertTableHandle
        implements ConnectorInsertTableHandle, ConnectorOutputTableHandle
{
    private final String schemaName;
    private final String tableName;
    private final String schemaAsJson;
    private final List<HiveColumnHandle> inputColumns;
    private final String filePrefix;
    private final FileFormat fileFormat;

    @JsonProperty("schemaName")
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty("tableName")
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty("schemaAsJson")
    public String getSchemaAsJson()
    {
        return schemaAsJson;
    }

    @JsonProperty("inputColumns")
    public List<HiveColumnHandle> getInputColumns()
    {
        return inputColumns;
    }

    @JsonProperty("filePrefix")
    public String getFilePrefix()
    {
        return filePrefix;
    }

    @JsonProperty("fileFormat")
    public FileFormat getFileFormat()
    {
        return fileFormat;
    }

    @JsonCreator
    public IcebergInsertTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("schemaAsJson") String schemaAsJson,
            @JsonProperty("inputColumns") List<HiveColumnHandle> inputColumns,
            @JsonProperty("filePrefix") String filePrefix,
            @JsonProperty("fileFormat") FileFormat fileFormat)
    {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.schemaAsJson = schemaAsJson;
        this.inputColumns = inputColumns;
        this.filePrefix = filePrefix;
        this.fileFormat = fileFormat;
    }
}
