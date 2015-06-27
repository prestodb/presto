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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class HiveWritableTableHandle
{
    private final String clientId;
    private final String schemaName;
    private final String tableName;
    private final List<HiveColumnHandle> inputColumns;
    private final String filePrefix;
    private final Optional<String> writePath;
    private final HiveStorageFormat hiveStorageFormat;

    public HiveWritableTableHandle(
            String clientId,
            String schemaName,
            String tableName,
            List<HiveColumnHandle> inputColumns,
            String filePrefix,
            Optional<String> writePath,
            HiveStorageFormat hiveStorageFormat)
    {
        this.clientId = requireNonNull(clientId, "clientId is null");
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.inputColumns = ImmutableList.copyOf(requireNonNull(inputColumns, "inputColumns is null"));
        this.filePrefix = requireNonNull(filePrefix, "filePrefix is null");
        this.writePath = requireNonNull(writePath, "writePath is null");
        this.hiveStorageFormat = requireNonNull(hiveStorageFormat, "hiveStorageFormat is null");
    }

    @JsonProperty
    public String getClientId()
    {
        return clientId;
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

    @JsonProperty
    public String getFilePrefix()
    {
        return filePrefix;
    }

    @JsonProperty
    public Optional<String> getWritePath()
    {
        return writePath;
    }

    @JsonProperty
    public HiveStorageFormat getHiveStorageFormat()
    {
        return hiveStorageFormat;
    }

    @Override
    public String toString()
    {
        return clientId + ":" + schemaName + "." + tableName;
    }
}
