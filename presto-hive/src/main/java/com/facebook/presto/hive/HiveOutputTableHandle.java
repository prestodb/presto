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

import com.facebook.presto.spi.ColumnType;
import com.facebook.presto.spi.OutputTableHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class HiveOutputTableHandle
        implements OutputTableHandle
{
    private final String clientId;
    private final String schemaName;
    private final String tableName;
    private final List<String> columnNames;
    private final List<ColumnType> columnTypes;
    private final String tableOwner;
    private final String targetPath;
    private final String temporaryPath;

    @JsonCreator
    public HiveOutputTableHandle(
            @JsonProperty("clientId") String clientId,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("columnNames") List<String> columnNames,
            @JsonProperty("columnTypes") List<ColumnType> columnTypes,
            @JsonProperty("tableOwner") String tableOwner,
            @JsonProperty("targetPath") String targetPath,
            @JsonProperty("temporaryPath") String temporaryPath)
    {
        this.clientId = checkNotNull(clientId, "clientId is null");
        this.schemaName = checkNotNull(schemaName, "schemaName is null");
        this.tableName = checkNotNull(tableName, "tableName is null");
        this.tableOwner = checkNotNull(tableOwner, "tableOwner is null");
        this.targetPath = checkNotNull(targetPath, "targetPath is null");
        this.temporaryPath = checkNotNull(temporaryPath, "temporaryPath is null");

        checkNotNull(columnNames, "columnNames is null");
        checkNotNull(columnTypes, "columnTypes is null");
        checkArgument(columnNames.size() == columnTypes.size(), "columnNames and columnTypes sizes don't match");
        this.columnNames = ImmutableList.copyOf(columnNames);
        this.columnTypes = ImmutableList.copyOf(columnTypes);
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
    public List<String> getColumnNames()
    {
        return columnNames;
    }

    @JsonProperty
    public List<ColumnType> getColumnTypes()
    {
        return columnTypes;
    }

    @JsonProperty
    public String getTableOwner()
    {
        return tableOwner;
    }

    @JsonProperty
    public String getTargetPath()
    {
        return targetPath;
    }

    @JsonProperty
    public String getTemporaryPath()
    {
        return temporaryPath;
    }

    @Override
    public String toString()
    {
        return "hive:" + schemaName + "." + tableName;
    }
}
