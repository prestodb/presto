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

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.Map;
import java.util.List;
import java.util.ArrayList;

import static java.util.UUID.randomUUID;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class HiveOutputTableHandle
        implements ConnectorOutputTableHandle
{
    private final String clientId;
    private final String schemaName;
    private final String tableName;
    private final List<String> columnNames;
    private final List<Type> columnTypes;
    private final String tableOwner;
    private final String targetPath;
    private final String temporaryPath;
    private final String outputFormat;
    private final String serdeLib;
    private final Map<String, String> serdeParameters;
    private final List<Boolean> partitionBitmap;
    private final boolean forInsert;
    private final String filePrefix;

    public HiveOutputTableHandle(
            String clientId,
            String schemaName,
            String tableName,
            List<String> columnNames,
            List<Type> columnTypes,
            String tableOwner,
            String targetPath,
            String temporaryPath)
    {
        this(clientId,
             schemaName,
             tableName,
             columnNames,
             columnTypes,
             tableOwner,
             targetPath,
             temporaryPath,
             "org.apache.hadoop.hive.ql.io.RCFileOutputFormat",
             "org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe",
             null,
             null,
             false);
    }

    public HiveOutputTableHandle(
            String clientId,
            String schemaName,
            String tableName,
            List<String> columnNames,
            List<Type> columnTypes,
            String tableOwner,
            String targetPath,
            String temporaryPath,
            String outputFormat,
            String serdeLib,
            Map<String, String> serdeParameters,
            List<Boolean> partitionBitmap,
            boolean forInsert)
    {
        this(clientId,
             schemaName,
             tableName,
             columnNames,
             columnTypes,
             tableOwner,
             targetPath,
             temporaryPath,
             outputFormat,
             serdeLib,
             serdeParameters,
             partitionBitmap,
             forInsert,
             randomUUID().toString());
    }

    @JsonCreator
    public HiveOutputTableHandle(
            @JsonProperty("clientId") String clientId,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("columnNames") List<String> columnNames,
            @JsonProperty("columnTypes") List<Type> columnTypes,
            @JsonProperty("tableOwner") String tableOwner,
            @JsonProperty("targetPath") String targetPath,
            @JsonProperty("temporaryPath") String temporaryPath,
            @JsonProperty("outputFormat") String outputFormat,
            @JsonProperty("serdeLib") String serdeLib,
            @JsonProperty("serdeParameters") Map<String, String> serdeParameters,
            @JsonProperty("partitionBitmap") List<Boolean> partitionBitmap,
            @JsonProperty("forInsert") boolean forInsert,
            @JsonProperty("filePrefix") String filePrefix)
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

        this.outputFormat = outputFormat;
        this.serdeLib = serdeLib;
        this.serdeParameters = serdeParameters;
        this.partitionBitmap = partitionBitmap;
        this.forInsert = forInsert;
        this.filePrefix = filePrefix;
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
    public List<Type> getColumnTypes()
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

    @JsonProperty
    public String getOutputFormat()
    {
        return outputFormat;
    }

    @JsonProperty
    public List<Boolean> getPartitionBitmap()
    {
        return partitionBitmap;
    }

    @JsonProperty("forInsert")
    public boolean forInsert()
    {
        return forInsert;
    }

    @JsonProperty
    public String getFilePrefix()
    {
        return filePrefix;
    }

    @JsonProperty
    public String getSerdeLib()
    {
        return serdeLib;
    }

    @JsonProperty
    public Map<String, String> getSerdeParameters()
    {
        return serdeParameters;
    }

    @Override
    public String toString()
    {
        return "hive:" + schemaName + "." + tableName;
    }

    public boolean hasTemporaryPath()
    {
        return !temporaryPath.equals(targetPath);
    }

    public boolean isOutputTablePartitioned()
    {
        return partitionBitmap != null;
    }

    public List<Type> getDataColumnTypes()
    {
        if (!isOutputTablePartitioned()) {
            return ImmutableList.copyOf(columnTypes);
        }

        List<Type> dataColumnTypes = new ArrayList<Type>();
        for (int i = 0; i < columnTypes.size(); i++) {
            if (!partitionBitmap.get(i)) {
                dataColumnTypes.add(columnTypes.get(i));
            }
        }

        return dataColumnTypes;
    }

    public List<String> getDataColumnNames()
    {
        if (!isOutputTablePartitioned()) {
            return columnNames;
        }

        List<String> dataColumnNames = new ArrayList<String>();
        for (int i = 0; i < columnNames.size(); i++) {
            if (!partitionBitmap.get(i)) {
                dataColumnNames.add(columnNames.get(i));
            }
        }

        return dataColumnNames;
    }

    public List<String> getPartitionColumnNames()
    {
        if (!isOutputTablePartitioned()) {
            return null;
        }

        List<String> partitionColumnNames = new ArrayList<String>();
        for (int i = 0; i < columnNames.size(); i++) {
            if (partitionBitmap.get(i)) {
                partitionColumnNames.add(columnNames.get(i));
            }
        }

        return partitionColumnNames;
    }
}
