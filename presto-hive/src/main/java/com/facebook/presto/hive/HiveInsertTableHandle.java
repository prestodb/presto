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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class HiveInsertTableHandle implements ConnectorInsertTableHandle
{
    private final String clientId;
    private final String schemaName;
    private final String tableName;
    private final List<String> columnNames;
    private final List<Type> columnTypes;
    private final String targetPath;
    private final String temporaryPath;
    private final String outputFormat;
    private final String serdeLib;
    private final Map<String, String> serdeParameters;
    private final List<Boolean> partitionBitmap;
    private final String filePrefix;
    private final ConnectorSession session;

    @JsonCreator
    public HiveInsertTableHandle(
            @JsonProperty("clientId") String clientId,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("columnNames") List<String> columnNames,
            @JsonProperty("columnTypes") List<Type> columnTypes,
            @JsonProperty("targetPath") String targetPath,
            @JsonProperty("temporaryPath") String temporaryPath,
            @JsonProperty("outputFormat") String outputFormat,
            @JsonProperty("serdeLib") String serdeLib,
            @JsonProperty("serdeParameters") Map<String, String> serdeParameters,
            @JsonProperty("partitionBitmap") List<Boolean> partitionBitmap,
            @JsonProperty("filePrefix") String filePrefix,
            @JsonProperty("connectorSession") ConnectorSession session)
    {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.targetPath = checkNotNull(targetPath, "targetPath is null");
        this.temporaryPath = checkNotNull(temporaryPath, "temporaryPath is null");
        this.clientId = checkNotNull(clientId, "clientId is null");

        checkNotNull(columnNames, "columnNames is null");
        checkNotNull(columnTypes, "columnTypes is null");
        checkArgument(columnNames.size() == columnTypes.size(), "columnNames and columnTypes sizes don't match");
        this.columnNames = ImmutableList.copyOf(columnNames);
        this.columnTypes = ImmutableList.copyOf(columnTypes);

        this.outputFormat = outputFormat;
        this.serdeLib = serdeLib;
        this.serdeParameters = serdeParameters;
        this.partitionBitmap = partitionBitmap;
        this.filePrefix = filePrefix;
        this.session = session;
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

    @JsonProperty
    public ConnectorSession getConnectorSession()
    {
        return session;
    }

    @Override
    public String toString()
    {
        return "Writing to: " + targetPath + " with Prefix: " + filePrefix;
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
