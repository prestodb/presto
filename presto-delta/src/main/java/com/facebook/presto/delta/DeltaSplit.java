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
package com.facebook.presto.delta;

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.NodeProvider;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

import static com.facebook.presto.spi.schedule.NodeSelectionStrategy.NO_PREFERENCE;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class DeltaSplit
        implements ConnectorSplit
{
    private final String connectorId;
    private final String schema;
    private final String table;
    private final String filePath;
    private final long start;
    private final long length;
    private final long fileSize;
    private final Map<String, String> partitionValues;

    @JsonCreator
    public DeltaSplit(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("schemaName") String schema,
            @JsonProperty("tableName") String table,
            @JsonProperty("filePath") String filePath,
            @JsonProperty("start") long start,
            @JsonProperty("length") long length,
            @JsonProperty("fileSize") long fileSize,
            @JsonProperty("partitionValues") Map<String, String> partitionValues)
    {
        checkArgument(start >= 0, "start must be positive");
        checkArgument(length >= 0, "length must be positive");
        checkArgument(fileSize >= 0, "fileSize must be positive");

        this.connectorId = requireNonNull(connectorId, "connector id is null");
        this.schema = requireNonNull(schema, "schema name is null");
        this.table = requireNonNull(table, "table name is null");
        this.filePath = requireNonNull(filePath, "filePath name is null");
        this.start = start;
        this.length = length;
        this.fileSize = fileSize;
        this.partitionValues = ImmutableMap.copyOf(requireNonNull(partitionValues, "partitionValues id is null"));
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public String getSchema()
    {
        return schema;
    }

    @JsonProperty
    public String getTable()
    {
        return table;
    }

    @JsonProperty
    public String getFilePath()
    {
        return filePath;
    }

    @JsonProperty
    public long getStart()
    {
        return start;
    }

    @JsonProperty
    public long getLength()
    {
        return length;
    }

    @JsonProperty
    public long getFileSize()
    {
        return fileSize;
    }

    @JsonProperty
    public Map<String, String> getPartitionValues()
    {
        return partitionValues;
    }

    @Override
    public NodeSelectionStrategy getNodeSelectionStrategy()
    {
        return NO_PREFERENCE;
    }

    @Override
    public List<HostAddress> getPreferredNodes(NodeProvider nodeProvider)
    {
        return ImmutableList.of(); // empty list indicates no preference.
    }

    @Override
    public OptionalLong getSplitSizeInBytes()
    {
        return OptionalLong.of(length);
    }

    @Override
    public Object getInfo()
    {
        return this;
    }
}
