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
package com.facebook.presto.redis;

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.spi.schedule.NodeSelectionStrategy.NO_PREFERENCE;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * Represents a Redis specific {@link ConnectorSplit}.
 */
public final class RedisSplit
        implements ConnectorSplit
{
    private final String connectorId;
    private final String schemaName;
    private final String tableName;
    private final String keyDataFormat;
    private final String keyName;
    private final String valueDataFormat;

    private final RedisDataType valueDataType;
    private final RedisDataType keyDataType;

    private final List<HostAddress> nodes;

    private final long start;
    private final long end;

    @JsonCreator
    public RedisSplit(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("keyDataFormat") String keyDataFormat,
            @JsonProperty("valueDataFormat") String valueDataFormat,
            @JsonProperty("keyName") String keyName,
            @JsonProperty("start") long start,
            @JsonProperty("end") long end,
            @JsonProperty("nodes") List<HostAddress> nodes)
    {
        this.connectorId = requireNonNull(connectorId, "connector id is null");
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "dataFormat is null");
        this.keyDataFormat = requireNonNull(keyDataFormat, "KeydataFormat is null");
        this.valueDataFormat = requireNonNull(valueDataFormat, "valueDataFormat is null");
        this.keyName = keyName;
        this.nodes = ImmutableList.copyOf(requireNonNull(nodes, "addresses is null"));
        this.start = start;
        this.end = end;
        this.valueDataType = toRedisDataType(valueDataFormat);
        this.keyDataType = toRedisDataType(keyDataFormat);
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
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
    public String getKeyDataFormat()
    {
        return keyDataFormat;
    }

    @JsonProperty
    public String getValueDataFormat()
    {
        return valueDataFormat;
    }

    @JsonProperty
    public String getKeyName()
    {
        return keyName;
    }

    @JsonProperty
    public List<HostAddress> getNodes()
    {
        return nodes;
    }

    public RedisDataType getValueDataType()
    {
        return valueDataType;
    }

    public RedisDataType getKeyDataType()
    {
        return keyDataType;
    }

    @JsonProperty
    public long getStart()
    {
        return start;
    }

    @JsonProperty
    public long getEnd()
    {
        return end;
    }

    @Override
    public NodeSelectionStrategy getNodeSelectionStrategy()
    {
        return NO_PREFERENCE;
    }

    @Override
    public List<HostAddress> getPreferredNodes(List<HostAddress> sortedCandidates)
    {
        return nodes;
    }

    @Override
    public Object getInfo()
    {
        return this;
    }

    private static RedisDataType toRedisDataType(String dataFormat)
    {
        switch (dataFormat) {
            case "hash":
                return RedisDataType.HASH;
            case "zset":
                return RedisDataType.ZSET;
            default:
                return RedisDataType.STRING;
        }
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("connectorId", connectorId)
                .add("schemaName", schemaName)
                .add("tableName", tableName)
                .add("keyDataFormat", keyDataFormat)
                .add("valueDataFormat", valueDataFormat)
                .add("keyName", keyName)
                .add("start", start)
                .add("end", end)
                .add("nodes", nodes)
                .toString();
    }
}
