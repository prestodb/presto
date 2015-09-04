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
package com.facebook.presto.raptor;

import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.raptor.util.MetadataUtil.checkSchemaName;
import static com.facebook.presto.raptor.util.MetadataUtil.checkTableName;
import static java.util.Objects.requireNonNull;

public class RaptorOutputTableHandle
        implements ConnectorOutputTableHandle
{
    private final String connectorId;
    private final String schemaName;
    private final String tableName;
    private final List<RaptorColumnHandle> columnHandles;
    private final List<Type> columnTypes;
    private final Optional<RaptorColumnHandle> sampleWeightColumnHandle;
    private final List<RaptorColumnHandle> sortColumnHandles;
    private final List<SortOrder> sortOrders;
    private final Optional<RaptorColumnHandle> temporalColumnHandle;

    @JsonCreator
    public RaptorOutputTableHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("columnHandles") List<RaptorColumnHandle> columnHandles,
            @JsonProperty("columnTypes") List<Type> columnTypes,
            @JsonProperty("sampleWeightColumnHandle") Optional<RaptorColumnHandle> sampleWeightColumnHandle,
            @JsonProperty("sortColumnHandles") List<RaptorColumnHandle> sortColumnHandles,
            @JsonProperty("sortOrders") List<SortOrder> sortOrders,
            @JsonProperty("temporalColumnHandle") Optional<RaptorColumnHandle> temporalColumnHandle)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.schemaName = checkSchemaName(schemaName);
        this.tableName = checkTableName(tableName);
        this.columnHandles = ImmutableList.copyOf(requireNonNull(columnHandles, "columnHandles is null"));
        this.columnTypes = ImmutableList.copyOf(requireNonNull(columnTypes, "columnTypes is null"));
        this.sampleWeightColumnHandle = requireNonNull(sampleWeightColumnHandle, "sampleWeightColumnHandle is null");
        this.sortOrders = requireNonNull(sortOrders, "sortOrders is null");
        this.sortColumnHandles = requireNonNull(sortColumnHandles, "sortColumnHandles is null");
        this.temporalColumnHandle = requireNonNull(temporalColumnHandle, "temporalColumnHandle is null");
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
    public List<RaptorColumnHandle> getColumnHandles()
    {
        return columnHandles;
    }

    @JsonProperty
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @JsonProperty
    public Optional<RaptorColumnHandle> getSampleWeightColumnHandle()
    {
        return sampleWeightColumnHandle;
    }

    @JsonProperty
    public List<RaptorColumnHandle> getSortColumnHandles()
    {
        return sortColumnHandles;
    }

    @JsonProperty
    public List<SortOrder> getSortOrders()
    {
        return sortOrders;
    }

    @JsonProperty
    public Optional<RaptorColumnHandle> getTemporalColumnHandle()
    {
        return temporalColumnHandle;
    }

    @Override
    public String toString()
    {
        return "raptor:" + schemaName + "." + tableName;
    }
}
