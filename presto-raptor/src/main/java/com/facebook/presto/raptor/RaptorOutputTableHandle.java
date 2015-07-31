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

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.raptor.util.MetadataUtil.checkSchemaName;
import static com.facebook.presto.raptor.util.MetadataUtil.checkTableName;
import static com.google.common.base.Preconditions.checkNotNull;

public class RaptorOutputTableHandle
        implements ConnectorOutputTableHandle
{
    private final String schemaName;
    private final String tableName;
    private final List<RaptorColumnHandle> columnHandles;
    private final List<Type> columnTypes;
    @Nullable
    private final RaptorColumnHandle sampleWeightColumnHandle;
    private final List<RaptorColumnHandle> sortColumnHandles;
    private final List<SortOrder> sortOrders;
    private final Optional<RaptorColumnHandle> temporalColumnHandle;

    @JsonCreator
    public RaptorOutputTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("columnHandles") List<RaptorColumnHandle> columnHandles,
            @JsonProperty("columnTypes") List<Type> columnTypes,
            @JsonProperty("sampleWeightColumnHandle") @Nullable RaptorColumnHandle sampleWeightColumnHandle,
            @JsonProperty("sortColumnHandles") List<RaptorColumnHandle> sortColumnHandles,
            @JsonProperty("sortOrders") List<SortOrder> sortOrders,
            @JsonProperty("temporalColumnHandle") Optional<RaptorColumnHandle> temporalColumnHandle)
    {
        this.schemaName = checkSchemaName(schemaName);
        this.tableName = checkTableName(tableName);
        this.columnHandles = ImmutableList.copyOf(checkNotNull(columnHandles, "columnHandles is null"));
        this.columnTypes = ImmutableList.copyOf(checkNotNull(columnTypes, "columnTypes is null"));
        this.sampleWeightColumnHandle = sampleWeightColumnHandle;
        this.sortOrders = checkNotNull(sortOrders, "sortOrders is null");
        this.sortColumnHandles = checkNotNull(sortColumnHandles, "sortColumnHandles is null");
        this.temporalColumnHandle = checkNotNull(temporalColumnHandle, "temporalColumnHandle is null");
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

    @Nullable
    @JsonProperty
    public RaptorColumnHandle getSampleWeightColumnHandle()
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
