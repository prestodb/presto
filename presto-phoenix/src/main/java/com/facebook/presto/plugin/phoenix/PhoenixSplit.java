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
package com.facebook.presto.plugin.phoenix;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import org.apache.phoenix.query.KeyRange;

import javax.annotation.Nullable;

import java.util.Base64;
import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class PhoenixSplit
        implements ConnectorSplit
{
    private final String connectorId;
    private final String catalogName;
    private final String schemaName;
    private final String tableName;
    private final TupleDomain<ColumnHandle> tupleDomain;
    private final String startRow;
    private final String stopRow;
    private final List<HostAddress> addresses;

    public PhoenixSplit(
            String connectorId,
            String catalogName,
            String schemaName,
            String tableName,
            TupleDomain<ColumnHandle> tupleDomain,
            KeyRange split,
            List<HostAddress> addresses)
    {
        this.connectorId = requireNonNull(connectorId, "connector id is null");
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.tableName = requireNonNull(tableName, "table name is null");
        this.tupleDomain = requireNonNull(tupleDomain, "tupleDomain is null");
        this.startRow = Base64.getEncoder().encodeToString(split.getLowerRange());
        this.stopRow = Base64.getEncoder().encodeToString(split.getUpperRange());
        this.addresses = addresses;
    }

    @JsonCreator
    public PhoenixSplit(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("catalogName") @Nullable String catalogName,
            @JsonProperty("schemaName") @Nullable String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("tupleDomain") TupleDomain<ColumnHandle> tupleDomain,
            @JsonProperty("startRow") String startRow,
            @JsonProperty("stopRow") String stopRow,
            @JsonProperty("addresses") List<HostAddress> addresses)
    {
        this.connectorId = requireNonNull(connectorId, "connector id is null");
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.tableName = requireNonNull(tableName, "table name is null");
        this.tupleDomain = requireNonNull(tupleDomain, "tupleDomain is null");
        this.startRow = startRow;
        this.stopRow = stopRow;
        this.addresses = addresses;
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    @Nullable
    public String getCatalogName()
    {
        return catalogName;
    }

    @JsonProperty
    @Nullable
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
    public TupleDomain<ColumnHandle> getTupleDomain()
    {
        return tupleDomain;
    }

    @JsonProperty
    public String getStartRow()
    {
        return startRow;
    }

    @JsonProperty
    public String getStopRow()
    {
        return stopRow;
    }

    @JsonProperty
    @Override
    public List<HostAddress> getAddresses()
    {
        return addresses;
    }

    public KeyRange getKeyRange()
    {
        byte[] byteStartRow = Base64.getDecoder().decode(startRow);
        byte[] byteStopRow = Base64.getDecoder().decode(stopRow);
        return KeyRange.getKeyRange(byteStartRow, byteStopRow);
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @Override
    public Object getInfo()
    {
        return ImmutableMap.builder()
                .put("hosts", addresses)
                .put("schema", schemaName)
                .put("table", tableName)
                .put("startRow", startRow)
                .put("stopRow", stopRow)
                .build();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(schemaName)
                .addValue(tableName)
                .addValue(startRow)
                .addValue(stopRow)
                .toString();
    }
}
