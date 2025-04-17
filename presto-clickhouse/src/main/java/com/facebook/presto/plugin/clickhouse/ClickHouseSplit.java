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
package com.facebook.presto.plugin.clickhouse;

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.plugin.clickhouse.optimization.ClickHouseExpression;
import com.facebook.presto.plugin.clickhouse.optimization.ClickHouseQueryGenerator;
import com.facebook.presto.plugin.clickhouse.optimization.ClickHouseQueryGenerator.GeneratedClickhouseSQL;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.NodeProvider;
import com.facebook.presto.spi.SplitWeight;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import jakarta.annotation.Nullable;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static com.facebook.presto.spi.schedule.NodeSelectionStrategy.NO_PREFERENCE;
import static java.util.Objects.requireNonNull;

public class ClickHouseSplit
        implements ConnectorSplit
{
    private final String connectorId;
    private final String catalogName;
    private final String schemaName;
    private final String tableName;
    private final TupleDomain<ColumnHandle> tupleDomain;
    private final Optional<ClickHouseExpression> additionalPredicate;
    private Optional<String> simpleExpression;
    private Optional<ClickHouseQueryGenerator.GeneratedClickhouseSQL> clickhouseSQL;

    @JsonCreator
    public ClickHouseSplit(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("catalogName") @Nullable String catalogName,
            @JsonProperty("schemaName") @Nullable String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("tupleDomain") TupleDomain<ColumnHandle> tupleDomain,
            @JsonProperty("additionalProperty") Optional<ClickHouseExpression> additionalPredicate,
            @JsonProperty("simpleExpression") Optional<String> simpleExpression,
            @JsonProperty("clickhouseSQL") Optional<ClickHouseQueryGenerator.GeneratedClickhouseSQL> clickhouseSQL)
    {
        this.connectorId = requireNonNull(connectorId, "connector id is null");
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.tableName = requireNonNull(tableName, "table name is null");
        this.tupleDomain = requireNonNull(tupleDomain, "tupleDomain is null");
        this.additionalPredicate = requireNonNull(additionalPredicate, "additionalPredicate is null");
        this.simpleExpression = simpleExpression;
        this.clickhouseSQL = clickhouseSQL;
    }

    @JsonProperty
    public Optional<String> getSimpleExpression()
    {
        return simpleExpression;
    }

    @JsonProperty
    public Optional<GeneratedClickhouseSQL> getClickhouseSQL()
    {
        return clickhouseSQL;
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
    public Optional<ClickHouseExpression> getAdditionalPredicate()
    {
        return additionalPredicate;
    }

    @Override
    public NodeSelectionStrategy getNodeSelectionStrategy()
    {
        return NO_PREFERENCE;
    }

    @Override
    public List<HostAddress> getPreferredNodes(NodeProvider nodeProvider)
    {
        return null;
    }

    public List<HostAddress> getAddresses()
    {
        return ImmutableList.of();
    }

    @Override
    public Object getInfo()
    {
        return this;
    }

    @Override
    public Object getSplitIdentifier()
    {
        return ConnectorSplit.super.getSplitIdentifier();
    }

    @Override
    public OptionalLong getSplitSizeInBytes()
    {
        return ConnectorSplit.super.getSplitSizeInBytes();
    }

    @Override
    public SplitWeight getSplitWeight()
    {
        return ConnectorSplit.super.getSplitWeight();
    }
}
